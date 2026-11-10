package recommend

import (
	"context"
	"fmt"
	"os"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/sHyanorn/recserve/internal/bench"
	"github.com/sHyanorn/recserve/internal/bundle"
	"github.com/sHyanorn/recserve/internal/shard"
)

// TestRealDataSwap swaps between the real bundle (see TestRealDataSmoke)
// and the one at RECSERVE_SWAP_BUNDLE while four workers send the requests
// of realQueries. Every response must
// equal, item for item, the one the version it names returns; budgets and
// the cache are off, so none degrades or comes from the cache. It reports
// the time to prepare and to drain each swap, and request latency with and
// without swaps; the workers compute the reference response in both phases.
func TestRealDataSwap(t *testing.T) {
	other := os.Getenv("RECSERVE_SWAP_BUNDLE")
	if other == "" {
		t.Skip("RECSERVE_SWAP_BUNDLE not set")
	}
	rd := loadRealData(t)
	qs := realQueries(t, rd)
	opt := bundle.Options{FirstTestTS: rd.first}
	ob, err := bundle.Load(other, opt)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := ob.Close(); err != nil {
			t.Error(err)
		}
	})
	refs := map[string]*Pipeline{}
	for _, b := range []*bundle.Bundle{rd.bundle, ob} {
		refs[b.ModelVersion] = newPipeline(t, b, rd.store)
	}
	if len(refs) != 2 {
		t.Fatalf("both bundles have version %s", rd.bundle.ModelVersion)
	}
	dirs := map[string]string{rd.bundle.ModelVersion: rd.dir, ob.ModelVersion: other}
	prepare := func(dir string) func() (*Pipeline, error) {
		return func() (*Pipeline, error) {
			b, err := bundle.Load(dir, opt)
			if err != nil {
				return nil, err
			}
			return New(b, rd.store, shard.DefaultConfig)
		}
	}
	p, err := prepare(rd.dir)()
	if err != nil {
		t.Fatal(err)
	}
	live := NewLive(p, discard)
	defer func() {
		if err := live.Close(); err != nil {
			t.Error(err)
		}
	}()

	const workers, swaps = 4, 20
	ctx := context.Background()
	// run sends requests from the workers until done returns true and
	// returns their latencies and the responses per version.
	run := func(done func(sent int64) bool) ([]time.Duration, map[string]int) {
		var (
			mu     sync.Mutex
			lat    []time.Duration
			served = map[string]int{}
			sent   atomic.Int64
			wg     sync.WaitGroup
		)
		for w := range workers {
			wg.Go(func() {
				var mine []time.Duration
				for i := w; ; i += workers {
					n := sent.Add(1)
					if done(n) {
						break
					}
					q := qs[i%len(qs)]
					start := time.Now()
					resp, err := live.Recommend(ctx, q.req)
					took := time.Since(start)
					if err != nil {
						t.Error(err)
						return
					}
					want, err := refs[resp.ModelVersion].Recommend(ctx, q.req)
					if err != nil {
						t.Error(err)
						return
					}
					if len(resp.Degraded) > 0 || !slices.Equal(resp.Items, want.Items) {
						t.Errorf("%+v on %s: %v %v, want %v", q.req, resp.ModelVersion, resp.Items, resp.Degraded, want.Items)
						return
					}
					mine = append(mine, took)
					mu.Lock()
					served[resp.ModelVersion]++
					mu.Unlock()
				}
				mu.Lock()
				lat = append(lat, mine...)
				mu.Unlock()
			})
		}
		wg.Wait()
		return lat, served
	}

	base, _ := run(func(sent int64) bool { return sent > int64(len(qs)) })
	var swapping atomic.Bool
	swapping.Store(true)
	var prep, drain []time.Duration
	go func() {
		defer swapping.Store(false)
		for i := range swaps {
			next := ob.ModelVersion
			if i%2 == 1 {
				next = rd.bundle.ModelVersion
			}
			res, err := live.Swap(prepare(dirs[next]))
			if err != nil {
				t.Error(err)
				return
			}
			prep, drain = append(prep, res.Prepare), append(drain, res.Drain)
		}
	}()
	during, served := run(func(int64) bool { return !swapping.Load() })
	if t.Failed() {
		return
	}
	if len(prep) != swaps || len(served) != 2 {
		t.Fatalf("%d swaps, responses per version %v", len(prep), served)
	}
	ms := func(d []time.Duration) string {
		s := bench.Summarize(d)
		return fmt.Sprintf("p50 %.1f ms, max %.1f ms", s.P50/1000, s.Max/1000)
	}
	t.Logf("%d swaps between %s and %s; prepare %s; drain %s", swaps, rd.bundle.ModelVersion, ob.ModelVersion, ms(prep), ms(drain))
	bs, ds := bench.Summarize(base), bench.Summarize(during)
	t.Logf("latency, %d workers: no swaps %d requests p50 %.0f µs p99 %.0f µs max %.1f ms; during swaps %d requests p50 %.0f µs p99 %.0f µs max %.1f ms; responses per version %v",
		workers, bs.N, bs.P50, bs.P99, bs.Max/1000, ds.N, ds.P50, ds.P99, ds.Max/1000, served)
}
