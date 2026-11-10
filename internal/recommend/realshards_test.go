package recommend

import (
	"context"
	"fmt"
	"slices"
	"testing"
	"time"

	"github.com/sHyanorn/recserve/internal/bench"
	"github.com/sHyanorn/recserve/internal/shard"
	"github.com/sHyanorn/recserve/internal/vec"
)

// realQuery is the vector retrieval input of one real request.
type realQuery struct {
	req  Request
	uv   []float32
	seen map[int32]bool
}

// realQueries returns 2,000 requests, one per 20th test event, k = 50, at
// the event's time, with their user vectors and seen items.
func realQueries(t *testing.T, rd realData) []realQuery {
	t.Helper()
	ctx := context.Background()
	var out []realQuery
	for i := 0; i < len(rd.test) && len(out) < 2000; i += 20 {
		r := rd.test[i]
		req := Request{User: r.User, K: 50, TS: r.TS}
		raw, err := rd.store.Before(ctx, r.User, r.TS, 0)
		if err != nil {
			t.Fatal(err)
		}
		hist := internalIDs(rd.bundle, raw)
		uv, err := rd.bundle.UserVector(ctx, hist)
		if err != nil {
			t.Fatal(err)
		}
		seen := make(map[int32]bool, len(hist))
		for _, id := range hist {
			seen[id] = true
		}
		out = append(out, realQuery{req, uv, seen})
	}
	return out
}

func summary(d []time.Duration) string {
	s := bench.Summarize(d)
	return fmt.Sprintf("p50 %.0f µs, p99 %.0f µs", s.P50, s.P99)
}

// recallAt is recall@k as docs/BENCHMARKS.md defines it for R2: a returned
// item counts if it scores at least the true k-th score less 1e-5.
func recallAt(got, truth []vec.Hit, k int) float64 {
	if len(truth) < k {
		k = len(truth)
	}
	if k == 0 {
		return 1
	}
	kth, n := truth[k-1].Score, 0
	for _, h := range got[:min(k, len(got))] {
		if h.Score >= kth-1e-5 {
			n++
		}
	}
	return float64(n) / float64(k)
}

// TestRealDataShards runs on the real bundle (see TestRealDataSmoke) and
// reports: the items left out of the shards for a zero vector; that exact
// search over two shards returns what one shard does, item by item; the
// pipeline's latency with one and two exact shards; and HNSW recall and
// latency against exact search, the inner product of unnormalized vectors
// being the score. Latencies are of sequential requests, alternating
// between the settings compared.
func TestRealDataShards(t *testing.T) {
	rd := loadRealData(t)
	qs := realQueries(t, rd)
	ctx := context.Background()
	one, two := shard.DefaultConfig, shard.DefaultConfig
	one.Shards = 1
	p1, err := New(rd.bundle, rd.store, one)
	if err != nil {
		t.Fatal(err)
	}
	p2, err := New(rd.bundle, rd.store, two)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("%d items, %d left out for a zero vector", rd.bundle.ItemCount, p1.Shards.Zeroes())

	truth := make([][]vec.Hit, len(qs))
	var withVector int
	for i, q := range qs {
		if q.uv == nil {
			continue
		}
		withVector++
		skip := func(id int32) bool { return q.seen[id] }
		a, err := p1.Shards.SearchAll(ctx, q.uv, VectorTop, skip)
		if err != nil {
			t.Fatal(err)
		}
		b, err := p2.Shards.SearchAll(ctx, q.uv, VectorTop, skip)
		if err != nil {
			t.Fatal(err)
		}
		if !slices.Equal(a, b) {
			t.Fatalf("request %d: two exact shards differ from one", i)
		}
		truth[i] = a
	}
	t.Logf("%d requests, %d with a user vector; exact search over two shards equals one shard on all", len(qs), withVector)

	var lat [2][]time.Duration
	for range 2 { // the first round warms up
		lat = [2][]time.Duration{}
		for _, q := range qs {
			for j, p := range []*Pipeline{p1, p2} {
				start := time.Now()
				if _, err := p.Recommend(ctx, q.req); err != nil {
					t.Fatal(err)
				}
				lat[j] = append(lat[j], time.Since(start))
			}
		}
	}
	t.Logf("pipeline, exact search: 1 shard %s; 2 shards %s", summary(lat[0]), summary(lat[1]))

	for _, n := range []int{1, 2} {
		cfg := shard.DefaultConfig
		cfg.Shards, cfg.Mode = n, shard.HNSW
		start := time.Now()
		s, err := shard.Build(rd.bundle.Vectors, cfg)
		if err != nil {
			t.Fatal(err)
		}
		build := time.Since(start)
		exact := p1.Shards
		if n == 2 {
			exact = p2.Shards
		}
		for _, ef := range []int{200, 400, 800} {
			s := s.WithEf(ef)
			var r100, r200 float64
			var dh, de []time.Duration
			for i, q := range qs {
				if q.uv == nil {
					continue
				}
				skip := func(id int32) bool { return q.seen[id] }
				start := time.Now()
				got, err := s.SearchAll(ctx, q.uv, VectorTop, skip)
				dh = append(dh, time.Since(start))
				if err != nil {
					t.Fatal(err)
				}
				start = time.Now()
				if _, err := exact.SearchAll(ctx, q.uv, VectorTop, skip); err != nil {
					t.Fatal(err)
				}
				de = append(de, time.Since(start))
				r100 += recallAt(got, truth[i], 100)
				r200 += recallAt(got, truth[i], VectorTop)
			}
			w := float64(withVector)
			t.Logf("HNSW, %d shards (build %v), ef %d: recall@100 %.4f, recall@200 %.4f; search %s; exact %s",
				n, build.Round(time.Millisecond), ef, r100/w, r200/w, summary(dh), summary(de))
		}
	}
}
