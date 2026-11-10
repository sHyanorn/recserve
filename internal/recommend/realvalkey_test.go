package recommend

import (
	"context"
	"fmt"
	"math/rand/v2"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/sHyanorn/recserve/internal/breaker"
	"github.com/sHyanorn/recserve/internal/cache"
	"github.com/sHyanorn/recserve/internal/dataset"
	"github.com/sHyanorn/recserve/internal/history"
	"github.com/sHyanorn/recserve/internal/infratest"
	"github.com/sHyanorn/recserve/internal/shard"
)

// stageTimes holds what one request spent reading its history and looking
// up the cache; the request's context carries it to the timed store and
// cache.
type stageTimes struct{ history, cache time.Duration }

type stageKey struct{}

type timedStore struct{ history.Store }

func (s timedStore) Before(ctx context.Context, user string, ts int64, n int) ([]string, error) {
	start := time.Now()
	items, err := s.Store.Before(ctx, user, ts, n)
	ctx.Value(stageKey{}).(*stageTimes).history = time.Since(start)
	return items, err
}

type timedCache struct{ Cache }

func (c timedCache) Get(ctx context.Context, k cache.Key) (Response, bool, error) {
	start := time.Now()
	resp, ok, err := c.Cache.Get(ctx, k)
	ctx.Value(stageKey{}).(*stageTimes).cache = time.Since(start)
	return resp, ok, err
}

// workers runs do(i) for i in [0, n) on four goroutines.
func workers(n int, do func(i int)) {
	var next atomic.Int64
	var wg sync.WaitGroup
	for range 4 {
		wg.Go(func() {
			for i := int(next.Add(1)) - 1; i < n; i = int(next.Add(1)) - 1 {
				do(i)
			}
		})
	}
	wg.Wait()
}

// TestRealDataValkey is the stage check of docs/BENCHMARKS.md (Valkey
// path): on the real bundle and the requests of realQueries, sent by four
// workers in two rounds, it reports the latency and the time of the history
// read and the cache lookup, with the store and the cache in the process
// and on Valkey, each with the cache on and off; and, as a control, the
// time of reading a 16-byte value from Valkey. The histories hold all three
// splits. It needs RECSERVE_DATA, RECSERVE_BUNDLE and the compose Valkey.
func TestRealDataValkey(t *testing.T) {
	rd := loadRealData(t)
	addr := infratest.ValkeyAddr(t)
	qs := realQueries(t, rd)
	ctx := context.Background()
	evs := make([]history.Event, 0, len(rd.train)+len(rd.test))
	for _, rows := range [][]dataset.Row{rd.train, rd.test} {
		for _, r := range rows {
			evs = append(evs, history.Event{ID: history.EventID(r.User, r.Item, r.TS, dataset.Kind), User: r.User, Item: r.Item, TS: r.TS})
		}
	}
	if err := rd.store.Append(ctx, evs); err != nil {
		t.Fatal(err)
	}
	vstore := history.NewValkey(infratest.ValkeyClient(t, addr))
	loaded := time.Now()
	if err := vstore.Append(ctx, evs); err != nil {
		t.Fatal(err)
	}
	t.Logf("%d events appended to Valkey in %v, for %d requests", len(evs), time.Since(loaded).Round(time.Millisecond), len(qs))
	vcache, err := cache.NewValkey[Response](infratest.ValkeyClient(t, addr), time.Hour)
	if err != nil {
		t.Fatal(err)
	}

	for _, c := range []struct {
		name  string
		store history.Store
		cache func() Cache
	}{
		{"process, cache off", rd.store, nil},
		{"process, cache on", rd.store, func() Cache { return cache.New[Response](len(qs), time.Hour) }},
		{"valkey, cache off", vstore, nil},
		{"valkey, cache on", vstore, func() Cache { return salted{vcache, rand.Uint64()} }},
	} {
		p, err := newLocal(rd.bundle, c.store, shard.DefaultConfig)
		if err != nil {
			t.Fatal(err)
		}
		br := breaker.New(5, time.Second, nil)
		if c.store == history.Store(vstore) {
			p.History = breaker.Store{Store: vstore, Breaker: br}
		}
		p.History = timedStore{p.History}
		if c.cache != nil {
			inner := c.cache()
			if c.store == history.Store(vstore) {
				inner = breaker.Cache[Response]{Cache: inner, Breaker: br}
			}
			p.Cache = timedCache{inner}
		}
		for round := range 2 {
			total, stage, read, lookup := make([]time.Duration, len(qs)), make([]time.Duration, len(qs)), make([]time.Duration, len(qs)), make([]time.Duration, len(qs))
			var hits, degraded atomic.Int64
			workers(len(qs), func(i int) {
				var st stageTimes
				start := time.Now()
				resp, err := p.Recommend(context.WithValue(ctx, stageKey{}, &st), qs[i].req)
				total[i] = time.Since(start)
				if err != nil {
					t.Error(err)
					return
				}
				stage[i], read[i], lookup[i] = st.history+st.cache, st.history, st.cache
				if resp.Cached != nil && *resp.Cached {
					hits.Add(1)
				}
				if len(resp.Degraded) > 0 || resp.CacheErr != nil {
					degraded.Add(1)
				}
			})
			if t.Failed() {
				t.FailNow()
			}
			if n := degraded.Load(); n > 0 {
				t.Fatalf("%s: %d requests degraded or failed on the cache", c.name, n)
			}
			if c.cache != nil && round == 1 && int(hits.Load()) != len(qs) {
				t.Fatalf("%s: %d hits in the second round", c.name, hits.Load())
			}
			t.Logf("%s, round %d: %d hits; latency %s; stage %s; history read %s; cache lookup %s",
				c.name, round+1, hits.Load(), summary(total), summary(stage), summary(read), summary(lookup))
		}
	}

	small, err := cache.NewValkey[string](infratest.ValkeyClient(t, addr), time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	key := cache.Key{0: 0xbe, 1: 0x9c}
	if err := small.Put(ctx, key, fmt.Sprintf("%014d", 0)); err != nil { // 16 bytes as JSON
		t.Fatal(err)
	}
	took := make([]time.Duration, len(qs))
	workers(len(took), func(i int) {
		start := time.Now()
		if _, ok, err := small.Get(ctx, key); err != nil || !ok {
			t.Error(ok, err)
		}
		took[i] = time.Since(start)
	})
	t.Logf("control, %d reads of a 16-byte value: %s", len(took), summary(took))
}
