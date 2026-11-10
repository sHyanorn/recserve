package recommend

import (
	"context"
	"runtime"
	"slices"
	"testing"
	"time"

	"github.com/sHyanorn/recserve/internal/cache"
	"github.com/sHyanorn/recserve/internal/shard"
)

// TestRealDataCache runs on the real bundle (see TestRealDataSmoke) and
// reports, on the requests of realQueries: the heap a cache entry takes
// with k = MaxK, the cache's own structures included, and so the heap of a
// full cache of the gateway's default size; and, sending the requests
// twice through a cache, the latency of misses (first round) and hits
// (second round), checking that each hit returns the items of its miss.
func TestRealDataCache(t *testing.T) {
	rd := loadRealData(t)
	qs := realQueries(t, rd)
	p, err := New(rd.bundle, rd.store, shard.DefaultConfig)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	before := heapAlloc()
	c := cache.New[Response](len(qs), time.Hour)
	p.Cache = c
	for _, q := range qs {
		q.req.K = MaxK
		if _, err := p.Recommend(ctx, q.req); err != nil {
			t.Fatal(err)
		}
	}
	n := c.Len()
	per := float64(heapAlloc()-before) / float64(n)
	const size = 13000 // the gateway's -cache-size
	t.Logf("k = %d: %d entries, %.0f bytes each on the heap; %d entries take %.1f MB", MaxK, n, per, size, per*size/1e6)
	runtime.KeepAlive(c)

	p.Cache = cache.New[Response](len(qs), time.Hour)
	first := make([]Response, len(qs))
	var miss, hit []time.Duration
	for round := range 2 {
		for i, q := range qs {
			start := time.Now()
			resp, err := p.Recommend(ctx, q.req)
			took := time.Since(start)
			if err != nil {
				t.Fatal(err)
			}
			switch {
			case round == 0 && !*resp.Cached:
				miss = append(miss, took)
				first[i] = resp
			case round == 0:
				first[i] = resp // a repeated request
			case !*resp.Cached:
				t.Fatalf("request %d missed in the second round", i)
			default:
				hit = append(hit, took)
				if !slices.Equal(resp.Items, first[i].Items) {
					t.Fatalf("request %d: hit %v, miss %v", i, resp.Items, first[i].Items)
				}
			}
		}
	}
	t.Logf("%d requests: %d misses, %s; %d hits, %s", len(qs), len(miss), summary(miss), len(hit), summary(hit))
}

func heapAlloc() uint64 {
	var m runtime.MemStats
	runtime.GC()
	runtime.GC()
	runtime.ReadMemStats(&m)
	return m.HeapAlloc
}
