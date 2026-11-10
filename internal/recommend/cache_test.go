package recommend

import (
	"context"
	"fmt"
	"math/rand/v2"
	"slices"
	"testing"
	"time"

	"github.com/sHyanorn/recserve/internal/bundle"
	"github.com/sHyanorn/recserve/internal/cache"
	"github.com/sHyanorn/recserve/internal/history"
	"github.com/sHyanorn/recserve/internal/shard"
	"github.com/sHyanorn/recserve/internal/vec"
)

// checkProperty5 sends requests from a small pool to two pipelines that
// share a cache and a store but serve bundles of different versions and
// vectors, and returns the first response that differs from the one the
// same pipeline computes without the cache. broken leaves the version out
// of the cache key.
func checkProperty5(seed uint64, cases int, broken bool) error {
	r := rand.New(rand.NewPCG(seed, 5))
	ctx := context.Background()
	var hits int
	for c := range cases {
		p1, err := randomPipeline(r, 2+r.IntN(40), 3)
		if err != nil {
			return err
		}
		b1 := p1.Bundle
		data := make([]float32, len(b1.Vectors.Data))
		for i := b1.Dim; i < len(data); i++ {
			data[i] = float32(r.IntN(7) - 3)
		}
		m, err := vec.NewMatrix(b1.Dim, data)
		if err != nil {
			return err
		}
		man := b1.Manifest
		man.ModelVersion = "t2"
		p2, err := New(bundle.New(man, b1.IDs, m, b1.Cooc, b1.Popular), p1.History, shard.DefaultConfig)
		if err != nil {
			return err
		}
		shared := cache.New[Response](1000, time.Hour)
		for _, p := range []*Pipeline{p1, p2} {
			p.Cache, p.keyWithoutVersion = shared, broken
		}
		for range 40 {
			p := []*Pipeline{p1, p2}[r.IntN(2)]
			req := Request{User: fmt.Sprint("u", r.IntN(2)), K: 5 + 5*r.IntN(2), TS: int64(50 * r.IntN(3)),
				Exclude: []string{fmt.Sprint("i", 1+r.IntN(2))}}
			got, err := p.Recommend(ctx, req)
			if err != nil {
				return err
			}
			q := *p
			q.Cache = nil
			want, err := q.Recommend(ctx, req)
			if err != nil {
				return err
			}
			if got.ModelVersion != want.ModelVersion || !slices.Equal(got.Items, want.Items) {
				return fmt.Errorf("case %d, %+v: %s %v, without the cache %s %v", c, req, got.ModelVersion, got.Items, want.ModelVersion, want.Items)
			}
			if *got.Cached {
				hits++
			}
		}
	}
	if hits == 0 {
		return fmt.Errorf("no request hit the cache")
	}
	return nil
}

func TestProperty5CacheKeepsVersionsApart(t *testing.T) {
	if err := checkProperty5(1, 100, false); err != nil {
		t.Fatal(err)
	}
}

func TestProperty5CatchesBrokenVariant(t *testing.T) {
	if err := checkProperty5(1, 100, true); err == nil {
		t.Fatal("the check passed a cache key without the version")
	}
}

// An event that arrives late, with a time before the user's latest event,
// changes the history but not its latest event; the next response must
// reflect it.
func TestCacheLateEvent(t *testing.T) {
	m := matrix(t, 2, []float32{0, 0, 1, 0, 0, 1, 1, 1})
	b := bundle.New(bundle.Manifest{ModelVersion: "t", ItemCount: 3, Dim: 2, MaxSeqLen: 5}, []string{"", "a", "b", "c"}, m, nil, []int32{1, 2, 3})
	store := storeWith(t, history.Event{ID: 1, User: "u", Item: "b", TS: 5})
	p := newPipeline(t, b, store)
	p.Cache = cache.New[Response](10, time.Hour)
	req := Request{User: "u", K: 3, TS: 10}
	ids := func() ([]string, bool) {
		resp, err := p.Recommend(context.Background(), req)
		if err != nil {
			t.Fatal(err)
		}
		var out []string
		for _, it := range resp.Items {
			out = append(out, it.ID)
		}
		return out, *resp.Cached
	}
	if got, cached := ids(); !slices.Contains(got, "a") || cached {
		t.Fatalf("before the late event: %v, cached %v", got, cached)
	}
	if _, cached := ids(); !cached {
		t.Fatal("the repeated request missed the cache")
	}
	if err := store.Append(context.Background(), []history.Event{{ID: 2, User: "u", Item: "a", TS: 3}}); err != nil {
		t.Fatal(err)
	}
	if got, cached := ids(); slices.Contains(got, "a") || cached {
		t.Fatalf("after the late event: %v, cached %v", got, cached)
	}
}

// A degraded response is not stored, and a request whose history cannot be
// read does not use the cache at all.
func TestCacheSkipsDegraded(t *testing.T) {
	p, err := randomPipeline(rand.New(rand.NewPCG(16, 16)), 30, 1)
	if err != nil {
		t.Fatal(err)
	}
	p.Bundle.Popular = []int32{1, 2, 3}
	p.Cache = cache.New[Response](10, time.Hour)
	var fail string
	p.stall = func(_ context.Context, stage string, _ int) error {
		if stage == fail {
			return errInjected
		}
		return nil
	}
	req := Request{User: "u0", K: 5, TS: 50}
	for i, c := range []struct {
		fail     string
		degraded bool
		cached   *bool
	}{
		{stageHistory, true, nil},
		{stageCooc, true, new(bool)},
		{"", false, new(bool)}, // neither degraded response was stored
		{stageHistory, true, nil},
		{"", false, func() *bool { b := true; return &b }()},
	} {
		fail = c.fail
		resp, err := p.Recommend(context.Background(), req)
		if err != nil {
			t.Fatal(err)
		}
		if (len(resp.Degraded) > 0) != c.degraded || (resp.Cached == nil) != (c.cached == nil) || (c.cached != nil && *resp.Cached != *c.cached) {
			t.Fatalf("request %d failing %q: degraded %v, cached %v", i, c.fail, resp.Degraded, resp.Cached)
		}
	}
	if p.Cache.Len() != 1 {
		t.Fatalf("%d entries, want 1", p.Cache.Len())
	}
}
