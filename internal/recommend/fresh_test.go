package recommend

import (
	"context"
	"fmt"
	"math/rand/v2"
	"slices"
	"testing"

	"github.com/sHyanorn/recserve/internal/bundle"
	"github.com/sHyanorn/recserve/internal/dataset"
	"github.com/sHyanorn/recserve/internal/fresh"
	"github.com/sHyanorn/recserve/internal/history"
	"github.com/sHyanorn/recserve/internal/vec"
)

// With the stream's state, the popular items are those of the window
// before the request time and the co-occurring items those of the pairs
// made before it; S1's pipeline keeps the bundle's co-occurrence table.
func TestFreshSources(t *testing.T) {
	const n = 6
	ids := []string{"", "i1", "i2", "i3", "i4", "i5", "i6"}
	cooc := make([]int32, (n+1)*bundle.CoocWidth)
	cooc[1*bundle.CoocWidth] = 3
	b := bundle.New(bundle.Manifest{ModelVersion: "t", ItemCount: n, Dim: 1, MaxSeqLen: 5}, ids, matrix(t, 1, make([]float32, n+1)), cooc, []int32{2})
	s := fresh.New(n)
	s.Add(fresh.Event{Item: 4, TS: 10}, fresh.Event{Item: 5, TS: 20}, fresh.Event{Item: 5, TS: 21},
		fresh.Event{Item: 6, TS: 30, Prev: []int32{1}})
	p := &Pipeline{Bundle: b, Fresh: s, FreshWindow: 15}
	ctx := context.Background()
	hits := func(h []vec.Hit, err error) []vec.Hit {
		t.Helper()
		if err != nil {
			t.Fatal(err)
		}
		return h
	}
	if got, want := hits(p.popular(ctx, 31)), []vec.Hit{{ID: 5, Score: 1}, {ID: 6, Score: 0.5}}; !slices.Equal(got, want) {
		t.Errorf("popular at 31 over 15 = %v, want %v (item 4 at 10 is outside)", got, want)
	}
	if got, want := hits(p.cooc(ctx, 31, []int32{1})), []vec.Hit{{ID: 6, Score: 1}}; !slices.Equal(got, want) {
		t.Errorf("co-occurrence of 1 at 31 = %v, want %v", got, want)
	}
	if got := hits(p.cooc(ctx, 30, []int32{1})); len(got) != 0 {
		t.Errorf("co-occurrence of 1 at 30 = %v, want none: the pair is made at 30", got)
	}
	p.bundleCooc = true
	if got, want := hits(p.cooc(ctx, 31, []int32{1})), []vec.Hit{{ID: 3, Score: 1}}; !slices.Equal(got, want) {
		t.Errorf("S1's co-occurrence of 1 = %v, want the bundle's %v", got, want)
	}
	p.Fresh = nil
	if got, want := hits(p.popular(ctx, 31)), []vec.Hit{{ID: 2, Score: 1}}; !slices.Equal(got, want) {
		t.Errorf("popular without the state = %v, want the bundle's %v", got, want)
	}
}

// Property 1 with the stream's state: a pipeline whose state holds every
// event answers a request as one whose state holds only the events before
// the request's time. The requests are at the events' own times, among
// many events at one time, and the check fails unless those events change
// the popular items read just after them for most requests.
func TestFreshPointInTime(t *testing.T) {
	r := rand.New(rand.NewPCG(21, 21))
	p, err := randomPipeline(r, 40, 15)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	h := p.History.(*history.Memory)
	var rows []dataset.Row
	for u := range 15 {
		user, prev := fmt.Sprint("u", u), 0
		for ts := range int64(101) { // the user's events at ts are those before ts + 1 and not before ts
			items, err := h.Before(ctx, user, ts+1, 0)
			if err != nil {
				t.Fatal(err)
			}
			for _, it := range items[prev:] {
				rows = append(rows, dataset.Row{User: user, Item: it, TS: ts})
			}
			prev = len(items)
		}
	}
	events, err := fresh.Events(ctx, h, p.Bundle.Lookup, rows)
	if err != nil {
		t.Fatal(err)
	}
	all := fresh.New(p.Bundle.ItemCount)
	all.Add(events...)
	full := *p
	// A ranker that reads the sources' scores, so their order counts.
	full.Fresh, full.FreshWindow, full.rankWith = all, 20, linearRanker
	var differ int
	for range 150 {
		e := rows[r.IntN(len(rows))]
		before := fresh.New(p.Bundle.ItemCount)
		for _, ev := range events {
			if ev.TS < e.TS {
				before.Add(ev)
			}
		}
		prefix := full
		prefix.Fresh = before
		req := Request{User: e.User, K: 20, TS: e.TS}
		got, err := full.Recommend(ctx, req)
		if err != nil {
			t.Fatal(err)
		}
		want, err := prefix.Recommend(ctx, req)
		if err != nil {
			t.Fatal(err)
		}
		if !slices.Equal(got.Items, want.Items) {
			t.Fatalf("%+v: with every event %v, with those before it %v", req, got.Items, want.Items)
		}
		if !slices.Equal(all.Popular(e.TS, 20, bundle.PopularTop), all.Popular(e.TS+1, 20, bundle.PopularTop)) {
			differ++
		}
	}
	if differ < 100 {
		t.Fatalf("the events at a request's time change the popular items just after it for %d of 150 requests", differ)
	}
}

// With the stream's state the cache key holds the request time and the
// window, which the response depends on; without it, it does not.
func TestFreshCacheKey(t *testing.T) {
	p, err := randomPipeline(rand.New(rand.NewPCG(22, 22)), 10, 2)
	if err != nil {
		t.Fatal(err)
	}
	hist := []int32{1, 2}
	at := func(ts int64) Request { return Request{User: "u0", K: 5, TS: ts} }
	if p.cacheKey(at(5), hist) != p.cacheKey(at(6), hist) {
		t.Error("without the state, the key depends on the request time")
	}
	p.Fresh, p.FreshWindow = fresh.New(10), 7
	k := p.cacheKey(at(5), hist)
	if k == p.cacheKey(at(6), hist) {
		t.Error("with the state, two request times give one key")
	}
	p.FreshWindow = 8
	if k == p.cacheKey(at(5), hist) {
		t.Error("with the state, two windows give one key")
	}
}
