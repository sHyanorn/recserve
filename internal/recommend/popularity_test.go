package recommend

import (
	"cmp"
	"context"
	"fmt"
	"math/rand/v2"
	"os"
	"path/filepath"
	"runtime"
	"slices"
	"testing"

	"golang.org/x/sync/errgroup"

	"github.com/sHyanorn/recserve/internal/bundle"
	"github.com/sHyanorn/recserve/internal/dataset"
	"github.com/sHyanorn/recserve/internal/fresh"
	"github.com/sHyanorn/recserve/internal/history"
	"github.com/sHyanorn/recserve/internal/npy"
)

const day = 86_400_000 // ms

// popularityWindows are the windows, in days, among which the popularity
// window is chosen on valid (docs/registration/m8-freshness.md, 6.2).
var popularityWindows = []int64{1, 3, 7, 14, 30}

// popularityRank is the popularity reference's rank, from 1, of item at ts:
// among the IDs 1 to items not in seen, by their events in [ts - w, ts) in
// s, ties to the lower ID; 0 if item is in seen. It is not cut at any k.
func popularityRank(s *fresh.State, items int, item int32, ts, w int64, seen map[int32]bool) int {
	if seen[item] {
		return 0
	}
	pop := s.Popular(ts, w, items) // every item with an event in the window, best first
	rank := 1
	for _, j := range pop {
		if j == item {
			return rank
		}
		if !seen[j] {
			rank++
		}
	}
	// No event in the window: after every other item with one, then by ID
	// among the unseen items without one.
	below := int(item) - 1
	inPop := make(map[int32]bool, len(pop))
	for _, j := range pop {
		inPop[j] = true
		if j < item {
			below--
		}
	}
	for j := range seen {
		if j < item && !inPop[j] {
			below--
		}
	}
	return rank + below
}

// streamState returns the stream state of rows, in b's internal IDs, each
// event paired with the user's latest events before it as h reads them; h
// must hold the rows. A row given twice is one event, as h keeps it.
func streamState(b *bundle.Bundle, h history.Store, rows []dataset.Row) (*fresh.State, error) {
	seen := make(map[uint64]bool, len(rows))
	var unique []dataset.Row
	for _, r := range rows {
		if id := history.EventID(r.User, r.Item, r.TS, dataset.Kind); !seen[id] {
			seen[id] = true
			unique = append(unique, r)
		}
	}
	// In time order, State.Add appends each event to its lists.
	slices.SortStableFunc(unique, func(a, b dataset.Row) int { return cmp.Compare(a.TS, b.TS) })
	evs, err := fresh.Events(context.Background(), h, b.Lookup, unique)
	if err != nil {
		return nil, err
	}
	s := fresh.New(b.ItemCount)
	s.Add(evs...)
	return s, nil
}

// popularityRanks returns, per event, the popularity reference's rank of
// its item at the event's time with window w, the history before it in h
// left out, GOMAXPROCS events at a time; 0 for an item b does not know.
func popularityRanks(b *bundle.Bundle, h history.Store, s *fresh.State, w int64, events []dataset.Row) ([]int, error) {
	ranks := make([]int, len(events))
	g, ctx := errgroup.WithContext(context.Background())
	g.SetLimit(runtime.GOMAXPROCS(0))
	for i, e := range events {
		g.Go(func() error {
			raw, err := h.Before(ctx, e.User, e.TS, 0)
			if err != nil {
				return err
			}
			seen := map[int32]bool{}
			for _, id := range internalIDs(b, raw) {
				seen[id] = true
			}
			if id, ok := b.Lookup(e.Item); ok {
				ranks[i] = popularityRank(s, b.ItemCount, id, e.TS, w, seen)
			}
			return nil
		})
	}
	return ranks, g.Wait()
}

// The case of test_fresh.py's test_ties_to_the_lower_id_and_popularity_rank,
// with the same ranks.
func TestPopularityRankAsFreshPy(t *testing.T) {
	s := fresh.New(4)
	s.Add(fresh.Event{Item: 3, TS: 1}, fresh.Event{Item: 2, TS: 2}, fresh.Event{Item: 4, TS: 3}, fresh.Event{Item: 4, TS: 4})
	for _, c := range []struct {
		item int32
		seen map[int32]bool
		want int
	}{{3, nil, 3}, {3, map[int32]bool{4: true}, 2}, {1, nil, 4}, {4, map[int32]bool{4: true}, 0}} {
		if got := popularityRank(s, 4, c.item, 10, 10, c.seen); got != c.want {
			t.Errorf("item %d, seen %v: rank %d, want %d", c.item, c.seen, got, c.want)
		}
	}
}

// On random events with many ties, the rank equals the position of the item
// in every unseen item sorted by its count in the window, then by ID.
func TestPopularityRankIsNaive(t *testing.T) {
	r := rand.New(rand.NewPCG(24, 24))
	const items = 30
	s := fresh.New(items)
	for range 400 {
		s.Add(fresh.Event{Item: int32(1 + r.IntN(items)), TS: int64(r.IntN(100))})
	}
	for range 500 {
		item, ts, w := int32(1+r.IntN(items)), int64(r.IntN(110)), int64(1+r.IntN(30))
		seen := map[int32]bool{}
		for range r.IntN(6) {
			seen[int32(1+r.IntN(items))] = true
		}
		var order []int32
		for j := int32(1); j <= items; j++ {
			if !seen[j] {
				order = append(order, j)
			}
		}
		slices.SortStableFunc(order, func(a, b int32) int { return s.Count(b, ts-w, ts) - s.Count(a, ts-w, ts) })
		want := slices.Index(order, item) + 1
		if got := popularityRank(s, items, item, ts, w, seen); got != want {
			t.Fatalf("item %d at %d over %d, seen %v: rank %d, want %d", item, ts, w, seen, got, want)
		}
	}
}

// TestRealDataPopularityReference checks the popularity reference on the
// valid split against tools/bundle/fresh.py's for each window of
// popularityWindows (docs/registration/m8-freshness.md, section 10): one
// request per valid event, on the store and a stream state holding the
// train and valid splits. It runs when RECSERVE_POPULARITY_DIR holds the
// popularity_<d>d.npy that fresh.py wrote with the items of the bundle in
// RECSERVE_BUNDLE, and fails if any event's rank differs.
func TestRealDataPopularityReference(t *testing.T) {
	dir := os.Getenv("RECSERVE_POPULARITY_DIR")
	if dir == "" {
		t.Skip("RECSERVE_POPULARITY_DIR not set")
	}
	rd := loadRealData(t)
	s, err := streamState(rd.bundle, rd.store, rd.train)
	if err != nil {
		t.Fatal(err)
	}
	for _, d := range popularityWindows {
		name := filepath.Join(dir, fmt.Sprintf("popularity_%dd.npy", d))
		raw, err := os.ReadFile(name)
		if err != nil {
			t.Fatal(err)
		}
		want, err := npy.Parse(raw)
		if err != nil || len(want.I64) != len(rd.valid) {
			t.Fatalf("%s: %v, want %d int64 ranks", name, err, len(rd.valid))
		}
		got, err := popularityRanks(rd.bundle, rd.store, s, d*day, rd.valid)
		if err != nil {
			t.Fatal(err)
		}
		var differ int
		var r10 float64
		for i, g := range got {
			r10 += recallAtRank(g, 10) / float64(len(got))
			if int64(g) != want.I64[i] {
				if differ == 0 {
					t.Errorf("%dd: event %d rank %d, fresh.py's %d", d, i, g, want.I64[i])
				}
				differ++
			}
		}
		t.Logf("%dd: recall@10 %.4f; %d of %d ranks differ from fresh.py's", d, r10, differ, len(got))
		if differ > 0 {
			t.Errorf("%dd: %d ranks differ", d, differ)
		}
	}
}

// streamState keeps a row given twice once, as the history store does,
// whatever the rows' order.
func TestStreamStateDedupes(t *testing.T) {
	b := bundle.New(bundle.Manifest{ModelVersion: "t", ItemCount: 3, Dim: 1, MaxSeqLen: 5}, []string{"", "a", "b", "c"}, matrix(t, 1, []float32{0, 1, 1, 1}), nil, nil)
	rows := []dataset.Row{{User: "u", Item: "a", TS: 3}, {User: "u", Item: "b", TS: 1}, {User: "u", Item: "c", TS: 2}, {User: "u", Item: "a", TS: 3}}
	h := history.NewMemory()
	for _, r := range rows {
		if err := h.Append(context.Background(), []history.Event{{ID: history.EventID(r.User, r.Item, r.TS, dataset.Kind), User: r.User, Item: r.Item, TS: r.TS}}); err != nil {
			t.Fatal(err)
		}
	}
	s, err := streamState(b, h, rows)
	if err != nil {
		t.Fatal(err)
	}
	if got := s.Count(1, 0, 10); got != 1 {
		t.Errorf("a's events: %d, want 1", got)
	}
	if got := s.Row(2, 10, 5); !slices.Equal(got, []int32{1, 3}) {
		t.Errorf("b's row %v, want [1 3], a once and c once", got)
	}
}
