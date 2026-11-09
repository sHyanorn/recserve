package recommend

import (
	"context"
	"fmt"
	"math/rand/v2"
	"testing"

	"github.com/sHyanorn/recserve/internal/bundle"
	"github.com/sHyanorn/recserve/internal/history"
	"github.com/sHyanorn/recserve/internal/vec"
)

// randomPipeline builds a bundle of n items with random vectors (some zero),
// co-occurrence rows and popularity list, and a store with random histories.
func randomPipeline(r *rand.Rand, n, users int) *Pipeline {
	const dim = 4
	data := make([]float32, (n+1)*dim)
	for i := dim; i < len(data); i++ {
		if r.IntN(10) > 0 {
			data[i] = float32(r.IntN(7) - 3)
		}
	}
	m, _ := vec.NewMatrix(dim, data)
	ids := make([]string, n+1)
	for i := 1; i <= n; i++ {
		ids[i] = fmt.Sprint("i", i)
	}
	cooc := make([]int32, (n+1)*bundle.CoocWidth)
	for i := 1; i <= n; i++ {
		for j := range r.IntN(8) {
			cooc[i*bundle.CoocWidth+j] = int32(1 + r.IntN(n))
		}
	}
	popular := make([]int32, r.IntN(n))
	for i := range popular {
		popular[i] = int32(1 + r.IntN(n))
	}
	b := bundle.New(bundle.Manifest{ModelVersion: "t", ItemCount: n, Dim: dim, MaxSeqLen: 5}, ids, m, cooc, popular)
	store := history.NewMemory()
	var events []history.Event
	for u := range users {
		for e := range r.IntN(3 * n) {
			item := int32(1 + r.IntN(n))
			ts := int64(r.IntN(100))
			events = append(events, history.Event{ID: uint64(u<<20 | e), User: fmt.Sprint("u", u), Item: item, TS: ts})
		}
	}
	_ = store.Append(context.Background(), events)
	return &Pipeline{Bundle: b, History: store}
}

// checkProperty2 returns the first response that contains an item from the
// user's history before the request (or the request's exclusions) or repeats an item.
func checkProperty2(seed uint64, cases int, broken bool) error {
	r := rand.New(rand.NewPCG(seed, 0))
	ctx := context.Background()
	for c := range cases {
		p := randomPipeline(r, 2+r.IntN(40), 3)
		p.skipSeenFilter = broken
		for range 10 {
			user := fmt.Sprint("u", r.IntN(4))
			ts := int64(r.IntN(110))
			exclude := []string{fmt.Sprint("i", 1+r.IntN(p.Bundle.ItemCount)), "unknown"}
			resp, err := p.Recommend(ctx, Request{User: user, K: 1 + r.IntN(MaxK), TS: ts, Exclude: exclude})
			if err != nil {
				return err
			}
			// Everything stored for the user before ts; the store keeps the latest history.Cap.
			banned := map[string]bool{exclude[0]: true}
			kept, _ := p.History.Before(ctx, user, ts, history.Cap)
			for _, id := range kept {
				banned[p.Bundle.IDs[id]] = true
			}
			got := map[string]bool{}
			for _, it := range resp.Items {
				if banned[it.ID] {
					return fmt.Errorf("case %d: %s at ts %d got seen item %s", c, user, ts, it.ID)
				}
				if got[it.ID] {
					return fmt.Errorf("case %d: item %s twice", c, it.ID)
				}
				got[it.ID] = true
			}
		}
	}
	return nil
}

func TestProperty2NoSeenOrRepeatedItems(t *testing.T) {
	if err := checkProperty2(1, 300, false); err != nil {
		t.Fatal(err)
	}
}

func TestProperty2CatchesBrokenVariant(t *testing.T) {
	if err := checkProperty2(1, 300, true); err == nil {
		t.Fatal("the check passed a pipeline without the seen filter")
	}
}

func TestMMRPrefersDiversity(t *testing.T) {
	// Items 1 and 2 point the same way; 3 is orthogonal and a little less
	// relevant; 4 sets the bottom of the relevance range.
	m, _ := vec.NewMatrix(2, []float32{0, 0, 1, 0, 1, 0, 0, 1, -1, 0})
	hits := []vec.Hit{{ID: 1, Score: 1}, {ID: 2, Score: 0.99}, {ID: 3, Score: 0.9}, {ID: 4, Score: 0}}
	got := mmr(m, hits, 2, 0.7)
	if got[0].ID != 1 || got[1].ID != 3 {
		t.Fatalf("got %v, want items 1 then 3", got)
	}
	if got := mmr(m, hits, 2, 1); got[1].ID != 2 {
		t.Fatalf("lambda 1 should rank by relevance only, got %v", got)
	}
}

func TestRejectsBadK(t *testing.T) {
	p := randomPipeline(rand.New(rand.NewPCG(1, 1)), 5, 1)
	for _, k := range []int{0, -1, MaxK + 1} {
		if _, err := p.Recommend(context.Background(), Request{User: "u0", K: k, TS: 50}); err == nil {
			t.Errorf("k=%d accepted", k)
		}
	}
}
