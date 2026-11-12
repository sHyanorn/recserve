package recommend

import (
	"context"
	"fmt"
	"math"
	"math/rand/v2"
	"slices"
	"testing"

	"github.com/sHyanorn/recserve/internal/bundle"
	"github.com/sHyanorn/recserve/internal/history"
	"github.com/sHyanorn/recserve/internal/onnx"
	"github.com/sHyanorn/recserve/internal/onnx/onnxtest"
	"github.com/sHyanorn/recserve/internal/shard"
	"github.com/sHyanorn/recserve/internal/vec"
)

// randomPipeline builds a bundle of n items with random vectors (some zero),
// co-occurrence rows and popularity list, and a store with random histories.
func randomPipeline(r *rand.Rand, n, users int) (*Pipeline, error) {
	const dim = 4
	data := make([]float32, (n+1)*dim)
	for i := dim; i < len(data); i++ {
		if r.IntN(10) > 0 {
			data[i] = float32(r.IntN(7) - 3)
		}
	}
	m, err := vec.NewMatrix(dim, data)
	if err != nil {
		return nil, err
	}
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
		for e := range r.IntN(300) {
			item := fmt.Sprint("i", 1+r.IntN(n+2)) // the last two are unknown to the bundle
			ts := int64(r.IntN(100))
			events = append(events, history.Event{ID: uint64(u<<20 | e), User: fmt.Sprint("u", u), Item: item, TS: ts})
		}
	}
	if err := store.Append(context.Background(), events); err != nil {
		return nil, err
	}
	return newLocal(b, store, shard.DefaultConfig)
}

// checkProperty2 returns the first response that contains an item from the
// user's history before the request (or the request's exclusions) or repeats
// an item; rk, if not nil, ranks the candidates, and f, if not nil, makes
// stages fail. When the stored history is unavailable, the history is the
// request's recent items (property 10).
func checkProperty2(seed uint64, cases int, broken bool, rk Ranker, f *fault) error {
	r := rand.New(rand.NewPCG(seed, 0))
	ctx := context.Background()
	for c := range cases {
		p, err := randomPipeline(r, 2+r.IntN(40), 3)
		if err != nil {
			return err
		}
		p.skipSeenFilter, p.rankWith = broken, rk
		if f != nil {
			p.stall = f.stall
		}
		for range 10 {
			user := fmt.Sprint("u", r.IntN(4))
			ts := int64(r.IntN(110))
			exclude := []string{fmt.Sprint("i", 1+r.IntN(p.Bundle.ItemCount)), "unknown"}
			recent := randomRecent(r, p.Bundle.ItemCount)
			resp, err := p.Recommend(ctx, Request{User: user, K: 1 + r.IntN(MaxK), TS: ts, Exclude: exclude, Recent: recent})
			if err != nil {
				return err
			}
			banned := map[string]bool{exclude[0]: true}
			if slices.Contains(resp.Degraded, HistoryUnavailable) {
				for _, raw := range recent {
					banned[raw] = true
				}
			} else {
				// Everything stored for the user before ts.
				kept, err := p.History.Before(ctx, user, ts, 0)
				if err != nil {
					return err
				}
				for _, raw := range kept {
					banned[raw] = true
				}
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
	if err := checkProperty2(1, 300, false, nil, nil); err != nil {
		t.Fatal(err)
	}
}

func TestProperty2CatchesBrokenVariant(t *testing.T) {
	if err := checkProperty2(1, 300, true, nil, nil); err == nil {
		t.Fatal("the check passed a pipeline without the seen filter")
	}
}

// TestCosineOfTinyRow: a row whose norm is too small for 1/norm to be a
// float32 counts as a zero row, so its cosine similarity is 0, not NaN or
// an infinity.
func TestCosineOfTinyRow(t *testing.T) {
	m, err := vec.NewMatrix(2, []float32{1e-45, 0, 1e-39, 1e-39, 1, 0})
	if err != nil {
		t.Fatal(err)
	}
	inv := m.InvNorms()
	for _, p := range [][2]int32{{0, 0}, {0, 2}, {1, 1}, {1, 2}} {
		if c := cosine(m, inv, p[0], p[1]); c != 0 {
			t.Errorf("cosine of rows %d and %d: %g, want 0", p[0], p[1], c)
		}
	}
}

func TestMMRPrefersDiversity(t *testing.T) {
	// Items 1 and 2 point the same way; 3 is orthogonal and a little less
	// relevant; 4 sets the bottom of the relevance range.
	m := matrix(t, 2, []float32{0, 0, 1, 0, 1, 0, 0, 1, -1, 0})
	hits := []vec.Hit{{ID: 1, Score: 1}, {ID: 2, Score: 0.99}, {ID: 3, Score: 0.9}, {ID: 4, Score: 0}}
	got := mmr(m, m.InvNorms(), hits, 2, 0.7)
	if got[0].ID != 1 || got[1].ID != 3 {
		t.Fatalf("got %v, want items 1 then 3", got)
	}
	if got := mmr(m, m.InvNorms(), hits, 2, 1); got[1].ID != 2 {
		t.Fatalf("lambda 1 should rank by relevance only, got %v", got)
	}
}

// When every candidate has the same score (one candidate, or a ranker that
// scores all alike), relevance is 1 for each and similarity alone decides;
// nothing is NaN. Items 1 and 2 point the same way, 3 is orthogonal, 4 is
// zero, so after 1 the order is 3, 4 (ties to the lower ID), then 2.
func TestMMREqualScores(t *testing.T) {
	m := matrix(t, 2, []float32{0, 0, 1, 0, 1, 0, 0, 1, 0, 0})
	hits := []vec.Hit{{ID: 1, Score: 5}, {ID: 2, Score: 5}, {ID: 3, Score: 5}, {ID: 4, Score: 5}}
	got := mmr(m, m.InvNorms(), hits, 4, MMRLambda)
	if want := []vec.Hit{{ID: 1, Score: 5}, {ID: 3, Score: 5}, {ID: 4, Score: 5}, {ID: 2, Score: 5}}; !slices.Equal(got, want) {
		t.Fatalf("got %v, want %v", got, want)
	}
	if got := mmr(m, m.InvNorms(), []vec.Hit{{ID: 2, Score: -3}}, 5, MMRLambda); !slices.Equal(got, []vec.Hit{{ID: 2, Score: -3}}) {
		t.Fatalf("one candidate: %v", got)
	}

	// Through the pipeline, with a ranker that gives every candidate 7.
	r := rand.New(rand.NewPCG(9, 9))
	for c := range 50 {
		p, err := randomPipeline(r, 2+r.IntN(40), 3)
		if err != nil {
			t.Fatal(err)
		}
		p.rankWith = positionRanker(0, 7)
		resp, err := p.Recommend(context.Background(), Request{User: fmt.Sprint("u", r.IntN(3)), K: 1 + r.IntN(MaxK), TS: int64(r.IntN(110))})
		if err != nil {
			t.Fatal(err)
		}
		for _, it := range resp.Items {
			if it.Score != 7 {
				t.Fatalf("case %d: score %v, want 7", c, it.Score)
			}
		}
	}
}

// mmrEager is MMR as defined: every round recomputes every remaining
// candidate's value and picks the highest, ties to the lower ID.
func mmrEager(m vec.Matrix, inv []float32, hits []vec.Hit, k int, lambda float64) []vec.Hit {
	if len(hits) == 0 {
		return nil
	}
	lo, hi := math.Inf(1), math.Inf(-1)
	for _, h := range hits {
		lo, hi = min(lo, h.Score), max(hi, h.Score)
	}
	rel := func(s float64) float64 {
		if hi == lo {
			return 1
		}
		return (s - lo) / (hi - lo)
	}
	redundancy := make([]float64, len(hits))
	used := make([]bool, len(hits))
	out := make([]vec.Hit, 0, min(k, len(hits)))
	for len(out) < k && len(out) < len(hits) {
		bi, bv := -1, math.Inf(-1)
		for i, h := range hits {
			if used[i] {
				continue
			}
			v := lambda*rel(h.Score) - (1-lambda)*redundancy[i]
			if v > bv || (v == bv && h.ID < hits[bi].ID) {
				bi, bv = i, v
			}
		}
		used[bi] = true
		out = append(out, hits[bi])
		for i, h := range hits {
			if !used[i] {
				redundancy[i] = max(redundancy[i], cosine(m, inv, h.ID, hits[bi].ID))
			}
		}
	}
	return out
}

// The lazy MMR picks exactly what the eager definition picks, in the same
// order, including when values tie. Small integer vectors and scores make
// ties and zero vectors common.
func TestMMRMatchesEager(t *testing.T) {
	r := rand.New(rand.NewPCG(3, 4))
	for trial := range 2000 {
		n, dim := 1+r.IntN(60), 1+r.IntN(4)
		data := make([]float32, (n+1)*dim)
		for i := dim; i < len(data); i++ {
			data[i] = float32(r.IntN(5) - 2)
		}
		m := matrix(t, dim, data)
		var hits []vec.Hit
		for _, id := range r.Perm(n) {
			if r.IntN(4) > 0 {
				hits = append(hits, vec.Hit{ID: int32(id + 1), Score: float64(r.IntN(6))})
			}
		}
		k := r.IntN(n + 2)
		lambda := []float64{0, 0.3, 0.7, 1}[r.IntN(4)]
		inv := m.InvNorms()
		got, want := mmr(m, inv, hits, k, lambda), mmrEager(m, inv, hits, k, lambda)
		if !slices.Equal(got, want) {
			t.Fatalf("trial %d k=%d lambda=%g: got %v, want %v", trial, k, lambda, got, want)
		}
	}
}

func TestRejectsBadK(t *testing.T) {
	p, err := randomPipeline(rand.New(rand.NewPCG(1, 1)), 5, 1)
	if err != nil {
		t.Fatal(err)
	}
	for _, k := range []int{0, -1, MaxK + 1} {
		if _, err := p.Recommend(context.Background(), Request{User: "u0", K: k, TS: 50}); err == nil {
			t.Errorf("k=%d accepted", k)
		}
	}
}

// A history whose items all have zero vectors (no training data) gives no
// user vector, so popularity decides instead of arbitrary zero scores.
func TestZeroVectorHistoryFallsBackToPopularity(t *testing.T) {
	m := matrix(t, 2, []float32{0, 0, 0, 0, 1, 0, 0, 1})
	b := bundle.New(bundle.Manifest{ModelVersion: "t", ItemCount: 3, Dim: 2, MaxSeqLen: 5}, []string{"", "a", "b", "c"}, m, nil, []int32{3, 2})
	store := storeWith(t, history.Event{ID: 1, User: "u", Item: "a", TS: 1})
	resp, err := newPipeline(t, b, store).Recommend(context.Background(), Request{User: "u", K: 2, TS: 2})
	if err != nil {
		t.Fatal(err)
	}
	if len(resp.Items) != 2 || resp.Items[0].ID != "c" || resp.Items[1].ID != "b" {
		t.Fatalf("got %v, want c then b", resp.Items)
	}
}

// The request path passes the request time to the store unchanged: an event
// at exactly that time is not history yet, so its item may be recommended.
func TestEventAtRequestTimeIsNotHistory(t *testing.T) {
	m := matrix(t, 2, []float32{0, 0, 1, 0, 0, 1})
	b := bundle.New(bundle.Manifest{ModelVersion: "t", ItemCount: 2, Dim: 2, MaxSeqLen: 5}, []string{"", "a", "b"}, m, nil, []int32{1, 2})
	store := storeWith(t, history.Event{ID: 1, User: "u", Item: "a", TS: 5})
	resp, err := newPipeline(t, b, store).Recommend(context.Background(), Request{User: "u", K: 1, TS: 5})
	if err != nil {
		t.Fatal(err)
	}
	if len(resp.Items) != 1 || resp.Items[0].ID != "a" {
		t.Fatalf("got %v, want a (popular, and its event is not before ts)", resp.Items)
	}
}

func matrix(t testing.TB, dim int, data []float32) vec.Matrix {
	t.Helper()
	m, err := vec.NewMatrix(dim, data)
	if err != nil {
		t.Fatal(err)
	}
	return m
}

// newPipeline returns a pipeline over b and h with the default shards.
func newPipeline(t testing.TB, b *bundle.Bundle, h history.Store) *Pipeline {
	t.Helper()
	p, err := newLocal(b, h, shard.DefaultConfig)
	if err != nil {
		t.Fatal(err)
	}
	return p
}

func storeWith(t testing.TB, events ...history.Event) *history.Memory {
	t.Helper()
	s := history.NewMemory()
	if err := s.Append(context.Background(), events); err != nil {
		t.Fatal(err)
	}
	return s
}

// TestONNXEncoderMatchesMean compares two pipelines over the items of the
// test encoder mean.onnx, one computing user vectors with it and one with the
// mean in Go: every response must hold the same items with the same scores
// within 1e-6. Property 2 holds for the second pipeline (TestProperty2NoSeenOrRepeatedItems),
// so it holds for the first on these requests too.
func TestONNXEncoderMatchesMean(t *testing.T) {
	if err := onnx.Init(onnxtest.Lib(t)); err != nil {
		t.Fatal(err)
	}
	const n, seqLen = onnxtest.MeanItems, 5
	r := rand.New(rand.NewPCG(5, 6))
	m := matrix(t, onnxtest.MeanDim, onnxtest.MeanVectors())
	ids := make([]string, n+1)
	cooc := make([]int32, (n+1)*bundle.CoocWidth)
	for i := 1; i <= n; i++ {
		ids[i] = fmt.Sprint("i", i)
		for j := range r.IntN(5) {
			cooc[i*bundle.CoocWidth+j] = int32(1 + r.IntN(n))
		}
	}
	popular := []int32{3, 7, 1}
	man := bundle.Manifest{ModelVersion: "t", ItemCount: n, Dim: onnxtest.MeanDim, MaxSeqLen: seqLen}
	enc, err := onnx.NewEncoder(onnxtest.Model(t, "mean.onnx"), seqLen, onnxtest.MeanDim)
	if err != nil {
		t.Fatal(err)
	}
	withEncoder := bundle.New(man, ids, m, cooc, popular)
	withEncoder.Encoder = enc
	t.Cleanup(func() {
		if err := withEncoder.Close(); err != nil {
			t.Error(err)
		}
	})
	var events []history.Event
	for u := range 20 {
		for e := range r.IntN(12) {
			events = append(events, history.Event{ID: uint64(u<<20 | e), User: fmt.Sprint("u", u), Item: fmt.Sprint("i", 1+r.IntN(n)), TS: int64(r.IntN(100))})
		}
	}
	store := storeWith(t, events...)
	viaMean := newPipeline(t, bundle.New(man, ids, m, cooc, popular), store)
	viaONNX := newPipeline(t, withEncoder, store)
	ctx := context.Background()
	for c := range 300 {
		req := Request{User: fmt.Sprint("u", r.IntN(21)), K: 1 + r.IntN(n), TS: int64(r.IntN(110)), Exclude: []string{fmt.Sprint("i", 1+r.IntN(n))}}
		want, err := viaMean.Recommend(ctx, req)
		if err != nil {
			t.Fatal(err)
		}
		got, err := viaONNX.Recommend(ctx, req)
		if err != nil {
			t.Fatal(err)
		}
		same := len(got.Items) == len(want.Items)
		for i := 0; same && i < len(got.Items); i++ {
			same = got.Items[i].ID == want.Items[i].ID && math.Abs(got.Items[i].Score-want.Items[i].Score) <= 1e-6
		}
		if !same {
			t.Fatalf("case %d %+v: encoder %v, mean %v", c, req, got.Items, want.Items)
		}
	}
}

// The co-occurrence score is 1 / ((s + 1)(r + 1)) in float64, as docs/SPEC.md
// §8 writes it: for s = 2, r = 10 it is 1/33, one bit away from (1/3)/11.
func TestCoocScore(t *testing.T) {
	const n = 20
	m, err := vec.NewMatrix(1, make([]float32, n+1))
	if err != nil {
		t.Fatal(err)
	}
	ids := make([]string, n+1)
	for i := 1; i <= n; i++ {
		ids[i] = fmt.Sprint("i", i)
	}
	cooc := make([]int32, (n+1)*bundle.CoocWidth)
	for r := range 11 {
		cooc[1*bundle.CoocWidth+r] = int32(5 + r) // item 15 at position 10 of item 1's row
	}
	p := &Pipeline{Bundle: bundle.New(bundle.Manifest{ItemCount: n, Dim: 1}, ids, m, cooc, nil)}
	hits, err := p.cooc(context.Background(), []int32{1, 2, 3}) // item 1 is seed 2
	if err != nil {
		t.Fatal(err)
	}
	seed, pos := 3.0, 11.0 // s + 1, r + 1, as float64 values, not constants
	want := 1 / (seed * pos)
	if len(hits) != 11 || hits[10] != (vec.Hit{ID: 15, Score: want}) {
		t.Fatalf("hits %v, want item 15 last with %v", hits, want)
	}
	if want == 1/seed/pos {
		t.Fatal("the case does not tell the two orders apart")
	}
}

// newLocal returns a pipeline over b and h with b's vectors in in-process
// shards built by cfg.
func newLocal(b *bundle.Bundle, h history.Store, cfg shard.Config) (*Pipeline, error) {
	s, err := shard.Build(b.Vectors, cfg)
	if err != nil {
		return nil, err
	}
	return New(b, h, s), nil
}
