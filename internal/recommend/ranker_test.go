package recommend

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"math"
	"math/rand/v2"
	"slices"
	"testing"
	"time"

	"github.com/sHyanorn/recserve/internal/bundle"
	"github.com/sHyanorn/recserve/internal/cache"
	"github.com/sHyanorn/recserve/internal/history"
	"github.com/sHyanorn/recserve/internal/onnx"
	"github.com/sHyanorn/recserve/internal/onnx/onnxtest"
	"github.com/sHyanorn/recserve/internal/vec"
)

type rankFunc func(ctx context.Context, features []float32) ([]float32, error)

func (f rankFunc) Rank(ctx context.Context, features []float32) ([]float32, error) {
	return f(ctx, features)
}

var nf = len(bundle.RankerFeatures)

// linearRanker scores as linear_ranker.onnx does, in Go.
var linearRanker = rankFunc(func(_ context.Context, x []float32) ([]float32, error) {
	return onnxtest.Linear(x), nil
})

// failingRanker returns err.
func failingRanker(err error) Ranker {
	return rankFunc(func(context.Context, []float32) ([]float32, error) { return nil, err })
}

// positionRanker scores row i by (5i mod 7) * scale + shift: small
// integers, ties included, so scaling is exact.
func positionRanker(scale, shift float32) Ranker {
	return rankFunc(func(_ context.Context, x []float32) ([]float32, error) {
		out := make([]float32, len(x)/nf)
		for i := range out {
			out[i] = float32(5*i%7)*scale + shift
		}
		return out, nil
	})
}

// The features of SPEC §8, computed by hand. Items 1 (1, 0), 2 (0, 2),
// 3 (3, 4), 4 (0, 0); history 1 then 2; item 2's co-occurring items 3, 4,
// item 1's 3; popular 4, 3. The user vector is (0.5, 1).
func TestRankerFeatures(t *testing.T) {
	// features writes the columns in this order.
	if want := []string{"vector_score", "cooc_score", "popular_score", "item_norm", "last_item_cosine", "log_history_len"}; !slices.Equal(bundle.RankerFeatures, want) {
		t.Fatalf("bundle.RankerFeatures %v, features computes %v", bundle.RankerFeatures, want)
	}
	m := matrix(t, 2, []float32{0, 0, 1, 0, 0, 2, 3, 4, 0, 0})
	cooc := make([]int32, 5*bundle.CoocWidth)
	cooc[2*bundle.CoocWidth], cooc[2*bundle.CoocWidth+1] = 3, 4
	cooc[1*bundle.CoocWidth] = 3
	b := bundle.New(bundle.Manifest{ModelVersion: "t", ItemCount: 4, Dim: 2, MaxSeqLen: 5}, []string{"", "a", "b", "c", "d"}, m, cooc, []int32{4, 3})
	store := storeWith(t, history.Event{ID: 1, User: "u", Item: "a", TS: 1}, history.Event{ID: 2, User: "u", Item: "b", TS: 2})
	var got []float32
	p := newPipeline(t, b, store)
	p.rankWith = rankFunc(func(_ context.Context, x []float32) ([]float32, error) {
		got = slices.Clone(x)
		return make([]float32, len(x)/nf), nil
	})
	if _, err := p.Recommend(context.Background(), Request{User: "u", K: 2, TS: 3}); err != nil {
		t.Fatal(err)
	}
	ln3 := float32(math.Log(3))
	want := []float32{
		5.5, 1, 0.5, 5, 0.8, ln3, // item 3: retrieval score 5.5, first
		0, 0.5, 1, 0, 0, ln3, // item 4
	}
	if !slices.Equal(got, want) {
		t.Fatalf("features\n%v\nwant\n%v", got, want)
	}
}

// With k = 1, MMR picks the highest relevance, so the ranker's best
// candidate wins, with the ranker's score.
func TestRankerScoresDecide(t *testing.T) {
	r := rand.New(rand.NewPCG(3, 3))
	for c := range 100 {
		p, err := randomPipeline(r, 2+r.IntN(40), 3)
		if err != nil {
			t.Fatal(err)
		}
		var top float32
		p.rankWith = rankFunc(func(ctx context.Context, x []float32) ([]float32, error) {
			s, _ := linearRanker(ctx, x) //nolint:errcheck // linearRanker never fails
			top = slices.Max(s)
			return s, nil
		})
		resp, err := p.Recommend(context.Background(), Request{User: fmt.Sprint("u", r.IntN(3)), K: 1, TS: int64(r.IntN(110))})
		if err != nil {
			t.Fatal(err)
		}
		if len(resp.Items) == 1 && resp.Items[0].Score != float64(top) {
			t.Fatalf("case %d: picked score %v, ranker's best %v", c, resp.Items[0].Score, top)
		}
	}
}

// MMR scales relevance to [0, 1] over the candidates, so a positive affine
// change of the ranker's scores changes no pick.
func TestRankerScaleDoesNotChangeOrder(t *testing.T) {
	r := rand.New(rand.NewPCG(4, 4))
	ctx := context.Background()
	for c := range 200 {
		p, err := randomPipeline(r, 2+r.IntN(40), 3)
		if err != nil {
			t.Fatal(err)
		}
		req := Request{User: fmt.Sprint("u", r.IntN(3)), K: 1 + r.IntN(MaxK), TS: int64(r.IntN(110))}
		p.rankWith = positionRanker(1, 0)
		want, err := p.Recommend(ctx, req)
		if err != nil {
			t.Fatal(err)
		}
		p.rankWith = positionRanker(10, 5)
		got, err := p.Recommend(ctx, req)
		if err != nil {
			t.Fatal(err)
		}
		same := len(got.Items) == len(want.Items)
		for i := 0; same && i < len(got.Items); i++ {
			same = got.Items[i].ID == want.Items[i].ID && got.Items[i].Score == 10*want.Items[i].Score+5
		}
		if !same {
			t.Fatalf("case %d: scores x10+5 gave %v, x1 gave %v", c, got.Items, want.Items)
		}
	}
}

// A ranker that times out or fails leaves the retrieval order: the response
// equals the one without a ranker, and reports why.
func TestRankerFailureKeepsRetrievalOrder(t *testing.T) {
	r := rand.New(rand.NewPCG(5, 5))
	ctx := context.Background()
	timeout := fmt.Errorf("ranker: %w", context.DeadlineExceeded)
	for c := range 100 {
		p, err := randomPipeline(r, 2+r.IntN(40), 3)
		if err != nil {
			t.Fatal(err)
		}
		req := Request{User: fmt.Sprint("u", r.IntN(3)), K: 1 + r.IntN(MaxK), TS: int64(r.IntN(110))}
		want, err := p.Recommend(ctx, req)
		if err != nil {
			t.Fatal(err)
		}
		if len(want.Degraded) != 0 {
			t.Fatalf("degraded without a ranker: %v", want.Degraded)
		}
		for _, f := range []struct {
			err    error
			reason string
		}{{timeout, RankerTimeout}, {onnx.ErrClosed, RankerError}} {
			reason := f.reason
			p.rankWith = failingRanker(f.err)
			got, err := p.Recommend(ctx, req)
			if err != nil {
				t.Fatal(err)
			}
			if !slices.Equal(got.Items, want.Items) {
				t.Fatalf("case %d, %s: %v, without a ranker %v", c, reason, got.Items, want.Items)
			}
			if len(want.Items) > 0 && !slices.Equal(got.Degraded, []string{reason}) {
				t.Fatalf("case %d: degraded %v, want %s", c, got.Degraded, reason)
			}
		}
		p.rankWith = nil
	}
}

// A request cancelled during ranking fails; cancellation is not a
// degradation.
func TestRankerCancelledRequestFails(t *testing.T) {
	p, err := randomPipeline(rand.New(rand.NewPCG(6, 6)), 30, 1)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	p.rankWith = rankFunc(func(ctx context.Context, _ []float32) ([]float32, error) {
		cancel()
		return nil, fmt.Errorf("ranker: %w", ctx.Err())
	})
	// Popularity gives candidates whatever the history.
	p.Bundle.Popular = []int32{1, 2, 3}
	if _, err := p.Recommend(ctx, Request{User: "u0", K: 5, TS: 50}); !errors.Is(err, context.Canceled) {
		t.Fatalf("err = %v, want context.Canceled", err)
	}
}

// More candidates than bundle.RankerRows: the ranker gets the best by
// retrieval score, and the rest are counted as dropped.
func TestRankerTruncates(t *testing.T) {
	const n, dim = 3000, 4
	r := rand.New(rand.NewPCG(7, 7))
	data := make([]float32, (n+1)*dim)
	for i := dim; i < len(data); i++ {
		data[i] = float32(r.NormFloat64())
	}
	cooc := make([]int32, (n+1)*bundle.CoocWidth)
	for i := 1; i <= n; i++ {
		for j := range bundle.CoocWidth {
			cooc[i*bundle.CoocWidth+j] = int32(1 + (i*bundle.CoocWidth+j)%n)
		}
	}
	ids := make([]string, n+1)
	for i := 1; i <= n; i++ {
		ids[i] = fmt.Sprint("i", i)
	}
	popular := make([]int32, PopularTop)
	for i := range popular {
		popular[i] = int32(n - i)
	}
	b := bundle.New(bundle.Manifest{ModelVersion: "t", ItemCount: n, Dim: dim, MaxSeqLen: 50}, ids, matrix(t, dim, data), cooc, popular)
	var events []history.Event
	for i := range CoocSeeds {
		events = append(events, history.Event{ID: uint64(i), User: "u", Item: fmt.Sprint("i", 1+97*i), TS: int64(i)})
	}
	store := storeWith(t, events...)
	var got []float32
	p := newPipeline(t, b, store)
	p.rankWith = rankFunc(func(_ context.Context, x []float32) ([]float32, error) {
		got = slices.Clone(x)
		return make([]float32, len(x)/nf), nil
	})
	p.Cache = cache.New[Response](10, time.Hour)
	req := Request{User: "u", K: 10, TS: 100}
	resp, err := p.Recommend(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
	// A hit ranked nothing, so it reports nothing dropped.
	if hit, err := p.Recommend(context.Background(), req); err != nil || !*hit.Cached || hit.Dropped != 0 {
		t.Fatalf("the hit: cached %v, dropped %d, %v", *hit.Cached, hit.Dropped, err)
	}

	// The candidates as the pipeline sees them, best retrieval score first.
	ctx := context.Background()
	raw, err := store.Before(ctx, "u", req.TS, 0)
	if err != nil {
		t.Fatal(err)
	}
	hist := internalIDs(b, raw)
	seen := map[int32]bool{}
	for _, id := range hist {
		seen[id] = true
	}
	uv, err := b.UserVector(ctx, hist)
	if err != nil {
		t.Fatal(err)
	}
	var d degradation
	results, _, err := p.retrieve(ctx, hist, uv, seen, &d)
	if err != nil || len(d.reasons) > 0 {
		t.Fatal(err, d.list())
	}
	all := p.retrievalScores(results, uv, seen)
	slices.SortFunc(all, func(a, b vec.Hit) int { return cmp.Or(cmp.Compare(b.Score, a.Score), cmp.Compare(a.ID, b.ID)) })
	if len(all) <= bundle.RankerRows {
		t.Fatalf("%d candidates; the test needs more than %d", len(all), bundle.RankerRows)
	}
	if rows := len(got) / nf; rows != bundle.RankerRows || resp.Dropped != len(all)-bundle.RankerRows {
		t.Fatalf("ranked %d rows, dropped %d; want %d and %d", rows, resp.Dropped, bundle.RankerRows, len(all)-bundle.RankerRows)
	}
	for i, h := range all[:bundle.RankerRows] {
		if got[i*nf] != float32(h.Score) { // vector_score is the retrieval score here
			t.Fatalf("row %d has vector_score %v, want candidate %d's %v", i, got[i*nf], h.ID, h.Score)
		}
	}
}

func TestProperty2WithRanker(t *testing.T) {
	for name, rk := range map[string]Ranker{"linear": linearRanker, "failing": failingRanker(onnx.ErrClosed)} {
		if err := checkProperty2(2, 300, false, rk, nil); err != nil {
			t.Fatalf("%s ranker: %v", name, err)
		}
	}
}

// TestONNXRankerMatchesGo compares a pipeline ranking with
// linear_ranker.onnx against one ranking with the same model in Go.
func TestONNXRankerMatchesGo(t *testing.T) {
	if err := onnx.Init(onnxtest.Lib(t)); err != nil {
		t.Fatal(err)
	}
	const n = onnxtest.MeanItems
	r := rand.New(rand.NewPCG(8, 8))
	m := matrix(t, onnxtest.MeanDim, onnxtest.MeanVectors())
	ids := make([]string, n+1)
	cooc := make([]int32, (n+1)*bundle.CoocWidth)
	for i := 1; i <= n; i++ {
		ids[i] = fmt.Sprint("i", i)
		for j := range r.IntN(5) {
			cooc[i*bundle.CoocWidth+j] = int32(1 + r.IntN(n))
		}
	}
	man := bundle.Manifest{ModelVersion: "t", ItemCount: n, Dim: onnxtest.MeanDim, MaxSeqLen: 5}
	b := bundle.New(man, ids, m, cooc, []int32{3, 7, 1})
	rk, err := onnx.NewRanker(onnxtest.Model(t, "linear_ranker.onnx"), nf, bundle.RankerRows)
	if err != nil {
		t.Fatal(err)
	}
	b.Ranker = rk
	t.Cleanup(func() {
		if err := b.Close(); err != nil {
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
	viaONNX := newPipeline(t, b, store)
	viaGo := newPipeline(t, b, store)
	viaGo.rankWith = linearRanker
	ctx := context.Background()
	for c := range 300 {
		req := Request{User: fmt.Sprint("u", r.IntN(21)), K: 1 + r.IntN(n), TS: int64(r.IntN(110))}
		want, err := viaGo.Recommend(ctx, req)
		if err != nil {
			t.Fatal(err)
		}
		got, err := viaONNX.Recommend(ctx, req)
		if err != nil {
			t.Fatal(err)
		}
		same := len(got.Items) == len(want.Items) && len(got.Degraded) == 0
		for i := 0; same && i < len(got.Items); i++ {
			same = got.Items[i].ID == want.Items[i].ID && math.Abs(got.Items[i].Score-want.Items[i].Score) <= 1e-5
		}
		if !same {
			t.Fatalf("case %d %+v: ONNX %v %v, Go %v", c, req, got.Items, got.Degraded, want.Items)
		}
	}
}
