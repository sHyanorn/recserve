package recommend

import (
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/sHyanorn/recserve/internal/history"
	"github.com/sHyanorn/recserve/internal/shard"
	"github.com/sHyanorn/recserve/internal/vec"
)

var errInjected = errors.New("injected")

// stageFault is how one stage, or one shard (shard >= 0), fails.
type stageFault struct {
	stage   string
	shard   int // -1: every call of the stage
	timeout bool
}

// fault makes stages fail through the stall hook, with errInjected or with
// an error that wraps context.DeadlineExceeded, as a stage past its budget
// returns; TestBudgetsBoundStages checks that budgets end stages that way.
type fault struct {
	name   string
	faults []stageFault
	reason string // what the response reports, if the stage runs
}

func (f *fault) stall(_ context.Context, stage string, sh int) error {
	for _, sf := range f.faults {
		if sf.stage != stage || (sf.shard >= 0 && sf.shard != sh) {
			continue
		}
		if sf.timeout {
			return fmt.Errorf("%s: %w", stage, context.DeadlineExceeded)
		}
		return fmt.Errorf("%s: %w", stage, errInjected)
	}
	return nil
}

func (f *fault) fails(stage string, sh int) bool {
	return slices.ContainsFunc(f.faults, func(sf stageFault) bool {
		return sf.stage == stage && (sf.shard < 0 || sf.shard == sh)
	})
}

var faults = []*fault{
	{"history error", []stageFault{{stageHistory, -1, false}}, HistoryUnavailable},
	{"history timeout", []stageFault{{stageHistory, -1, true}}, HistoryUnavailable},
	{"encoder error", []stageFault{{stageEncoder, -1, false}}, EncoderError},
	{"encoder timeout", []stageFault{{stageEncoder, -1, true}}, EncoderTimeout},
	{"shard 1 error", []stageFault{{stageVector, 1, false}}, ShardsPartial},
	{"shard 0 timeout", []stageFault{{stageVector, 0, true}}, ShardsPartial},
	{"all shards error", []stageFault{{stageVector, -1, false}}, VectorError},
	{"all shards timeout", []stageFault{{stageVector, -1, true}}, VectorTimeout},
	{"shard 0 error, shard 1 timeout", []stageFault{{stageVector, 0, false}, {stageVector, 1, true}}, VectorError},
	{"cooc error", []stageFault{{stageCooc, -1, false}}, CoocError},
	{"cooc timeout", []stageFault{{stageCooc, -1, true}}, CoocTimeout},
	{"popular error", []stageFault{{stagePopular, -1, false}}, PopularError},
	{"popular timeout", []stageFault{{stagePopular, -1, true}}, PopularTimeout},
	{"ranker error", []stageFault{{stageRanker, -1, false}}, RankerError},
	{"ranker timeout", []stageFault{{stageRanker, -1, true}}, RankerTimeout},
}

// randomRecent returns up to 8 raw item IDs of a catalog of n items, and
// sometimes one the catalog lacks.
func randomRecent(r *rand.Rand, n int) []string {
	out := make([]string, r.IntN(9))
	for i := range out {
		out[i] = fmt.Sprint("i", 1+r.IntN(n))
	}
	if r.IntN(4) == 0 {
		out = append(out, "unknown")
	}
	return out
}

// want returns what a request should return under f: each failed stage
// skipped or replaced by its fallback, computed by the pipeline's own steps
// without the hook or budgets.
func want(t *testing.T, p *Pipeline, f *fault, req Request) Response {
	t.Helper()
	ctx := context.Background()
	q := *p
	q.stall, q.Budgets = nil, Budgets{}
	b := p.Bundle
	var d degradation
	raws := req.Recent
	if f.fails(stageHistory, -1) {
		d.add(f.reason, nil)
	} else {
		var err error
		if raws, err = p.History.Before(ctx, req.User, req.TS, 0); err != nil {
			t.Fatal(err)
		}
	}
	var hist []int32
	for _, raw := range raws {
		if id, ok := b.Lookup(raw); ok {
			hist = append(hist, id)
		}
	}
	seen := map[int32]bool{}
	for _, id := range hist {
		seen[id] = true
	}
	for _, raw := range req.Exclude {
		if id, ok := b.Lookup(raw); ok {
			seen[id] = true
		}
	}
	var uv []float32
	if f.fails(stageEncoder, -1) {
		d.add(f.reason, nil)
	} else {
		var err error
		if uv, err = b.UserVector(ctx, hist); err != nil {
			t.Fatal(err)
		}
	}
	results := make([][]vec.Hit, 3)
	var resp Response
	if uv != nil {
		n := p.Shards.Config().Shards
		resp.Coverage = &Coverage{Shards: n}
		var answered [][]vec.Hit
		for i := range n {
			if f.fails(stageVector, i) {
				continue
			}
			hits, err := p.Shards.Search(ctx, i, uv, VectorTop, seen)
			if err != nil {
				t.Fatal(err)
			}
			answered = append(answered, hits)
		}
		resp.Coverage.Answered = len(answered)
		if len(answered) < n {
			d.add(f.reason, nil)
		}
		results[srcVector] = shard.Merge(answered, VectorTop)
	}
	var err error
	if f.fails(stageCooc, -1) {
		d.add(f.reason, nil)
	} else if results[srcCooc], err = q.cooc(ctx, hist); err != nil {
		t.Fatal(err)
	}
	if f.fails(stagePopular, -1) {
		d.add(f.reason, nil)
	} else if results[srcPopular], err = q.popular(ctx); err != nil {
		t.Fatal(err)
	}
	scored := q.retrievalScores(results, uv, seen)
	if rk := q.ranker(); rk != nil && len(scored) > 0 {
		if f.fails(stageRanker, -1) {
			d.add(f.reason, nil)
		} else if scored, _, err = q.rank(ctx, rk, scored, results, hist, uv); err != nil {
			t.Fatal(err)
		}
	}
	for _, h := range mmr(b.Vectors, b.InvNorms, scored, req.K, MMRLambda) {
		resp.Items = append(resp.Items, Item{ID: b.IDs[h.ID], Score: h.Score})
	}
	resp.Degraded = d.list()
	return resp
}

// Each fault, on random pipelines with a ranker: the response reports the
// fault's reason when the failed stage ran, falls back as the stage's rule
// says, reports the shards that answered, and keeps the error unless it is
// a timeout.
func TestDegradation(t *testing.T) {
	for _, f := range faults {
		t.Run(f.name, func(t *testing.T) {
			t.Parallel()
			r := rand.New(rand.NewPCG(11, 11))
			var reported int
			for c := range 30 {
				p, err := randomPipeline(r, 2+r.IntN(40), 3)
				if err != nil {
					t.Fatal(err)
				}
				p.rankWith, p.stall = linearRanker, f.stall
				for range 5 {
					req := Request{User: fmt.Sprint("u", r.IntN(4)), K: 1 + r.IntN(MaxK), TS: int64(r.IntN(110)),
						Exclude: []string{fmt.Sprint("i", 1+r.IntN(p.Bundle.ItemCount))}, Recent: randomRecent(r, p.Bundle.ItemCount)}
					got, err := p.Recommend(context.Background(), req)
					if err != nil {
						t.Fatal(err)
					}
					w := want(t, p, f, req)
					if !slices.Equal(got.Items, w.Items) && (len(got.Items) > 0 || len(w.Items) > 0) {
						t.Fatalf("case %d %+v:\n got %v\nwant %v", c, req, got.Items, w.Items)
					}
					if !slices.Equal(got.Degraded, w.Degraded) {
						t.Fatalf("case %d: degraded %v, want %v", c, got.Degraded, w.Degraded)
					}
					if (got.Coverage == nil) != (w.Coverage == nil) || (got.Coverage != nil && *got.Coverage != *w.Coverage) {
						t.Fatalf("case %d: coverage %v, want %v", c, got.Coverage, w.Coverage)
					}
					timeoutOnly := !slices.ContainsFunc(f.faults, func(sf stageFault) bool { return !sf.timeout })
					for _, fl := range got.Failures {
						if fl.Reason != f.reason || !errors.Is(fl.Err, errInjected) {
							t.Fatalf("case %d: failure %v", c, fl)
						}
					}
					if len(got.Degraded) > 0 {
						reported++
						if timeoutOnly != (len(got.Failures) == 0) {
							t.Fatalf("case %d: failures %v for faults %+v", c, got.Failures, f.faults)
						}
					}
				}
			}
			if reported == 0 {
				t.Fatal("the failed stage never ran")
			}
		})
	}
}

// Property 2 holds whatever stage fails.
func TestProperty2UnderDegradation(t *testing.T) {
	for _, f := range faults {
		t.Run(f.name, func(t *testing.T) {
			t.Parallel()
			if err := checkProperty2(3, 30, false, linearRanker, f); err != nil {
				t.Fatal(err)
			}
		})
	}
}

// Property 10, within one gateway: with the stored history unavailable,
// the response is the one a store holding only the request's recent items
// would give, and reports history_unavailable.
func TestProperty10RecentReplacesHistory(t *testing.T) {
	r := rand.New(rand.NewPCG(12, 12))
	for _, f := range faults[:2] {
		for c := range 100 {
			p, err := randomPipeline(r, 2+r.IntN(40), 3)
			if err != nil {
				t.Fatal(err)
			}
			p.rankWith, p.stall = linearRanker, f.stall
			req := Request{User: "u0", K: 1 + r.IntN(MaxK), TS: 50,
				Exclude: []string{fmt.Sprint("i", 1+r.IntN(p.Bundle.ItemCount))}, Recent: randomRecent(r, p.Bundle.ItemCount)}
			got, err := p.Recommend(context.Background(), req)
			if err != nil {
				t.Fatal(err)
			}
			var evs []history.Event
			for i, raw := range req.Recent {
				evs = append(evs, history.Event{ID: uint64(i), User: req.User, Item: raw, TS: int64(i)})
			}
			q := *p
			q.History, q.stall, q.Budgets = storeWith(t, evs...), nil, Budgets{}
			w, err := q.Recommend(context.Background(), Request{User: req.User, K: req.K, TS: int64(len(req.Recent)), Exclude: req.Exclude})
			if err != nil {
				t.Fatal(err)
			}
			if !slices.Equal(got.Items, w.Items) || !slices.Equal(got.Degraded, []string{HistoryUnavailable}) {
				t.Fatalf("%s, case %d: %v %v; from a store of the recent items %v", f.name, c, got.Items, got.Degraded, w.Items)
			}
		}
	}
}

// Each budget ends its stage: a stage that waits for its context's end
// times out after the budget, and the request degrades. Only the stalled
// stage has a budget, except that shards also run within the retrieval
// budget, as do the other sources.
func TestBudgetsBoundStages(t *testing.T) {
	const d = 2 * time.Millisecond
	for _, c := range []struct {
		stage   string
		budgets Budgets
		reason  string
	}{
		{stageHistory, Budgets{History: d}, HistoryUnavailable},
		{stageEncoder, Budgets{Encoder: d}, EncoderTimeout},
		{stageVector, Budgets{Shard: d}, VectorTimeout},
		{stageVector, Budgets{Retrieval: d}, VectorTimeout},
		{stageVector, Budgets{Retrieval: time.Hour, Shard: d}, VectorTimeout},
		{stageRanker, Budgets{Ranker: d}, RankerTimeout},
	} {
		t.Run(fmt.Sprint(c.stage, c.budgets), func(t *testing.T) {
			r := rand.New(rand.NewPCG(15, 15))
			for i := 0; ; i++ {
				if i == 100 {
					t.Fatal("the stage never ran")
				}
				p, err := randomPipeline(r, 2+r.IntN(40), 3)
				if err != nil {
					t.Fatal(err)
				}
				p.rankWith, p.Budgets = linearRanker, c.budgets
				// waited runs from the request's start: a stage's budget starts
				// before its hook runs, by a goroutine's start for a shard
				// under the retrieval budget.
				var mu sync.Mutex
				ran, waited := false, time.Duration(0)
				start := time.Now()
				p.stall = func(ctx context.Context, stage string, _ int) error {
					if stage != c.stage {
						return nil
					}
					var err error
					select {
					case <-ctx.Done():
						err = ctx.Err()
					case <-time.After(10 * time.Second):
						err = errors.New("the budget did not end the stage")
					}
					mu.Lock()
					ran, waited = true, max(waited, time.Since(start))
					mu.Unlock()
					return err
				}
				resp, err := p.Recommend(context.Background(), Request{User: fmt.Sprint("u", r.IntN(3)), K: 10, TS: int64(r.IntN(110))})
				if err != nil {
					t.Fatal(err)
				}
				if !ran {
					continue
				}
				if !slices.Contains(resp.Degraded, c.reason) || waited < d {
					t.Fatalf("degraded %v after waiting %v; want %s after %v", resp.Degraded, waited, c.reason, d)
				}
				if c.stage == stageVector && *resp.Coverage != (Coverage{Shards: 2}) {
					t.Fatalf("coverage %v", *resp.Coverage)
				}
				return
			}
		})
	}
}

// Several stages fail at once: the reasons come in the order of Reasons,
// once each.
func TestDegradedOrder(t *testing.T) {
	p, err := randomPipeline(rand.New(rand.NewPCG(13, 13)), 30, 1)
	if err != nil {
		t.Fatal(err)
	}
	p.Bundle.Popular = []int32{1, 2, 3}
	f := &fault{faults: []stageFault{
		{stageRanker, -1, false}, {stageCooc, -1, true}, {stageEncoder, -1, false}, {stageHistory, -1, false},
	}}
	p.rankWith, p.stall = linearRanker, f.stall
	for range 20 { // an order taken from a map would differ between runs
		resp, err := p.Recommend(context.Background(), Request{User: "u0", K: 5, TS: 50, Recent: []string{"i1", "i2"}})
		if err != nil {
			t.Fatal(err)
		}
		if want := []string{HistoryUnavailable, EncoderError, CoocTimeout, RankerError}; !slices.Equal(resp.Degraded, want) {
			t.Fatalf("degraded %v, want %v", resp.Degraded, want)
		}
		var reasons []string
		for _, fl := range resp.Failures {
			reasons = append(reasons, fl.Reason)
		}
		if want := []string{HistoryUnavailable, EncoderError, RankerError}; !slices.Equal(reasons, want) {
			t.Fatalf("failures %v, want %v", reasons, want)
		}
	}
}

// A request that ends during any stage fails with its context's error,
// whether cancelled or past its caller's deadline, even while the stage has
// a budget of its own.
func TestEndedRequestFails(t *testing.T) {
	stages := []struct {
		stage string
		shard int
	}{{stageHistory, -1}, {stageEncoder, -1}, {stageVector, 0}, {stageVector, 1}, {stageCooc, -1}, {stagePopular, -1}, {stageRanker, -1}}
	long := Budgets{History: time.Minute, Encoder: time.Minute, Retrieval: time.Minute, Shard: time.Minute, Ranker: time.Minute}
	for _, st := range stages {
		for _, budgets := range []Budgets{{}, long} {
			for _, want := range []error{context.Canceled, context.DeadlineExceeded} {
				name := fmt.Sprintf("%s %d, budgets %v, %v", st.stage, st.shard, budgets != Budgets{}, want)
				t.Run(name, func(t *testing.T) {
					t.Parallel()
					r := rand.New(rand.NewPCG(14, 14))
					for c := 0; ; c++ {
						if c == 100 {
							t.Fatal("the stage never ran")
						}
						p, err := randomPipeline(r, 2+r.IntN(40), 3)
						if err != nil {
							t.Fatal(err)
						}
						p.rankWith, p.Budgets = linearRanker, budgets
						ctx, cancel := context.WithCancel(context.Background())
						if errors.Is(want, context.DeadlineExceeded) {
							ctx, cancel = context.WithTimeout(context.Background(), 20*time.Millisecond)
						}
						ran := false
						p.stall = func(sctx context.Context, stage string, sh int) error {
							if stage != st.stage || sh != st.shard {
								return nil
							}
							ran = true
							if errors.Is(want, context.Canceled) {
								cancel()
							}
							<-sctx.Done()
							return sctx.Err()
						}
						resp, err := p.Recommend(ctx, Request{User: fmt.Sprint("u", r.IntN(3)), K: 10, TS: int64(r.IntN(110))})
						cancel()
						if !ran {
							if err != nil {
								t.Fatal(err)
							}
							continue
						}
						if !errors.Is(err, want) {
							t.Fatalf("err %v, degraded %v; want %v", err, resp.Degraded, want)
						}
						return
					}
				})
			}
		}
	}
}
