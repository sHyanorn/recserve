package recommend

import (
	"context"
	"fmt"
	"os"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/sHyanorn/recserve/internal/bundle"
	"github.com/sHyanorn/recserve/internal/onnx"
	"github.com/sHyanorn/recserve/internal/onnx/onnxtest"
	"github.com/sHyanorn/recserve/internal/shard"
)

// overshoot measures, for requests in which one stage runs out of time, how
// long after the stage's deadline the stage returned: the time the first
// later stage starts, or Recommend returns if none does.
type overshoot struct {
	stage string
	after []string // the stages that may start next

	mu       sync.Mutex
	deadline time.Time
	end      time.Time
	wait     bool // the stage waits for its context's end
}

func (o *overshoot) hook(ctx context.Context, stage string, _ int) error {
	if stage == o.stage {
		d, _ := ctx.Deadline()
		o.mu.Lock()
		if d.After(o.deadline) { // the latest of the shards'
			o.deadline = d
		}
		o.mu.Unlock()
		if o.wait {
			<-ctx.Done()
			return ctx.Err()
		}
		return nil
	}
	if slices.Contains(o.after, stage) {
		o.mu.Lock()
		if o.end.IsZero() {
			o.end = time.Now()
		}
		o.mu.Unlock()
	}
	return nil
}

// run sends every request through p with o as the hook and returns the
// overshoots of the requests that report one of reasons.
func (o *overshoot) run(t *testing.T, p *Pipeline, qs []realQuery, reasons ...string) []time.Duration {
	t.Helper()
	p.stall = o.hook
	defer func() { p.stall = nil }()
	var out []time.Duration
	for _, q := range qs {
		o.deadline, o.end = time.Time{}, time.Time{}
		resp, err := p.Recommend(context.Background(), q.req)
		if err != nil {
			t.Fatal(err)
		}
		if o.end.IsZero() {
			o.end = time.Now()
		}
		if slices.ContainsFunc(resp.Degraded, func(r string) bool { return slices.Contains(reasons, r) }) {
			out = append(out, o.end.Sub(o.deadline))
		}
	}
	return out
}

// TestRealDataDegradation runs on the real bundle (see TestRealDataSmoke)
// and reports, on the requests of realQueries, ranked in Go with the linear
// ranker unless said otherwise:
//
//  1. for each stage made to run out of time, how long after its deadline
//     it returned (p50, p99, max), with a hook that waits for the deadline
//     and, for the stages whose own work checks the context, with the work
//     itself: exact shard search, the ONNX user encoder (with a bundle that
//     has one) and an ONNX ranker of 300 nodes (slow_ranker.onnx);
//  2. the reasons reported with the default budgets, the bundle's own
//     ranker or none, and that every response without one equals the
//     response without budgets.
func TestRealDataDegradation(t *testing.T) {
	rd := loadRealData(t)
	qs := realQueries(t, rd)
	p, err := newLocal(rd.bundle, rd.store, shard.DefaultConfig)
	if err != nil {
		t.Fatal(err)
	}
	p.rankWith = linearRanker
	retrieval := []string{stageVector, stageCooc, stagePopular}
	const d = time.Millisecond
	for _, c := range []struct {
		stage   string
		after   []string
		budgets Budgets
		reason  string
	}{
		{stageHistory, []string{stageEncoder}, Budgets{History: d}, HistoryUnavailable},
		{stageEncoder, retrieval, Budgets{Encoder: d}, EncoderTimeout},
		{stageVector, []string{stageRanker}, Budgets{Shard: d}, VectorTimeout},
		{stageCooc, []string{stageRanker}, Budgets{Retrieval: d}, CoocTimeout},
		{stagePopular, []string{stageRanker}, Budgets{Retrieval: d}, PopularTimeout},
		{stageRanker, nil, Budgets{Ranker: d}, RankerTimeout},
	} {
		p.Budgets = c.budgets
		o := &overshoot{stage: c.stage, after: c.after, wait: true}
		got := o.run(t, p, qs, c.reason)
		t.Logf("waiting %s, budget %v: %d of %d timed out; overshoot %s", c.stage, d, len(got), len(qs), overshootSummary(got))
	}

	// The work itself, with budgets shorter than it takes.
	p.Budgets = Budgets{Shard: 100 * time.Microsecond}
	got := (&overshoot{stage: stageVector, after: []string{stageRanker}}).run(t, p, qs, VectorTimeout, ShardsPartial)
	t.Logf("exact search, shard budget 100µs: %d of %d with a shard timed out; overshoot past the later deadline %s", len(got), len(qs), overshootSummary(got))
	encoderBudgets := []time.Duration{5 * time.Microsecond, 20 * time.Microsecond}
	if rd.bundle.Encoder == nil {
		encoderBudgets = nil
		t.Logf("ONNX encoder: not measured, the bundle's user vector is the mean")
	}
	for _, budget := range encoderBudgets {
		p.Budgets = Budgets{Encoder: budget}
		got := (&overshoot{stage: stageEncoder, after: retrieval}).run(t, p, qs, EncoderTimeout)
		t.Logf("ONNX encoder, budget %v: %d of %d timed out; overshoot %s", budget, len(got), len(qs), overshootSummary(got))
	}
	switch {
	case os.Getenv("RECSERVE_ORT_LIB") != "":
		rk, err := onnx.NewRanker(onnxtest.Model(t, "slow_ranker.onnx"), len(bundle.RankerFeatures), bundle.RankerRows)
		if err != nil {
			t.Fatal(err)
		}
		defer func() {
			if err := rk.Close(); err != nil {
				t.Error(err)
			}
		}()
		// The first run sets up the session's memory; time the second.
		x := make([]float32, bundle.RankerRows*len(bundle.RankerFeatures))
		if _, err := rk.Rank(context.Background(), x); err != nil {
			t.Fatal(err)
		}
		start := time.Now()
		if _, err := rk.Rank(context.Background(), x); err != nil {
			t.Fatal(err)
		}
		full := time.Since(start)
		p.rankWith, p.Budgets = rk, Budgets{Ranker: full / 4}
		got := (&overshoot{stage: stageRanker}).run(t, p, qs, RankerTimeout)
		t.Logf("ONNX ranker of 300 nodes (a run %v), budget %v: %d of %d timed out; overshoot %s", full, full/4, len(got), len(qs), overshootSummary(got))
	case os.Getenv("RECSERVE_REQUIRE_ORT") != "":
		t.Fatal("RECSERVE_REQUIRE_ORT is set but RECSERVE_ORT_LIB is not")
	default:
		t.Logf("ONNX ranker: not measured, RECSERVE_ORT_LIB not set")
	}

	// Default budgets against none, as the gateway serves the bundle.
	p.rankWith = nil
	counts := map[string]int{}
	for _, q := range qs {
		p.Budgets = Budgets{}
		want, err := p.Recommend(context.Background(), q.req)
		if err != nil {
			t.Fatal(err)
		}
		if len(want.Degraded) != 0 {
			t.Fatalf("degraded without budgets: %v", want.Degraded)
		}
		p.Budgets = DefaultBudgets
		got, err := p.Recommend(context.Background(), q.req)
		if err != nil {
			t.Fatal(err)
		}
		for _, r := range got.Degraded {
			counts[r]++
			if strings.HasSuffix(r, "_error") {
				t.Errorf("%+v: %s: %v", q.req, r, got.Failures)
			}
		}
		if len(got.Degraded) == 0 && !slices.Equal(got.Items, want.Items) {
			t.Fatalf("%+v: default budgets %v, none %v", q.req, got.Items, want.Items)
		}
	}
	t.Logf("default budgets, %d requests: degraded %v", len(qs), counts)
}

func overshootSummary(d []time.Duration) string {
	if len(d) == 0 {
		return "none"
	}
	return fmt.Sprintf("%s, max %v", summary(d), slices.Max(d).Round(time.Microsecond))
}
