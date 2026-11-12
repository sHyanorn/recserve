package recommend

import (
	"context"
	"fmt"
	"math"
	"math/rand/v2"
	"os"
	"runtime"
	"slices"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"golang.org/x/sync/errgroup"

	"github.com/sHyanorn/recserve/internal/bench"
	"github.com/sHyanorn/recserve/internal/bundle"
	"github.com/sHyanorn/recserve/internal/dataset"
	"github.com/sHyanorn/recserve/internal/history"
	"github.com/sHyanorn/recserve/internal/shard"
	"github.com/sHyanorn/recserve/internal/vec"
)

// The quality evaluation's settings (docs/BENCHMARKS.md, Recommendation
// quality).
const (
	qualityK         = 20 // items per request
	bootstrapSamples = 1000
	bootstrapSeed    = 1
)

// errForced fails a stage as the end of its budget would.
var errForced = fmt.Errorf("forced by the quality test: %w", context.DeadlineExceeded)

// forcedKey keys, in a request's context, the number of stages the hook
// failed in it (*atomic.Int32).
type forcedKey struct{}

// qualityCondition is a condition of the evaluation: the shards searched
// and the stage the hook fails, with the reason a response then reports.
type qualityCondition struct {
	name   string
	shards Shards
	stage  string // "" for none
	shard  int    // with stageVector, the shard whose search fails
	reason string
}

// qualityConditions returns the pipeline's conditions: healthy, each
// stage forced to fail on its own, and HNSW.
func qualityConditions(exact, hnsw Shards) []qualityCondition {
	return []qualityCondition{
		{name: "healthy", shards: exact},
		{name: "encoder fails", shards: exact, stage: stageEncoder, reason: EncoderTimeout},
		{name: "co-occurrence fails", shards: exact, stage: stageCooc, reason: CoocTimeout},
		{name: "ranker fails", shards: exact, stage: stageRanker, reason: RankerTimeout},
		{name: "shard 1 of 2 fails", shards: exact, stage: stageVector, shard: 1, reason: ShardsPartial},
		{name: fmt.Sprintf("HNSW, ef %d", hnsw.Config().Ef), shards: hnsw},
	}
}

// hook returns the stall hook that fails c's stage, for vector retrieval
// only c.shard's search, and counts each failure in the request's context;
// nil if c fails none.
func (c qualityCondition) hook() func(context.Context, string, int) error {
	if c.stage == "" {
		return nil
	}
	return func(ctx context.Context, stage string, i int) error {
		if stage != c.stage || (stage == stageVector && i != c.shard) {
			return nil
		}
		ctx.Value(forcedKey{}).(*atomic.Int32).Add(1)
		return errForced
	}
}

// runCondition sends one request per event through p under c's hook, k =
// qualityK at the event's time, GOMAXPROCS at a time, and returns in event
// order the rank of each event's item in its response, and the number of
// requests in which the hook failed a stage. It fails unless every
// response reports c's reason exactly when the hook failed a stage in its
// request, and no other, and if c fails a stage in no request.
func runCondition(p *Pipeline, c qualityCondition, events []dataset.Row) ([]int, int, error) {
	p.stall = c.hook()
	ranks := make([]int, len(events))
	degraded := make([][]string, len(events))
	forced := make([]int32, len(events))
	g, ctx := errgroup.WithContext(context.Background())
	g.SetLimit(runtime.GOMAXPROCS(0))
	for i, e := range events {
		g.Go(func() error {
			var n atomic.Int32
			resp, err := p.Recommend(context.WithValue(ctx, forcedKey{}, &n), Request{User: e.User, K: qualityK, TS: e.TS})
			if err != nil {
				return fmt.Errorf("event %d: %w", i, err)
			}
			ids := make([]string, len(resp.Items))
			for j, it := range resp.Items {
				ids[j] = it.ID
			}
			ranks[i], degraded[i], forced[i] = rankOf(ids, e.Item), resp.Degraded, n.Load()
			return nil
		})
	}
	if err := g.Wait(); err != nil {
		return nil, 0, err
	}
	var nForced, bad int
	var first string
	for i := range events {
		var want []string
		if forced[i] > 0 {
			nForced++
			want = []string{c.reason}
		}
		if !slices.Equal(degraded[i], want) {
			if bad == 0 {
				first = fmt.Sprintf("event %d, failed %d times, degraded %v", i, forced[i], degraded[i])
			}
			bad++
		}
	}
	if bad > 0 {
		return nil, 0, fmt.Errorf("%d of %d responses report other than %q exactly when forced; first %s", bad, len(events), c.reason, first)
	}
	if c.stage != "" && nForced == 0 {
		return nil, 0, fmt.Errorf("the hook failed %s in no request", c.stage)
	}
	return ranks, nForced, nil
}

// referenceRanks returns, per event, the rank of its item in exactTop of
// the bundle's user vector of the history before the event, GOMAXPROCS
// events at a time, and the number of events whose history gives no user
// vector, whose reference is empty.
func referenceRanks(b *bundle.Bundle, h history.Store, events []dataset.Row) ([]int, int, error) {
	ranks := make([]int, len(events))
	var none atomic.Int64
	g, ctx := errgroup.WithContext(context.Background())
	g.SetLimit(runtime.GOMAXPROCS(0))
	for i, e := range events {
		g.Go(func() error {
			raw, err := h.Before(ctx, e.User, e.TS, 0)
			if err != nil {
				return err
			}
			hist := internalIDs(b, raw)
			uv, err := b.UserVector(ctx, hist)
			if err != nil {
				return fmt.Errorf("event %d: %w", i, err)
			}
			if uv == nil {
				none.Add(1)
			}
			seen := make(map[int32]bool, len(hist))
			for _, id := range hist {
				seen[id] = true
			}
			if id, ok := b.Lookup(e.Item); ok {
				ranks[i] = rankOf(exactTop(b.Vectors, uv, seen, qualityK), id)
			}
			return nil
		})
	}
	err := g.Wait()
	return ranks, int(none.Load()), err
}

// exactTop returns the IDs of the k rows of m from 1 up (row 0 is padding)
// with the highest inner product with uv, best first, ties to the lower
// ID, leaving out the IDs in seen; nil if uv is.
func exactTop(m vec.Matrix, uv []float32, seen map[int32]bool, k int) []int32 {
	if uv == nil {
		return nil
	}
	t := vec.NewTop(k)
	for id := int32(1); int(id) < m.Rows(); id++ {
		if !seen[id] {
			t.Push(vec.Hit{ID: id, Score: vec.Dot(uv, m.Row(id))})
		}
	}
	hits := t.Hits()
	ids := make([]int32, len(hits))
	for i, h := range hits {
		ids[i] = h.ID
	}
	return ids
}

// rankOf returns the position of x in list, from 1; 0 if absent.
func rankOf[T comparable](list []T, x T) int {
	return slices.Index(list, x) + 1
}

// recallAtRank is Recall@k of one held-out item at rank (from 1, 0 if
// absent): 1 if it is in the top k.
func recallAtRank(rank, k int) float64 {
	if rank >= 1 && rank <= k {
		return 1
	}
	return 0
}

// ndcgAtRank is NDCG@k of one held-out item at rank (from 1, 0 if absent):
// 1/log2(rank+1) in the top k, 0 otherwise.
func ndcgAtRank(rank, k int) float64 {
	if rank >= 1 && rank <= k {
		return 1 / math.Log2(float64(rank+1))
	}
	return 0
}

// qualityMetrics are the metrics of the evaluation, of one event's rank.
var qualityMetrics = []struct {
	name string
	of   func(rank int) float64
}{
	{"Recall@10", func(r int) float64 { return recallAtRank(r, 10) }},
	{"Recall@20", func(r int) float64 { return recallAtRank(r, 20) }},
	{"NDCG@10", func(r int) float64 { return ndcgAtRank(r, 10) }},
}

// pairedBootstrap returns the mean of a[i] - b[i] and the 95% percentile
// interval of that mean over resamples resamples of the indices, drawn with
// replacement by a PCG seeded with seed; the percentiles are nearest-rank
// (bench.Percentile).
func pairedBootstrap(a, b []float64, resamples int, seed uint64) (mean, lo, hi float64) {
	d := make([]float64, len(a))
	for i := range d {
		d[i] = a[i] - b[i]
		mean += d[i]
	}
	n := float64(len(d))
	mean /= n
	r := rand.New(rand.NewPCG(seed, 0))
	means := make([]float64, resamples)
	for j := range means {
		var s float64
		for range d {
			s += d[r.IntN(len(d))]
		}
		means[j] = s / n
	}
	slices.Sort(means)
	return mean, bench.Percentile(means, 0.025), bench.Percentile(means, 0.975)
}

// qualityRow is a condition's rank of each event's item.
type qualityRow struct {
	name  string
	ranks []int
}

// qualityReport returns the bundle version, the number of events and of
// requests without a user vector, then a markdown table of each row's
// metrics and, for every row but rows[base], its difference from rows[base]
// with the paired bootstrap's 95% interval.
func qualityReport(version string, events, noVector int, rows []qualityRow, base int) string {
	values := func(ranks []int, of func(int) float64) []float64 {
		out := make([]float64, len(ranks))
		for i, r := range ranks {
			out[i] = of(r)
		}
		return out
	}
	var sb strings.Builder
	fmt.Fprintf(&sb, "- bundle: %s\n- test events: %d, one request each\n- requests whose history gives no user vector: %d\n\n", version, events, noVector)
	sb.WriteString("| Condition |")
	for _, m := range qualityMetrics {
		fmt.Fprintf(&sb, " %s |", m.name)
	}
	for _, m := range qualityMetrics {
		fmt.Fprintf(&sb, " Δ %s [95%% CI] |", m.name)
	}
	sb.WriteString("\n|---|" + strings.Repeat("---:|", 2*len(qualityMetrics)) + "\n")
	for i, row := range rows {
		fmt.Fprintf(&sb, "| %s |", row.name)
		var diffs strings.Builder
		for _, m := range qualityMetrics {
			v := values(row.ranks, m.of)
			fmt.Fprintf(&sb, " %.4f |", meanOf(v))
			if i == base {
				diffs.WriteString(" |")
				continue
			}
			d, lo, hi := pairedBootstrap(v, values(rows[base].ranks, m.of), bootstrapSamples, bootstrapSeed)
			fmt.Fprintf(&diffs, " %+.4f [%+.4f, %+.4f] |", d, lo, hi)
		}
		sb.WriteString(diffs.String() + "\n")
	}
	return sb.String()
}

func meanOf(v []float64) float64 {
	var s float64
	for _, x := range v {
		s += x
	}
	return s / float64(len(v))
}

// TestRealDataQuality evaluates recommendation quality as docs/BENCHMARKS.md
// fixes it (Recommendation quality), on the real data and bundle (see
// TestRealDataSmoke) with the test split preloaded as well: one request per
// test event through the pipeline in process, every budget off, under each
// condition of qualityConditions, and the reference. It fails if a
// condition is not valid. The table goes to the file RECSERVE_QUALITY_OUT
// names, if set, and to the log.
func TestRealDataQuality(t *testing.T) {
	rd := loadRealData(t)
	evs := make([]history.Event, len(rd.test))
	for i, r := range rd.test {
		evs[i] = history.Event{ID: history.EventID(r.User, r.Item, r.TS, dataset.Kind), User: r.User, Item: r.Item, TS: r.TS}
	}
	if err := rd.store.Append(context.Background(), evs); err != nil {
		t.Fatal(err)
	}
	cfg := shard.DefaultConfig
	cfg.Shards, cfg.Mode = 2, shard.Exact
	exact, err := shard.Build(rd.bundle.Vectors, cfg)
	if err != nil {
		t.Fatal(err)
	}
	cfg.Mode = shard.HNSW
	hnsw, err := shard.Build(rd.bundle.Vectors, cfg)
	if err != nil {
		t.Fatal(err)
	}

	start := time.Now()
	ref, noVector, err := referenceRanks(rd.bundle, rd.store, rd.test)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("reference: %v", time.Since(start).Round(time.Second))
	rows := []qualityRow{{"reference", ref}}
	for _, c := range qualityConditions(exact, hnsw) {
		start := time.Now()
		ranks, forced, err := runCondition(New(rd.bundle, rd.store, c.shards), c, rd.test)
		if err != nil {
			t.Errorf("%s: not valid: %v", c.name, err)
			continue
		}
		t.Logf("%s: the hook failed a stage in %d of %d requests; %v", c.name, forced, len(rd.test), time.Since(start).Round(time.Second))
		rows = append(rows, qualityRow{c.name, ranks})
	}
	if t.Failed() {
		t.FailNow()
	}
	report := qualityReport(rd.bundle.ModelVersion, len(rd.test), noVector, rows, 1)
	t.Logf("\n%s", report)
	if out := os.Getenv("RECSERVE_QUALITY_OUT"); out != "" {
		if err := os.WriteFile(out, []byte(report), 0o644); err != nil {
			t.Fatal(err)
		}
	}
}

// The conditions on a random pipeline, ranked in Go, whose users u20 to
// u24 have no history: each is valid, and shard 1's search fails only in
// the requests with a user vector.
func TestQualityConditions(t *testing.T) {
	r := rand.New(rand.NewPCG(14, 14))
	p, err := randomPipeline(r, 60, 20)
	if err != nil {
		t.Fatal(err)
	}
	cfg := shard.DefaultConfig
	cfg.Mode = shard.HNSW
	hnsw, err := shard.Build(p.Bundle.Vectors, cfg)
	if err != nil {
		t.Fatal(err)
	}
	events := make([]dataset.Row, 500)
	for i := range events {
		events[i] = dataset.Row{User: fmt.Sprint("u", r.IntN(25)), Item: fmt.Sprint("i", 1+r.IntN(62)), TS: int64(r.IntN(100))}
	}
	var rows []qualityRow
	for _, c := range qualityConditions(p.Shards, hnsw) {
		q := New(p.Bundle, p.History, c.shards)
		q.rankWith = linearRanker
		ranks, forced, err := runCondition(q, c, events)
		if err != nil {
			t.Fatalf("%s: %v", c.name, err)
		}
		if c.stage == stageVector && forced == len(events) {
			t.Errorf("%s: failed in every request", c.name)
		}
		rows = append(rows, qualityRow{c.name, ranks})
	}
	t.Logf("\n%s", qualityReport("t", len(events), 0, rows, 0))
}

func TestQualityRecallNDCG(t *testing.T) {
	if r := rankOf([]string{"a", "b", "c"}, "c"); r != 3 {
		t.Errorf("rank of c = %d, want 3", r)
	}
	if r := rankOf([]int32{4, 5}, 6); r != 0 {
		t.Errorf("rank of an absent item = %d, want 0", r)
	}
	for _, c := range []struct {
		rank           int
		r10, r20, ndcg float64
	}{
		{0, 0, 0, 0},
		{1, 1, 1, 1},
		{3, 1, 1, 0.5},
		{7, 1, 1, 1.0 / 3},
		{10, 1, 1, 1 / math.Log2(11)},
		{11, 0, 1, 0},
		{20, 0, 1, 0},
		{21, 0, 0, 0},
	} {
		got := []float64{recallAtRank(c.rank, 10), recallAtRank(c.rank, 20), ndcgAtRank(c.rank, 10)}
		for i, want := range []float64{c.r10, c.r20, c.ndcg} {
			if math.Abs(got[i]-want) > 1e-12 {
				t.Errorf("rank %d: %s = %v, want %v", c.rank, qualityMetrics[i].name, got[i], want)
			}
		}
	}
}

// The reference leaves out the seen items and breaks ties to the lower ID.
func TestQualityReferenceTop(t *testing.T) {
	m, err := vec.NewMatrix(2, []float32{9, 9, 2, 0, 3, 1, 2, 5, 0, 0, -1, 0})
	if err != nil {
		t.Fatal(err)
	}
	if got, want := exactTop(m, []float32{1, 0}, map[int32]bool{2: true}, 3), []int32{1, 3, 4}; !slices.Equal(got, want) {
		t.Errorf("top 3 = %v, want %v", got, want)
	}
	if got := exactTop(m, nil, nil, 3); got != nil {
		t.Errorf("top 3 without a user vector = %v, want none", got)
	}
}

// Hits of 25% against 20% of 4,000 events, a difference of 0.05 whose
// per-event standard deviation is sqrt(0.3475): the interval holds it,
// excludes 0, has about the normal interval's width, and depends on the
// seed alone.
func TestPairedBootstrap(t *testing.T) {
	const n = 4000
	a, b := make([]float64, n), make([]float64, n)
	for i := range n {
		if i%4 == 0 {
			a[i] = 1
		}
		if i%5 == 0 {
			b[i] = 1
		}
	}
	d, lo, hi := pairedBootstrap(a, b, bootstrapSamples, bootstrapSeed)
	if math.Abs(d-0.05) > 1e-12 {
		t.Errorf("mean difference %v, want 0.05", d)
	}
	if !(0 < lo && lo < 0.05 && 0.05 < hi) {
		t.Errorf("interval [%v, %v], want it to hold 0.05 and not 0", lo, hi)
	}
	normal := 2 * 1.96 * math.Sqrt(0.3475/n)
	if w := hi - lo; math.Abs(w/normal-1) > 0.15 {
		t.Errorf("interval width %v, normal interval's %v", w, normal)
	}
	if d2, lo2, hi2 := pairedBootstrap(a, b, bootstrapSamples, bootstrapSeed); d2 != d || lo2 != lo || hi2 != hi {
		t.Errorf("same seed: %v [%v, %v], then %v [%v, %v]", d, lo, hi, d2, lo2, hi2)
	}
	if _, lo2, hi2 := pairedBootstrap(a, b, bootstrapSamples, bootstrapSeed+1); lo2 == lo && hi2 == hi {
		t.Errorf("seeds 1 and 2 give the same interval [%v, %v]", lo, hi)
	}
	if d, lo, hi := pairedBootstrap(a, a, bootstrapSamples, bootstrapSeed); d != 0 || lo != 0 || hi != 0 {
		t.Errorf("no difference: %v [%v, %v], want 0 [0, 0]", d, lo, hi)
	}
}
