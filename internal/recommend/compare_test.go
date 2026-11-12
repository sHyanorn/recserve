package recommend

import (
	"context"
	"fmt"
	"os"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/sHyanorn/recserve/internal/bundle"
	"github.com/sHyanorn/recserve/internal/dataset"
	"github.com/sHyanorn/recserve/internal/history"
	"github.com/sHyanorn/recserve/internal/shard"
	"github.com/sHyanorn/recserve/internal/vec"
)

// eventSubset is a subset of the test events: keep[i] says whether event i
// is in it.
type eventSubset struct {
	name string
	keep []bool
}

// comparisonSubsets returns the subsets of the secondary readings of
// docs/BENCHMARKS.md (seqrec-lab's bundle against the baseline): the events
// whose target is in b's items, those whose target has a non-zero vector in
// base, and those in both. A target is looked up by its raw ID in each
// bundle.
func comparisonSubsets(base, b *bundle.Bundle, events []dataset.Row) []eventSubset {
	inB, inBase, both := make([]bool, len(events)), make([]bool, len(events)), make([]bool, len(events))
	for i, e := range events {
		_, inB[i] = b.Lookup(e.Item)
		if id, ok := base.Lookup(e.Item); ok {
			inBase[i] = slices.ContainsFunc(base.Vectors.Row(id), func(x float32) bool { return x != 0 })
		}
		both[i] = inB[i] && inBase[i]
	}
	return []eventSubset{
		{"target in bundle B's items", inB},
		{"target with a non-zero vector in the baseline", inBase},
		{"both", both},
	}
}

// bundleRef is a bundle's reference row: its version, the rank of each
// event's item, and the number of requests without a user vector.
type bundleRef struct {
	version  string
	ranks    []int
	noVector int
}

// comparisonReport returns the comparison of b's reference row with base's
// (docs/BENCHMARKS.md, seqrec-lab's bundle against the baseline): the
// primary result on all events, b minus base for each metric with the
// paired bootstrap's 95% interval over events, then the same on each
// subset, then healthy, b's healthy pipeline row, described only.
func comparisonReport(base, b bundleRef, subsets []eventSubset, healthy []int) string {
	var sb strings.Builder
	n := len(base.ranks)
	fmt.Fprintf(&sb, "- baseline bundle: %s\n- bundle B: %s\n- test events: %d, one request each\n", base.version, b.version, n)
	fmt.Fprintf(&sb, "- requests whose history gives no user vector: baseline %d, bundle B %d\n\n", base.noVector, b.noVector)
	all := make([]bool, n)
	for i := range all {
		all[i] = true
	}
	sb.WriteString("## Primary result\n\nReference rows on all test events; Δ is bundle B minus the baseline. A difference whose interval excludes 0 is a difference, otherwise none is shown.\n\n")
	comparisonTable(&sb, base, b, all, true)
	sb.WriteString("\n## Secondary readings\n\nNot used to qualify the primary result.\n")
	for _, s := range subsets {
		fmt.Fprintf(&sb, "\n### %s\n\n", s.name)
		comparisonTable(&sb, base, b, s.keep, false)
	}
	sb.WriteString("\n## Bundle B's healthy pipeline row\n\nDescribed only, not compared.\n\n|")
	for _, m := range qualityMetrics {
		fmt.Fprintf(&sb, " %s |", m.name)
	}
	sb.WriteString("\n|" + strings.Repeat("---:|", len(qualityMetrics)) + "\n|")
	for _, m := range qualityMetrics {
		fmt.Fprintf(&sb, " %.4f |", meanOf(metricValues(healthy, all, m.of)))
	}
	sb.WriteString("\n")
	return sb.String()
}

// comparisonTable writes the events kept by keep, their count, each
// bundle's metrics on them and the differences; reading adds whether each
// interval excludes 0.
func comparisonTable(sb *strings.Builder, base, b bundleRef, keep []bool, reading bool) {
	var events int
	for _, k := range keep {
		if k {
			events++
		}
	}
	fmt.Fprintf(sb, "Events: %d\n", events)
	if events == 0 {
		return
	}
	sb.WriteString("\n| Row |")
	for _, m := range qualityMetrics {
		fmt.Fprintf(sb, " %s |", m.name)
	}
	sb.WriteString("\n|---|" + strings.Repeat("---:|", len(qualityMetrics)) + "\n")
	for _, r := range []struct {
		name string
		ref  bundleRef
	}{{"baseline reference", base}, {"bundle B reference", b}} {
		fmt.Fprintf(sb, "| %s |", r.name)
		for _, m := range qualityMetrics {
			fmt.Fprintf(sb, " %.4f |", meanOf(metricValues(r.ref.ranks, keep, m.of)))
		}
		sb.WriteString("\n")
	}
	sb.WriteString("| Δ [95% CI] |")
	var readings []string
	for _, m := range qualityMetrics {
		d, lo, hi := pairedBootstrap(metricValues(b.ranks, keep, m.of), metricValues(base.ranks, keep, m.of), bootstrapSamples, bootstrapSeed)
		fmt.Fprintf(sb, " %+.4f [%+.4f, %+.4f] |", d, lo, hi)
		r := "none shown"
		if lo > 0 || hi < 0 {
			r = "difference"
		}
		readings = append(readings, fmt.Sprintf("%s: %s", m.name, r))
	}
	sb.WriteString("\n")
	if reading {
		fmt.Fprintf(sb, "\n%s.\n", strings.Join(readings, "; "))
	}
}

// metricValues returns of(rank) of the events kept by keep, in event order.
func metricValues(ranks []int, keep []bool, of func(int) float64) []float64 {
	var out []float64
	for i, r := range ranks {
		if keep[i] {
			out = append(out, of(r))
		}
	}
	return out
}

// compareRealBundles compares the reference row of the bundle in dir, B,
// with base's, ranks and noVector, on rd's test events with the test split
// preloaded (docs/BENCHMARKS.md, seqrec-lab's bundle against the baseline),
// and runs B's healthy pipeline. Loaded twice, the same bundle must give
// the same ranks. The report goes to the file RECSERVE_COMPARE_OUT names, if
// set, and to the log.
func compareRealBundles(t *testing.T, rd realData, dir string, ranks []int, noVector int) {
	b, err := bundle.Load(dir, bundle.Options{FirstTestTS: rd.first})
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := b.Close(); err != nil {
			t.Error(err)
		}
	}()
	start := time.Now()
	refB, noVectorB, err := referenceRanks(b, rd.store, rd.test)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("bundle B reference: %v", time.Since(start).Round(time.Second))
	if b.ModelVersion == rd.bundle.ModelVersion && (!slices.Equal(refB, ranks) || noVectorB != noVector) {
		t.Fatalf("bundle B is the baseline, %s, and its reference differs", b.ModelVersion)
	}
	cfg := shard.DefaultConfig
	cfg.Shards, cfg.Mode = 2, shard.Exact
	exact, err := shard.Build(b.Vectors, cfg)
	if err != nil {
		t.Fatal(err)
	}
	start = time.Now()
	healthy, _, err := runCondition(New(b, rd.store, exact), qualityCondition{name: "healthy", shards: exact}, rd.test)
	if err != nil {
		t.Fatalf("bundle B, healthy: not valid: %v", err)
	}
	t.Logf("bundle B healthy: %v", time.Since(start).Round(time.Second))
	report := comparisonReport(
		bundleRef{rd.bundle.ModelVersion, ranks, noVector},
		bundleRef{b.ModelVersion, refB, noVectorB},
		comparisonSubsets(rd.bundle, b, rd.test), healthy)
	t.Logf("\n%s", report)
	if out := os.Getenv("RECSERVE_COMPARE_OUT"); out != "" {
		if err := os.WriteFile(out, []byte(report), 0o644); err != nil {
			t.Fatal(err)
		}
	}
}

// comparisonBundles returns two bundles with the mean encoder whose items
// overlap in other internal IDs, a history store and test events at ts 10.
// The baseline holds a to d and z, whose vector is zero; B holds d, c, x
// and b, in that order. u1 has seen b, u2 a (not in B), u3 x (not in the
// baseline).
func comparisonBundles(t *testing.T) (base, b *bundle.Bundle, h history.Store, events []dataset.Row) {
	t.Helper()
	matrix := func(data ...float32) vec.Matrix {
		m, err := vec.NewMatrix(2, data)
		if err != nil {
			t.Fatal(err)
		}
		return m
	}
	base = bundle.New(bundle.Manifest{ModelVersion: "base", ItemCount: 5, Dim: 2, MaxSeqLen: 5},
		[]string{"", "a", "b", "c", "d", "z"}, matrix(0, 0, 1, 0, 0, 1, 1, 1, 2, 0, 0, 0), nil, nil)
	b = bundle.New(bundle.Manifest{ModelVersion: "b", ItemCount: 4, Dim: 2, MaxSeqLen: 5},
		[]string{"", "d", "c", "x", "b"}, matrix(0, 0, 0, 1, 1, 0, 1, 2, 2, 0), nil, nil)
	h = history.NewMemory()
	var evs []history.Event
	for _, e := range []dataset.Row{{User: "u1", Item: "b", TS: 1}, {User: "u2", Item: "a", TS: 1}, {User: "u3", Item: "x", TS: 1}} {
		evs = append(evs, history.Event{ID: history.EventID(e.User, e.Item, e.TS, dataset.Kind), User: e.User, Item: e.Item, TS: e.TS})
	}
	if err := h.Append(context.Background(), evs); err != nil {
		t.Fatal(err)
	}
	for _, e := range []struct{ user, item string }{{"u1", "c"}, {"u1", "x"}, {"u2", "d"}, {"u3", "c"}, {"u1", "z"}, {"u1", "a"}} {
		events = append(events, dataset.Row{User: e.user, Item: e.item, TS: 10})
	}
	return base, b, h, events
}

// Each bundle ranks the targets by their raw IDs: c is 3 in the baseline
// and 2 in B, where 3 is x. A target outside a bundle's items is a miss,
// and so is every target of a request without a user vector: u3's under
// the baseline, u2's under B.
func TestComparisonRawIDs(t *testing.T) {
	base, b, h, events := comparisonBundles(t)
	for _, c := range []struct {
		b        *bundle.Bundle
		ranks    []int
		noVector int
	}{
		// u1's vector (0, 1) ranks c, a, d, z; u2's (1, 0) d, c, b, z.
		{base, []int{1, 0, 1, 0, 4, 2}, 1},
		// u1's vector (2, 0) ranks c, x, d; u3's (1, 2) d, b, c.
		{b, []int{1, 2, 0, 3, 0, 0}, 1},
	} {
		ranks, noVector, err := referenceRanks(c.b, h, events)
		if err != nil {
			t.Fatal(err)
		}
		if !slices.Equal(ranks, c.ranks) || noVector != c.noVector {
			t.Errorf("%s: ranks %v, %d without a user vector; want %v, %d", c.b.ModelVersion, ranks, noVector, c.ranks, c.noVector)
		}
	}
	want := map[string][]bool{
		"target in bundle B's items":                    {true, true, true, true, false, false},
		"target with a non-zero vector in the baseline": {true, false, true, true, false, true},
		"both": {true, false, true, true, false, false},
	}
	subsets := comparisonSubsets(base, b, events)
	if len(subsets) != len(want) {
		t.Fatalf("%d subsets, want %d", len(subsets), len(want))
	}
	for _, s := range subsets {
		if !slices.Equal(s.keep, want[s.name]) {
			t.Errorf("%s: %v, want %v", s.name, s.keep, want[s.name])
		}
	}
}

// The report's differences are B minus the baseline, on all events and on
// each subset alone; the same bundle on both sides differs by 0 [0, 0]; an
// empty subset is reported as such.
func TestComparisonReport(t *testing.T) {
	base, b, h, events := comparisonBundles(t)
	refBase, noBase, err := referenceRanks(base, h, events)
	if err != nil {
		t.Fatal(err)
	}
	refB, noB, err := referenceRanks(b, h, events)
	if err != nil {
		t.Fatal(err)
	}
	healthy := []int{1, 1, 1, 1, 1, 1}
	report := comparisonReport(bundleRef{"base", refBase, noBase}, bundleRef{"b", refB, noB}, comparisonSubsets(base, b, events), healthy)
	t.Logf("\n%s", report)
	// Recall@10: B 3/6, the baseline 4/6 on all events; on the 4 targets
	// in B, 3/4 against 2/4; on the 4 with a non-zero vector in the
	// baseline, 2/4 against 3/4; on the 3 in both, 2/3 against 2/3.
	for _, s := range []string{
		"- requests whose history gives no user vector: baseline 1, bundle B 1\n",
		"## Primary result\n\nReference rows on all test events; Δ is bundle B minus the baseline. A difference whose interval excludes 0 is a difference, otherwise none is shown.\n\nEvents: 6\n",
		"| baseline reference | 0.6667 | 0.6667 |",
		"| bundle B reference | 0.5000 | 0.5000 |",
		"| Δ [95% CI] | -0.1667 [",
		"### target in bundle B's items\n\nEvents: 4\n\n| Row | Recall@10 | Recall@20 | NDCG@10 |\n|---|---:|---:|---:|\n| baseline reference | 0.5000 | 0.5000 |",
		"| bundle B reference | 0.7500 | 0.7500 |",
		"| Δ [95% CI] | +0.2500 [",
		"### target with a non-zero vector in the baseline\n\nEvents: 4\n\n| Row | Recall@10 | Recall@20 | NDCG@10 |\n|---|---:|---:|---:|\n| baseline reference | 0.7500 | 0.7500 |",
		"| Δ [95% CI] | -0.2500 [",
		"### both\n\nEvents: 3\n\n| Row | Recall@10 | Recall@20 | NDCG@10 |\n|---|---:|---:|---:|\n| baseline reference | 0.6667 | 0.6667 |",
		"| Δ [95% CI] | +0.0000 [",
		"| 1.0000 | 1.0000 | 1.0000 |\n",
	} {
		if !strings.Contains(report, s) {
			t.Errorf("report lacks %q", s)
		}
	}

	same := comparisonReport(bundleRef{"base", refBase, noBase}, bundleRef{"base", refBase, noBase}, comparisonSubsets(base, base, events), healthy)
	if got, want := strings.Count(same, "+0.0000 [+0.0000, +0.0000]"), 4*len(qualityMetrics); got != want {
		t.Errorf("same bundle: %d differences of 0 [0, 0], want %d\n%s", got, want, same)
	}
	if !strings.Contains(same, "Recall@10: none shown; Recall@20: none shown; NDCG@10: none shown.") {
		t.Errorf("same bundle: a difference is shown\n%s", same)
	}

	empty := comparisonReport(bundleRef{"base", refBase, noBase}, bundleRef{"b", refB, noB},
		[]eventSubset{{"none", make([]bool, len(events))}}, healthy)
	if !strings.Contains(empty, "### none\n\nEvents: 0\n\n## Bundle B's healthy") {
		t.Errorf("empty subset:\n%s", empty)
	}
}
