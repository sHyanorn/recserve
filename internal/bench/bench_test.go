package bench

import (
	"encoding/binary"
	"math"
	"math/rand/v2"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"
)

func TestPercentile(t *testing.T) {
	v := make([]int64, 10_000) // 1 to 10,000
	for i := range v {
		v[i] = int64(i + 1)
	}
	for _, c := range []struct {
		p    float64
		want int64
	}{{0, 1}, {.5, 5000}, {.9, 9000}, {.99, 9900}, {.999, 9990}, {.9999, 9999}, {1, 10_000}, {99.9 / 100, 9990}} {
		if got := Percentile(v, c.p); got != c.want {
			t.Errorf("p=%v: got %d, want %d", c.p, got, c.want)
		}
	}
	if got := Percentile([]int64{7}, .99); got != 7 {
		t.Errorf("one sample: got %d", got)
	}
	if got := Percentile[int64](nil, .5); got != 0 {
		t.Errorf("no samples: got %d", got)
	}
}

func TestSummarize(t *testing.T) {
	d := make([]time.Duration, 1000)
	for i := range d {
		d[i] = time.Duration(i+1) * time.Microsecond
	}
	rand.New(rand.NewPCG(1, 2)).Shuffle(len(d), func(i, j int) { d[i], d[j] = d[j], d[i] })
	if got, want := Summarize(d), (Summary{N: 1000, P50: 500, P90: 900, P99: 990, P999: 999, Max: 1000}); got != want {
		t.Fatalf("got %+v, want %+v", got, want)
	}
	if got := Summarize(nil); got != (Summary{}) {
		t.Fatalf("no samples: %+v", got)
	}
}

func TestMedianAndNoisy(t *testing.T) {
	if got := Median([]float64{3, 1, 2}); got != 2 {
		t.Errorf("odd: %v", got)
	}
	if got := Median([]float64{4, 1, 3, 2}); got != 2.5 {
		t.Errorf("even: %v, want the mean of the middle two", got)
	}
	if !Noisy(111, 100) || Noisy(110, 100) || Noisy(1e9, 0) {
		t.Error("Noisy flags above 10% only, and never without a reference")
	}
}

func TestBracketed(t *testing.T) {
	baselines := func(v ...float64) func() float64 {
		return func() float64 { x := v[0]; v = v[1:]; return x }
	}
	runs := 0
	fn := func() int { runs++; return runs }
	// Quiet at once: one attempt.
	got, n := Bracketed(100, baselines(100, 105), fn)
	if got != 1 || n != (Noise{Reference: 100, Before: 100, After: 105, Attempt: 1}) {
		t.Fatalf("quiet: run %d, %+v", got, n)
	}
	// Noisy after the first run, quiet before and after the second.
	runs = 0
	got, n = Bracketed(100, baselines(100, 120, 101, 102), fn)
	if got != 2 || n.Attempt != 2 || n.Flagged || n.Before != 101 {
		t.Fatalf("second attempt: run %d, %+v", got, n)
	}
	// Noisy every time: the last attempt, flagged.
	runs = 0
	got, n = Bracketed(100, baselines(200, 100, 200, 100, 100, 200), fn)
	if got != NoiseAttempts || n.Attempt != NoiseAttempts || !n.Flagged || n.After != 200 {
		t.Fatalf("always noisy: run %d, %+v", got, n)
	}
}

func TestGCBetween(t *testing.T) {
	buckets := []float64{math.Inf(-1), 0, 1e-3, 2e-3, math.Inf(1)}
	a := GCSnap{counts: []uint64{0, 5, 1, 0}, buckets: buckets, cycles: 10}
	b := GCSnap{counts: []uint64{0, 7, 1, 0}, buckets: buckets, cycles: 12}
	if got, want := GCBetween(a, b), (GCStats{Cycles: 2, Pauses: 2, MaxPauseLower: 0, MaxPauseUpper: 1}); got != want {
		t.Fatalf("got %+v, want %+v", got, want)
	}
	b.counts[3] = 1 // a pause above the last finite boundary
	if got := GCBetween(a, b); got.MaxPauseLower != 2 || got.MaxPauseUpper != 2 || got.Pauses != 3 {
		t.Fatalf("open bucket: %+v", got)
	}
	if got := GCBetween(a, a); got != (GCStats{}) {
		t.Fatalf("no change: %+v", got)
	}
	// The runtime's own histogram: a forced collection pauses at least twice.
	s := ReadGC()
	runtime.GC()
	if got := GCBetween(s, ReadGC()); got.Cycles < 1 || got.Pauses < 2 || got.MaxPauseUpper <= 0 || got.MaxPauseUpper < got.MaxPauseLower {
		t.Fatalf("after runtime.GC: %+v", got)
	}
	if HeapObjectsMiB() <= 0 {
		t.Fatal("no heap objects")
	}
}

func writeFile(t *testing.T, path string, v any) {
	t.Helper()
	f, err := os.Create(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := binary.Write(f, binary.LittleEndian, v); err != nil {
		t.Fatal(err)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}
}

// writeSet writes a set of base rows (1, i) for i = 0..n-1 in 2 dimensions
// and one query (0, 1): row i's similarity is i, so the true neighbors are
// n-1, n-2, ... Rows 0 and 1 are both (1, 0) and tie.
func writeSet(t *testing.T, dir string, n int, truth []int32) {
	t.Helper()
	base := make([]float32, 0, 2*n)
	for i := range n {
		base = append(base, 1, float32(i))
	}
	base[3] = 0
	if err := os.WriteFile(filepath.Join(dir, "s.dim"), []byte("2\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	writeFile(t, filepath.Join(dir, "s_base.f32"), base)
	writeFile(t, filepath.Join(dir, "s_query.f32"), []float32{0, 1})
	writeFile(t, filepath.Join(dir, "s_gt.i32"), truth)
}

func descending(n int) []int32 {
	truth := make([]int32, TruthK)
	for r := range truth {
		truth[r] = int32(n - 1 - r)
	}
	return truth
}

func TestANNSet(t *testing.T) {
	const n = 150
	dir := t.TempDir()
	writeSet(t, dir, n, descending(n))
	s, err := LoadANNSet(dir, "s")
	if err != nil {
		t.Fatal(err)
	}
	if s.NBase() != n || s.NQuery() != 1 || s.Dim != 2 {
		t.Fatalf("loaded %d rows, %d queries, dim %d", s.NBase(), s.NQuery(), s.Dim)
	}
	if err := s.CheckTruth(0); err != nil {
		t.Fatal(err)
	}
	top10 := descending(n)[:10]
	for name, c := range map[string]struct {
		got  []int32
		want float64
	}{
		"exact":            {top10, 1},
		"one wrong":        {append(append([]int32{}, top10[:9]...), 3), 0.9},
		"repeated":         {append(append([]int32{}, top10[:9]...), top10[0]), 0.9},
		"padded with -1":   {append(append([]int32{}, top10[:9]...), -1), 0.9},
		"short":            {top10[:5], 0.5},
		"beyond k ignored": {append(append([]int32{}, top10[:9]...), 3, top10[9]), 0.9},
		"out of range":     {append(append([]int32{}, top10[:9]...), n), 0.9},
	} {
		if got := s.Recall(0, c.got, 10); got != c.want {
			t.Errorf("%s: recall %v, want %v", name, got, c.want)
		}
	}
	// A tie with the true k-th neighbor counts: rows 0 and 1 are both at
	// similarity 0. Make row 1 the 100th neighbor of a set of 101 rows.
	writeSet(t, dir, 101, descending(101))
	s, err = LoadANNSet(dir, "s")
	if err != nil {
		t.Fatal(err)
	}
	if got := s.Recall(0, append(descending(101)[:99], 0), TruthK); got != 1 {
		t.Errorf("tie with the 100th neighbor: recall %v, want 1", got)
	}
}

func TestCheckTruthCatchesWrongTruth(t *testing.T) {
	const n = 150
	swapped := descending(n)
	swapped[0], swapped[1] = swapped[1], swapped[0]
	repeated := descending(n)
	repeated[5] = repeated[4]
	shifted := descending(n)
	for r := range shifted {
		shifted[r]-- // misses row n-1, the best
	}
	for name, truth := range map[string][]int32{"swapped": swapped, "repeated": repeated, "missing the best": shifted} {
		dir := t.TempDir()
		writeSet(t, dir, n, truth)
		s, err := LoadANNSet(dir, "s")
		if err != nil {
			t.Fatal(err)
		}
		if err := s.CheckTruth(0); err == nil {
			t.Errorf("%s: accepted", name)
		}
	}
}

func TestLoadANNSetRejectsMismatches(t *testing.T) {
	for name, edit := range map[string]func(dir string){
		"truth for two queries": func(dir string) {
			writeFile(t, filepath.Join(dir, "s_gt.i32"), append(descending(150), descending(150)...))
		},
		"truth beyond the base": func(dir string) {
			writeFile(t, filepath.Join(dir, "s_gt.i32"), append(descending(150)[1:], 150))
		},
		"partial row": func(dir string) { writeFile(t, filepath.Join(dir, "s_query.f32"), []float32{0, 1, 2}) },
		"dimension":   func(dir string) { _ = os.WriteFile(filepath.Join(dir, "s.dim"), []byte("x"), 0o644) },
		"missing":     func(dir string) { _ = os.Remove(filepath.Join(dir, "s_base.f32")) },
	} {
		dir := t.TempDir()
		writeSet(t, dir, 150, descending(150))
		edit(dir)
		if _, err := LoadANNSet(dir, "s"); err == nil {
			t.Errorf("%s: accepted", name)
		}
	}
}

func TestEnv(t *testing.T) {
	e := Env(map[string]string{"set": "x"})
	for _, k := range []string{"go", "gomaxprocs", "numcpu", "date", "commit", "cpu", "cpu_flags", "memory", "kernel", "cpu_affinity"} {
		if _, ok := e[k]; !ok {
			t.Errorf("no %s", k)
		}
	}
	if e["set"] != "x" || !strings.HasSuffix(e["memory"], "kB") {
		t.Errorf("env %v", e)
	}
}
