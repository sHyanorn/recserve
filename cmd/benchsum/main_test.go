package main

import (
	"compress/gzip"
	"errors"
	"math"
	"os"
	"path/filepath"
	"slices"
	"testing"
)

// one answer per 10 ms from 0 to end ms, latency by due time.
func answers(end float64, latency func(due float64) float64, degraded func(due float64) []string) []sample {
	var out []sample
	for due := 0.0; due < end; due += 10 {
		out = append(out, sample{due: due, latency: latency(due), status: 200, degraded: degraded(due), answered: 2, shards: 2})
	}
	return out
}

func none(float64) []string { return nil }

func TestDescribe(t *testing.T) {
	s := []sample{
		{due: 0, latency: 1, status: 200, answered: 2, shards: 2},
		{due: 1, latency: 3, status: 200, degraded: []string{"shards_partial"}, answered: 1, shards: 2},
		{due: 2, latency: 2, status: 200, degraded: []string{"history_unavailable", "shards_partial"}, answered: -1, shards: -1},
		{due: 3, latency: 9, status: 503},
		{due: 4, latency: 7, status: 0},
		{events: true, due: 5, latency: 4, status: 202},
		{due: 50, latency: 100, status: 200}, // outside
	}
	st := describe(s, 0, 50)
	if st.Answers != 3 || st.P50 != 2 || st.Max != 3 || st.Errors != 2 || st.Busy != 1 || st.Events != 1 || st.EventP99 != 4 {
		t.Fatalf("%+v", st)
	}
	if st.Degraded != 2.0/3 || st.Reasons["shards_partial"] != 2.0/3 || st.Reasons["history_unavailable"] != 1.0/3 || st.Coverage != 0.75 {
		t.Fatalf("shares %+v", st)
	}
}

// Recovery is the first whole second after removal from which a 10 s
// window is back within 1.2 times the healthy p99.
func TestRecovery(t *testing.T) {
	s := answers(60_000, func(due float64) float64 {
		if due >= 30_000 && due < 34_000 {
			return 30
		}
		return 2
	}, none)
	healthy := describe(s, 0, 30_000)
	if got, ok := recovery(s, healthy, 30_000, 60_000); !ok || got != 4 {
		t.Fatalf("recovery %d %v, want 4 s", got, ok)
	}
	if _, ok := recovery(s, healthy, 30_000, 39_000); ok {
		t.Fatal("recovered with no 10 s window left")
	}
	slow := answers(60_000, func(due float64) float64 { return 2 + float64(min(due, 30_000))/10_000 }, none)
	if _, ok := recovery(slow, describe(slow, 0, 10_000), 30_000, 60_000); ok {
		t.Fatal("recovered while p99 stays at 2.5 times healthy")
	}
}

func TestJudge(t *testing.T) {
	ok := func(f faultRun) faultRun {
		f.Phases = map[string]stats{"healthy": {P99: 5}, "fault": {P99: 20}, "recovery": {P99: 5}}
		zero := 0
		f.Recovery = &zero
		return f
	}
	cpu := 3.0
	cases := []struct {
		name string
		f    faultRun
		pass bool
	}{
		{"F2 partial", ok(faultRun{Fault: "F2", Excess: map[string]float64{"shards_partial": 0.9}}), true},
		{"F2 other shard too", ok(faultRun{Fault: "F2", Excess: map[string]float64{"shards_partial": 0.9, "vector_timeout": 0.02}}), false},
		{"F3 history", ok(faultRun{Fault: "F3", Excess: map[string]float64{"history_unavailable": 1}}), true},
		{"F1 degraded", ok(faultRun{Fault: "F1", Excess: map[string]float64{"shards_partial": 0.05}, ReplicaCPU: &cpu}), false},
		{"F1 replica idle", ok(faultRun{Fault: "F1"}), false},
		{"F1", ok(faultRun{Fault: "F1", ReplicaCPU: &cpu}), true},
		{"F6 degraded is reported only", ok(faultRun{Fault: "F6", Excess: map[string]float64{"shards_partial": 0.4}}), true},
		{"F5 after keepalive", ok(faultRun{Fault: "F5", SilentExcess: map[string]float64{"shards_partial": 0.03}, ReplicaCPU: &cpu}), false},
	}
	for _, c := range cases {
		if got := judge(c.f); (len(got) == 0) != c.pass {
			t.Errorf("%s: failures %v, want pass %v", c.name, got, c.pass)
		}
	}
	errs := ok(faultRun{Fault: "F6"})
	errs.Phases["recovery"] = stats{Errors: 1}
	slow := ok(faultRun{Fault: "F6"})
	slow.Phases["fault"] = stats{P99: 51}
	late := ok(faultRun{Fault: "F6"})
	ten := 10
	late.Recovery = &ten
	for _, f := range []faultRun{errs, slow, late} {
		if got := judge(f); len(got) == 0 {
			t.Errorf("%+v passed", f.Phases)
		}
	}
}

func TestVerdict(t *testing.T) {
	r := func(p99 float64, valid, noisy bool) runSummary {
		return runSummary{Valid: valid, Noisy: noisy, stats: stats{P99: p99}}
	}
	cases := []struct {
		runs        []runSummary
		meets, stop bool
	}{
		{[]runSummary{r(10, true, false), r(50, true, false), r(12, true, false)}, true, false},
		{[]runSummary{r(10, true, false), r(51, true, false), r(12, true, false)}, false, false},
		{[]runSummary{r(10, true, true), r(11, true, false), r(12, true, false)}, false, false},
		{[]runSummary{r(10, false, false), r(11, true, false), r(12, true, false)}, false, true},
		{[]runSummary{r(90, true, false), r(101, true, false), r(120, true, false)}, false, true},
	}
	for i, c := range cases {
		if meets, stop, _ := verdict(c.runs); meets != c.meets || stop != c.stop {
			t.Errorf("case %d: meets %v stop %v", i, meets, stop)
		}
	}
}

// The quantile interpolates within the bucket that holds it, between two
// scrapes: here 100 calls, 60 within 1 ms and 40 within 2 ms after the
// first scrape's 10 within 1 ms.
func TestHistogramQuantile(t *testing.T) {
	dir := t.TempDir()
	write := func(name, body string) metrics {
		p := filepath.Join(dir, name)
		if err := os.WriteFile(p, []byte(body), 0o644); err != nil {
			t.Fatal(err)
		}
		m, err := readMetrics(p)
		if err != nil {
			t.Fatal(err)
		}
		return m
	}
	const head = "# TYPE h histogram\n"
	before := write("b", head+`h_bucket{shard="0",le="0.001"} 10
h_bucket{shard="0",le="0.002"} 10
h_bucket{shard="0",le="+Inf"} 10
h_sum{shard="0"} 0.005
h_count{shard="0"} 10
`)
	after := write("a", head+`h_bucket{shard="0",le="0.001"} 70
h_bucket{shard="0",le="0.002"} 110
h_bucket{shard="0",le="+Inf"} 110
h_sum{shard="0"} 0.1
h_count{shard="0"} 110
`)
	got := histogramQuantile(after, before, "h", map[string]string{"shard": "0"}, 0.95)
	if want := 0.001 + 0.001*(95.0-60)/40; math.Abs(got-want) > 1e-12 {
		t.Fatalf("p95 %g, want %g", got, want)
	}
	// A tenth of the calls past the highest finite bound: ranks count them,
	// and a quantile among them is that bound, as in Prometheus.
	slow := write("s", head+`h_bucket{shard="0",le="0.001"} 50
h_bucket{shard="0",le="0.002"} 90
h_bucket{shard="0",le="+Inf"} 100
h_sum{shard="0"} 1
h_count{shard="0"} 100
`)
	for q, want := range map[float64]float64{0.5: 0.001, 0.95: 0.002} {
		if got := histogramQuantile(slow, nil, "h", map[string]string{"shard": "0"}, q); math.Abs(got-want) > 1e-12 {
			t.Errorf("q %g: %g, want %g", q, got, want)
		}
	}
}

func TestSamplesRoundTrip(t *testing.T) {
	path := filepath.Join(t.TempDir(), "s.csv.gz")
	if err := writeGz(path, "i,path,due_ms,latency_ms,status,outcome,degraded,answered,shards\n"+
		"5,/v1/recommend,10.5,2.25,200,recommend_degraded,a|b,1,2\n6,/v1/events,11.000,3.000,202,events_ok,,-1,-1\n"); err != nil {
		t.Fatal(err)
	}
	s, err := readSamples(path)
	if err != nil {
		t.Fatal(err)
	}
	if len(s) != 2 || s[0].due != 10.5 || !slices.Equal(s[0].degraded, []string{"a", "b"}) || s[0].answered != 1 || !s[1].events || s[1].shards != -1 {
		t.Fatalf("%+v", s)
	}
}

func writeGz(path, body string) error {
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	z := gzip.NewWriter(f)
	if _, err := z.Write([]byte(body)); err != nil {
		return err
	}
	return errors.Join(z.Close(), f.Close())
}

func TestLower(t *testing.T) {
	for _, c := range []struct {
		before, after []float64
		want          string
	}{
		{[]float64{3, 4, 5}, []float64{1, 2, 2.9}, "lower after"},
		{[]float64{3, 4, 5}, []float64{1, 2, 3}, "not resolved on this machine"},
		{[]float64{1, 1, 1}, []float64{2, 3, 4}, "lower before"},
	} {
		if got := lower(c.before, c.after); got != c.want {
			t.Errorf("%v %v: %s, want %s", c.before, c.after, got, c.want)
		}
	}
	m := benchLine.FindStringSubmatch("BenchmarkDot/64-4   \t 51234567\t        23.45 ns/op")
	if m == nil || m[1] != "BenchmarkDot/64" || m[2] != "23.45" {
		t.Fatalf("parsed %q", m)
	}
}

func TestLagDrained(t *testing.T) {
	path := filepath.Join(t.TempDir(), "lag")
	// Removal at 1.5 s. The gateway is read before ingest, so with writes
	// going on a drained consumer reads as having applied more than was accepted.
	if err := os.WriteFile(path, []byte("1000 50 0\n2000 60 30\n3000 70 71\n4000 80 80\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	got, err := lagDrained(path, 1500)
	if err != nil {
		t.Fatal(err)
	}
	if got == nil || *got != 1.5 {
		t.Fatalf("drained after %v s, want 1.5", got)
	}
}

func TestNextRate(t *testing.T) {
	m, miss, stop := rateVerdict{meets: true}, rateVerdict{}, rateVerdict{stop: true}
	v := map[float64]rateVerdict{}
	step := func(bisect bool, wantNext float64, wantDone bool, wantHeadline float64) {
		t.Helper()
		next, done, headline := nextRate(v, bisect)
		if next != wantNext || done != wantDone || headline != wantHeadline {
			t.Fatalf("got %g %v %g, want %g %v %g", next, done, headline, wantNext, wantDone, wantHeadline)
		}
	}
	step(true, 100, false, 0)
	v[100], v[200], v[400], v[800] = m, m, m, m
	step(true, 1200, false, 0)
	// 1200 misses, 1600 meets: the bisection starts from 1600, the highest grid
	// rate that meets, not below the first miss.
	v[1200], v[1600] = miss, m
	step(true, 2000, false, 0)
	v[2000] = stop
	step(false, 0, true, 0)
	step(true, 1800, false, 0)
	v[1800] = m
	step(true, 1900, false, 0)
	v[1900] = miss
	step(true, 1850, false, 0)
	v[1850] = m
	step(true, 0, true, 1850)

	// Nothing meets: 50 runs as well.
	v = map[float64]rateVerdict{100: miss, 200: stop}
	step(true, 50, false, 0)
	v[50] = m
	step(true, 0, true, 50)
}
