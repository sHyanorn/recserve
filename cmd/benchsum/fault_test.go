package main

import (
	"fmt"
	"math"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// The recovery window's degraded share must be within 1 point of the
// healthy phase's from below as well as from above.
func TestRecoveryDegradedEitherWay(t *testing.T) {
	// 3% degraded in every second but [30, 35 s), where none is.
	s := answers(60_000, func(float64) float64 { return 2 }, func(due float64) []string {
		if (due < 30_000 || due >= 35_000) && int(due/10)%100 < 3 {
			return []string{"history_unavailable"}
		}
		return nil
	})
	healthy := describe(s, 0, 30_000)
	// From 30 s the window holds 1.5% and from 31 s 1.8%, more than 1 point
	// below 3%; from 32 s 2.1%.
	if got, ok := recovery(s, healthy, 30_000, 60_000); !ok || got != 2 {
		t.Fatalf("recovery %d %v, want 2 s", got, ok)
	}
}

// Errors and F4's p99 are judged from 0 s, the warmup included.
func TestJudgeFromZero(t *testing.T) {
	f4 := func(warmup stats) faultRun {
		zero := 0.0
		return faultRun{
			Fault: "F4", LagDrainedS: &zero, Histories: "ok",
			Phases: map[string]stats{"warmup": warmup, "healthy": {P99: 5}, "fault": {P99: 9}, "recovery": {P99: 5}},
		}
	}
	if got := judge(f4(stats{P99: 20})); len(got) != 0 {
		t.Fatalf("failures %v", got)
	}
	if got := judge(f4(stats{P99: 20, Errors: 1})); len(got) == 0 {
		t.Error("an error in the warmup passed")
	}
	if got := judge(f4(stats{P99: 51})); len(got) == 0 {
		t.Error("F4's warmup p99 of 51 ms passed")
	}
}

const testT0 = 1_794_355_200_000.0

// writeReads writes a reads file with a read 3 ms after each second s of
// [0, n] from testT0, holding lines(s).
func writeReads(t *testing.T, path string, n int, lines func(s int) string) {
	t.Helper()
	var b strings.Builder
	for s := 0; s <= n; s++ {
		fmt.Fprintf(&b, "read %.0f\n%s", testT0+float64(s)*1000+3, lines(s))
	}
	if err := os.WriteFile(path, []byte(b.String()), 0o644); err != nil {
		t.Fatal(err)
	}
}

// testReads writes reads as writeReads does and parses them.
func testReads(t *testing.T, n int, lines func(s int) string) runReads {
	t.Helper()
	path := filepath.Join(t.TempDir(), "run.reads")
	writeReads(t, path, n, lines)
	r, err := readReads(path, testT0)
	if err != nil {
		t.Fatal(err)
	}
	return r
}

// shardCalls returns reads in which shard 0 answers 100 calls a second,
// annshard-1 per(s) of them in second s.
func shardCalls(t *testing.T, n int, per func(s int) int) runReads {
	return testReads(t, n, shardLines(per))
}

// shardLines returns the lines of reads in which shard 0 answers 100 calls
// a second, annshard-1 per(s) of them in second s.
func shardLines(per func(s int) int) func(s int) string {
	var c0, c1 int
	return func(s int) string {
		out := fmt.Sprintf("recserve_shard_calls_total{replica=\"toxiproxy:19090\",result=\"answered\",shard=\"0\"} %d\n"+
			"recserve_shard_calls_total{replica=\"toxiproxy:19091\",result=\"answered\",shard=\"0\"} %d\n"+
			"recserve_shard_calls_total{replica=\"toxiproxy:19091\",result=\"deadline\",shard=\"0\"} 7\n"+
			"recserve_shard_calls_total{replica=\"toxiproxy:19092\",result=\"answered\",shard=\"1\"} %d\n", c0, c1, 100*s)
		a := per(s)
		c0, c1 = c0+100-a, c1+a
		return out
	}
}

func TestReadReads(t *testing.T) {
	path := filepath.Join(t.TempDir(), "run.reads")
	body := "read 1000\n" +
		"recserve_valkey_calls_total{call=\"history\",result=\"ok\"} 10\n" +
		"go_gc_duration_seconds_sum 0.5\n" +
		"cpu gateway 2000000\ncpu annshard-1 7\n" +
		"read 2100\ncpu gateway 3000000\n" + // the gateway did not answer
		"read 3002\n" +
		"recserve_valkey_calls_total{call=\"history\",result=\"ok\"} 30\n" +
		"recserve_shard_ejections_total{replica=\"toxiproxy:19091\",shard=\"0\"} 0\n" +
		"go_gc_duration_seconds_sum 0.75\n" +
		"cpu gateway 5000000\n"
	if err := os.WriteFile(path, []byte(body), 0o644); err != nil {
		t.Fatal(err)
	}
	r, err := readReads(path, 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(r.list) != 2 || r.list[1].ms != 3002 || r.list[0].cpu["annshard-1"] != 7 || r.list[1].cpu["gateway"] != 5e6 {
		t.Fatalf("%+v", r.list)
	}
	if _, ok := r.at(2); ok {
		t.Error("a read at 2 s, where the gateway did not answer")
	}
	b, ok := r.at(3)
	if !ok || b.m.sum("recserve_valkey_calls_total", map[string]string{"result": "ok"}) != 30 || b.m.sum("go_gc_duration_seconds_sum", nil) != 0.75 {
		t.Fatalf("read at 3 s: %v %v", b, ok)
	}
	if !r.outlierDetection() {
		t.Error("ejections in a read, and no outlier detection")
	}
	if shardCalls(t, 2, func(int) int { return 50 }).outlierDetection() {
		t.Error("outlier detection without ejections in any read")
	}
}

// annshard-1's share in [210, 270 s) passes at 40% and fails below.
func TestReplicaShare(t *testing.T) {
	for _, c := range []struct {
		per  int
		pass bool
	}{{40, true}, {39, false}} {
		r := shardCalls(t, 270, func(s int) int {
			if s < 210 {
				return 0
			}
			return c.per
		})
		got, ok := r.share(210, 270)
		if !ok || math.Abs(got-float64(c.per)/100) > 1e-12 {
			t.Fatalf("share %v %v, want %d%%", got, ok, c.per)
		}
		f := passing("F1")
		f.ReplicaShare = &got
		if fails := judge(f); (len(fails) == 0) != c.pass {
			t.Errorf("share %v: failures %v", got, fails)
		}
	}
	f := passing("F5")
	f.ReplicaShare = nil
	if len(judge(f)) == 0 {
		t.Error("F5 passed without reads at 210 and 270 s")
	}
}

// The replica is back from the first whole second after removal that
// starts 10 seconds in a row each with a share of at least 30%.
func TestReplicaReturn(t *testing.T) {
	r := shardCalls(t, 270, func(s int) int {
		switch {
		case s < 152:
			return 0
		case s == 156:
			return 20 // a dip: [152, 162 s) does not count
		case s == 160:
			return 30
		}
		return 50
	})
	got := r.returned(150, 270)
	if got == nil || *got != 7 {
		t.Fatalf("returned %v s after removal, want 7", got)
	}
	ten, three := 10, 3
	if v := later(&three, got); v == nil || *v != *got {
		t.Errorf("later of 3 s and %v: %v", *got, v)
	}
	if v := later(&ten, got); v == nil || *v != 10 {
		t.Errorf("later of 10 s and %v: %v", *got, v)
	}
	if later(nil, got) != nil || later(&three, nil) != nil {
		t.Error("recovered with one of the two missing")
	}
	// The later of the two is judged within M6's limits: 10 s for F1 and F5.
	for _, c := range []struct {
		s    float64
		pass bool
	}{{9.996, true}, {10.004, false}} {
		f := passing("F5")
		f.Recovery = &c.s
		if fails := judge(f); (len(fails) == 0) != c.pass {
			t.Errorf("recovery %v s: failures %v", c.s, fails)
		}
	}
	f := passing("F6")
	f.Return = nil
	if len(judge(f)) == 0 {
		t.Error("F6 passed with annshard-1 not back")
	}
}

// passing returns a run of fault that passes its M7 criteria without
// outlier detection.
func passing(fault string) faultRun {
	zero, share, back := 0.0, 0.5, 1.0
	return faultRun{
		Fault:  fault,
		Phases: map[string]stats{"warmup": {P99: 5}, "healthy": {P99: 5}, "fault": {P99: 20}, "recovery": {P99: 5}},
		Silent: []float64{0, 0, 0}, Recovery: &back, Return: &back, ReplicaShare: &share, LagDrainedS: &zero, Histories: "ok",
	}
}

func TestJudgeOutlierDetection(t *testing.T) {
	for _, fault := range []string{"F1", "F2", "F3", "F4", "F5", "F6"} {
		if got := judge(passing(fault)); len(got) != 0 {
			t.Errorf("%s: failures %v", fault, got)
		}
	}
	none, one := 0.0, 1.0
	cases := []struct {
		name string
		f    func(f *faultRun)
		pass bool // with outlier detection; without it every case passes
	}{
		{"F5 partial in [0, 13 s) 1 point above healthy", func(f *faultRun) {
			f.Fault, f.Silent[0], f.Phases["healthy"] = "F5", 0.015, stats{P99: 5, Reasons: map[string]float64{"shards_partial": 0.005}}
		}, true},
		{"F5 partial in [0, 13 s) 12 points", func(f *faultRun) { f.Fault, f.Silent[0] = "F5", 0.12 }, false},
		{"F6 partial 1 point above healthy", func(f *faultRun) {
			f.Fault, f.Excess = "F6", map[string]float64{"shards_partial": 0.01}
		}, true},
		{"F6 partial 11 points", func(f *faultRun) { f.Fault, f.Excess = "F6", map[string]float64{"shards_partial": 0.11} }, false},
		{"F2 an ejection in the healthy phase", func(f *faultRun) { f.Fault, f.HealthyEjections = "F2", &one }, false},
		{"F6 an ejection in the healthy phase", func(f *faultRun) { f.Fault, f.HealthyEjections = "F6", &one }, false},
		{"F2 no reads at 30 and 90 s", func(f *faultRun) { f.Fault, f.HealthyEjections = "F2", nil }, false},
	}
	for _, c := range cases {
		f := passing("")
		f.HealthyEjections = &none
		c.f(&f)
		if got := judge(f); len(got) != 0 {
			t.Errorf("%s, without outlier detection: failures %v", c.name, got)
		}
		f.OutlierDetection = true
		if got := judge(f); (len(got) == 0) != c.pass {
			t.Errorf("%s, with outlier detection: failures %v, want pass %v", c.name, got, c.pass)
		}
	}
	ejections := testReads(t, 100, func(s int) string {
		return fmt.Sprintf("recserve_shard_ejections_total{replica=\"toxiproxy:19091\",shard=\"0\"} %d\n", s/60)
	})
	if v, ok := ejections.delta("recserve_shard_ejections_total", nil, 30, 90); !ok || v != 1 {
		t.Errorf("ejections in [30, 90 s): %v %v, want 1", v, ok)
	}
}

// The healthy phase's ejections are read from the last read before 30 s
// to the last before 90 s. The reads are 3 ms after each whole second: an
// ejection first read at 30.003 s may have come after 30 s and counts, one
// first read at 90.003 s may have come after 90 s and does not.
func TestHealthyEjectionsReadBefore30And90(t *testing.T) {
	for _, c := range []struct {
		first int // second of the first read that counts the ejection
		want  float64
	}{{29, 0}, {30, 1}, {89, 1}, {90, 0}} {
		r := testReads(t, 100, func(s int) string {
			n := 0
			if s >= c.first {
				n = 1
			}
			return fmt.Sprintf("recserve_shard_ejections_total{replica=\"toxiproxy:19091\",shard=\"0\"} %d\n", n)
		})
		if v, ok := r.deltaBefore("recserve_shard_ejections_total", nil, 30_000, injectAt); !ok || v != c.want {
			t.Errorf("ejection first read in second %d: %v %v, want %v", c.first, v, ok, c.want)
		}
	}
	if _, ok := testReads(t, 100, func(int) string { return "" }).deltaBefore("x", nil, 0, injectAt); ok {
		t.Error("a delta from before the first read")
	}
}

// The breaker table names an opening's phase by 90 and 150 s, not by the
// script's marks a few ms later.
func TestPhaseAtTheCriteriaBounds(t *testing.T) {
	f := faultRun{InjectS: 90.005, RemoveS: 150.004, Windows: make([]stats, 270)}
	for s, want := range map[float64]string{29.999: "warmup", 89.999: "healthy", 90.001: "fault", 150.001: "recovery", 270: "after the load"} {
		if got := f.phaseAt(s); got != want {
			t.Errorf("phase at %.3f s: %s, want %s", s, got, want)
		}
	}
}

func TestAttribute(t *testing.T) {
	for _, c := range []struct {
		failed map[string]float64
		want   string
	}{
		{map[string]float64{"timeout": 5}, "timeouts"},
		{map[string]float64{"timeout": 9, "cancelled": 3, "refused": 40}, "timeouts"},
		{map[string]float64{"timeout": 5, "error": 1}, "not attributed"},
		{map[string]float64{"error": 5}, "errors"},
		{map[string]float64{"error": 6, "timeout": 1}, "not attributed"},
		{map[string]float64{"error": 4, "connecting": 3}, "not attributed"},
		{map[string]float64{"timeout": 4, "refused": 10}, "not attributed"},
		{nil, "not attributed"},
	} {
		if got := attribute(c.failed); got != c.want {
			t.Errorf("%v: %s, want %s", c.failed, got, c.want)
		}
	}
}

// An opening is attributed by the calls that failed in the interval between
// reads that holds it and the one before, here [40, 42 s), and reported
// beside the median 2 s interval of [30, 90 s).
func TestOpenings(t *testing.T) {
	var timeouts, errs, gc float64
	r := testReads(t, 100, func(s int) string {
		out := fmt.Sprintf("recserve_valkey_calls_total{call=\"history\",result=\"ok\"} %d\n"+
			"recserve_valkey_calls_total{call=\"history\",result=\"timeout\"} %g\n"+
			"recserve_valkey_calls_total{call=\"cache_get\",result=\"error\"} %g\n"+
			"recserve_valkey_calls_total{call=\"history\",result=\"refused\"} 0\n"+
			"go_gc_duration_seconds_sum %g\ncpu gateway %d\ncpu valkey %d\n", 275*s, timeouts, errs, gc, 5_000_000*s, 1_000_000*s)
		switch s {
		case 40, 41:
			timeouts += 3 // in [40, 41 s) and [41, 42 s)
			gc += 0.010
		case 70:
			errs += 5
		case 71:
			errs++
		default:
			gc += 0.001
		}
		return out
	})
	path := filepath.Join(t.TempDir(), "run.breaker")
	at := func(s float64) string {
		return fmt.Sprintf(`{"time":%q,"level":"WARN","msg":"valkey circuit breaker"`, msTime(testT0+s*1000))
	}
	log := at(41.5) + `,"from":"closed","to":"open"}` + "\n" +
		at(42.5) + `,"from":"open","to":"half-open"}` + "\n" +
		at(42.501) + `,"from":"half-open","to":"open"}` + "\n" +
		at(71.2) + `,"from":"closed","to":"open"}` + "\n" +
		at(-2) + `,"from":"closed","to":"open"}` + "\n"
	if err := os.WriteFile(path, []byte(log), 0o644); err != nil {
		t.Fatal(err)
	}
	changes, err := readOpenings(path)
	if err != nil {
		t.Fatal(err)
	}
	if len(changes) != 3 {
		t.Fatalf("openings %v, want those from closed only", changes)
	}
	o := r.openings(changes)
	if len(o) != 3 {
		t.Fatalf("%+v", o)
	}
	a := o[0]
	if math.Abs(a.AtS-41.5) > 1e-9 || a.FromS != 40.003 || a.ToS != 42.003 || a.Failed["timeout"] != 6 || a.Failed["error"] != 0 || a.Attribution != "timeouts" {
		t.Errorf("opening at 41.5 s: %+v", a)
	}
	if math.Abs(a.GCPauseMs-20) > 1e-9 || math.Abs(a.MedianGCPauseMs-2) > 1e-9 || a.CPUMs["gateway"] != 10 || a.MedianCPUMs["valkey"] != 2 {
		t.Errorf("opening at 41.5 s, GC and CPU: %+v", a)
	}
	if b := o[1]; b.Failed["error"] != 6 || b.Attribution != "errors" {
		t.Errorf("opening at 71.2 s: %+v", b)
	}
	if c := o[2]; c.Attribution != "no reads" {
		t.Errorf("opening before the first read: %+v", c)
	}
}

// An F1 run from its files: annshard-1 has no calls from the kill at 90 s
// until 152 s, the samples are alike throughout, and the breaker opened
// once, at 100.5 s.
func TestSummarizeFault(t *testing.T) {
	dir := t.TempDir()
	if err := os.Mkdir(filepath.Join(dir, "raw"), 0o755); err != nil {
		t.Fatal(err)
	}
	write := func(name, body string) {
		if err := os.WriteFile(filepath.Join(dir, name), []byte(body), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	write("F1-run1.json", fmt.Sprintf(`{"start":%q,"target_rate":100,"achieved_rate":100,"warmup_s":0,"duration_s":270,"valid":true}`, msTime(testT0)))
	write("F1-run1.marks", fmt.Sprintf("inject %.0f\nremove %.0f\n", testT0+90_002, testT0+150_004))
	for _, side := range []string{"before", "after"} {
		write("F1-run1.noise-"+side+".json", `{"noisy": false}`)
	}
	for i := range 4 {
		write(fmt.Sprintf("F1-run1.b%d.metrics", i), fmt.Sprintf("# TYPE recserve_shard_hedges_total counter\nrecserve_shard_hedges_total{result=\"won\",shard=\"0\"} %d\n", i))
	}
	write("F1-run1.breaker", `{"time":"`+msTime(testT0+100_500)+`","level":"WARN","msg":"valkey circuit breaker","from":"closed","to":"open"}`+"\n")
	var b strings.Builder
	b.WriteString("i,path,due_ms,latency_ms,status,outcome,degraded,answered,shards\n")
	for due := 0; due < 270_000; due += 10 {
		fmt.Fprintf(&b, "%d,/v1/recommend,%d,2,200,recommend_ok,,2,2\n", due/10, due)
	}
	raw := filepath.Join(dir, "raw", "F1-run1.csv.gz")
	if err := writeGz(raw, b.String()); err != nil {
		t.Fatal(err)
	}
	reads := filepath.Join(dir, "raw", "F1-run1.reads")
	writeReads(t, reads, 270, shardLines(func(s int) int {
		if s >= 90 && s < 152 {
			return 0
		}
		return 50
	}))
	f, err := summarizeFault(filepath.Join(dir, "F1-run1"), raw, reads, "F1")
	if err != nil {
		t.Fatal(err)
	}
	if !f.Valid || !f.Pass || f.Phases["warmup"].Answers != 3000 || f.Hedges["healthy"]["0/won"] != 1 || f.OutlierDetection {
		t.Fatalf("%+v", f)
	}
	if f.WindowRecovery == nil || *f.WindowRecovery != 0 || f.Return == nil || *f.Return != 2 || *f.Recovery != *f.Return {
		t.Errorf("recovery %v, return %v, later %v", f.WindowRecovery, f.Return, f.Recovery)
	}
	if f.ReplicaShare == nil || *f.ReplicaShare != 0.5 || len(f.ReplicaShares) != 270 || f.ReplicaShares[100] == nil || *f.ReplicaShares[100] != 0 {
		t.Errorf("shares %v %v", f.ReplicaShare, f.ReplicaShares)
	}
	if len(f.Breaker) != 1 || f.Breaker[0].FromS != 99.003 || f.Breaker[0].Attribution != "not attributed" {
		t.Errorf("breaker %+v", f.Breaker)
	}
	if err := writeJSON(filepath.Join(dir, "F1-run1.fault.json"), f); err != nil {
		t.Fatal(err)
	}
	// With outlier detection, the healthy phase's ejections are those read
	// from the last read before 30 s to the last before 90 s.
	for first, want := range map[int]float64{30: 1, 90: 0} {
		ej := filepath.Join(dir, "raw", fmt.Sprintf("F1-run1.ej%d.reads", first))
		calls := shardLines(func(int) int { return 50 })
		writeReads(t, ej, 270, func(s int) string {
			n := 0
			if s >= first {
				n = 1
			}
			return calls(s) + fmt.Sprintf("recserve_shard_ejections_total{replica=\"toxiproxy:19091\",shard=\"0\"} %d\n", n)
		})
		g, err := summarizeFault(filepath.Join(dir, "F1-run1"), raw, ej, "F1")
		if err != nil || !g.OutlierDetection || g.HealthyEjections == nil || *g.HealthyEjections != want {
			t.Errorf("ejection first read in second %d: healthy ejections %v, want %v; %v", first, g.HealthyEjections, want, err)
		}
	}
	// F4's lag drain counts from 150 s, not from the removal mark at
	// 150.004 s: the first reading with nothing left, at 150.002 s, gives 0.002.
	write("F1-run1.lag", fmt.Sprintf("%.0f 5 4\n%.0f 5 5\n%.0f 6 6\n", testT0+149_000, testT0+150_002, testT0+151_000))
	write("F1-run1.histories", "ok\n")
	if f4, err := summarizeFault(filepath.Join(dir, "F1-run1"), raw, reads, "F4"); err != nil || f4.LagDrainedS == nil || *f4.LagDrainedS != 0.002 {
		t.Errorf("F4 lag drained %v, want 0.002 s; %v", f4.LagDrainedS, err)
	}
	// With nothing injected the same files are judged for validity only.
	if n, err := summarizeFault(filepath.Join(dir, "F1-run1"), raw, reads, "none"); err != nil || !n.Valid || n.Pass || n.Failures != nil || n.ReplicaShare != nil || len(n.Breaker) != 1 {
		t.Errorf("none: %+v %v", n, err)
	}
	var tb, bb strings.Builder
	if err := faultTable(&tb, dir); err != nil {
		t.Fatal(err)
	}
	if err := breakerTable(&bb, dir); err != nil {
		t.Fatal(err)
	}
	sections := strings.Split(tb.String(), "\n\n")
	if len(sections) != 3 {
		t.Fatalf("fault tables:\n%s", tb.String())
	}
	for table, want := range map[string]string{
		sections[0]: "| F1-run1 | true | true | 2.00 | 2.00 | 2.00 | 2.00 | 0.0% |  | 0 | 2.000 | 50.0% | 2.000 | no | 0 |  |",
		sections[1]: "| F1-run1 | true |  | 1 |",
		sections[2]: "| F1 | 1 | 1 | 1 | 0 | not judged: 1 valid runs |",
		bb.String(): "| F1-run1 | true | 100.500 | fault | 99.003 to 101.003 | 0 | 0 |  | not attributed | 0.00 (0.00) |  |",
	} {
		lines := strings.Split(strings.TrimSpace(table), "\n")
		cells := func(l string) int { return strings.Count(l, "|") - 1 }
		if len(lines) != 3 || lines[2] != want || cells(lines[0]) != cells(lines[2]) || cells(lines[1]) != cells(lines[2]) {
			t.Errorf("table:\n%s\nwant the row\n%s", table, want)
		}
	}
}

func msTime(ms float64) string { return time.UnixMilli(int64(ms)).UTC().Format(time.RFC3339Nano) }

// The phases are cut at 90 and 150 s, as the criteria write them, not at
// the script's marks a few ms later.
func TestPhasesAtTheCriteriaBounds(t *testing.T) {
	s := answers(270_000, func(float64) float64 { return 2 }, none)
	for i := range s {
		if s[i].due == 90_000 || s[i].due == 150_000 {
			s[i].status = 503
		}
	}
	p := phaseStats(s, 270_000)
	if p["healthy"].Errors != 0 || p["fault"].Errors != 1 || p["recovery"].Errors != 1 {
		t.Fatalf("errors healthy %d, fault %d, recovery %d; want 0, 1, 1", p["healthy"].Errors, p["fault"].Errors, p["recovery"].Errors)
	}
}

// F5's [15, 60 s) after the pause ends 60 s after 90 s, not at the removal.
func TestSilentWindowEnds60sAfterThePause(t *testing.T) {
	if w := silentWindows[len(silentWindows)-1]; injectAt+w[1] != 150_000 || injectAt+w[0] != 105_000 {
		t.Fatalf("last silent window %v after %d ms", w, injectAt)
	}
}

// A recovery window or a return may start in any second of the recovery
// phase, also one whose 10 s run past its end; recovery counts from 150 s.
func TestRecoveryStartsWithinThePhase(t *testing.T) {
	s := answers(270_000, func(due float64) float64 {
		if due < 262_000 {
			return 30
		}
		return 2
	}, none)
	healthy := describe(answers(1000, func(float64) float64 { return 2 }, none), 0, 1000)
	if got, ok := recovery(s, healthy, removeAt, 270_000); !ok || got != 112 {
		t.Fatalf("recovery %d %v, want 112 s, the window [262, 272 s)", got, ok)
	}
	r := shardCalls(t, 270, func(s int) int {
		if s < 265 {
			return 0
		}
		return 50
	})
	if got := r.returned(removeAt/1000, 270); got == nil || *got != 115 {
		t.Fatalf("returned %v, want 115 s", got)
	}
}

// An invalid run is neither a pass nor a fail.
func TestInvalidRunNotJudged(t *testing.T) {
	f := faultRun{Fault: "F1", Invalid: []string{"noise baseline flagged"}, Phases: map[string]stats{"fault": {P99: 80}}}
	f.verdict()
	if f.Pass || f.Failures != nil {
		t.Fatalf("invalid run judged: pass %v, failures %v", f.Pass, f.Failures)
	}
}

// The breaker table's medians take the intervals within [30, 90 s): not one
// that ends at the read in second 90.
func TestHealthyIntervals(t *testing.T) {
	r := runReads{t0: 1000}
	at := func(s float64) read { return read{ms: r.t0 + s*1000} }
	for _, c := range []struct {
		a, b float64
		want bool
	}{{30.005, 31.005, true}, {88.005, 89.005, true}, {89.005, 90.005, false}, {29.995, 30.995, false}} {
		if got := r.healthy(at(c.a), at(c.b)); got != c.want {
			t.Errorf("[%v, %v s]: %v, want %v", c.a, c.b, got, c.want)
		}
	}
}

// Attempts of a run are kept apart: the run's row is its valid attempt,
// and a fault's verdict comes from its valid runs only.
func TestChosenAttempts(t *testing.T) {
	a := func(name string, valid, pass bool) faultRun {
		run, n := runOf(name)
		return faultRun{Name: name, Run: run, Attempt: n, Fault: "F2", Valid: valid, Pass: pass}
	}
	runs := chosen([]faultRun{a("F2-run1-a1", false, false), a("F2-run1-a2", true, true), a("F2-run2-a1", false, false),
		a("F2-run2-a2", false, false), a("F2-run3-a1", true, true)})
	if len(runs) != 3 || runs[0].Name != "F2-run1-a2" || runs[1].Name != "F2-run2-a2" || runs[2].Name != "F2-run3-a1" {
		t.Fatalf("chosen %v", runs)
	}
	if v := faultVerdict(runs); v != "not judged: 2 valid runs" {
		t.Fatalf("verdict %q", v)
	}
	runs[1] = a("F2-run2-a3", true, false)
	if v := faultVerdict(runs); v != "fail: F2-run2" {
		t.Fatalf("verdict %q", v)
	}
}
