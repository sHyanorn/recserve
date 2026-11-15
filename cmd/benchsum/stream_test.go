package main

import (
	"fmt"
	"math"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// streamFixture writes the files of a run of the stream scenario: 90
// recommendations and 10 events in its window, its gateway's CPU 100 ms,
// and edit's changes, a file's new body by suffix ("" removes it).
func streamFixture(t *testing.T, dir, name string, edit map[string]string) string {
	t.Helper()
	prefix := filepath.Join(dir, name)
	files := map[string]string{
		".json": `{"target_rate":200,"achieved_rate":200,"warmup_s":0,"duration_s":1,"valid":true,"versions":{"vON":90}}`,
		".at0.metrics": "recserve_stream_events_total{result=\"applied\"} 5\nrecserve_stream_events_total{result=\"duplicate\"} 0\nrecserve_stream_events_total{result=\"poison\"} 0\n" +
			"# TYPE recserve_shard_search_seconds histogram\nrecserve_shard_search_seconds_bucket{shard=\"0\",le=\"+Inf\"} 100\nrecserve_shard_search_seconds_sum{shard=\"0\"} 1\nrecserve_shard_search_seconds_count{shard=\"0\"} 100\nrecserve_shard_hedges_total{result=\"won\",shard=\"0\"} 1\nrecserve_shard_hedges_total{result=\"lost\",shard=\"0\"} 1\n",
		".at1.metrics": "recserve_stream_events_total{result=\"applied\"} 15\nrecserve_stream_events_total{result=\"duplicate\"} 0\nrecserve_stream_events_total{result=\"poison\"} 0\nrecserve_stream_lag_records 7\n" +
			"# TYPE recserve_shard_search_seconds histogram\nrecserve_shard_search_seconds_bucket{shard=\"0\",le=\"+Inf\"} 200\nrecserve_shard_search_seconds_sum{shard=\"0\"} 1\nrecserve_shard_search_seconds_count{shard=\"0\"} 200\nrecserve_shard_hedges_total{result=\"won\",shard=\"0\"} 4\nrecserve_shard_hedges_total{result=\"lost\",shard=\"0\"} 7\nrecserve_shard_hedges_total{result=\"denied\",shard=\"0\"} 50\n" +
			"recserve_shard_calls_total{replica=\"annshard-0:9090\",result=\"answered\",shard=\"0\"} 150\nrecserve_shard_calls_total{replica=\"annshard-1:9090\",result=\"answered\",shard=\"0\"} 50\n",
		".end.metrics":       "recserve_stream_lag_records 0\nprocess_resident_memory_bytes 3e+08\ngo_memstats_heap_inuse_bytes 2e+08\n",
		".ready.metrics":     "process_resident_memory_bytes 2.5e+08\ngo_memstats_heap_inuse_bytes 1.5e+08\n",
		".at0.cpu":           "gateway 0\nkafka 0\nannshard-0 0\nannshard-1 0\nannshard-2 7\n",
		".at1.cpu":           "gateway 100000000\nkafka 9000000\nannshard-0 30000000\nannshard-1 20000000\nannshard-3 5\n",
		".noise-before.json": `{"noisy": false}`,
		".noise-after.json":  `{"noisy": false}`,
		".boot":              "b0000000-0000\n",
		".commit":            "c0ffee\n",
		".image":             "sha256:1mage\n",
		".restarts0":         "gateway 0\nkafka 2\n",
		".restarts1":         "gateway 0\nkafka 2\n",
		".started":           "2026-11-15T05:39:07.5Z\n",
		".serving":           `{"time":"2026-11-15T05:39:11.25Z","level":"INFO","msg":"serving"}` + "\n",
		".hwm":               "VmHWM:\t  409600 kB\n",
	}
	for k, v := range edit {
		files[k] = v
	}
	for suffix, body := range files {
		if body == "" {
			continue
		}
		if err := os.WriteFile(prefix+suffix, []byte(body), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	var b strings.Builder
	b.WriteString("i,path,due_ms,latency_ms,status,outcome,degraded,answered,shards\n")
	for i := range 100 {
		if i%10 == 9 {
			fmt.Fprintf(&b, "%d,/v1/events,%d,1,202,events_ok,,-1,-1\n", i, i)
		} else {
			fmt.Fprintf(&b, "%d,/v1/recommend,%d,2,200,recommend_ok,,2,2\n", i, i)
		}
	}
	if err := writeGz(prefix+".csv.gz", b.String()); err != nil {
		t.Fatal(err)
	}
	return prefix
}

func TestStreamRun(t *testing.T) {
	dir := t.TempDir()
	s, err := summarizeStream(streamFixture(t, dir, "200-on-1", nil), filepath.Join(dir, "200-on-1.csv.gz"), "on", "vON")
	if err != nil {
		t.Fatal(err)
	}
	if !s.Valid || s.Boot != "b0000000-0000" || s.Answers != 90 {
		t.Fatalf("valid %v %v, boot %q, %d answers", s.Valid, s.Problems, s.Boot, s.Answers)
	}
	if got := s.CPU["gateway"]; math.Abs(got-100.0/90) > 1e-12 {
		t.Errorf("gateway %v ms per recommendation, want %v", got, 100.0/90)
	}
	for _, c := range []string{"annshard-2", "annshard-3"} {
		if _, ok := s.CPU[c]; ok {
			t.Errorf("%s, read at one end of the window only, has a CPU reading", c)
		}
	}
	// Shard 0: annshard-0 and -1, 50 ms over 100 calls; 9 hedges over 90 answers.
	if got := s.ReplicaCPUPerCall["0"]; len(s.ReplicaCPUPerCall) != 1 || math.Abs(got-0.5) > 1e-12 {
		t.Errorf("replica CPU per call %v, want shard 0 at 0.5 ms", s.ReplicaCPUPerCall)
	}
	if s.Hedges == nil || math.Abs(*s.Hedges-0.1) > 1e-12 {
		t.Errorf("hedges per recommendation %v, want 0.1", s.Hedges)
	}
	if s.Commit != "c0ffee" || s.Image != "sha256:1mage" || s.CPUPerRequest != nil {
		t.Errorf("commit %q, image %q, the earlier CPU field %v", s.Commit, s.Image, s.CPUPerRequest)
	}
	m := s.Memory
	for name, c := range map[string]struct {
		got  *float64
		want float64
	}{
		"ready RSS": {m.ReadyRSS, 2.5e8}, "ready heap": {m.ReadyHeap, 1.5e8}, "end RSS": {m.EndRSS, 3e8}, "end heap": {m.EndHeap, 2e8},
		"peak RSS": {m.PeakRSS, 409600 * 1024}, "ready s": {s.ReadyS, 3.75}, "applied": {s.Applied, 10}, "duplicate": {s.Duplicate, 0}, "lag": {s.Lag, 0},
	} {
		if c.got == nil || math.Abs(*c.got-c.want) > 1e-9 {
			t.Errorf("%s: %v, want %v", name, c.got, c.want)
		}
	}
}

// A reading that could not be taken is missing, not 0.
func TestStreamRunMissingReadings(t *testing.T) {
	dir := t.TempDir()
	p := streamFixture(t, dir, "200-on-1", map[string]string{
		".hwm": "", ".ready.metrics": "", ".serving": "", ".end.metrics": "",
		".at1.metrics": "recserve_stream_events_total{result=\"applied\"} 15\n",
	})
	s, err := summarizeStream(p, p+".csv.gz", "on", "vON")
	if err != nil {
		t.Fatal(err)
	}
	for name, v := range map[string]*float64{
		"peak RSS": s.Memory.PeakRSS, "ready RSS": s.Memory.ReadyRSS, "ready heap": s.Memory.ReadyHeap, "end RSS": s.Memory.EndRSS,
		"ready s": s.ReadyS, "duplicate": s.Duplicate, "lag": s.Lag, "hedges": s.Hedges,
	} {
		if v != nil {
			t.Errorf("%s: %v, want missing", name, *v)
		}
	}
	var b strings.Builder
	streamTable(&b, []streamRun{s}, true)
	if !strings.Contains(b.String(), "missing") {
		t.Errorf("table without missing readings:\n%s", b.String())
	}
}

// A run is invalid if its gateway served another bundle, kept a state in
// the off group or none in the on group, or a container restarted.
func TestStreamRunChecks(t *testing.T) {
	for _, c := range []struct {
		name, group, version string
		edit                 map[string]string
		problem              string
	}{
		{"version", "on", "vOFF", nil, "versions answered"},
		{"no state", "on", "vON", map[string]string{".at1.metrics": "process_resident_memory_bytes 3e+08\n"}, "keeps no stream state"},
		{"a state", "off", "vON", nil, "keeps a stream state"},
		{"restart", "on", "vON", map[string]string{".restarts1": "gateway 1\nkafka 2\n"}, "container gateway restarted"},
		{"gone", "on", "vON", map[string]string{".restarts1": "kafka 2\n"}, "container gateway restarted"},
		{"not found", "on", "vON", map[string]string{".restarts0": "gateway -1\nkafka 2\n", ".restarts1": "gateway -1\nkafka 2\n"}, "container gateway restarted or missing"},
	} {
		t.Run(c.name, func(t *testing.T) {
			dir := t.TempDir()
			p := streamFixture(t, dir, "200-x-1", c.edit)
			s, err := summarizeStream(p, p+".csv.gz", c.group, c.version)
			if err != nil {
				t.Fatal(err)
			}
			if s.Valid || !strings.Contains(strings.Join(s.Problems, "; "), c.problem) {
				t.Fatalf("valid %v, problems %v; want %q", s.Valid, s.Problems, c.problem)
			}
		})
	}
}

// A gateway that restarted before the run has no ready time: docker gives
// its last start only.
func TestStreamRunGatewayRestartedBefore(t *testing.T) {
	dir := t.TempDir()
	p := streamFixture(t, dir, "200-on-1", map[string]string{".restarts0": "gateway 1\nkafka 2\n", ".restarts1": "gateway 1\nkafka 2\n"})
	s, err := summarizeStream(p, p+".csv.gz", "on", "vON")
	if err != nil {
		t.Fatal(err)
	}
	if !s.Valid || s.ReadyS != nil {
		t.Errorf("valid %v %v, ready %v; want valid, ready missing", s.Valid, s.Problems, s.ReadyS)
	}
}

func run3(group string, rate float64, gw []float64, p99 float64) []streamRun {
	var out []streamRun
	for i, v := range gw {
		applied := 10.0
		out = append(out, streamRun{
			runSummary: runSummary{Name: fmt.Sprintf("%g-%s-%d", rate, group, i+1), Rate: rate, Valid: true, stats: stats{P99: p99, Answers: 10, Events: 10}},
			Group:      group, Boot: "b", Commit: "c", CPU: map[string]float64{"gateway": v, "kafka": 1},
			ReplicaCPUPerCall: map[string]float64{"0": 1, "1": 1}, Applied: &applied,
		})
	}
	return out
}

func TestStreamReadings(t *testing.T) {
	runs := append(run3("off", 100, []float64{1, 2, 3}, 5), run3("on", 100, []float64{3, 4, 9}, 7)...)
	got := streamReadings(runs)
	for _, want := range []string{"on over off 2.000 (4.000 / 2.000 ms)", "p99 on minus off +2.00 ms", "keeps the state at this rate: yes", "per call: no anomaly"} {
		if !strings.Contains(got, want) {
			t.Errorf("readings %q lack %q", got, want)
		}
	}
	if !strings.HasSuffix(got, "heap in use missing") {
		t.Errorf("notes on clean runs, whose memory was not read: %q", got)
	}
	runs[4].ReplicaCPUPerCall["1"] = 2 // on's range of shard 1 is now 1 to 2, off's 1: they touch, which counts as overlap
	if got := streamReadings(runs); !strings.Contains(got, "no anomaly") {
		t.Errorf("touching ranges: %q", got)
	}
	for _, r := range runs[3:] {
		r.ReplicaCPUPerCall["1"] = 2
	}
	if got := streamReadings(runs); !strings.Contains(got, "measurement anomaly: shard 1") {
		t.Errorf("ranges apart: %q", got)
	}
	runs[0].Degraded = 0.02
	if got := streamReadings(runs); !strings.Contains(got, "not judged") {
		t.Errorf("a run over 1%% degraded: %q", got)
	}
	runs[5].P99 = 51
	if got := streamReadings(runs); !strings.Contains(got, "not shown at this rate (100-on-3 p99 51.00 ms)") {
		t.Errorf("an on run over 50 ms: %q", got)
	}
	little := 5.0
	runs[3].Applied = &little
	if got := streamReadings(runs); !strings.Contains(got, "the state applied 5 of the 10 events") {
		t.Errorf("a state that took too few events: %q", got)
	}
	runs[1].Commit = "d"
	if got := streamReadings(runs); !strings.Contains(got, "another boot or commit") {
		t.Errorf("a run of another commit: %q", got)
	}
	delete(runs[1].CPU, "gateway")
	if got := streamReadings(runs); !strings.Contains(got, "on over off missing") {
		t.Errorf("a missing reading: %q", got)
	}
	runs[2].Answers = 0
	if got := streamReadings(runs); !strings.Contains(got, "p99 on minus off missing") {
		t.Errorf("a run without answers: %q", got)
	}
}

// The table marks a degraded share above 1% and shows a share or a p99
// that has no request to come from as missing, not as 0.
func TestStreamTableMarks(t *testing.T) {
	runs := run3("on", 100, []float64{1, 1, 1}, 5)
	runs[0].Degraded = 0.02
	runs[1].Answers = 0
	runs[2].Events, runs[2].EventP99 = 0, 0
	var b strings.Builder
	streamTable(&b, runs, false)
	row := map[string]string{}
	for _, l := range strings.Split(b.String(), "\n") {
		if f := strings.Split(l, " | "); len(f) > 9 {
			row[f[1]] = strings.Join(f[6:10], " | ")
		}
	}
	for name, want := range map[string]string{
		"100-on-1": "5.00 | 2.00% (above 1%) | 10 | 0.00",
		"100-on-2": "missing | missing | 10 | 0.00",
		"100-on-3": "5.00 | 0.00% | 0 | missing",
	} {
		if row[name] != want {
			t.Errorf("%s: p99, degraded, events and their p99 %q, want %q", name, row[name], want)
		}
	}
}

// Phase 2 judges each group by its own runs, and refuses runs of another
// boot.
func TestStreamNext(t *testing.T) {
	var runs []streamRun
	for _, rate := range []float64{100, 200, 400, 800} {
		runs = append(runs, run3("off", rate, []float64{1, 1, 1}, 5)...)
		p99 := 5.0
		if rate == 800 {
			p99 = 120 // on's grid stops at 800
		}
		runs = append(runs, run3("on", rate, []float64{1, 1, 1}, p99)...)
	}
	if next, done, _, err := streamNext(runs, "off", "b"); err != nil || done || next != 1200 {
		t.Errorf("off: %v %v %v, want 1200", next, done, err)
	}
	if next, done, _, err := streamNext(runs, "on", "b"); err != nil || done || next != 600 {
		t.Errorf("on: %v %v %v, want the bisection's 600", next, done, err)
	}
	runs[0].Boot = "other"
	if _, _, _, err := streamNext(runs, "off", "b"); err == nil {
		t.Error("a run of another boot was taken")
	}
}

func TestBenchMedians(t *testing.T) {
	path := filepath.Join(t.TempDir(), "micro.txt")
	body := "boot: b\nBenchmarkRealStreamTake-1 \t 30000\t 4000 ns/op\t 210.5 heap-B/event\n" +
		"BenchmarkRealStreamTake-1 \t 30000\t 2000 ns/op\t 190.5 heap-B/event\nBenchmarkRealStreamTake-1 \t 30000\t 3000 ns/op\t 200.5 heap-B/event\nPASS\n"
	if err := os.WriteFile(path, []byte(body), 0o644); err != nil {
		t.Fatal(err)
	}
	got, err := benchMedians(path)
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{"| BenchmarkRealStreamTake-1 | ns/op | 3 | 3000 | 2000 | 4000 |", "| BenchmarkRealStreamTake-1 | heap-B/event | 3 | 200.5 | 190.5 | 210.5 |"} {
		if !strings.Contains(got, want) {
			t.Errorf("table lacks %q:\n%s", want, got)
		}
	}
}
