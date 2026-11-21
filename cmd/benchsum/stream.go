package main

import (
	"bufio"
	"cmp"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"maps"
	"math"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"time"
)

// The stream state's cost in serving (docs/BENCHMARKS.md, Stream state in
// serving; D062). A reading that could not be taken is nil, written as
// null and shown as "missing", never as 0.

// streamRun is one run of the stream scenario: a sweep run and what the
// stream state adds to it.
type streamRun struct {
	runSummary
	Group   string `json:"group"`   // off or on
	Version string `json:"version"` // the bundle the group serves
	Boot    string `json:"boot"`
	Commit  string `json:"commit"` // the commit the binaries and images were built from, "-dirty" if the tree differed
	Image   string `json:"image"`  // the gateway container's image
	// Each container's CPU time over the window per recommendation answered,
	// for the containers read at both ends of the window.
	CPU map[string]float64 `json:"cpu_ms_per_recommendation"`
	// Each shard's replicas' CPU time together over the window per call
	// the gateway made to them, hedges included, by shard; and the second
	// replicas asked per recommendation answered.
	ReplicaCPUPerCall map[string]float64 `json:"replica_cpu_ms_per_call"`
	Hedges            *float64           `json:"hedges_per_recommendation"`
	Memory            memory             `json:"memory"`
	ReadyS            *float64           `json:"ready_s"` // from the gateway container's start to its log line that it serves
	// The stream counters over the window and the records left to read at
	// its end, in the on group.
	Applied   *float64 `json:"applied"`
	Duplicate *float64 `json:"duplicate"`
	Poison    *float64 `json:"poison"`
	Lag       *float64 `json:"lag_records"`
}

// memory is the gateway's, in bytes: resident and Go heap in use just
// before loadgen starts and when the window ends, and its peak resident
// memory at the end of the run.
type memory struct {
	ReadyRSS  *float64 `json:"ready_rss"`
	ReadyHeap *float64 `json:"ready_heap_inuse"`
	EndRSS    *float64 `json:"end_rss"`
	EndHeap   *float64 `json:"end_heap_inuse"`
	PeakRSS   *float64 `json:"peak_rss"`
}

// value returns the value of the series of name whose labels include want,
// a histogram's count of observations, summed over them; nil if there is
// none.
func (m metrics) value(name string, want map[string]string) *float64 {
	var total float64
	found := false
	for _, x := range m[name].GetMetric() {
		if has(x, want) {
			found = true
			total += x.GetGauge().GetValue() + x.GetCounter().GetValue() + x.GetUntyped().GetValue() + float64(x.GetHistogram().GetSampleCount())
		}
	}
	if !found {
		return nil
	}
	return &total
}

// optional returns a file's content, or nil if there is no file or it is
// empty.
func optional(path string) ([]byte, error) {
	data, err := os.ReadFile(path)
	if errors.Is(err, fs.ErrNotExist) {
		return nil, nil
	}
	if err != nil || len(data) == 0 {
		return nil, err
	}
	return data, nil
}

// optionalMetrics reads a scrape, nil if there is none.
func optionalMetrics(path string) (metrics, error) {
	data, err := optional(path)
	if data == nil || err != nil {
		return nil, err
	}
	return readMetrics(path)
}

// summarizeStream reads a run of the stream scenario of group, which serves
// version: the files of a sweep run (summarizeRun) and PREFIX.boot,
// the boot it ran on; PREFIX.restarts0 and .restarts1, each container's
// restart count once the services were up and at the end; PREFIX.started
// and .serving, the gateway container's start and its log line that it
// serves; PREFIX.ready.metrics, its scrape just before loadgen started;
// PREFIX.hwm, its process's VmHWM line at the end.
func summarizeStream(prefix, raw, group, version string) (streamRun, error) {
	base, err := summarizeRun(prefix, raw)
	if err != nil {
		return streamRun{}, err
	}
	s := streamRun{runSummary: base, Group: group, Version: version, CPU: map[string]float64{}, ReplicaCPUPerCall: map[string]float64{}}
	s.CPUPerRequest = nil // replaced by CPU, which leaves out a container not read at both ends
	if group != "off" && group != "on" {
		return streamRun{}, fmt.Errorf("group %q, want off or on", group)
	}
	for _, f := range []struct {
		suffix string
		v      *string
	}{{".boot", &s.Boot}, {".commit", &s.Commit}, {".image", &s.Image}} {
		data, err := os.ReadFile(prefix + f.suffix)
		if err != nil {
			return streamRun{}, err
		}
		*f.v = strings.TrimSpace(string(data))
	}
	problem := func(format string, a ...any) {
		s.Valid = false
		s.Problems = append(s.Problems, fmt.Sprintf(format, a...))
	}
	var r struct {
		Versions map[string]int `json:"versions"`
	}
	if err := readJSON(prefix+".json", &r); err != nil {
		return streamRun{}, err
	}
	if len(r.Versions) != 1 || r.Versions[version] == 0 {
		problem("versions answered %v, want %s only", r.Versions, version)
	}
	again, err := restarted(prefix)
	if err != nil {
		return streamRun{}, err
	}
	for _, c := range again {
		problem("container %s restarted or missing", c)
	}
	// CPU per recommendation only where both ends were read: a container
	// missing at one end has no reading, not the time since it started.
	c0, err := readCPU(prefix + ".at0.cpu")
	if err != nil {
		return streamRun{}, err
	}
	c1, err := readCPU(prefix + ".at1.cpu")
	if err != nil {
		return streamRun{}, err
	}
	for c, v := range c1 {
		if v0, ok := c0[c]; ok && s.Answers > 0 {
			s.CPU[c] = (v - v0) / 1e6 / float64(s.Answers)
		}
	}
	ready, err := optionalMetrics(prefix + ".ready.metrics")
	if err != nil {
		return streamRun{}, err
	}
	end, err := optionalMetrics(prefix + ".end.metrics")
	if err != nil {
		return streamRun{}, err
	}
	m0, err := readMetrics(prefix + ".at0.metrics")
	if err != nil {
		return streamRun{}, err
	}
	m1, err := readMetrics(prefix + ".at1.metrics")
	if err != nil {
		return streamRun{}, err
	}
	s.Memory.ReadyRSS, s.Memory.ReadyHeap = ready.value("process_resident_memory_bytes", nil), ready.value("go_memstats_heap_inuse_bytes", nil)
	s.Memory.EndRSS, s.Memory.EndHeap = end.value("process_resident_memory_bytes", nil), end.value("go_memstats_heap_inuse_bytes", nil)
	if s.Memory.PeakRSS, err = readHWM(prefix + ".hwm"); err != nil {
		return streamRun{}, err
	}
	if s.ReadyS, err = readyTime(prefix); err != nil {
		return streamRun{}, err
	}
	// Docker gives the start of a restarted container's last process only.
	if r0, err := readCPU(prefix + ".restarts0"); err != nil {
		return streamRun{}, err
	} else if r0["gateway"] != 0 {
		s.ReadyS = nil
	}
	s.replicas(m0, m1, c0, c1)
	// The stream counters exist exactly when the gateway keeps the state.
	const counter = "recserve_stream_events_total"
	kept := m1.value(counter, nil) != nil
	switch {
	case group == "on" && !kept:
		problem("the gateway keeps no stream state")
	case group == "off" && kept:
		problem("the gateway keeps a stream state")
	case group == "on":
		for _, c := range []struct {
			result string
			v      **float64
		}{{"applied", &s.Applied}, {"duplicate", &s.Duplicate}, {"poison", &s.Poison}} {
			a, b := m0.value(counter, map[string]string{"result": c.result}), m1.value(counter, map[string]string{"result": c.result})
			if a != nil && b != nil {
				d := *b - *a
				*c.v = &d
			}
		}
		s.Lag = end.value("recserve_stream_lag_records", nil)
	}
	return s, nil
}

// restarted returns the containers whose restart count in PREFIX.restarts1,
// read at the end of a run, differs from PREFIX.restarts0, read once the
// services were up, and those missing from either or read as -1, not
// found.
func restarted(prefix string) ([]string, error) {
	r0, err := readCPU(prefix + ".restarts0")
	if err != nil {
		return nil, err
	}
	r1, err := readCPU(prefix + ".restarts1")
	if err != nil {
		return nil, err
	}
	names := map[string]bool{}
	for c := range r0 {
		names[c] = true
	}
	for c := range r1 {
		names[c] = true
	}
	var out []string
	for _, c := range slices.Sorted(maps.Keys(names)) {
		a, okA := r0[c]
		b, okB := r1[c]
		if !okA || !okB || a != b || a < 0 {
			out = append(out, c)
		}
	}
	return out, nil
}

// replicas sets the replicas' CPU per call and the hedges per
// recommendation. A shard's replicas are the containers its calls name
// (recserve_shard_calls_total, replica "<container>:<port>"); its calls are
// the observations of recserve_shard_search_seconds.
func (s *streamRun) replicas(m0, m1 metrics, c0, c1 map[string]float64) {
	containers := map[string]map[string]bool{}
	for _, x := range m1["recserve_shard_calls_total"].GetMetric() {
		var shard, replica string
		for _, l := range x.GetLabel() {
			switch l.GetName() {
			case "shard":
				shard = l.GetValue()
			case "replica":
				replica, _, _ = strings.Cut(l.GetValue(), ":")
			}
		}
		if containers[shard] == nil {
			containers[shard] = map[string]bool{}
		}
		containers[shard][replica] = true
	}
	for shard, cs := range containers {
		a, b := m0.value("recserve_shard_search_seconds", map[string]string{"shard": shard}), m1.value("recserve_shard_search_seconds", map[string]string{"shard": shard})
		if a == nil || b == nil || *b <= *a {
			continue
		}
		var cpu float64
		read := true
		for c := range cs {
			v0, ok0 := c0[c]
			v1, ok1 := c1[c]
			read = read && ok0 && ok1
			cpu += v1 - v0
		}
		if read {
			s.ReplicaCPUPerCall[shard] = cpu / 1e6 / (*b - *a)
		}
	}
	var asked float64
	for _, r := range []string{"won", "lost"} {
		a, b := m0.value("recserve_shard_hedges_total", map[string]string{"result": r}), m1.value("recserve_shard_hedges_total", map[string]string{"result": r})
		if a == nil || b == nil || s.Answers == 0 {
			return
		}
		asked += *b - *a
	}
	h := asked / float64(s.Answers)
	s.Hedges = &h
}

// readHWM reads a "VmHWM: <n> kB" line, nil if there is none.
func readHWM(path string) (*float64, error) {
	data, err := optional(path)
	if data == nil || err != nil {
		return nil, err
	}
	f := strings.Fields(string(data))
	if len(f) != 3 || f[0] != "VmHWM:" || f[2] != "kB" {
		return nil, fmt.Errorf("%s: %q", path, data)
	}
	kb, err := strconv.ParseFloat(f[1], 64)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", path, err)
	}
	b := 1024 * kb
	return &b, nil
}

// readyTime returns the seconds from PREFIX.started, the container's start
// as docker gives it, to the time of the log line in PREFIX.serving; nil if
// either is missing.
func readyTime(prefix string) (*float64, error) {
	started, err := optional(prefix + ".started")
	if started == nil || err != nil {
		return nil, err
	}
	serving, err := optional(prefix + ".serving")
	if serving == nil || err != nil {
		return nil, err
	}
	t0, err := time.Parse(time.RFC3339Nano, strings.TrimSpace(string(started)))
	if err != nil {
		return nil, err
	}
	var line struct {
		Time time.Time `json:"time"`
		Msg  string    `json:"msg"`
	}
	if err := json.Unmarshal(serving, &line); err != nil || line.Msg != "serving" {
		return nil, fmt.Errorf("%s: not the line that the gateway serves: %q", prefix+".serving", serving)
	}
	d := line.Time.Sub(t0).Seconds()
	return &d, nil
}

func readStreamRuns(dirs []string) ([]streamRun, error) {
	var out []streamRun
	for _, dir := range dirs {
		paths, err := filepath.Glob(filepath.Join(dir, "*.summary.json"))
		if err != nil {
			return nil, err
		}
		slices.Sort(paths)
		for _, p := range paths {
			var s streamRun
			if err := readJSON(p, &s); err != nil {
				return nil, err
			}
			if s.Group == "" {
				return nil, fmt.Errorf("%s: not a run of the stream scenario", p)
			}
			out = append(out, s)
		}
	}
	return out, nil
}

// medianOf returns the median of the values that were read, and how many
// were; NaN if none was.
func medianOf(vs []*float64) (float64, int) {
	var got []float64
	for _, v := range vs {
		if v != nil {
			got = append(got, *v)
		}
	}
	if len(got) == 0 {
		return math.NaN(), 0
	}
	slices.Sort(got)
	n := len(got)
	if n%2 == 1 {
		return got[n/2], n
	}
	return (got[n/2-1] + got[n/2]) / 2, n
}

func cpuOf(r streamRun, c string) *float64 {
	if v, ok := r.CPU[c]; ok {
		return &v
	}
	return nil
}

func show(v *float64, scale float64, format string) string {
	if v == nil {
		return "missing"
	}
	return fmt.Sprintf(format, *v*scale)
}

// streamContainers are the CPU columns, in order.
var streamContainers = []string{"gateway", "kafka", "ingest", "valkey", "annshard-0", "annshard-1", "annshard-2", "annshard-3"}

// shards are the shards whose replicas' CPU per call is a column.
var shards = []string{"0", "1"}

// p99Of is a run's p99 of the recommendations, nil if it answered none.
func p99Of(r streamRun) *float64 {
	if r.Answers == 0 {
		return nil
	}
	return &r.P99
}

// eventP99Of is a run's p99 of /v1/events, nil if none was accepted.
func eventP99Of(r streamRun) *float64 {
	if r.Events == 0 {
		return nil
	}
	return &r.EventP99
}

// degradedOf is a run's share of degraded recommendations, marked above 1%,
// and missing if it answered none.
func degradedOf(r streamRun) string {
	if r.Answers == 0 {
		return "missing"
	}
	if r.Degraded > 0.01 {
		return fmt.Sprintf("%.2f%% (above 1%%)", 100*r.Degraded)
	}
	return fmt.Sprintf("%.2f%%", 100*r.Degraded)
}

// streamTable writes, per rate, each run of both groups and, with
// readings, the readings of docs/BENCHMARKS.md, Stream state in serving.
func streamTable(w *strings.Builder, runs []streamRun, readings bool) {
	byRate := map[float64][]streamRun{}
	for _, r := range runs {
		byRate[r.Rate] = append(byRate[r.Rate], r)
	}
	fmt.Fprintf(w, "| Rate | Run | Boot | Commit | Valid | Noisy | p99 ms | Degraded | Events | Events p99 ms |")
	for _, c := range streamContainers {
		fmt.Fprintf(w, " %s ms CPU per recommendation |", c)
	}
	for _, sh := range shards {
		fmt.Fprintf(w, " shard %s replicas ms CPU per call |", sh)
	}
	fmt.Fprintln(w, " Hedges per recommendation | RSS ready MB | Heap ready MB | RSS end MB | Heap end MB | Peak RSS MB | Ready s | Applied | Duplicate | Poison | Lag |")
	fmt.Fprintf(w, "|%s\n", strings.Repeat("---|", 10+len(streamContainers)+len(shards)+11))
	rates := slices.Sorted(maps.Keys(byRate))
	for _, rate := range rates {
		rs := byRate[rate]
		slices.SortFunc(rs, func(a, b streamRun) int { return cmp.Or(cmp.Compare(a.Group, b.Group), cmp.Compare(a.Name, b.Name)) })
		for _, r := range rs {
			fmt.Fprintf(w, "| %.0f | %s | %.8s | %.12s | %v | %v | %s | %s | %d | %s |", rate, r.Name, r.Boot, r.Commit, r.Valid, r.Noisy,
				show(p99Of(r), 1, "%.2f"), degradedOf(r), r.Events, show(eventP99Of(r), 1, "%.2f"))
			for _, c := range streamContainers {
				fmt.Fprintf(w, " %s |", show(cpuOf(r, c), 1, "%.3f"))
			}
			for _, sh := range shards {
				fmt.Fprintf(w, " %s |", show(perCall(r, sh), 1, "%.4f"))
			}
			m := r.Memory
			fmt.Fprintf(w, " %s | %s | %s | %s | %s | %s | %s |", show(r.Hedges, 1, "%.4f"), show(m.ReadyRSS, 1e-6, "%.0f"), show(m.ReadyHeap, 1e-6, "%.0f"),
				show(m.EndRSS, 1e-6, "%.0f"), show(m.EndHeap, 1e-6, "%.0f"), show(m.PeakRSS, 1e-6, "%.0f"), show(r.ReadyS, 1, "%.2f"))
			if r.Group == "on" {
				fmt.Fprintf(w, " %s | %s | %s | %s |\n", show(r.Applied, 1, "%.0f"), show(r.Duplicate, 1, "%.0f"), show(r.Poison, 1, "%.0f"), show(r.Lag, 1, "%.0f"))
			} else {
				fmt.Fprintln(w, " | | | |")
			}
		}
	}
	if !readings {
		return
	}
	fmt.Fprintln(w)
	for _, rate := range rates {
		fmt.Fprintf(w, "%.0f recommend requests/s: %s\n", rate, streamReadings(byRate[rate]))
	}
}

func perCall(r streamRun, shard string) *float64 {
	if v, ok := r.ReplicaCPUPerCall[shard]; ok {
		return &v
	}
	return nil
}

// streamReadings returns a rate's readings 1 to 4, from the runs of one
// boot. A reading that needs a value that is missing is missing.
func streamReadings(rs []streamRun) string {
	var on, off []streamRun
	var notes []string
	few := true // every run with at most 1% of its recommendations degraded
	for _, r := range rs {
		if r.Group == "on" {
			on = append(on, r)
		} else {
			off = append(off, r)
		}
		if !r.Valid || r.Noisy {
			notes = append(notes, fmt.Sprintf("%s valid %v noisy %v", r.Name, r.Valid, r.Noisy))
		}
		if r.Boot != rs[0].Boot || r.Commit != rs[0].Commit {
			notes = append(notes, fmt.Sprintf("%s from another boot or commit", r.Name))
		}
		if r.Group == "on" && (r.Applied == nil || *r.Applied < 0.99*float64(r.Events)) {
			notes = append(notes, fmt.Sprintf("%s: the state applied %s of the %d events accepted in the window", r.Name, show(r.Applied, 1, "%.0f"), r.Events))
		}
		few = few && r.Degraded <= 0.01
	}
	if len(on) != 3 || len(off) != 3 {
		notes = append(notes, fmt.Sprintf("%d on and %d off runs, want 3 each", len(on), len(off)))
	}
	// med is the median of f over runs, false if a run has no value.
	med := func(runs []streamRun, f func(streamRun) *float64) (float64, bool) {
		vs := make([]*float64, len(runs))
		for i, r := range runs {
			vs[i] = f(r)
		}
		v, n := medianOf(vs)
		return v, n > 0 && n == len(runs)
	}
	ratio := func(c string) string {
		f := func(r streamRun) *float64 { return cpuOf(r, c) }
		a, okA := med(on, f)
		b, okB := med(off, f)
		if !okA || !okB {
			return "missing"
		}
		return fmt.Sprintf("%.3f (%.3f / %.3f ms)", a/b, a, b)
	}
	diff := func(f func(streamRun) *float64, scale float64, format string) string {
		a, okA := med(on, f)
		b, okB := med(off, f)
		if !okA || !okB {
			return "missing"
		}
		return fmt.Sprintf(format, (a-b)*scale)
	}
	var misses []string
	if len(on) != 3 {
		misses = append(misses, fmt.Sprintf("%d on runs", len(on)))
	}
	for _, r := range on {
		for _, c := range []struct {
			miss bool
			what string
		}{{!r.Valid, "invalid"}, {r.Noisy, "noisy"}, {r.Answers == 0, "no recommendation answered"},
			{r.Answers > 0 && r.P99 > 50, fmt.Sprintf("p99 %.2f ms", r.P99)}, {r.Degraded > 0.01, fmt.Sprintf("%.2f%% degraded", 100*r.Degraded)}} {
			if c.miss {
				misses = append(misses, r.Name+" "+c.what)
			}
		}
	}
	kept := "yes"
	if len(misses) > 0 {
		kept = "not shown at this rate (" + strings.Join(misses, ", ") + ")"
	}
	replicas := "not judged: a run has more than 1% of its recommendations degraded"
	if few {
		var anomalies []string
		for _, sh := range shards {
			aLo, aHi, okA := perCallRange(on, sh)
			bLo, bHi, okB := perCallRange(off, sh)
			switch {
			case !okA || !okB:
				anomalies = append(anomalies, "shard "+sh+" missing")
			case aHi < bLo || bHi < aLo:
				anomalies = append(anomalies, "shard "+sh)
			}
		}
		replicas = "no anomaly"
		if len(anomalies) > 0 {
			replicas = "measurement anomaly: " + strings.Join(anomalies, ", ")
		}
	}
	out := fmt.Sprintf("(1) gateway CPU per recommendation, on over off %s; kafka %s; p99 on minus off %s; (2) the gateway keeps the state at this rate: %s; (3) replicas' CPU per call: %s; (4) on minus off, RSS before loadgen %s, heap in use %s",
		ratio("gateway"), ratio("kafka"), diff(p99Of, 1, "%+.2f ms"), kept, replicas,
		diff(func(r streamRun) *float64 { return r.Memory.ReadyRSS }, 1e-6, "%+.0f MB"),
		diff(func(r streamRun) *float64 { return r.Memory.ReadyHeap }, 1e-6, "%+.0f MB"))
	if len(notes) > 0 {
		out += "; " + strings.Join(notes, "; ")
	}
	return out
}

// perCallRange returns the lowest and highest replica CPU per call of a
// shard over runs, false if a run has no value.
func perCallRange(runs []streamRun, shard string) (lo, hi float64, ok bool) {
	var vs []float64
	for _, r := range runs {
		if v, found := r.ReplicaCPUPerCall[shard]; found {
			vs = append(vs, v)
		}
	}
	if len(vs) == 0 || len(vs) < len(runs) {
		return 0, 0, false
	}
	return slices.Min(vs), slices.Max(vs), true
}

// streamNext returns phase 2's next rate for a group, from its runs on
// boot, or done with its headline: the grid and bisection of Load tests
// (nextRate), to 4,000 requests/s. A run from another boot is an error.
func streamNext(runs []streamRun, group, boot string) (next float64, done bool, headline float64, err error) {
	byRate := map[float64][]runSummary{}
	for _, r := range runs {
		if r.Group != group {
			continue
		}
		if r.Boot != boot {
			return 0, false, 0, fmt.Errorf("%s ran on boot %s, not %s", r.Name, r.Boot, boot)
		}
		byRate[r.Rate] = append(byRate[r.Rate], r.runSummary)
	}
	verdicts := map[float64]rateVerdict{}
	for rate, rs := range byRate {
		if len(rs) >= 3 {
			meets, stop, _ := verdict(rs)
			verdicts[rate] = rateVerdict{meets, stop}
		}
	}
	next, done, headline = nextRate(verdicts, true, 4000)
	return next, done, headline, nil
}

// streamSwapRun is the run of swaps: each swap and the windows' latency.
type streamSwapRun struct {
	Name      string       `json:"name"`
	Valid     bool         `json:"valid"`
	Noisy     bool         `json:"noisy"`
	Boot      string       `json:"boot"`
	Restarted []string     `json:"restarted"` // containers that restarted during the run
	Swaps     []streamSwap `json:"swaps"`
	PeakRSS   *float64     `json:"peak_rss"`
	Swap      swapGroup    `json:"swap_windows"`  // the 1 s windows that overlap a swap
	Others    swapGroup    `json:"other_windows"` // the rest of the measured window
}

type streamSwap struct {
	Start, End float64  // ms from the first request's due time
	Version    string   `json:"model_version"` // as the answer names it; empty if the swap failed
	PrepareMS  *float64 `json:"prepare_ms"`
	DrainMS    *float64 `json:"drain_ms"`
	Before     scrape   `json:"before"` // just before the admin request
	After      scrape   `json:"after"`  // just after its answer
	Answer     string   `json:"answer"`
}

// scrape is the gateway's resident memory, Go heap in use and records left
// to read, in one scrape of its metrics.
type scrape struct {
	RSS  *float64 `json:"rss"`
	Heap *float64 `json:"heap_inuse"`
	Lag  *float64 `json:"lag_records"`
}

// summarizeStreamSwap reads a swap run: its sweep-run files, PREFIX.swaps,
// lines of "<request ms> <answer ms> <answer>", and each swap's scrapes
// PREFIX.swapN.before.metrics and .after.metrics.
func summarizeStreamSwap(prefix, raw string) (streamSwapRun, error) {
	var r loadReport
	if err := readJSON(prefix+".json", &r); err != nil {
		return streamSwapRun{}, err
	}
	samples, err := readSamples(raw)
	if err != nil {
		return streamSwapRun{}, err
	}
	s := streamSwapRun{Name: filepath.Base(prefix), Valid: r.Valid}
	if s.Noisy, err = noisy(prefix); err != nil {
		return streamSwapRun{}, err
	}
	boot, err := os.ReadFile(prefix + ".boot")
	if err != nil {
		return streamSwapRun{}, err
	}
	s.Boot = strings.TrimSpace(string(boot))
	if s.PeakRSS, err = readHWM(prefix + ".hwm"); err != nil {
		return streamSwapRun{}, err
	}
	again, err := restarted(prefix)
	if err != nil {
		return streamSwapRun{}, err
	}
	s.Restarted = again
	s.Valid = s.Valid && len(again) == 0
	f, err := os.Open(prefix + ".swaps")
	if err != nil {
		return streamSwapRun{}, err
	}
	defer f.Close() //nolint:errcheck // read only
	start := float64(r.Start.UnixMilli())
	sc := bufio.NewScanner(f)
	for sc.Scan() {
		fields := strings.SplitN(sc.Text(), " ", 3)
		if len(fields) < 2 {
			return streamSwapRun{}, fmt.Errorf("%s: %q", prefix+".swaps", sc.Text())
		}
		var w streamSwap
		var e1, e2 error
		w.Start, e1 = strconv.ParseFloat(fields[0], 64)
		w.End, e2 = strconv.ParseFloat(fields[1], 64)
		if err := errors.Join(e1, e2); err != nil {
			return streamSwapRun{}, fmt.Errorf("%s: %q: %w", prefix+".swaps", sc.Text(), err)
		}
		w.Start, w.End = w.Start-start, w.End-start
		if len(fields) == 3 {
			w.Answer = fields[2]
			_ = json.Unmarshal([]byte(fields[2]), &w) //nolint:errcheck // an answer that is not a swap's leaves the fields empty
		}
		if w.Version == "" {
			w.PrepareMS, w.DrainMS = nil, nil
			s.Valid = false
		}
		n := len(s.Swaps) + 1
		for _, side := range []struct {
			name string
			s    *scrape
		}{{"before", &w.Before}, {"after", &w.After}} {
			m, err := optionalMetrics(fmt.Sprintf("%s.swap%d.%s.metrics", prefix, n, side.name))
			if err != nil {
				return streamSwapRun{}, err
			}
			*side.s = scrape{m.value("process_resident_memory_bytes", nil), m.value("go_memstats_heap_inuse_bytes", nil), m.value("recserve_stream_lag_records", nil)}
		}
		s.Swaps = append(s.Swaps, w)
	}
	if err := sc.Err(); err != nil {
		return streamSwapRun{}, err
	}
	var in, out []sample
	for t := r.Warmup * 1000; t+1000 <= (r.Warmup+r.Duration)*1000; t += 1000 {
		group, list := &s.Others, &out
		if slices.ContainsFunc(s.Swaps, func(w streamSwap) bool { return t < w.End && t+1000 > w.Start }) {
			group, list = &s.Swap, &in
		}
		group.Windows++
		for _, x := range samples {
			if x.due >= t && x.due < t+1000 {
				*list = append(*list, x)
			}
		}
	}
	for _, g := range []struct {
		group *swapGroup
		list  []sample
	}{{&s.Swap, in}, {&s.Others, out}} {
		st := describe(g.list, math.Inf(-1), math.Inf(1))
		g.group.P99, g.group.Max = st.P99, st.Max
	}
	return s, nil
}

// benchMedians reads `go test -bench` output, ten counts or any number,
// and returns per benchmark and unit the median and range, in the order
// the benchmarks first appear.
func benchMedians(path string) (string, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return "", err
	}
	type key struct{ name, unit string }
	values := map[key][]float64{}
	var order []key
	for line := range strings.Lines(string(data)) {
		f := strings.Fields(line)
		if len(f) < 4 || !strings.HasPrefix(f[0], "Benchmark") {
			continue
		}
		for i := 2; i+1 < len(f); i += 2 {
			v, err := strconv.ParseFloat(f[i], 64)
			if err != nil {
				return "", fmt.Errorf("%s: %q: %w", path, line, err)
			}
			k := key{f[0], f[i+1]}
			if _, ok := values[k]; !ok {
				order = append(order, k)
			}
			values[k] = append(values[k], v)
		}
	}
	var b strings.Builder
	b.WriteString("| Benchmark | Unit | Counts | Median | Min | Max |\n|---|---|---|---|---|---|\n")
	for _, k := range order {
		vs := slices.Sorted(slices.Values(values[k]))
		ptrs := make([]*float64, len(vs))
		for i := range vs {
			ptrs[i] = &vs[i]
		}
		m, n := medianOf(ptrs)
		fmt.Fprintf(&b, "| %s | %s | %d | %g | %g | %g |\n", k.name, k.unit, n, m, vs[0], vs[len(vs)-1])
	}
	return b.String(), nil
}

// streamSwapTable writes a swap run: each swap, then the windows.
func streamSwapTable(w *strings.Builder, s streamSwapRun) {
	fmt.Fprintf(w, "%s: valid %v, noisy %v, boot %.8s, containers restarted %v, peak RSS %s MB\n\n", s.Name, s.Valid, s.Noisy, s.Boot, s.Restarted, show(s.PeakRSS, 1e-6, "%.0f"))
	fmt.Fprintln(w, "| Swap | To | Request s | Prepare ms | Drain ms | RSS before MB | after | Heap before MB | after | Lag before | after |")
	fmt.Fprintln(w, "|---|---|---|---|---|---|---|---|---|---|---|")
	for i, x := range s.Swaps {
		to := x.Version
		if to == "" {
			to = "failed: " + x.Answer
		}
		fmt.Fprintf(w, "| %d | %s | %.1f | %s | %s | %s | %s | %s | %s | %s | %s |\n", i+1, to, x.Start/1000, show(x.PrepareMS, 1, "%.0f"), show(x.DrainMS, 1, "%.0f"),
			show(x.Before.RSS, 1e-6, "%.0f"), show(x.After.RSS, 1e-6, "%.0f"), show(x.Before.Heap, 1e-6, "%.0f"), show(x.After.Heap, 1e-6, "%.0f"),
			show(x.Before.Lag, 1, "%.0f"), show(x.After.Lag, 1, "%.0f"))
	}
	fmt.Fprintf(w, "\nWindows that overlap a swap: %d, p99 %.2f ms, max %.2f ms; the others: %d, p99 %.2f ms, max %.2f ms\n",
		s.Swap.Windows, s.Swap.P99, s.Swap.Max, s.Others.Windows, s.Others.P99, s.Others.Max)
}
