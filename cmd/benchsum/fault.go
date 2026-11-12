package main

import (
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"

	dto "github.com/prometheus/client_model/go"
)

// faultRun is one fault run judged by the criteria of docs/BENCHMARKS.md
// (Faults, setup and pass criteria (M6), as amended for M7), or, with fault
// none, a run for the breaker's openings, judged for validity only. Its
// phases are warmup [0, 30 s), healthy [30 s, inject), fault [inject,
// removal) and recovery [removal, end), from the due time of the run's
// first request; inject and removal are when the script acted.
type faultRun struct {
	Name    string           `json:"name"`
	Run     string           `json:"run"`
	Attempt int              `json:"attempt"`
	Fault   string           `json:"fault"`
	Valid   bool             `json:"valid"`
	Invalid []string         `json:"invalid,omitempty"`
	Noisy   bool             `json:"noisy"`
	InjectS float64          `json:"inject_s"`
	RemoveS float64          `json:"remove_s"`
	Phases  map[string]stats `json:"phases"`
	Excess  map[string]float64
	// M6's recovery, with the window's degraded share within 1 point of the
	// healthy phase's either way, in whole s from removal; nil: none.
	WindowRecovery *int `json:"window_recovery_s"`
	// F1, F5 and F6: s from removal to annshard-1's return; nil: not back.
	Return *float64 `json:"replica_return_s,omitempty"`
	// The later of the two for F1, F5 and F6, else WindowRecovery; nil: not recovered.
	Recovery *float64 `json:"recovery_s"`
	// F1, F5 and F6: annshard-1's share of its shard's answered calls in
	// [210, 270 s), nil without both reads, and in each second.
	ReplicaShare  *float64   `json:"replica_share_210_270_s,omitempty"`
	ReplicaShares []*float64 `json:"replica_share_by_second,omitempty"`
	// Whether the gateway counted ejections, and with it the ejections
	// in the healthy phase: from the last read before 30 s to the last
	// before 90 s (nil without both reads).
	OutlierDetection bool     `json:"outlier_detection"`
	HealthyEjections *float64 `json:"healthy_ejections,omitempty"`
	// F5: the share of answers with shards_partial in [0, 13 s), [13, 15 s)
	// and [15 s, removal) after the pause, and the excess of each reason in the last.
	Silent       []float64                     `json:"silent_shards_partial,omitempty"`
	SilentExcess map[string]float64            `json:"silent_excess,omitempty"`
	Hedges       map[string]map[string]float64 `json:"hedges"`                  // per phase from 30 s, by shard/result
	Failovers    map[string]map[string]float64 `json:"failovers"`               // per phase from 30 s, by replica
	LagDrainedS  *float64                      `json:"lag_drained_s,omitempty"` // F4; nil: not drained
	Histories    string                        `json:"histories,omitempty"`     // F4: the property 9 check's result
	Breaker      []opening                     `json:"breaker_openings"`
	Pass         bool                          `json:"pass"`
	Failures     []string                      `json:"failures,omitempty"`
	Windows      []stats                       `json:"windows"` // each second from the first request
}

var phaseNames = []string{"warmup", "healthy", "fault", "recovery"}

// The fault is injected at 90 s and removed at 150 s, ms from the first
// request's due time: the criteria's phases are cut there, and their times
// after the injection or the removal count from there. The script's marks,
// a few ms later, are reported.
const (
	injectAt = 90_000
	removeAt = 150_000
)

// phaseStats describes each phase of samples, the run ending at end (ms).
func phaseStats(samples []sample, end float64) map[string]stats {
	bounds := []float64{0, 30_000, injectAt, removeAt, end}
	out := map[string]stats{}
	for i, p := range phaseNames {
		out[p] = describe(samples, bounds[i], bounds[i+1])
	}
	return out
}

// silentWindows are F5's intervals after the pause, ms.
var silentWindows = [][2]float64{{0, 13_000}, {13_000, 15_000}, {15_000, 60_000}}

// replicaFaults are the faults of annshard-1, judged by its share.
var replicaFaults = map[string]bool{"F1": true, "F5": true, "F6": true}

// summarizeFault reads the files of the run prefix, its samples (raw) and
// its per-second reads, and judges it.
func summarizeFault(prefix, raw, readsPath, fault string) (faultRun, error) {
	var r loadReport
	if err := readJSON(prefix+".json", &r); err != nil {
		return faultRun{}, err
	}
	samples, err := readSamples(raw)
	if err != nil {
		return faultRun{}, err
	}
	marks, err := readMarks(prefix + ".marks")
	if err != nil {
		return faultRun{}, err
	}
	start := float64(r.Start.UnixMilli())
	reads, err := readReads(readsPath, start)
	if err != nil {
		return faultRun{}, err
	}
	changes, err := readOpenings(prefix + ".breaker")
	if err != nil {
		return faultRun{}, err
	}
	inject, removal := marks["inject"]-start, marks["remove"]-start
	end := (r.Warmup + r.Duration) * 1000
	f := faultRun{Name: filepath.Base(prefix), Fault: fault, InjectS: inject / 1000, RemoveS: removal / 1000, Phases: phaseStats(samples, end)}
	f.Run, f.Attempt = runOf(f.Name)
	healthy, during := f.Phases["healthy"], f.Phases["fault"]
	f.Excess = excess(during, healthy)
	if t, ok := recovery(samples, healthy, removeAt, end); ok {
		f.WindowRecovery = &t
		v := float64(t)
		f.Recovery = &v
	}
	if replicaFaults[fault] {
		endS := int(end / 1000)
		if v, ok := reads.share(210, 270); ok {
			f.ReplicaShare = &v
		}
		f.ReplicaShares = reads.shares(endS)
		f.Return = reads.returned(removeAt/1000, endS)
		f.Recovery = later(f.WindowRecovery, f.Return)
	}
	if f.OutlierDetection = reads.outlierDetection(); f.OutlierDetection {
		if v, ok := reads.deltaBefore("recserve_shard_ejections_total", nil, 30_000, injectAt); ok {
			f.HealthyEjections = &v
		}
	}
	f.Breaker = reads.openings(changes)
	f.Windows = windows(samples, 0, end)
	if f.Noisy, err = noisy(prefix); err != nil {
		return faultRun{}, err
	}
	if r.Achieved < 0.99*r.Target {
		f.Invalid = append(f.Invalid, fmt.Sprintf("achieved %.1f/s", r.Achieved))
	}
	if healthy.Errors > 0 || healthy.P99 > 50 || healthy.Degraded > 0.01 {
		f.Invalid = append(f.Invalid, fmt.Sprintf("healthy phase: %d errors, p99 %.2f ms, %.2f%% degraded", healthy.Errors, healthy.P99, 100*healthy.Degraded))
	}
	if f.Noisy {
		f.Invalid = append(f.Invalid, "noise baseline flagged")
	}
	f.Valid = len(f.Invalid) == 0
	if err := f.readCounters(prefix); err != nil {
		return faultRun{}, err
	}
	if fault == "F5" {
		silent := describe(samples, injectAt+15_000, injectAt+60_000)
		f.SilentExcess = excess(silent, healthy)
		for _, w := range silentWindows {
			f.Silent = append(f.Silent, describe(samples, injectAt+w[0], injectAt+w[1]).Reasons["shards_partial"])
		}
	}
	if fault == "F4" {
		if f.LagDrainedS, err = lagDrained(prefix+".lag", start+removeAt); err != nil {
			return faultRun{}, err
		}
		data, err := os.ReadFile(prefix + ".histories")
		if err != nil {
			return faultRun{}, err
		}
		f.Histories = strings.TrimSpace(string(data))
	}
	f.verdict()
	return f, nil
}

// verdict judges f by its fault's criteria, if it is a valid fault run: an
// invalid run is neither a pass nor a fail.
func (f *faultRun) verdict() {
	if f.Fault != "none" && f.Valid {
		f.Failures = judge(*f)
		f.Pass = len(f.Failures) == 0
	}
}

// attemptName matches the name of an attempt, its run's and its number.
var attemptName = regexp.MustCompile(`^(.*-run[0-9]+)-a([0-9]+)$`)

// runOf returns the run and the attempt of the attempt named name; a name
// without an attempt is the run's only one.
func runOf(name string) (string, int) {
	m := attemptName.FindStringSubmatch(name)
	if m == nil {
		return name, 1
	}
	n, err := strconv.Atoi(m[2])
	if err != nil {
		return name, 1
	}
	return m[1], n
}

// readCounters takes the hedges and failovers of each phase from 30 s from
// the gateway's metrics read at the phase boundaries (b0 to b3).
func (f *faultRun) readCounters(prefix string) error {
	var m [4]metrics
	for i := range m {
		var err error
		if m[i], err = readMetrics(fmt.Sprintf("%s.b%d.metrics", prefix, i)); err != nil {
			return err
		}
	}
	f.Hedges, f.Failovers = map[string]map[string]float64{}, map[string]map[string]float64{}
	for i, p := range phaseNames[1:] {
		f.Hedges[p], f.Failovers[p] = map[string]float64{}, map[string]float64{}
		for _, x := range m[i+1]["recserve_shard_hedges_total"].GetMetric() {
			shard, result := label(x.GetLabel(), "shard"), label(x.GetLabel(), "result")
			want := map[string]string{"shard": shard, "result": result}
			f.Hedges[p][shard+"/"+result] = x.GetCounter().GetValue() - m[i].sum("recserve_shard_hedges_total", want)
		}
		before := m[i].by("recserve_shard_failovers_total", "replica")
		for r, v := range m[i+1].by("recserve_shard_failovers_total", "replica") {
			f.Failovers[p][r] = v - before[r]
		}
	}
	return nil
}

// phaseAt names the phase that holds s seconds from the first request, the
// phases cut at 90 and 150 s as the criteria are, not at the script's marks.
func (f faultRun) phaseAt(s float64) string {
	switch {
	case s < 0:
		return "before the load"
	case s < 30:
		return "warmup"
	case s < injectAt/1000:
		return "healthy"
	case s < removeAt/1000:
		return "fault"
	case s < float64(len(f.Windows)):
		return "recovery"
	}
	return "after the load"
}

// judge returns the criteria f fails; a valid run passes with none.
func judge(f faultRun) []string {
	var out []string
	fail := func(format string, a ...any) { out = append(out, fmt.Sprintf(format, a...)) }
	for _, p := range phaseNames {
		if n := f.Phases[p].Errors; n > 0 {
			fail("%d errors in the %s phase", n, p)
		}
	}
	if p := f.Phases["fault"].P99; p > 50 {
		fail("fault phase p99 %.2f ms", p)
	}
	excessBut := func(ex map[string]float64, allowed string) {
		for r, v := range ex {
			if r != allowed && v > 0.01 {
				fail("%s exceeds the healthy phase's share by %.2f points", r, 100*v)
			}
		}
	}
	recoveredWithin := func(s float64) {
		if f.Recovery == nil || *f.Recovery > s {
			fail("not recovered within %g s", s)
		}
	}
	replicaBack := func() {
		switch {
		case f.ReplicaShare == nil:
			fail("no reads at 210 and 270 s, or no call answered between them")
		case *f.ReplicaShare < shareLate:
			fail("annshard-1's share in [210, 270 s) %.2f%%", 100**f.ReplicaShare)
		}
		if f.Return == nil {
			fail("annshard-1 not back: no 10 s with a share of at least 30%% in each second")
		}
	}
	// With outlier detection: no ejection in the healthy phase, and the
	// excess of shards_partial in the span the fault names, if any, at most
	// 1 point.
	outliers := func(span string, partial float64) {
		if !f.OutlierDetection {
			return
		}
		switch {
		case f.HealthyEjections == nil:
			fail("no reads before 30 and 90 s for the ejections")
		case *f.HealthyEjections != 0:
			fail("%.0f ejections in the healthy phase", *f.HealthyEjections)
		}
		if span != "" && partial > 0.01 {
			fail("shards_partial in %s exceeds the healthy phase's share by %.2f points", span, 100*partial)
		}
	}
	switch f.Fault {
	case "F1":
		excessBut(f.Excess, "")
		recoveredWithin(10)
		replicaBack()
	case "F2":
		excessBut(f.Excess, "shards_partial")
		recoveredWithin(5)
		outliers("", 0)
	case "F3":
		excessBut(f.Excess, "history_unavailable")
		recoveredWithin(5)
	case "F4":
		for _, p := range phaseNames {
			if v := f.Phases[p].P99; v > 50 {
				fail("%s phase p99 %.2f ms", p, v)
			}
		}
		excessBut(f.Excess, "")
		if f.LagDrainedS == nil || *f.LagDrainedS > 30 {
			fail("consumer lag not drained within 30 s")
		}
		if f.Histories != "ok" {
			fail("histories: %s", f.Histories)
		}
	case "F5":
		excessBut(f.SilentExcess, "")
		recoveredWithin(10)
		replicaBack()
		var partial float64
		if len(f.Silent) > 0 {
			partial = f.Silent[0] - f.Phases["healthy"].Reasons["shards_partial"]
		}
		outliers("[0, 13 s) after the pause", partial)
	case "F6":
		recoveredWithin(5)
		replicaBack()
		outliers("the fault phase", f.Excess["shards_partial"])
	default:
		fail("unknown fault %q", f.Fault)
	}
	return out
}

func label(ls []*dto.LabelPair, name string) string {
	for _, l := range ls {
		if l.GetName() == name {
			return l.GetValue()
		}
	}
	return ""
}

// readMarks reads the script's lines "inject <unix ms>" and "remove <unix ms>".
func readMarks(path string) (map[string]float64, error) {
	list, err := readMarkList(path)
	if err != nil {
		return nil, err
	}
	out := map[string]float64{}
	for _, m := range list {
		out[m.name] = m.ms
	}
	for _, n := range []string{"inject", "remove"} {
		if _, ok := out[n]; !ok {
			return nil, fmt.Errorf("%s: no %s mark", path, n)
		}
	}
	return out, nil
}

// lagDrained reads lines of "<unix ms> <events accepted> <records applied>"
// and returns the seconds from removal (Unix ms: 150 s from the first
// request, as the criteria count) until the first reading at or after it
// with nothing left to apply; nil if none. The two counts are read one
// after the other, the gateway's first, so a consumer that has caught up
// may read as having applied more than was accepted.
func lagDrained(path string, removal float64) (*float64, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	for line := range strings.Lines(string(data)) {
		f := strings.Fields(line)
		if len(f) != 3 {
			continue
		}
		var v [3]float64
		for i := range v {
			if v[i], err = strconv.ParseFloat(f[i], 64); err != nil {
				return nil, fmt.Errorf("%s: %w", path, err)
			}
		}
		if v[0] >= removal && v[1] <= v[2] {
			s := (v[0] - removal) / 1000
			return &s, nil
		}
	}
	return nil, nil
}
