package main

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	dto "github.com/prometheus/client_model/go"
)

// faultRun is one fault run judged by the criteria of docs/BENCHMARKS.md
// (Faults, setup and pass criteria). Its phases are healthy [30 s, inject),
// fault [inject, removal) and recovery [removal, end), from the due time of
// the run's first request; inject and removal are when the script acted.
type faultRun struct {
	Name     string           `json:"name"`
	Fault    string           `json:"fault"`
	Valid    bool             `json:"valid"`
	Invalid  []string         `json:"invalid,omitempty"`
	Noisy    bool             `json:"noisy"`
	InjectS  float64          `json:"inject_s"`
	RemoveS  float64          `json:"remove_s"`
	Phases   map[string]stats `json:"phases"`
	Excess   map[string]float64
	Recovery *int `json:"recovery_s"` // nil: not recovered
	// F5: the share of answers with shards_partial in [0, 13 s), [13, 15 s)
	// and [15 s, removal) after the pause, and the excess of each reason in the last.
	Silent       []float64                     `json:"silent_shards_partial,omitempty"`
	SilentExcess map[string]float64            `json:"silent_excess,omitempty"`
	Hedges       map[string]map[string]float64 `json:"hedges"`    // per phase, by shard/result
	Failovers    map[string]map[string]float64 `json:"failovers"` // per phase, by replica
	ReplicaCPU   *float64                      `json:"replica_cpu_last_60s_ms,omitempty"`
	LagDrainedS  *float64                      `json:"lag_drained_s,omitempty"` // F4; nil: not drained
	Histories    string                        `json:"histories,omitempty"`     // F4: the property 9 check's result
	Pass         bool                          `json:"pass"`
	Failures     []string                      `json:"failures,omitempty"`
	Windows      []stats                       `json:"windows"` // each second from the first request
}

var phaseNames = []string{"healthy", "fault", "recovery"}

func summarizeFault(prefix, raw, fault string) (faultRun, error) {
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
	inject, removal := marks["inject"]-start, marks["remove"]-start
	end := (r.Warmup + r.Duration) * 1000
	f := faultRun{Name: filepath.Base(prefix), Fault: fault, InjectS: inject / 1000, RemoveS: removal / 1000, Phases: map[string]stats{}}
	bounds := []float64{30_000, inject, removal, end}
	for i, p := range phaseNames {
		f.Phases[p] = describe(samples, bounds[i], bounds[i+1])
	}
	healthy, during := f.Phases["healthy"], f.Phases["fault"]
	f.Excess = excess(during, healthy)
	if t, ok := recovery(samples, healthy, removal, end); ok {
		f.Recovery = &t
	}
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
		silent := describe(samples, inject+15_000, removal)
		f.SilentExcess = excess(silent, healthy)
		for _, w := range [][2]float64{{0, 13_000}, {13_000, 15_000}, {15_000, removal - inject}} {
			f.Silent = append(f.Silent, describe(samples, inject+w[0], inject+w[1]).Reasons["shards_partial"])
		}
	}
	if fault == "F1" || fault == "F5" {
		c0, err0 := readCPU(prefix + ".late0.cpu")
		c1, err1 := readCPU(prefix + ".late1.cpu")
		if err := errors.Join(err0, err1); err != nil {
			return faultRun{}, err
		}
		v := (c1["annshard-1"] - c0["annshard-1"]) / 1e6
		f.ReplicaCPU = &v
	}
	if fault == "F4" {
		if f.LagDrainedS, err = lagDrained(prefix+".lag", marks["remove"]); err != nil {
			return faultRun{}, err
		}
		data, err := os.ReadFile(prefix + ".histories")
		if err != nil {
			return faultRun{}, err
		}
		f.Histories = strings.TrimSpace(string(data))
	}
	f.Failures = judge(f)
	f.Pass = len(f.Failures) == 0
	return f, nil
}

// readCounters takes each phase's hedges and failovers from the gateway's
// metrics read at the phase boundaries (b0 to b3).
func (f *faultRun) readCounters(prefix string) error {
	var m [4]metrics
	for i := range m {
		var err error
		if m[i], err = readMetrics(fmt.Sprintf("%s.b%d.metrics", prefix, i)); err != nil {
			return err
		}
	}
	f.Hedges, f.Failovers = map[string]map[string]float64{}, map[string]map[string]float64{}
	for i, p := range phaseNames {
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
	recoveredWithin := func(s int) {
		if f.Recovery == nil || *f.Recovery > s {
			fail("not recovered within %d s", s)
		}
	}
	replicaServes := func() {
		if f.ReplicaCPU == nil || *f.ReplicaCPU <= 0 {
			fail("annshard-1 used no CPU in the last 60 s")
		}
	}
	switch f.Fault {
	case "F1":
		excessBut(f.Excess, "")
		recoveredWithin(10)
		replicaServes()
	case "F2":
		excessBut(f.Excess, "shards_partial")
		recoveredWithin(5)
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
		replicaServes()
	case "F6":
		recoveredWithin(5)
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
// and returns the seconds from removal until the first reading at or after
// it with nothing left to apply; nil if none. The two counts are read one
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
