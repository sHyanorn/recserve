package main

import (
	"encoding/json"
	"fmt"
	"maps"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"time"
)

// opening is a change of the gateway's Valkey circuit breaker from closed
// to open, attributed by the Valkey calls that failed in the interval
// between reads that holds it and the one before (docs/BENCHMARKS.md,
// Valkey circuit breaker openings).
type opening struct {
	AtS         float64            `json:"at_s"`             // from the first request's due time
	FromS       float64            `json:"from_s"`           // the interval's first read
	ToS         float64            `json:"to_s"`             // and its last
	Failed      map[string]float64 `json:"failed,omitempty"` // Valkey calls in the interval by result other than ok
	Attribution string             `json:"attribution"`      // timeouts, errors, not attributed, or no reads
	GCPauseMs   float64            `json:"gc_pause_ms"`      // the gateway's, in the interval
	CPUMs       map[string]float64 `json:"cpu_ms,omitempty"` // each container's, in the interval
	// The median of each over the intervals of as many reads in [30, 90 s).
	MedianGCPauseMs float64            `json:"median_gc_pause_ms"`
	MedianCPUMs     map[string]float64 `json:"median_cpu_ms,omitempty"`
}

// readOpenings returns the Unix ms of each change from closed to open in
// the gateway's log lines of its breaker.
func readOpenings(path string) ([]float64, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var out []float64
	for line := range strings.Lines(string(data)) {
		var l struct {
			Time     time.Time
			From, To string
		}
		if err := json.Unmarshal([]byte(line), &l); err != nil {
			return nil, fmt.Errorf("%s: %w", path, err)
		}
		if l.From == "closed" && l.To == "open" {
			out = append(out, float64(l.Time.UnixNano())/1e6)
		}
	}
	return out, nil
}

// attribute names what made the breaker open from the failed calls by
// result: at least 5 timeouts and no error, at least 5 errors and no
// timeout, or neither.
func attribute(failed map[string]float64) string {
	t, e := failed["timeout"], failed["error"]
	switch {
	case t >= 5 && e == 0:
		return "timeouts"
	case e >= 5 && t == 0:
		return "errors"
	}
	return "not attributed"
}

// openings attributes each change to open at the Unix ms in changes.
func (r runReads) openings(changes []float64) []opening {
	var out []opening
	for _, c := range changes {
		o := opening{AtS: (c - r.t0) / 1000, Attribution: "no reads"}
		if k := slices.IndexFunc(r.list, func(rd read) bool { return rd.ms > c }); k > 0 {
			a, b := r.list[max(k-2, 0)], r.list[k]
			o.FromS, o.ToS = (a.ms-r.t0)/1000, (b.ms-r.t0)/1000
			o.Failed = map[string]float64{}
			before := a.m.by("recserve_valkey_calls_total", "result")
			for res, v := range b.m.by("recserve_valkey_calls_total", "result") {
				if d := v - before[res]; res != "ok" && d > 0 {
					o.Failed[res] = d
				}
			}
			o.Attribution = attribute(o.Failed)
			o.GCPauseMs, o.CPUMs = usage(a, b)
			o.MedianGCPauseMs, o.MedianCPUMs = r.medianUsage(k - max(k-2, 0))
		}
		out = append(out, o)
	}
	return out
}

// usage is the gateway's GC pause time and each container's CPU time from
// read a to read b, in ms. A container missing from either, or whose
// counter went back (it was restarted), is left out.
func usage(a, b read) (gc float64, cpu map[string]float64) {
	const name = "go_gc_duration_seconds_sum"
	gc = 1000 * (b.m.sum(name, nil) - a.m.sum(name, nil))
	cpu = map[string]float64{}
	for c, v := range b.cpu {
		if v0, ok := a.cpu[c]; ok && v >= v0 {
			cpu[c] = (v - v0) / 1e6
		}
	}
	return gc, cpu
}

// healthy reports whether the interval from read a to read b lies within
// [30, 90 s), the healthy phase.
func (r runReads) healthy(a, b read) bool {
	return a.ms-r.t0 >= 30_000 && b.ms-r.t0 < 90_000
}

// medianUsage is the median of usage over the intervals of n reads in a row
// within [30, 90 s).
func (r runReads) medianUsage(n int) (gc float64, cpu map[string]float64) {
	var gcs []float64
	cpus := map[string][]float64{}
	for i := 0; i+n < len(r.list); i++ {
		a, b := r.list[i], r.list[i+n]
		if !r.healthy(a, b) {
			continue
		}
		g, c := usage(a, b)
		gcs = append(gcs, g)
		for k, v := range c {
			cpus[k] = append(cpus[k], v)
		}
	}
	cpu = map[string]float64{}
	for k, v := range cpus {
		slices.Sort(v)
		cpu[k] = median(v)
	}
	if len(gcs) == 0 {
		return 0, cpu
	}
	slices.Sort(gcs)
	return median(gcs), cpu
}

// breakerTable writes a row per opening of the breaker in the fault runs of
// dir, in name order.
func breakerTable(w *strings.Builder, dir string) error {
	paths, err := filepath.Glob(filepath.Join(dir, "*.fault.json"))
	if err != nil {
		return err
	}
	slices.Sort(paths)
	fmt.Fprintln(w, "| Run | Valid | Opened at s | Phase | Reads s | Timeouts | Errors | Other failed | Attribution | GC pause ms (median) | CPU ms (median) |")
	fmt.Fprintln(w, "|---|---|---|---|---|---|---|---|---|---|---|")
	for _, p := range paths {
		var f faultRun
		if err := readJSON(p, &f); err != nil {
			return err
		}
		for _, o := range f.Breaker {
			var other, cpu []string
			for _, res := range slices.Sorted(maps.Keys(o.Failed)) {
				if res != "timeout" && res != "error" {
					other = append(other, fmt.Sprintf("%s %.0f", res, o.Failed[res]))
				}
			}
			for _, c := range slices.Sorted(maps.Keys(o.CPUMs)) {
				cpu = append(cpu, fmt.Sprintf("%s %.0f (%.0f)", c, o.CPUMs[c], o.MedianCPUMs[c]))
			}
			reads := "none"
			if o.Attribution != "no reads" {
				reads = fmt.Sprintf("%.3f to %.3f", o.FromS, o.ToS)
			}
			fmt.Fprintf(w, "| %s | %v | %.3f | %s | %s | %.0f | %.0f | %s | %s | %.2f (%.2f) | %s |\n", f.Name, f.Valid, o.AtS, f.phaseAt(o.AtS), reads,
				o.Failed["timeout"], o.Failed["error"], strings.Join(other, ", "), o.Attribution, o.GCPauseMs, o.MedianGCPauseMs, strings.Join(cpu, ", "))
		}
	}
	return nil
}
