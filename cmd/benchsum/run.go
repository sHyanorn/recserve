package main

import (
	"fmt"
	"math"
	"path/filepath"
	"slices"
	"strings"
)

// runSummary is one load run of a sweep (docs/BENCHMARKS.md, Load tests).
type runSummary struct {
	Name     string   `json:"name"`
	Rate     float64  `json:"rate"`
	Valid    bool     `json:"valid"` // loadgen's checks: 99% sent, nothing failed, every answer with arm and version
	Problems []string `json:"problems,omitempty"`
	Noisy    bool     `json:"noisy"`
	Achieved float64  `json:"achieved_rate"`
	stats
	PastBudget    float64            `json:"past_budget_share"` // history read or cache call past -budget-history, over recommend requests
	CacheHitRate  float64            `json:"cache_hit_rate"`
	CPUPerRequest map[string]float64 `json:"cpu_ms_per_request"` // each container's CPU time over the window, per answer
}

// summarizeRun reads the files of the run prefix (dir/name) and raw, its
// samples: the loadgen report, and the gateway's metrics and the
// containers' CPU time taken when the measurement started (at0) and ended
// (at1).
func summarizeRun(prefix, raw string) (runSummary, error) {
	var r loadReport
	if err := readJSON(prefix+".json", &r); err != nil {
		return runSummary{}, err
	}
	samples, err := readSamples(raw)
	if err != nil {
		return runSummary{}, err
	}
	m0, err := readMetrics(prefix + ".at0.metrics")
	if err != nil {
		return runSummary{}, err
	}
	m1, err := readMetrics(prefix + ".at1.metrics")
	if err != nil {
		return runSummary{}, err
	}
	c0, err := readCPU(prefix + ".at0.cpu")
	if err != nil {
		return runSummary{}, err
	}
	c1, err := readCPU(prefix + ".at1.cpu")
	if err != nil {
		return runSummary{}, err
	}
	n, err := noisy(prefix)
	if err != nil {
		return runSummary{}, err
	}
	s := runSummary{
		Name: filepath.Base(prefix), Rate: r.Target, Valid: r.Valid, Problems: r.Problems, Noisy: n, Achieved: r.Achieved,
		stats: describe(samples, r.Warmup*1000, inf), CPUPerRequest: map[string]float64{},
	}
	delta := func(name string, want map[string]string) float64 { return m1.sum(name, want) - m0.sum(name, want) }
	if requests := delta("recserve_http_requests_total", map[string]string{"route": "recommend"}); requests > 0 {
		late := delta("recserve_degraded_total", map[string]string{"reason": "history_unavailable"}) +
			delta("recserve_cache_lookups_total", map[string]string{"result": "error"})
		s.PastBudget = late / requests
	}
	if lookups := delta("recserve_cache_lookups_total", nil); lookups > 0 {
		s.CacheHitRate = delta("recserve_cache_lookups_total", map[string]string{"result": "hit"}) / lookups
	}
	for name, v := range c1 {
		if s.Answers > 0 {
			s.CPUPerRequest[name] = (v - c0[name]) / 1e6 / float64(s.Answers)
		}
	}
	return s, nil
}

const inf = 1e300

// verdict judges a rate by its runs: it meets the headline condition if
// every run is valid, unflagged and has p99 ≤ 50 ms, and the sweep stops
// there if a run is invalid or the median p99 exceeds 100 ms.
func verdict(runs []runSummary) (meets, stop bool, medianP99 float64) {
	if len(runs) == 0 {
		return false, true, 0
	}
	meets = meetsAsWritten(runs)
	p99 := make([]float64, len(runs))
	for i, r := range runs {
		p99[i] = r.P99
		meets = meets && r.Degraded <= 0.01 // amended in M6 (D054)
		stop = stop || !r.Valid
	}
	slices.Sort(p99)
	medianP99 = p99[len(p99)/2]
	return meets, stop || medianP99 > 100, medianP99
}

// meetsAsWritten is D005's headline condition before its amendment (D054):
// every run valid and unflagged with p99 ≤ 50 ms, whatever share degraded.
func meetsAsWritten(runs []runSummary) bool {
	for _, r := range runs {
		if !r.Valid || r.Noisy || r.P99 > 50 {
			return false
		}
	}
	return len(runs) > 0
}

// sweepTable writes one row per rate: the run with the median p99
// represents it, as docs/BENCHMARKS.md has it.
func sweepTable(w *strings.Builder, byRate map[float64][]runSummary, containers []string) {
	rates := make([]float64, 0, len(byRate))
	for r := range byRate {
		rates = append(rates, r)
	}
	slices.Sort(rates)
	fmt.Fprintf(w, "| Rate | Verdict | D005 as written | p50 | p90 | p99 | p99.9 | max | p99 of each run | Degraded, each run | Past budget | Cache hits |")
	for _, c := range containers {
		fmt.Fprintf(w, " %s ms CPU |", c)
	}
	fmt.Fprintf(w, " Noisy |\n|%s\n", strings.Repeat("---|", 13+len(containers)))
	for _, rate := range rates {
		runs := slices.Clone(byRate[rate])
		slices.SortFunc(runs, func(a, b runSummary) int { return cmpFloat(a.P99, b.P99) })
		meets, stop, _ := verdict(runs)
		word := map[bool]string{true: "meets", false: "misses"}
		v := word[meets]
		if stop {
			v += ", stop"
		}
		m := runs[len(runs)/2]
		var each, degraded, flags []string
		for _, r := range byRate[rate] {
			each = append(each, fmt.Sprintf("%.2f", r.P99))
			degraded = append(degraded, fmt.Sprintf("%.2f", 100*r.Degraded))
			flags = append(flags, fmt.Sprint(r.Noisy))
		}
		fmt.Fprintf(w, "| %.0f | %s | %s | %.2f | %.2f | %.2f | %.2f | %.2f | %s | %s%% | %.2f%% | %.1f%% |", rate, v, word[meetsAsWritten(runs)],
			m.P50, m.P90, m.P99, m.P999, m.Max, strings.Join(each, " / "), strings.Join(degraded, " / "), 100*m.PastBudget, 100*m.CacheHitRate)
		for _, c := range containers {
			fmt.Fprintf(w, " %.2f |", m.CPUPerRequest[c])
		}
		fmt.Fprintf(w, " %s |\n", strings.Join(flags, " / "))
	}
}

func cmpFloat(a, b float64) int {
	switch {
	case a < b:
		return -1
	case a > b:
		return 1
	}
	return 0
}

// rateVerdict is a rate's verdict once its three runs are done.
type rateVerdict struct{ meets, stop bool }

// nextRate returns the next rate of a sweep from the verdicts of the rates
// run so far (docs/BENCHMARKS.md, Load tests): the grid 100, 200, 400, 800,
// then steps of 400, up to the first rate that misses the headline
// condition or stops the sweep (D054); then, with bisect, halving between
// the highest grid rate that meets the condition and the next grid rate
// until the two are 50 apart. done comes with the headline, the lower of
// the two, when bisecting (0 if no rate meets).
func nextRate(verdicts map[float64]rateVerdict, bisect bool) (next float64, done bool, headline float64) {
	var grid []float64
	for g := 100.0; ; {
		grid = append(grid, g)
		v, ok := verdicts[g]
		if !ok {
			return g, false, 0
		}
		if v.stop || !v.meets {
			break
		}
		if g < 800 {
			g *= 2
		} else {
			g += 400
		}
	}
	if !bisect {
		return 0, true, 0
	}
	// The last grid rate does not meet the condition, so every rate that
	// does has a next one.
	var lo, hi float64
	for i, g := range grid {
		if verdicts[g].meets {
			lo, hi = g, grid[i+1]
		}
	}
	if lo == 0 {
		v, ok := verdicts[50]
		if !ok {
			return 50, false, 0
		}
		if v.meets {
			return 0, true, 50
		}
		return 0, true, 0
	}
	for hi-lo > 50 {
		mid := math.Floor((lo+hi)/2/50) * 50
		if mid == lo {
			mid = lo + 50
		}
		v, ok := verdicts[mid]
		if !ok {
			return mid, false, 0
		}
		if v.meets {
			lo = mid
		} else {
			hi = mid
		}
	}
	return 0, true, lo
}
