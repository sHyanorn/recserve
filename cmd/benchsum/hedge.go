package main

import (
	"fmt"
	"math"
	"path/filepath"
	"slices"
	"strings"
)

// hedgeRun is one run of docs/BENCHMARKS.md's Hedge delay, between the
// gateway's scrapes at the start (at0) and the end (at1) of the window.
type hedgeRun struct {
	runSummary
	P95       [2]float64 // per shard, ms, of a call to a replica
	PerSearch [2]float64 // hedges sent (won or lost) per search, per shard
}

// hedgeTable writes the hedge-delay runs of dir: delay0-run* with hedging
// off, whose larger shard p95 gives the default delay, the median over the
// runs rounded up to 0.5 ms, and default-run* with that delay. It returns
// the default, 0 without delay0 runs.
func hedgeTable(w *strings.Builder, dir string) (float64, error) {
	paths, err := filepath.Glob(filepath.Join(dir, "*-run*.summary.json"))
	if err != nil {
		return 0, err
	}
	slices.Sort(paths)
	fmt.Fprintln(w, "| Run | Valid | Noisy | p99 ms | Shard 0 p95 ms | Shard 1 p95 ms | Hedges per search, shard 0 | Shard 1 |")
	fmt.Fprintln(w, "|---|---|---|---|---|---|---|---|")
	var larger []float64
	for _, p := range paths {
		prefix := strings.TrimSuffix(p, ".summary.json")
		h, err := readHedgeRun(prefix)
		if err != nil {
			return 0, err
		}
		if strings.HasPrefix(h.Name, "delay0-") {
			larger = append(larger, max(h.P95[0], h.P95[1]))
		}
		fmt.Fprintf(w, "| %s | %v | %v | %.2f | %.2f | %.2f | %.3f | %.3f |\n", h.Name, h.Valid, h.Noisy, h.P99, h.P95[0], h.P95[1], h.PerSearch[0], h.PerSearch[1])
	}
	if len(larger) == 0 {
		return 0, nil
	}
	slices.Sort(larger)
	return math.Ceil(2*median(larger)) / 2, nil
}

func readHedgeRun(prefix string) (hedgeRun, error) {
	var h hedgeRun
	if err := readJSON(prefix+".summary.json", &h.runSummary); err != nil {
		return h, err
	}
	at0, err := readMetrics(prefix + ".at0.metrics")
	if err != nil {
		return h, err
	}
	at1, err := readMetrics(prefix + ".at1.metrics")
	if err != nil {
		return h, err
	}
	delta := func(name string, want map[string]string) float64 { return at1.sum(name, want) - at0.sum(name, want) }
	for s := range 2 {
		id := fmt.Sprint(s)
		shard := map[string]string{"shard": id}
		h.P95[s] = 1000 * histogramQuantile(at1, at0, "recserve_shard_search_seconds", shard, 0.95)
		calls := delta("recserve_shard_search_seconds", shard)
		hedges := delta("recserve_shard_hedges_total", map[string]string{"shard": id, "result": "won"}) +
			delta("recserve_shard_hedges_total", map[string]string{"shard": id, "result": "lost"})
		// Every search makes one call, plus one per hedge and per failover.
		if searches := calls - hedges - delta("recserve_shard_failovers_total", shard); searches > 0 {
			h.PerSearch[s] = hedges / searches
		}
	}
	return h, nil
}

// median of sorted values.
func median(v []float64) float64 {
	n := len(v)
	if n%2 == 1 {
		return v[n/2]
	}
	return (v[n/2-1] + v[n/2]) / 2
}
