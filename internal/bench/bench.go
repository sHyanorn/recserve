// Package bench holds what the benchmark commands share, as fixed in
// docs/BENCHMARKS.md: exact percentiles, the noise baseline, GC and heap
// statistics, the ANN datasets with their recall definition, and the
// environment record.
package bench

import (
	"encoding/json"
	"math"
	"os"
	"slices"
	"time"
)

// Percentile returns the nearest-rank p-quantile (0 <= p <= 1) of sorted:
// the value of rank ceil(p n), which rounding error must not push up.
func Percentile[T any](sorted []T, p float64) T {
	if len(sorted) == 0 {
		var zero T
		return zero
	}
	rank := int(math.Ceil(p*float64(len(sorted)) - 1e-9))
	return sorted[min(max(rank, 1), len(sorted))-1]
}

// Summary is a latency distribution in microseconds.
type Summary struct {
	N    int     `json:"n"`
	P50  float64 `json:"p50_us"`
	P90  float64 `json:"p90_us"`
	P99  float64 `json:"p99_us"`
	P999 float64 `json:"p999_us"`
	Max  float64 `json:"max_us"`
}

// Summarize sorts d in place and returns its percentiles.
func Summarize(d []time.Duration) Summary {
	if len(d) == 0 {
		return Summary{}
	}
	slices.Sort(d)
	us := func(p float64) float64 { return float64(Percentile(d, p)) / 1e3 }
	return Summary{N: len(d), P50: us(.5), P90: us(.9), P99: us(.99), P999: us(.999), Max: us(1)}
}

// WriteJSON writes v to path, indented.
func WriteJSON(path string, v any) error {
	b, err := json.MarshalIndent(v, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(path, append(b, '\n'), 0o644)
}
