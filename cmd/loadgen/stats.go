package main

import (
	"math"
	"slices"
	"time"
)

// summary describes one latency distribution, in microseconds.
type summary struct {
	Count int     `json:"count"`
	P50   float64 `json:"p50"`
	P90   float64 `json:"p90"`
	P99   float64 `json:"p99"`
	P999  float64 `json:"p99_9"`
	P9999 float64 `json:"p99_99"`
	Max   float64 `json:"max"`
	// Curve holds [percentile, value] pairs that halve the distance to
	// 100 five times per halving, as HdrHistogram's percentile output does,
	// for plotting against 1/(1-p).
	Curve [][2]float64 `json:"curve"`
}

// summarize sorts samples, in nanoseconds, and describes them. Percentiles
// are exact: the nearest-rank value of the sorted samples.
func summarize(samples []int64) summary {
	if len(samples) == 0 {
		return summary{}
	}
	slices.Sort(samples)
	s := summary{
		Count: len(samples),
		P50:   at(samples, 50), P90: at(samples, 90), P99: at(samples, 99),
		P999: at(samples, 99.9), P9999: at(samples, 99.99),
		Max: micros(samples[len(samples)-1]),
	}
	for k := 0; ; k++ {
		p := 100 * (1 - math.Pow(0.5, float64(k)/5))
		if (100-p)/100*float64(len(samples)) < 1 {
			break
		}
		s.Curve = append(s.Curve, [2]float64{p, at(samples, p)})
	}
	s.Curve = append(s.Curve, [2]float64{100, s.Max})
	return s
}

// at returns the p-th percentile of sorted samples, in microseconds: the
// sample of rank ceil(p/100 * n), which rounding error must not push up.
func at(sorted []int64, p float64) float64 {
	rank := int(math.Ceil(p*float64(len(sorted))/100 - 1e-9))
	return micros(sorted[max(rank, 1)-1])
}

func micros(ns int64) float64 { return float64(ns) / float64(time.Microsecond) }
