package main

import (
	"math/rand/v2"
	"testing"
)

func TestSummarize(t *testing.T) {
	// 1 to 10,000 µs, shuffled.
	samples := make([]int64, 10_000)
	for i := range samples {
		samples[i] = int64(i+1) * 1000
	}
	rand.New(rand.NewPCG(1, 2)).Shuffle(len(samples), func(i, j int) { samples[i], samples[j] = samples[j], samples[i] })
	s := summarize(samples)
	if s.Count != 10_000 || s.P50 != 5000 || s.P90 != 9000 || s.P99 != 9900 || s.P999 != 9990 || s.P9999 != 9999 || s.Max != 10_000 {
		t.Fatalf("summary %+v", s)
	}
	prev := [2]float64{-1, 0}
	for _, pt := range s.Curve {
		if pt[0] <= prev[0] || pt[1] < prev[1] {
			t.Fatalf("curve not increasing at %v after %v", pt, prev)
		}
		prev = pt
	}
	if first, last := s.Curve[0], s.Curve[len(s.Curve)-1]; first != [2]float64{0, 1} || last != [2]float64{100, 10_000} {
		t.Fatalf("curve runs from %v to %v, want [0 1] to [100 10000]", first, last)
	}
	if got := summarize(nil); got.Count != 0 || got.Curve != nil {
		t.Fatalf("summary of no samples %+v", got)
	}
}
