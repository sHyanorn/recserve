// Command noisebase measures the noise baseline of docs/BENCHMARKS.md n
// times and reports the median. With the default n = 10 on an idle machine
// the median is the session reference; with -ref the median is checked
// against a reference. Every baseline of a session is taken with the same
// binary, since the workload's speed depends on its code placement.
package main

import (
	"flag"
	"fmt"
	"log"

	"github.com/sHyanorn/recserve/internal/bench"
)

func main() {
	n := flag.Int("n", 10, "measurements")
	cpu := flag.Int("cpu", 0, "CPU to pin to")
	ref := flag.Float64("ref", 0, "session reference in ns per pass; flags a median more than 10% slower")
	out := flag.String("out", "", "JSON result file")
	quiet := flag.Bool("q", false, "print only the median, in ns per pass")
	flag.Parse()
	if *n < 1 {
		log.Fatal("-n must be at least 1")
	}
	v := make([]float64, *n)
	for i := range v {
		var err error
		if v[i], err = bench.MeasureNoise(*cpu); err != nil {
			log.Fatal(err)
		}
	}
	med := bench.Median(v)
	noisy := bench.Noisy(med, *ref)
	if *quiet {
		fmt.Println(med)
	} else {
		fmt.Printf("median %.0f ns per pass, runs %.0f, noisy %v\n", med, v, noisy)
	}
	if *out == "" {
		return
	}
	res := map[string]any{"median_ns": med, "runs_ns": v, "cpu": *cpu, "env": bench.Env(nil)}
	if *ref > 0 {
		res["reference_ns"], res["noisy"] = *ref, noisy
	}
	if err := bench.WriteJSON(*out, res); err != nil {
		log.Fatal(err)
	}
}
