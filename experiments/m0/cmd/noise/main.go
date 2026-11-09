// Command noise measures the noise baseline n times and reports the median.
// With the default n = 10 on an idle machine, the median is the session
// reference; with -ref, a single measurement is checked against it.
package main

import (
	"flag"
	"fmt"
	"log"

	"github.com/sHyanorn/recserve/experiments/m0/internal/bench"
)

func main() {
	n := flag.Int("n", 10, "measurements")
	cpu := flag.Int("cpu", 0, "CPU to pin to")
	ref := flag.Float64("ref", 0, "session reference in ns per pass; flags a median more than 10% slower")
	out := flag.String("out", "", "JSON result file")
	flag.Parse()
	v := make([]float64, *n)
	for i := range v {
		v[i] = bench.MeasureNoise(*cpu)
	}
	med := bench.Median(v)
	noisy := bench.Noisy(med, *ref)
	fmt.Printf("median %.0f ns per pass, runs %v, noisy %v\n", med, v, noisy)
	if *out != "" {
		res := map[string]any{"median_ns": med, "runs_ns": v, "env": bench.Env(nil)}
		if *ref > 0 {
			res["reference_ns"], res["noisy"] = *ref, noisy
		}
		err := bench.WriteJSON(*out, res)
		if err != nil {
			log.Fatal(err)
		}
	}
}
