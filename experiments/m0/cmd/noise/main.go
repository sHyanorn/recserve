// Command noise measures the session reference of the noise baseline: the
// median of ten baseline measurements on an idle machine.
package main

import (
	"flag"
	"fmt"
	"log"
	"slices"

	"github.com/sHyanorn/recserve/experiments/m0/internal/bench"
)

func main() {
	n := flag.Int("n", 10, "measurements")
	cpu := flag.Int("cpu", 0, "CPU to pin to")
	out := flag.String("out", "", "JSON result file")
	flag.Parse()
	v := make([]float64, *n)
	for i := range v {
		v[i] = bench.MeasureNoise(*cpu)
	}
	s := slices.Clone(v)
	slices.Sort(s)
	ref := s[len(s)/2]
	fmt.Printf("reference %.0f ns per pass, runs %v\n", ref, v)
	if *out != "" {
		err := bench.WriteJSON(*out, map[string]any{"reference_ns": ref, "runs_ns": v, "env": bench.Env(nil)})
		if err != nil {
			log.Fatal(err)
		}
	}
}
