package main

import (
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"strconv"
	"strings"
)

// compareTable writes the before and after ranges of scripts/bench-compare.sh's
// runs, and a verdict per quantity: lower after or before only if the three
// runs of one side are all below the three of the other (docs/BENCHMARKS.md).
func compareTable(w *strings.Builder, dir string) error {
	micro, err := microRanges(dir)
	if err != nil {
		return err
	}
	fmt.Fprintln(w, "| Quantity | Before | After | Verdict |")
	fmt.Fprintln(w, "|---|---|---|---|")
	names := make([]string, 0, len(micro))
	for k := range micro {
		names = append(names, k)
	}
	slices.Sort(names)
	for _, k := range names {
		row(w, k+" ns/op", micro[k]["before"], micro[k]["after"])
	}
	runs, err := readSummaries(dir, func(runSummary) bool { return true })
	if err != nil {
		return err
	}
	type key struct {
		rate float64
		side string
	}
	p99, replicas, gateway := map[key][]float64{}, map[key][]float64{}, map[key][]float64{}
	var rates []float64
	for _, r := range runs {
		side := "before"
		if strings.Contains(r.Name, "-after-") {
			side = "after"
		}
		k := key{r.Rate, side}
		p99[k] = append(p99[k], r.P99)
		var cpu float64
		for c, v := range r.CPUPerRequest {
			if strings.HasPrefix(c, "annshard-") {
				cpu += v
			}
		}
		replicas[k] = append(replicas[k], cpu)
		gateway[k] = append(gateway[k], r.CPUPerRequest["gateway"])
		if !slices.Contains(rates, r.Rate) {
			rates = append(rates, r.Rate)
		}
	}
	slices.Sort(rates)
	for _, rate := range rates {
		b, a := key{rate, "before"}, key{rate, "after"}
		row(w, fmt.Sprintf("%.0f/s p99 ms", rate), p99[b], p99[a])
		row(w, fmt.Sprintf("%.0f/s replicas' CPU ms per request", rate), replicas[b], replicas[a])
		row(w, fmt.Sprintf("%.0f/s gateway's CPU ms per request", rate), gateway[b], gateway[a])
	}
	return nil
}

func row(w *strings.Builder, name string, before, after []float64) {
	fmt.Fprintf(w, "| %s | %s | %s | %s |\n", name, span(before), span(after), lower(before, after))
}

func span(v []float64) string {
	if len(v) == 0 {
		return ""
	}
	return fmt.Sprintf("%.3g to %.3g", slices.Min(v), slices.Max(v))
}

func lower(before, after []float64) string {
	switch {
	case len(before) == 0 || len(after) == 0:
		return "missing runs"
	case slices.Max(after) < slices.Min(before):
		return "lower after"
	case slices.Max(before) < slices.Min(after):
		return "lower before"
	}
	return "not resolved on this machine"
}

var benchLine = regexp.MustCompile(`^(Benchmark\S+?)(?:-\d+)?\s+\d+\s+([0-9.]+) ns/op`)

// microRanges reads micro-SIDE-N.txt, go test -bench output: ns/op by
// benchmark and side.
func microRanges(dir string) (map[string]map[string][]float64, error) {
	paths, err := filepath.Glob(filepath.Join(dir, "micro-*.txt"))
	if err != nil {
		return nil, err
	}
	out := map[string]map[string][]float64{}
	for _, p := range paths {
		side := strings.Split(filepath.Base(p), "-")[1]
		data, err := os.ReadFile(p)
		if err != nil {
			return nil, err
		}
		for line := range strings.Lines(string(data)) {
			m := benchLine.FindStringSubmatch(line)
			if m == nil {
				continue
			}
			v, err := strconv.ParseFloat(m[2], 64)
			if err != nil {
				return nil, err
			}
			if out[m[1]] == nil {
				out[m[1]] = map[string][]float64{}
			}
			out[m[1]][side] = append(out[m[1]][side], v)
		}
	}
	return out, nil
}
