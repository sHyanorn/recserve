package main

import (
	"fmt"
	"maps"
	"math"
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
	for _, k := range slices.Sorted(maps.Keys(micro)) {
		row(w, k+" ns/op", micro[k]["before"], micro[k]["after"])
	}
	ann, err := annRanges(dir)
	if err != nil {
		return err
	}
	efs := slices.Sorted(maps.Keys(ann.p99))
	for _, ef := range efs {
		row(w, fmt.Sprintf("annbench ef %d p99 ms", ef), ann.p99[ef]["before"], ann.p99[ef]["after"])
	}
	for _, ef := range efs {
		b, a := ann.recall[ef]["before"], ann.recall[ef]["after"]
		fmt.Fprintf(w, "| annbench ef %d recall@100 | %s | %s | %s |\n", ef, recallSpan(b), recallSpan(a), recallVerdict(b, a))
	}
	runs, err := readSummaries(dir)
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
	if ann.points > 0 {
		fmt.Fprintf(w, "\nannbench points left noisy after three attempts: %d of %d\n", ann.noisy, ann.points)
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

// annResults holds annbench's query results of ann-SIDE-N.json: p99 in ms
// and recall@100 by ef and side, and how many points stayed flagged.
type annResults struct {
	p99, recall   map[int]map[string][]float64
	noisy, points int
}

func annRanges(dir string) (annResults, error) {
	r := annResults{p99: map[int]map[string][]float64{}, recall: map[int]map[string][]float64{}}
	paths, err := filepath.Glob(filepath.Join(dir, "ann-*.json"))
	if err != nil {
		return r, err
	}
	for _, p := range paths {
		side := strings.Split(filepath.Base(p), "-")[1]
		var res struct {
			Points []struct {
				Ef      int     `json:"ef"`
				Recall  float64 `json:"recall"`
				Latency struct {
					P99 float64 `json:"p99_us"`
				} `json:"latency"`
				Noise struct {
					Noisy bool `json:"noisy"`
				} `json:"noise"`
			} `json:"points"`
		}
		if err := readJSON(p, &res); err != nil {
			return r, err
		}
		for _, pt := range res.Points {
			if r.p99[pt.Ef] == nil {
				r.p99[pt.Ef], r.recall[pt.Ef] = map[string][]float64{}, map[string][]float64{}
			}
			r.p99[pt.Ef][side] = append(r.p99[pt.Ef][side], pt.Latency.P99/1000)
			r.recall[pt.Ef][side] = append(r.recall[pt.Ef][side], pt.Recall)
			r.points++
			if pt.Noise.Noisy {
				r.noisy++
			}
		}
	}
	return r, nil
}

func recallSpan(v []float64) string {
	if len(v) == 0 {
		return ""
	}
	if slices.Min(v) == slices.Max(v) {
		return fmt.Sprintf("%.4f", v[0])
	}
	return fmt.Sprintf("%.4f to %.4f", slices.Min(v), slices.Max(v))
}

// recallVerdict checks docs/BENCHMARKS.md's bound on the same graph file:
// recall@100 before and after differs by at most 0.001. Recalls are counts
// over 10^6, so the difference is compared in those units.
func recallVerdict(before, after []float64) string {
	if len(before) == 0 || len(after) == 0 {
		return "missing runs"
	}
	d := max(math.Abs(slices.Max(after)-slices.Min(before)), math.Abs(slices.Max(before)-slices.Min(after)))
	if math.Round(d*1e6) <= 1000 {
		return "within 0.001"
	}
	return fmt.Sprintf("differs by %.4f", d)
}
