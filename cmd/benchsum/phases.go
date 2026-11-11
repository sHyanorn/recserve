package main

import (
	"fmt"
	"maps"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
)

// mark is a script's line "<name> <unix ms>".
type mark struct {
	name string
	ms   float64
}

// readMarkList reads a file of marks, in its order.
func readMarkList(path string) ([]mark, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var out []mark
	for line := range strings.Lines(string(data)) {
		f := strings.Fields(line)
		if len(f) != 2 {
			continue
		}
		v, err := strconv.ParseFloat(f[1], 64)
		if err != nil {
			return nil, fmt.Errorf("%s: %w", path, err)
		}
		out = append(out, mark{f[0], v})
	}
	return out, nil
}

// phase is a part of a run: before the first mark, then from each mark to the next.
type phase struct {
	Name string `json:"phase"`
	stats
}

type phaseRun struct {
	Name    string  `json:"name"`
	Valid   bool    `json:"valid"`
	Phases  []phase `json:"phases"`
	Windows []stats `json:"windows"` // each second of the measured window
}

// summarizePhases cuts the measured window of the run prefix at its marks
// (prefix.marks): "before" up to the first mark, then one phase per mark.
func summarizePhases(prefix, raw string) (phaseRun, error) {
	var r loadReport
	if err := readJSON(prefix+".json", &r); err != nil {
		return phaseRun{}, err
	}
	samples, err := readSamples(raw)
	if err != nil {
		return phaseRun{}, err
	}
	marks, err := readMarkList(prefix + ".marks")
	if err != nil {
		return phaseRun{}, err
	}
	start, end := r.Warmup*1000, (r.Warmup+r.Duration)*1000
	t0 := float64(r.Start.UnixMilli())
	bounds := []mark{{"before", start}}
	for _, m := range marks {
		bounds = append(bounds, mark{m.name, m.ms - t0})
	}
	p := phaseRun{Name: filepath.Base(prefix), Valid: r.Valid, Windows: windows(samples, start, end)}
	for i, b := range bounds {
		to := end
		if i+1 < len(bounds) {
			to = bounds[i+1].ms
		}
		p.Phases = append(p.Phases, phase{b.name, describe(samples, b.ms, to)})
	}
	return p, nil
}

// phaseTable summarizes every run of dir that has marks, its samples in
// dir/raw, and writes the phases' table.
func phaseTable(w *strings.Builder, dir string) error {
	paths, err := filepath.Glob(filepath.Join(dir, "*.marks"))
	if err != nil {
		return err
	}
	slices.Sort(paths)
	fmt.Fprintln(w, "| Run | Phase | Answers | p50 ms | p99 ms | max ms | Errors | Degraded | By reason |")
	fmt.Fprintln(w, "|---|---|---|---|---|---|---|---|---|")
	for _, path := range paths {
		prefix := strings.TrimSuffix(path, ".marks")
		p, err := summarizePhases(prefix, filepath.Join(dir, "raw", filepath.Base(prefix)+".csv.gz"))
		if err != nil {
			return err
		}
		if err := writeJSON(prefix+".phases.json", p); err != nil {
			return err
		}
		for _, ph := range p.Phases {
			var reasons []string
			for _, r := range slices.Sorted(maps.Keys(ph.Reasons)) {
				reasons = append(reasons, fmt.Sprintf("%s %.3f%%", r, 100*ph.Reasons[r]))
			}
			fmt.Fprintf(w, "| %s | %s | %d | %.2f | %.2f | %.2f | %d | %.3f%% | %s |\n", p.Name, ph.Name, ph.Answers, ph.P50, ph.P99, ph.Max, ph.Errors,
				100*ph.Degraded, strings.Join(reasons, ", "))
		}
	}
	return nil
}
