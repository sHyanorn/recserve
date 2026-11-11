// Command benchsum turns the files of the benchmark scripts (scripts/bench-*.sh)
// into the summaries and verdicts of docs/BENCHMARKS.md:
//
//	benchsum run -prefix P -samples S            a sweep run: P.summary.json
//	benchsum rate -dir D -rate R                 whether rate R meets the headline condition, and whether the sweep stops
//	benchsum next -dir D [-bisect]               the sweep's next rate, or "done headline=H" (H with -bisect)
//	benchsum sweep -dir D                        a sweep's table, from its run summaries
//	benchsum fault -prefix P -samples S -fault F a fault run judged: P.fault.json
//	benchsum faults -dir D                       the fault runs' table
//	benchsum compare -dir D                      before and after an optimization (scripts/bench-compare.sh)
//	benchsum quantile -after M [-before M0] -name N [-label k=v] -q Q
//	                                             a quantile of a histogram in a /metrics scrape, or between two
//	benchsum swap -prefix P -samples S             a run of bundle swaps under load: P.swap.json
//	benchsum swaps -dir D                        the swap runs' table and what GOGC=off says of the largest latencies
//	benchsum phases -dir D                       runs cut at their marks (D/*.marks, samples in D/raw): *.phases.json and a table
//	benchsum hedge -dir D                        the hedge-delay runs' table, then default_ms=, the default delay
//	benchsum histories -valkey A -preload P1,P2 -split S -samples S
//	                                             property 9 after a run with writes: "ok" or what differs
package main

import (
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"slices"
	"strings"
)

func main() {
	if err := run(os.Args[1:], os.Stdout); err != nil {
		fmt.Fprintln(os.Stderr, "benchsum:", err)
		os.Exit(1)
	}
}

func run(args []string, out io.Writer) error {
	if len(args) == 0 {
		return fmt.Errorf("a subcommand is required: run, rate, next, sweep, fault, faults, swap, swaps, phases, quantile, hedge or histories")
	}
	fs := flag.NewFlagSet(args[0], flag.ContinueOnError)
	prefix := fs.String("prefix", "", "path of the run's files without their suffixes")
	samples := fs.String("samples", "", "the run's samples, written by loadgen -samples")
	dir := fs.String("dir", "", "directory of a sweep's or of the fault runs' files")
	rate := fs.Float64("rate", 0, "rate of the sweep")
	fault := fs.String("fault", "", "F1 to F6, as docs/BENCHMARKS.md names them")
	after := fs.String("after", "", "a /metrics scrape")
	before := fs.String("before", "", "an earlier scrape of the same process")
	name := fs.String("name", "", "histogram name")
	lbl := fs.String("label", "", "k=v: only the series with that label")
	q := fs.Float64("q", 0.95, "quantile")
	bisect := fs.Bool("bisect", false, "next: bisect after the grid, as the headline sweep does")
	preload := fs.String("preload", "", "histories: comma-separated splits the gateway preloaded")
	split := fs.String("split", "", "histories: the split loadgen replayed")
	valkeyAddr := fs.String("valkey", "", "histories: the Valkey server that holds the histories")
	if err := fs.Parse(args[1:]); err != nil {
		return err
	}
	switch args[0] {
	case "run":
		s, err := summarizeRun(*prefix, *samples)
		if err != nil {
			return err
		}
		if err := writeJSON(*prefix+".summary.json", s); err != nil {
			return err
		}
		return printf(out, "%s: valid %v, noisy %v, p99 %.2f ms, degraded %.2f%%\n", s.Name, s.Valid, s.Noisy, s.P99, 100*s.Degraded)
	case "rate":
		runs, err := readSummaries(*dir, func(s runSummary) bool { return s.Rate == *rate })
		if err != nil {
			return err
		}
		meets, stop, p99 := verdict(runs)
		return printf(out, "meets=%v stop=%v runs=%d median_p99_ms=%.2f\n", meets, stop, len(runs), p99)
	case "next":
		runs, err := readSummaries(*dir, func(runSummary) bool { return true })
		if err != nil {
			return err
		}
		byRate := map[float64][]runSummary{}
		for _, r := range runs {
			byRate[r.Rate] = append(byRate[r.Rate], r)
		}
		verdicts := map[float64]rateVerdict{}
		for rate, rs := range byRate {
			if len(rs) >= 3 {
				meets, stop, _ := verdict(rs)
				verdicts[rate] = rateVerdict{meets, stop}
			}
		}
		next, done, headline := nextRate(verdicts, *bisect)
		if done {
			return printf(out, "done headline=%g\n", headline)
		}
		return printf(out, "%g\n", next)
	case "sweep":
		runs, err := readSummaries(*dir, func(runSummary) bool { return true })
		if err != nil {
			return err
		}
		byRate := map[float64][]runSummary{}
		containers := map[string]bool{}
		for _, r := range runs {
			byRate[r.Rate] = append(byRate[r.Rate], r)
			for c := range r.CPUPerRequest {
				containers[c] = true
			}
		}
		names := make([]string, 0, len(containers))
		for c := range containers {
			names = append(names, c)
		}
		slices.Sort(names)
		var b strings.Builder
		sweepTable(&b, byRate, names)
		return printf(out, "%s", b.String())
	case "fault":
		f, err := summarizeFault(*prefix, *samples, *fault)
		if err != nil {
			return err
		}
		if err := writeJSON(*prefix+".fault.json", f); err != nil {
			return err
		}
		return printf(out, "%s %s: valid %v %v, pass %v %v\n", f.Name, f.Fault, f.Valid, f.Invalid, f.Pass, f.Failures)
	case "compare":
		var b strings.Builder
		if err := compareTable(&b, *dir); err != nil {
			return err
		}
		return printf(out, "%s", b.String())
	case "faults":
		var b strings.Builder
		if err := faultTable(&b, *dir); err != nil {
			return err
		}
		return printf(out, "%s", b.String())
	case "quantile":
		a, err := readMetrics(*after)
		if err != nil {
			return err
		}
		var b metrics
		if *before != "" {
			if b, err = readMetrics(*before); err != nil {
				return err
			}
		}
		want := map[string]string{}
		if k, v, ok := strings.Cut(*lbl, "="); ok {
			want[k] = v
		}
		return printf(out, "%g\n", histogramQuantile(a, b, *name, want, *q))
	case "swap":
		s, err := summarizeSwap(*prefix, *samples)
		if err != nil {
			return err
		}
		if err := writeJSON(*prefix+".swap.json", s); err != nil {
			return err
		}
		return printf(out, "%s: valid %v, noisy %v, swap windows p99 %.2f max %.2f ms, others p99 %.2f max %.2f ms\n", s.Name, s.Valid, s.Noisy, s.Swap.P99, s.Swap.Max, s.Others.P99, s.Others.Max)
	case "swaps":
		var b strings.Builder
		if err := swapTable(&b, *dir); err != nil {
			return err
		}
		return printf(out, "%s", b.String())
	case "phases":
		var b strings.Builder
		if err := phaseTable(&b, *dir); err != nil {
			return err
		}
		return printf(out, "%s", b.String())
	case "hedge":
		var b strings.Builder
		d, err := hedgeTable(&b, *dir)
		if err != nil {
			return err
		}
		return printf(out, "%sdefault_ms=%g\n", b.String(), d)
	case "histories":
		got, err := histories(*valkeyAddr, *preload, *split, *samples)
		if err != nil {
			return err
		}
		return printf(out, "%s\n", got)
	default:
		return fmt.Errorf("unknown subcommand %q", args[0])
	}
}

func readSummaries(dir string, keep func(runSummary) bool) ([]runSummary, error) {
	paths, err := filepath.Glob(filepath.Join(dir, "*.summary.json"))
	if err != nil {
		return nil, err
	}
	var out []runSummary
	for _, p := range paths {
		var s runSummary
		if err := readJSON(p, &s); err != nil {
			return nil, err
		}
		if keep(s) {
			out = append(out, s)
		}
	}
	return out, nil
}

// faultTable writes a row per fault run, in name order.
func faultTable(w *strings.Builder, dir string) error {
	paths, err := filepath.Glob(filepath.Join(dir, "*.fault.json"))
	if err != nil {
		return err
	}
	slices.Sort(paths)
	fmt.Fprintln(w, "| Run | Valid | Pass | Healthy p99 | Fault p99 | Fault max | Recovery p99 | Fault degraded | Excess | Recovery s | Errors | Failures |")
	fmt.Fprintln(w, "|---|---|---|---|---|---|---|---|---|---|---|---|")
	for _, p := range paths {
		var f faultRun
		if err := readJSON(p, &f); err != nil {
			return err
		}
		h, d, r := f.Phases["healthy"], f.Phases["fault"], f.Phases["recovery"]
		var ex []string
		for k, v := range f.Excess {
			if v > 0.001 {
				ex = append(ex, fmt.Sprintf("%s %+.1f", k, 100*v))
			}
		}
		slices.Sort(ex)
		rec := "not recovered"
		if f.Recovery != nil {
			rec = fmt.Sprint(*f.Recovery)
		}
		fmt.Fprintf(w, "| %s | %v | %v | %.2f | %.2f | %.2f | %.2f | %.1f%% | %s | %s | %d | %s |\n", f.Name, f.Valid, f.Pass, h.P99, d.P99, d.Max, r.P99,
			100*d.Degraded, strings.Join(ex, ", "), rec, h.Errors+d.Errors+r.Errors, strings.Join(f.Failures, "; "))
	}
	return nil
}

func printf(w io.Writer, format string, a ...any) error {
	_, err := fmt.Fprintf(w, format, a...)
	return err
}
