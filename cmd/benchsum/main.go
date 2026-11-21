// Command benchsum turns the files of the benchmark scripts (scripts/bench-*.sh)
// into the summaries and verdicts of docs/BENCHMARKS.md:
//
//	benchsum run -prefix P -samples S            a sweep run: P.summary.json
//	benchsum rate -dir D -rate R                 whether rate R meets the headline condition, and whether the sweep stops
//	benchsum next -dir D [-bisect] [-grid-max R] the sweep's next rate, or "done headline=H" (H with -bisect)
//	benchsum sweep -dir D                        a sweep's table, from its run summaries
//	benchsum fault -prefix P -samples S -reads R -fault F
//	                                             a fault run judged (F none: a breaker run, for validity only): P.fault.json
//	benchsum faults -dir D                       the fault runs' table
//	benchsum breakers -dir D                     the breaker's openings in the fault runs, one row each
//	benchsum compare -dir D                      before and after an optimization (scripts/bench-compare.sh)
//	benchsum quantile -after M [-before M0] -name N [-label k=v,...] -q Q
//	                                             a quantile of a histogram in a /metrics scrape, or between two
//	benchsum swap -prefix P -samples S           a run of bundle swaps under load: P.swap.json
//	benchsum swaps -dir D                        the swap runs' table and what GOGC=off says of the largest latencies
//	benchsum phases -dir D                       runs cut at their marks (D/*.marks, samples in D/raw): *.phases.json and a table
//	benchsum hedge -dir D                        the hedge-delay runs' table, then default_ms=, the default delay
//	benchsum histories -valkey A -preload P1,P2 -split S -samples S
//	                                             property 9 after a run with writes: "ok" or what differs
//	benchsum stream -prefix P -samples S -group G -version V
//	                                             a run of the stream scenario, group off or on serving V: P.summary.json
//	benchsum streamtable -dir D1[,D2] [-readings=false]
//	                                             the stream runs' table and, unless false, each rate's readings
//	benchsum streamnext -dir D1[,D2] -group G -boot B
//	                                             a group's next rate in phase 2, or "done headline=H": none, a rate, or "4000 or more"
//	benchsum streamswap -prefix P -samples S     the run of swaps with the stream state: P.swap.json and its table
//	benchsum bench -file F                       median and range of each benchmark's counts in go test -bench output
//	benchsum value -after M -name N [-label k=v,...]
//	                                             the value of a series in a /metrics scrape; an error and no output if it has none
package main

import (
	"flag"
	"fmt"
	"io"
	"maps"
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

// parseLabels reads the -label flag, a comma-separated list of k=v; empty
// means no label.
func parseLabels(s string) (map[string]string, error) {
	want := map[string]string{}
	if s == "" {
		return want, nil
	}
	for kv := range strings.SplitSeq(s, ",") {
		k, v, ok := strings.Cut(kv, "=")
		if !ok {
			return nil, fmt.Errorf("-label %q: %q is not k=v", s, kv)
		}
		want[k] = v
	}
	return want, nil
}

func run(args []string, out io.Writer) error {
	if len(args) == 0 {
		return fmt.Errorf("a subcommand is required: run, rate, next, sweep, fault, faults, breakers, swap, swaps, phases, quantile, hedge, histories, stream, streamtable, streamnext, streamswap, bench or value")
	}
	fs := flag.NewFlagSet(args[0], flag.ContinueOnError)
	prefix := fs.String("prefix", "", "path of the run's files without their suffixes")
	samples := fs.String("samples", "", "the run's samples, written by loadgen -samples")
	reads := fs.String("reads", "", "fault: the run's per-second reads, written by scripts/bench-fault.sh")
	dir := fs.String("dir", "", "directory of a sweep's or of the fault runs' files")
	rate := fs.Float64("rate", 0, "rate of the sweep")
	fault := fs.String("fault", "", "F1 to F6, as docs/BENCHMARKS.md names them, or none")
	after := fs.String("after", "", "a /metrics scrape")
	before := fs.String("before", "", "an earlier scrape of the same process")
	name := fs.String("name", "", "histogram name")
	lbl := fs.String("label", "", "k=v[,k=v...]: only the series with those labels")
	q := fs.Float64("q", 0.95, "quantile")
	bisect := fs.Bool("bisect", false, "next: bisect after the grid, as the headline sweep does")
	gridMax := fs.Float64("grid-max", 0, "next: the grid's last rate, for a sweep whose grid ended there with no rate stopping it (0: none)")
	preload := fs.String("preload", "", "histories: comma-separated splits the gateway preloaded")
	split := fs.String("split", "", "histories: the split loadgen replayed")
	valkeyAddr := fs.String("valkey", "", "histories: the Valkey server that holds the histories")
	group := fs.String("group", "", "stream, streamnext: off or on")
	version := fs.String("version", "", "stream: the bundle version the group serves")
	boot := fs.String("boot", "", "streamnext: the boot that phase 2 runs on")
	file := fs.String("file", "", "bench: go test -bench output")
	readings := fs.Bool("readings", true, "streamtable: the readings of each rate below the table")
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
		runs, err := readSummaries(*dir)
		if err != nil {
			return err
		}
		runs = slices.DeleteFunc(runs, func(s runSummary) bool { return s.Rate != *rate })
		meets, stop, p99 := verdict(runs)
		return printf(out, "meets=%v stop=%v runs=%d median_p99_ms=%.2f\n", meets, stop, len(runs), p99)
	case "next":
		runs, err := readSummaries(*dir)
		if err != nil {
			return err
		}
		verdicts := map[float64]rateVerdict{}
		for rate, rs := range byRate(runs) {
			if len(rs) >= 3 {
				meets, stop, _ := verdict(rs)
				verdicts[rate] = rateVerdict{meets, stop}
			}
		}
		next, done, headline := nextRate(verdicts, *bisect, *gridMax)
		if done {
			return printf(out, "done headline=%g\n", headline)
		}
		return printf(out, "%g\n", next)
	case "sweep":
		runs, err := readSummaries(*dir)
		if err != nil {
			return err
		}
		containers := map[string]bool{}
		for _, r := range runs {
			for c := range r.CPUPerRequest {
				containers[c] = true
			}
		}
		var b strings.Builder
		sweepTable(&b, byRate(runs), slices.Sorted(maps.Keys(containers)))
		return printf(out, "%s", b.String())
	case "fault":
		f, err := summarizeFault(*prefix, *samples, *reads, *fault)
		if err != nil {
			return err
		}
		if err := writeJSON(*prefix+".fault.json", f); err != nil {
			return err
		}
		if f.Fault == "none" {
			return printf(out, "%s %s: valid %v %v, %d breaker openings\n", f.Name, f.Fault, f.Valid, f.Invalid, len(f.Breaker))
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
	case "breakers":
		var b strings.Builder
		if err := breakerTable(&b, *dir); err != nil {
			return err
		}
		return printf(out, "%s", b.String())
	case "stream":
		s, err := summarizeStream(*prefix, *samples, *group, *version)
		if err != nil {
			return err
		}
		if err := writeJSON(*prefix+".summary.json", s); err != nil {
			return err
		}
		return printf(out, "%s: valid %v %v, noisy %v, p99 %.2f ms, degraded %.2f%%\n", s.Name, s.Valid, s.Problems, s.Noisy, s.P99, 100*s.Degraded)
	case "streamtable":
		runs, err := readStreamRuns(strings.Split(*dir, ","))
		if err != nil {
			return err
		}
		var b strings.Builder
		streamTable(&b, runs, *readings)
		return printf(out, "%s", b.String())
	case "streamnext":
		runs, err := readStreamRuns(strings.Split(*dir, ","))
		if err != nil {
			return err
		}
		next, done, headline, err := streamNext(runs, *group, *boot)
		if err != nil {
			return err
		}
		switch {
		case done && headline == 0:
			return printf(out, "done headline=none\n")
		case done && headline >= 4000:
			return printf(out, "done headline=%g or more\n", headline)
		case done:
			return printf(out, "done headline=%g\n", headline)
		}
		return printf(out, "%g\n", next)
	case "streamswap":
		s, err := summarizeStreamSwap(*prefix, *samples)
		if err != nil {
			return err
		}
		if err := writeJSON(*prefix+".swap.json", s); err != nil {
			return err
		}
		var b strings.Builder
		streamSwapTable(&b, s)
		return printf(out, "%s", b.String())
	case "bench":
		t, err := benchMedians(*file)
		if err != nil {
			return err
		}
		return printf(out, "%s", t)
	case "value":
		m, err := readMetrics(*after)
		if err != nil {
			return err
		}
		want, err := parseLabels(*lbl)
		if err != nil {
			return err
		}
		v := m.value(*name, want)
		if v == nil {
			return fmt.Errorf("%s{%s}: no such series", *name, *lbl)
		}
		return printf(out, "%g\n", *v)
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
		want, err := parseLabels(*lbl)
		if err != nil {
			return err
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

func readSummaries(dir string) ([]runSummary, error) {
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
		out = append(out, s)
	}
	return out, nil
}

func byRate(runs []runSummary) map[float64][]runSummary {
	out := map[float64][]runSummary{}
	for _, r := range runs {
		out[r.Rate] = append(out[r.Rate], r)
	}
	return out
}

// readFaultRuns reads the attempts of dir, in name order.
func readFaultRuns(dir string) ([]faultRun, error) {
	paths, err := filepath.Glob(filepath.Join(dir, "*.fault.json"))
	if err != nil {
		return nil, err
	}
	slices.Sort(paths)
	out := make([]faultRun, len(paths))
	for i, p := range paths {
		if err := readJSON(p, &out[i]); err != nil {
			return nil, err
		}
		if out[i].Run == "" {
			out[i].Run, out[i].Attempt = runOf(out[i].Name)
		}
	}
	return out, nil
}

// chosen returns each run's valid attempt or, if it has none, its last, in
// run order. The script stops at a run's first valid attempt.
func chosen(attempts []faultRun) []faultRun {
	by := map[string]faultRun{}
	for _, a := range attempts {
		if c, ok := by[a.Run]; !ok || (!c.Valid && (a.Valid || a.Attempt > c.Attempt)) {
			by[a.Run] = a
		}
	}
	var out []faultRun
	for _, k := range slices.Sorted(maps.Keys(by)) {
		out = append(out, by[k])
	}
	return out
}

// faultVerdict is a fault's verdict by the rule (BENCHMARKS, Faults): it
// passes if all three of its valid runs pass, and fails if one of them
// fails; with fewer valid runs and none failed it is not judged.
func faultVerdict(runs []faultRun) string {
	var valid int
	var failed []string
	for _, r := range runs {
		if r.Valid {
			valid++
			if !r.Pass {
				failed = append(failed, r.Run)
			}
		}
	}
	switch {
	case len(runs) > 0 && runs[0].Fault == "none":
		return "not judged"
	case len(failed) > 0:
		return "fail: " + strings.Join(failed, ", ")
	case valid >= 3:
		return "pass"
	}
	return fmt.Sprintf("not judged: %d valid runs", valid)
}

// faultTable writes a row per fault run, from its chosen attempt; then a
// row per attempt, with why it was invalid and its breaker openings; then
// each fault's runs, attempts and verdict.
func faultTable(w *strings.Builder, dir string) error {
	attempts, err := readFaultRuns(dir)
	if err != nil {
		return err
	}
	runs := chosen(attempts)
	fmt.Fprintln(w, "| Run | Valid | Pass | Healthy p99 | Fault p99 | Fault max | Recovery p99 | Fault degraded | Excess | Window recovery s | annshard-1 back s | annshard-1 [210, 270 s) | Recovery s | Outlier detection | Errors | Failures |")
	fmt.Fprintln(w, "|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|")
	for _, f := range runs {
		h, d, r := f.Phases["healthy"], f.Phases["fault"], f.Phases["recovery"]
		var ex []string
		for k, v := range f.Excess {
			if v > 0.001 {
				ex = append(ex, fmt.Sprintf("%s %+.1f", k, 100*v))
			}
		}
		slices.Sort(ex)
		pass := fmt.Sprint(f.Pass)
		switch {
		case f.Fault == "none":
			pass = "not judged"
		case !f.Valid:
			pass = "" // neither a pass nor a fail
		}
		window, back, share, rec := "not recovered", "", "", "not recovered"
		if f.WindowRecovery != nil {
			window = fmt.Sprint(*f.WindowRecovery)
		}
		if replicaFaults[f.Fault] {
			back, share = "not back", "no reads"
			if f.Return != nil {
				back = fmt.Sprintf("%.3f", *f.Return)
			}
			if f.ReplicaShare != nil {
				share = fmt.Sprintf("%.1f%%", 100**f.ReplicaShare)
			}
		}
		if f.Recovery != nil {
			rec = fmt.Sprintf("%.3f", *f.Recovery)
		}
		od := "no"
		if f.OutlierDetection {
			od = "yes, ejections in [30, 90 s): no reads"
			if f.HealthyEjections != nil {
				od = fmt.Sprintf("yes, ejections in [30, 90 s): %.0f", *f.HealthyEjections)
			}
		}
		var errs int
		for _, ph := range phaseNames {
			errs += f.Phases[ph].Errors
		}
		fmt.Fprintf(w, "| %s | %v | %s | %.2f | %.2f | %.2f | %.2f | %.1f%% | %s | %s | %s | %s | %s | %s | %d | %s |\n", f.Name, f.Valid, pass, h.P99, d.P99, d.Max, r.P99,
			100*d.Degraded, strings.Join(ex, ", "), window, back, share, rec, od, errs, strings.Join(f.Failures, "; "))
	}
	fmt.Fprintln(w, "\n| Attempt | Valid | Invalid because | Breaker openings |")
	fmt.Fprintln(w, "|---|---|---|---|")
	for _, a := range attempts {
		fmt.Fprintf(w, "| %s | %v | %s | %d |\n", a.Name, a.Valid, strings.Join(a.Invalid, "; "), len(a.Breaker))
	}
	fmt.Fprintln(w, "\n| Fault | Runs | Valid runs | Attempts | Invalid attempts | Verdict |")
	fmt.Fprintln(w, "|---|---|---|---|---|---|")
	byFault := map[string][]faultRun{}
	for _, r := range runs {
		byFault[r.Fault] = append(byFault[r.Fault], r)
	}
	for _, k := range slices.Sorted(maps.Keys(byFault)) {
		var valid, n, invalid int
		for _, r := range byFault[k] {
			if r.Valid {
				valid++
			}
		}
		for _, a := range attempts {
			if a.Fault == k {
				n++
				if !a.Valid {
					invalid++
				}
			}
		}
		fmt.Fprintf(w, "| %s | %d | %d | %d | %d | %s |\n", k, len(byFault[k]), valid, n, invalid, faultVerdict(byFault[k]))
	}
	return nil
}

func printf(w io.Writer, format string, a ...any) error {
	_, err := fmt.Fprintf(w, format, a...)
	return err
}
