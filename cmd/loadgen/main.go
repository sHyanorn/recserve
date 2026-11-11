// Command loadgen offers the test events of the split to a running gateway
// at a fixed rate and reports the latency distribution (docs/BENCHMARKS.md,
// Load tests).
//
// Latency counts from the time each request was due, so a stall of the
// gateway is charged to every request it delayed. A run is valid only if
// the target rate was met and every request succeeded; loadgen exits with
// an error otherwise.
package main

import (
	"cmp"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"math"
	"os"
	"os/signal"
	"slices"
	"strings"
	"syscall"
	"time"

	"github.com/sHyanorn/recserve/internal/dataset"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	err := run(ctx, os.Args[1:], os.Stdout)
	stop()
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run(ctx context.Context, args []string, stdout io.Writer) error {
	fs := flag.NewFlagSet("loadgen", flag.ContinueOnError)
	var cfg config
	fs.StringVar(&cfg.addr, "addr", "localhost:8080", "gateway address")
	fs.Float64Var(&cfg.rate, "rate", 100, "requests per second")
	fs.DurationVar(&cfg.warmup, "warmup", 10*time.Second, "how long to run before measuring")
	fs.DurationVar(&cfg.duration, "duration", 60*time.Second, "how long to measure")
	fs.IntVar(&cfg.conns, "conns", 64, "keep-alive connections")
	split := fs.String("split", "", "split file whose events are replayed in time order (required)")
	k := fs.Int("k", 20, "items per recommendation")
	writeShare := fs.Float64("write-share", 0, "share of requests that write the event instead of asking for recommendations")
	repeat := fs.Float64("repeat", 0, "share of requests that copy one of the 1,000 before them, as a client asking again")
	seed := fs.Uint64("seed", 1, "seed of the repeats")
	fs.StringVar(&cfg.samples, "samples", "", "gzipped CSV file for one sample per measured request")
	out := fs.String("out", "", "file to write the JSON report to")
	if err := fs.Parse(args); err != nil {
		return err
	}
	// The comparisons are written so that NaN fails them.
	requests := cfg.rate * (cfg.warmup + cfg.duration).Seconds()
	if !(cfg.rate > 0) || cfg.duration <= 0 || cfg.warmup < 0 || cfg.conns <= 0 || !(requests < math.MaxInt32) ||
		!(*writeShare >= 0 && *writeShare <= 1) || !(*repeat >= 0 && *repeat <= 1) || *k < 1 || *split == "" {
		return errors.New("loadgen: -split is required; -rate, -duration, -conns and -k must be positive, -warmup not negative, -write-share and -repeat in [0, 1]")
	}
	rows, err := dataset.Read(*split)
	if err != nil {
		return err
	}
	if len(rows) == 0 {
		return errors.New("loadgen: the split is empty")
	}
	ordered := make([]row, len(rows))
	for i, r := range rows {
		ordered[i] = row{r.User, r.Item, r.TS}
	}
	slices.SortStableFunc(ordered, func(a, b row) int { return cmp.Compare(a.ts, b.ts) })
	cfg.work = newDatasetWorkload(ordered, *k, *writeShare, *repeat, *seed, int(requests))
	r, err := measure(ctx, cfg)
	if err != nil {
		return err
	}
	if err := printReport(stdout, r); err != nil {
		return err
	}
	if *out != "" {
		data, err := json.MarshalIndent(r, "", "  ")
		if err != nil {
			return err
		}
		if err := os.WriteFile(*out, append(data, '\n'), 0o644); err != nil {
			return err
		}
	}
	if !r.Valid {
		return fmt.Errorf("loadgen: invalid run: %s", strings.Join(r.Problems, "; "))
	}
	return nil
}

var latencyRows = []struct{ key, name string }{
	{"reply", "due to reply"},
	{"service", "service time"},
	{"send_lag", "send lag"},
	{"hiccup", "loadgen hiccup"},
}

// printReport writes r as a short table.
func printReport(w io.Writer, r report) error {
	counts := make([]string, 0, len(r.Outcomes))
	for k, n := range r.Outcomes {
		counts = append(counts, fmt.Sprintf("%s %d", k, n))
	}
	slices.Sort(counts)
	var b strings.Builder
	fmt.Fprintf(&b, "target %.0f/s, achieved %.0f/s over %gs after %gs warmup, %d connections\n",
		r.TargetRate, r.Achieved, r.Duration, r.Warmup, r.Conns)
	fmt.Fprintf(&b, "%s\n", strings.Join(counts, ", "))
	fmt.Fprintf(&b, "%-16s %9s %8s %8s %8s %8s %8s %8s\n", "latency (µs)", "count", "p50", "p90", "p99", "p99.9", "p99.99", "max")
	for _, row := range latencyRows {
		s := r.Latency[row.key]
		fmt.Fprintf(&b, "%-16s %9d %8.0f %8.0f %8.0f %8.0f %8.0f %8.0f\n", row.name, s.Count, s.P50, s.P90, s.P99, s.P999, s.P9999, s.Max)
	}
	if r.Valid {
		b.WriteString("valid\n")
	} else {
		fmt.Fprintf(&b, "invalid: %s\n", strings.Join(r.Problems, "; "))
	}
	_, err := io.WriteString(w, b.String())
	return err
}
