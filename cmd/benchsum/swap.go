package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"time"
)

// swapRun is one run of docs/BENCHMARKS.md's Bundle swaps under load.
// Times are in ms from the due time of the run's first request.
type swapRun struct {
	Name    string     `json:"name"`
	Valid   bool       `json:"valid"`
	Noisy   bool       `json:"noisy"`
	Swaps   []swapTime `json:"swaps"`
	Swap    swapGroup  `json:"swap_windows"`  // the 1 s windows that overlap a swap
	Others  swapGroup  `json:"other_windows"` // the rest of the measured window
	Windows []stats    `json:"windows"`       // each second of the measured window
}

type swapTime struct {
	Start, End    float64
	PrepareMS     float64 `json:"prepare_ms"`
	DrainMS       float64 `json:"drain_ms"`
	GatewayCPUMS  float64 `json:"gateway_cpu_ms"`
	ReplicasCPUMS float64 `json:"replicas_cpu_ms"`
}

type swapGroup struct {
	Windows  int     `json:"windows"`
	P99      float64 `json:"p99_ms"`
	Max      float64 `json:"max_ms"`
	GCs      int     `json:"gc_cycles"`
	GCPauses float64 `json:"gc_pause_ms"` // stop-the-world, summed
}

// gcLine matches a line of GODEBUG=gctrace=1: the cycle's number, its start
// in s since the process started, and its clock times, of which the first
// and the last are the stop-the-world pauses.
var gcLine = regexp.MustCompile(`^gc (\d+) @([0-9.]+)s \d+%: ([0-9.]+)\+[0-9.]+\+([0-9.]+) ms clock`)

func summarizeSwap(prefix, raw string) (swapRun, error) {
	var r loadReport
	if err := readJSON(prefix+".json", &r); err != nil {
		return swapRun{}, err
	}
	samples, err := readSamples(raw)
	if err != nil {
		return swapRun{}, err
	}
	s := swapRun{Name: filepath.Base(prefix), Valid: r.Valid, Windows: windows(samples, r.Warmup*1000, (r.Warmup+r.Duration)*1000)}
	if s.Noisy, err = noisy(prefix); err != nil {
		return swapRun{}, err
	}
	start := float64(r.Start.UnixMilli())
	if s.Swaps, err = readSwaps(prefix, start); err != nil {
		return swapRun{}, err
	}
	gcs, err := readGCs(prefix, start)
	if err != nil {
		return swapRun{}, err
	}
	overlaps := func(from, to float64) bool {
		return slices.ContainsFunc(s.Swaps, func(w swapTime) bool { return from < w.End && to > w.Start })
	}
	var in, out []sample
	for t := r.Warmup * 1000; t+1000 <= (r.Warmup+r.Duration)*1000; t += 1000 {
		group, list := &s.Others, &out
		if overlaps(t, t+1000) {
			group, list = &s.Swap, &in
		}
		group.Windows++
		for _, x := range samples {
			if x.due >= t && x.due < t+1000 {
				*list = append(*list, x)
			}
		}
		for _, g := range gcs {
			if g[0] >= t && g[0] < t+1000 {
				group.GCs++
				group.GCPauses += g[1]
			}
		}
	}
	for _, g := range []struct {
		group *swapGroup
		list  []sample
	}{{&s.Swap, in}, {&s.Others, out}} {
		st := describe(g.list, math.Inf(-1), math.Inf(1))
		g.group.P99, g.group.Max = st.P99, st.Max
	}
	return s, nil
}

// readSwaps reads PREFIX.swaps, lines of "<request ms> <answer ms> <answer>",
// and each swap's CPU readings PREFIX.swapN.cpu0 and .cpu1.
func readSwaps(prefix string, start float64) ([]swapTime, error) {
	f, err := os.Open(prefix + ".swaps")
	if err != nil {
		return nil, err
	}
	defer f.Close() //nolint:errcheck // read only
	var out []swapTime
	sc := bufio.NewScanner(f)
	for sc.Scan() {
		fields := strings.SplitN(sc.Text(), " ", 3)
		if len(fields) != 3 {
			return nil, fmt.Errorf("%s: %q", prefix+".swaps", sc.Text())
		}
		var w swapTime
		var errs [3]error
		w.Start, errs[0] = strconv.ParseFloat(fields[0], 64)
		w.End, errs[1] = strconv.ParseFloat(fields[1], 64)
		errs[2] = json.Unmarshal([]byte(fields[2]), &w)
		for _, err := range errs {
			if err != nil {
				return nil, fmt.Errorf("%s: %q: %w", prefix+".swaps", sc.Text(), err)
			}
		}
		w.Start, w.End = w.Start-start, w.End-start
		n := len(out) + 1
		c0, err := readCPU(fmt.Sprintf("%s.swap%d.cpu0", prefix, n))
		if err != nil {
			return nil, err
		}
		c1, err := readCPU(fmt.Sprintf("%s.swap%d.cpu1", prefix, n))
		if err != nil {
			return nil, err
		}
		for c := range c1 {
			d := (c1[c] - c0[c]) / 1e6
			switch {
			case c == "gateway":
				w.GatewayCPUMS = d
			case strings.HasPrefix(c, "annshard-"):
				w.ReplicasCPUMS += d
			}
		}
		out = append(out, w)
	}
	return out, sc.Err()
}

// readGCs reads the gateway's GC trace, PREFIX.gctrace, and the time its
// container started, PREFIX.started; it returns each cycle's start in ms
// from start and its stop-the-world pauses in ms. A container restarted
// after its process exited logs both processes; only the last one's cycles,
// after the last fall in the cycle number, are kept.
func readGCs(prefix string, start float64) ([][2]float64, error) {
	data, err := os.ReadFile(prefix + ".started")
	if err != nil {
		return nil, err
	}
	began, err := time.Parse(time.RFC3339Nano, strings.TrimSpace(string(data)))
	if err != nil {
		return nil, err
	}
	data, err = os.ReadFile(prefix + ".gctrace")
	if err != nil {
		return nil, err
	}
	var out [][2]float64
	last := 0.0
	for line := range strings.Lines(string(data)) {
		m := gcLine.FindStringSubmatch(line)
		if m == nil {
			continue
		}
		var v [4]float64
		for i := range v {
			if v[i], err = strconv.ParseFloat(m[i+1], 64); err != nil {
				return nil, err
			}
		}
		if v[0] <= last {
			out = out[:0]
		}
		last = v[0]
		out = append(out, [2]float64{float64(began.UnixMilli()) + 1000*v[1] - start, v[2] + v[3]})
	}
	return out, nil
}

// swapTable writes the swap runs of dir and the attribution of
// docs/BENCHMARKS.md: with GOGC=off (runs named gc-off-*), is the median
// over runs of the max over swap windows at most 1.2 times the median of the
// max over the other windows?
func swapTable(w *strings.Builder, dir string) error {
	paths, err := filepath.Glob(filepath.Join(dir, "*.swap.json"))
	if err != nil {
		return err
	}
	slices.Sort(paths)
	fmt.Fprintln(w, "| Run | Valid | Noisy | Swap windows p99 | max | GCs | GC pauses ms | Other windows p99 | max | GCs | GC pauses ms | Prepare ms | Drain ms | Gateway CPU ms | Replicas CPU ms |")
	fmt.Fprintln(w, "|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|")
	var swapMax, otherMax []float64
	for _, p := range paths {
		var s swapRun
		if err := readJSON(p, &s); err != nil {
			return err
		}
		if strings.HasPrefix(s.Name, "gc-off-") {
			swapMax, otherMax = append(swapMax, s.Swap.Max), append(otherMax, s.Others.Max)
		}
		var prep, drain, gw, rep []float64
		for _, x := range s.Swaps {
			prep, drain, gw, rep = append(prep, x.PrepareMS), append(drain, x.DrainMS), append(gw, x.GatewayCPUMS), append(rep, x.ReplicasCPUMS)
		}
		fmt.Fprintf(w, "| %s | %v | %v | %.2f | %.2f | %d | %.2f | %.2f | %.2f | %d | %.2f | %s | %s | %s | %s |\n", s.Name, s.Valid, s.Noisy,
			s.Swap.P99, s.Swap.Max, s.Swap.GCs, s.Swap.GCPauses, s.Others.P99, s.Others.Max, s.Others.GCs, s.Others.GCPauses,
			span(prep), span(drain), span(gw), span(rep))
	}
	if len(swapMax) == 0 {
		return nil
	}
	slices.Sort(swapMax)
	slices.Sort(otherMax)
	a, b := median(swapMax), median(otherMax)
	cause := "turning GC off does not remove them"
	if a <= 1.2*b {
		cause = "GC"
	}
	fmt.Fprintf(w, "\nGOGC=off: median max over swap windows %.2f ms, over the other windows %.2f ms (ratio %.2f): %s\n", a, b, a/b, cause)
	return nil
}
