package main

import (
	"fmt"
	"math"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestSwapRun(t *testing.T) {
	dir := t.TempDir()
	p := filepath.Join(dir, "gc-off-run1")
	write := func(suffix, body string) {
		if err := os.WriteFile(p+suffix, []byte(body), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	t0 := time.Date(2026, 11, 11, 0, 0, 0, 0, time.UTC)
	ms := t0.UnixMilli()
	write(".json", `{"start":"2026-11-11T00:00:00Z","target_rate":100,"achieved_rate":100,"warmup_s":10,"duration_s":6,"valid":true}`)
	write(".noise-before.json", `{"noisy":false}`)
	write(".noise-after.json", `{"noisy":false}`)
	// Requests every 10 ms of the measured 10 to 16 s: 1 ms, but 30 ms during
	// the first swap's second and 5 ms for one request at 11.5 s.
	var b strings.Builder
	b.WriteString("i,path,due_ms,latency_ms,status,outcome,degraded,answered,shards\n")
	for due := 10_000; due < 16_000; due += 10 {
		lat := 1
		switch {
		case due >= 12_000 && due < 13_000:
			lat = 30
		case due == 11_500:
			lat = 5
		}
		fmt.Fprintf(&b, "%d,/v1/recommend,%d,%d,200,recommend_ok,,2,2\n", due/10, due, lat)
	}
	if err := writeGz(p+".csv.gz", b.String()); err != nil {
		t.Fatal(err)
	}
	// Swaps over 12.2 to 12.6 s and 14.9 to 15.1 s: the windows from 12, 14 and 15 s.
	write(".swaps", fmt.Sprintf("%d %d {\"model_version\":\"a\",\"previous\":\"b\",\"prepare_ms\":350.5,\"drain_ms\":2}\n%d %d {\"prepare_ms\":1}\n",
		ms+12_200, ms+12_600, ms+14_900, ms+15_100))
	write(".swap1.cpu0", "gateway 1000000\nannshard-0 0\nannshard-1 0\nvalkey 5\n")
	write(".swap1.cpu1", "gateway 3000000\nannshard-0 1000000\nannshard-1 2000000\nvalkey 9\n")
	write(".swap2.cpu0", "gateway 0\n")
	write(".swap2.cpu1", "gateway 0\n")
	// The process started 2 s before the first request: GCs at 12.3 s (a
	// swap window), 13.5 s (another) and -1 s (before the measurement). The
	// log begins with the trace of an earlier process in the same container,
	// which exited; its times are not from that start.
	write(".started", t0.Add(-2*time.Second).Format(time.RFC3339Nano)+"\n")
	write(".gctrace", "gc 1 @0.100s 1%: 1+1+1 ms clock, x\ngc 2 @13.600s 1%: 5+1+5 ms clock, x\n"+"gc 1 @14.300s 1%: 0.1+2+0.2 ms clock, 0.4+0.1/1/0+0.8 ms cpu, 4->4->1 MB, 4 MB goal, 0 MB stacks, 0 MB globals, 4 P\n"+
		"gc 2 @15.500s 1%: 0.5+1+0.5 ms clock, x\ngc 3 @1.000s 0%: 9+1+9 ms clock, x\nnot a gc line\n")

	s, err := summarizeSwap(p, p+".csv.gz")
	if err != nil {
		t.Fatal(err)
	}
	near := func(a, b float64) bool { return math.Abs(a-b) < 1e-9 }
	if s.Swap.Windows != 3 || s.Others.Windows != 3 || s.Swap.Max != 30 || s.Others.Max != 5 {
		t.Errorf("windows %+v and %+v", s.Swap, s.Others)
	}
	if s.Swap.GCs != 1 || !near(s.Swap.GCPauses, 0.3) || s.Others.GCs != 1 || !near(s.Others.GCPauses, 1) {
		t.Errorf("GCs %+v and %+v", s.Swap, s.Others)
	}
	if w := s.Swaps[0]; len(s.Swaps) != 2 || w.Start != 12_200 || w.End != 12_600 || w.PrepareMS != 350.5 || w.GatewayCPUMS != 2 || w.ReplicasCPUMS != 3 {
		t.Errorf("swaps %+v", s.Swaps)
	}
	if err := writeJSON(p+".swap.json", s); err != nil {
		t.Fatal(err)
	}
	var tb strings.Builder
	if err := swapTable(&tb, dir); err != nil {
		t.Fatal(err)
	}
	if want := "over the other windows 5.00 ms (ratio 6.00): turning GC off does not remove them"; !strings.Contains(tb.String(), want) {
		t.Errorf("table lacks %q:\n%s", want, tb.String())
	}
}
