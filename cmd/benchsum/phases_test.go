package main

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestPhaseTable(t *testing.T) {
	dir := t.TempDir()
	if err := os.Mkdir(filepath.Join(dir, "raw"), 0o755); err != nil {
		t.Fatal(err)
	}
	write := func(name, body string) {
		if err := os.WriteFile(filepath.Join(dir, name), []byte(body), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	write("run1.json", `{"start":"2026-11-11T00:00:00Z","warmup_s":1,"duration_s":4,"valid":true}`)
	const t0 = 1_794_355_200_000 // 2026-11-11T00:00:00Z in ms
	write("run1.marks", fmt.Sprintf("gateways %d\nswap %d\n", t0+2000, t0+3500))
	// One request per 10 ms of the measured 1 to 5 s: 1 ms, 10 ms from 2 to 3.5 s, one 503 at 4 s.
	var b strings.Builder
	b.WriteString("i,path,due_ms,latency_ms,status,outcome,degraded,answered,shards\n")
	for due := 1000; due < 5000; due += 10 {
		lat, status, degraded := 1, 200, ""
		if due >= 2000 && due < 3500 {
			lat, degraded = 10, "shards_partial"
		}
		if due == 4000 {
			status = 503
		}
		fmt.Fprintf(&b, "%d,/v1/recommend,%d,%d,%d,x,%s,2,2\n", due/10, due, lat, status, degraded)
	}
	if err := writeGz(filepath.Join(dir, "raw", "run1.csv.gz"), b.String()); err != nil {
		t.Fatal(err)
	}
	var tb strings.Builder
	if err := phaseTable(&tb, dir); err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{
		"| run1 | before | 100 | 1.00 | 1.00 | 1.00 | 0 | 0.000% |  |",
		"| run1 | gateways | 150 | 10.00 | 10.00 | 10.00 | 0 | 100.000% | shards_partial 100.000% |",
		"| run1 | swap | 149 | 1.00 | 1.00 | 1.00 | 1 | 0.000% |  |",
	} {
		if !strings.Contains(tb.String(), want) {
			t.Errorf("table lacks %q:\n%s", want, tb.String())
		}
	}
	if _, err := os.Stat(filepath.Join(dir, "run1.phases.json")); err != nil {
		t.Error(err)
	}
}
