package main

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestCompareTableANN(t *testing.T) {
	dir := t.TempDir()
	// run writes annbench's points at ef 120 and 400: p99 in µs, recall.
	run := func(name string, p99 [2]float64, recall [2]float64, noisy bool) {
		var pts []string
		for i, ef := range []int{120, 400} {
			pts = append(pts, fmt.Sprintf(`{"ef":%d,"k":100,"recall":%g,"latency":{"p99_us":%g},"noise":{"noisy":%t}}`, ef, recall[i], p99[i], noisy && i == 1))
		}
		body := `{"set":"glove","points":[` + strings.Join(pts, ",") + `]}`
		if err := os.WriteFile(filepath.Join(dir, name), []byte(body), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	for n, d := range []float64{0, 10, 20} {
		run(fmt.Sprintf("ann-before-%d.json", n+1), [2]float64{1500 + d, 4000 + d}, [2]float64{0.7119, 0.8423}, false)
		run(fmt.Sprintf("ann-after-%d.json", n+1), [2]float64{900 + d, 3990 + d}, [2]float64{0.7121, 0.8440}, n == 2)
	}
	var b strings.Builder
	if err := compareTable(&b, dir); err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{
		"| annbench ef 120 p99 ms | 1.5 to 1.52 | 0.9 to 0.92 | lower after |",
		"| annbench ef 400 p99 ms | 4 to 4.02 | 3.99 to 4.01 | not resolved on this machine |",
		"| annbench ef 120 recall@100 | 0.7119 | 0.7121 | within 0.001 |",
		"| annbench ef 400 recall@100 | 0.8423 | 0.8440 | differs by 0.0017 |",
		"annbench points left noisy after three attempts: 1 of 12",
	} {
		if !strings.Contains(b.String(), want) {
			t.Errorf("missing %q in\n%s", want, b.String())
		}
	}
}
