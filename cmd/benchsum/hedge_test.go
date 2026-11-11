package main

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestHedgeTable(t *testing.T) {
	dir := t.TempDir()
	write := func(name, body string) {
		if err := os.WriteFile(filepath.Join(dir, name), []byte(body), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	// hist gives a shard's calls cumulative at 1, 2 and 3 ms; 100 calls end by 3 ms.
	hist := func(shard int, c1, c2 int) string {
		var b strings.Builder
		for i, c := range []int{c1, c2, 100, 100} {
			le := []string{"0.001", "0.002", "0.003", "+Inf"}[i]
			fmt.Fprintf(&b, "recserve_shard_search_seconds_bucket{shard=\"%d\",le=\"%s\"} %d\n", shard, le, c)
		}
		fmt.Fprintf(&b, "recserve_shard_search_seconds_sum{shard=\"%d\"} 0.1\nrecserve_shard_search_seconds_count{shard=\"%d\"} 100\n", shard, shard)
		return b.String()
	}
	run := func(name string, s0, s1 [2]int, extra string) {
		write(name+".summary.json", `{"name":"`+name+`","valid":true,"p99_ms":3}`)
		write(name+".at0.metrics", "")
		write(name+".at1.metrics", "# TYPE recserve_shard_search_seconds histogram\n"+hist(0, s0[0], s0[1])+hist(1, s1[0], s1[1])+extra)
	}
	// The larger shard's p95: 1.9 ms (shard 1), 2.5 ms (shard 0), 1 ms.
	run("delay0-run1", [2]int{95, 100}, [2]int{50, 100}, "")
	run("delay0-run2", [2]int{0, 90}, [2]int{95, 100}, "")
	run("delay0-run3", [2]int{95, 100}, [2]int{95, 100}, "")
	// Shard 0: 100 calls, 4 hedges won and 6 lost, so 90 searches. Shard 1: 100 calls, 1 hedge, 1 failover, 98 searches.
	run("default-run1", [2]int{95, 100}, [2]int{95, 100}, `# TYPE recserve_shard_hedges_total counter
recserve_shard_hedges_total{shard="0",result="won"} 4
recserve_shard_hedges_total{shard="0",result="lost"} 6
recserve_shard_hedges_total{shard="0",result="denied"} 7
recserve_shard_hedges_total{shard="1",result="won"} 1
# TYPE recserve_shard_failovers_total counter
recserve_shard_failovers_total{shard="1",replica="r"} 1
`)
	var b strings.Builder
	d, err := hedgeTable(&b, dir)
	if err != nil {
		t.Fatal(err)
	}
	if d != 2 {
		t.Errorf("default %g ms, want the median 1.9 rounded up to 2", d)
	}
	if want := "| default-run1 | true | false | 3.00 | 1.00 | 1.00 | 0.111 | 0.010 |"; !strings.Contains(b.String(), want) {
		t.Errorf("table lacks %q:\n%s", want, b.String())
	}
}
