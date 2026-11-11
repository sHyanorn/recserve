package main

import (
	"bufio"
	"encoding/binary"
	"encoding/json"
	"errors"
	"io"
	"math"
	"math/rand/v2"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"testing"

	"github.com/sHyanorn/recserve/internal/bench"
	"github.com/sHyanorn/recserve/internal/vec"
)

const (
	testN, testQ, testDim = 3000, 40, 16
)

// writeSet writes a clustered set of unit vectors in prep.py's format, with
// the exact top 100 by float64 inner product as ground truth.
func writeSet(t *testing.T, dir string) {
	t.Helper()
	r := rand.New(rand.NewPCG(5, 1))
	centers := make([]float64, 30*testDim)
	for i := range centers {
		centers[i] = r.NormFloat64()
	}
	rows := make([]float32, (testN+testQ)*testDim)
	for i := range testN + testQ {
		c := r.IntN(30)
		var norm float64
		row := make([]float64, testDim)
		for j := range row {
			row[j] = centers[c*testDim+j] + 0.3*r.NormFloat64()
			norm += row[j] * row[j]
		}
		for j, v := range row {
			rows[i*testDim+j] = float32(v / math.Sqrt(norm))
		}
	}
	base, queries := rows[:testN*testDim], rows[testN*testDim:]
	truth := make([]int32, 0, testQ*bench.TruthK)
	for q := range testQ {
		hits := make([]vec.Hit, testN)
		for i := range hits {
			hits[i] = vec.Hit{ID: int32(i), Score: vec.Dot(queries[q*testDim:(q+1)*testDim], base[i*testDim:(i+1)*testDim])}
		}
		slices.SortFunc(hits, func(a, b vec.Hit) int {
			if vec.Better(a, b) {
				return -1
			}
			return 1
		})
		for _, h := range hits[:bench.TruthK] {
			truth = append(truth, h.ID)
		}
	}
	if err := os.WriteFile(filepath.Join(dir, "t.dim"), []byte("16\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	for name, v := range map[string]any{"t_base.f32": base, "t_query.f32": queries, "t_gt.i32": truth} {
		f, err := os.Create(filepath.Join(dir, name))
		if err != nil {
			t.Fatal(err)
		}
		if err := binary.Write(f, binary.LittleEndian, v); err != nil {
			t.Fatal(err)
		}
		if err := f.Close(); err != nil {
			t.Fatal(err)
		}
	}
}

func readJSON[T any](t *testing.T, path string) T {
	t.Helper()
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var v T
	if err := json.Unmarshal(b, &v); err != nil {
		t.Fatal(err)
	}
	return v
}

func TestBuildAndQuery(t *testing.T) {
	dir := t.TempDir()
	writeSet(t, dir)
	measure := func() float64 { return 1 }
	o := options{dataDir: dir, set: "t", graph: filepath.Join(dir, "g.bin"), m: 8, efc: 64, workers: 2, seed: 1,
		efs: []int{10, 20, 200}, ks: []int{10, 100}, check: testQ}

	o.mode, o.out = "checkgt", filepath.Join(dir, "check.json")
	if err := run(o, measure, t.Logf); err != nil {
		t.Fatal(err)
	}

	o.mode, o.out = "build", filepath.Join(dir, "build.json")
	if err := run(o, measure, t.Logf); err != nil {
		t.Fatal(err)
	}
	b := readJSON[buildRecord](t, o.out)
	if b.Nodes != testN || b.M != 8 || b.EfC != 64 || b.Workers != 2 || b.BuildSeconds <= 0 || b.HeapObjectsMiB <= 0 ||
		b.UnreachableLayer0 > testN/200 || b.Noise.Before != 1 || b.Noise.Attempt != 1 {
		t.Fatalf("build record %+v", b)
	}

	o.mode, o.out, o.cpuprofile = "query", filepath.Join(dir, "query.json"), filepath.Join(dir, "cpu.pprof")
	if err := run(o, measure, t.Logf); err != nil {
		t.Fatal(err)
	}
	if p, err := os.ReadFile(o.cpuprofile); err != nil || len(p) < 2 || p[0] != 0x1f || p[1] != 0x8b {
		t.Fatalf("CPU profile: %v, %d bytes; want a gzipped profile", err, len(p))
	}
	r := readJSON[queryRecord](t, o.out)
	if r.M != 8 || r.EfC != 64 || r.Queries != testQ || r.UnreachableLayer0 != b.UnreachableLayer0 {
		t.Fatalf("query record %+v", r)
	}
	var got [][2]int
	for _, p := range r.Points {
		got = append(got, [2]int{p.K, p.Ef})
		if p.Latency.N != testQ || p.Latency.P99 <= 0 || p.QPS <= 0 || p.Noise.Attempt != 1 || p.Noise.Flagged {
			t.Errorf("k=%d ef=%d: %+v", p.K, p.Ef, p)
		}
		if p.Ef == 200 && p.Recall < 0.95 {
			t.Errorf("k=%d ef=200: recall %.4f", p.K, p.Recall)
		}
	}
	if want := [][2]int{{10, 10}, {10, 20}, {10, 200}, {100, 200}}; !slices.Equal(got, want) {
		t.Fatalf("points (k, ef) %v, want %v: ef below k is skipped", got, want)
	}
}

func TestCheckTruthFailsOnWrongTruth(t *testing.T) {
	dir := t.TempDir()
	writeSet(t, dir)
	path := filepath.Join(dir, "t_gt.i32")
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	// Query 7's best and 50th neighbors trade places.
	at := func(r int) []byte { i := 4 * (7*bench.TruthK + r); return b[i : i+4] }
	x := slices.Clone(at(0))
	copy(at(0), at(49))
	copy(at(49), x)
	if err := os.WriteFile(path, b, 0o644); err != nil {
		t.Fatal(err)
	}
	o := options{mode: "checkgt", dataDir: dir, set: "t", check: testQ}
	if err := run(o, func() float64 { return 1 }, t.Logf); err == nil {
		t.Fatal("swapped neighbors accepted")
	}
}

func TestBadInput(t *testing.T) {
	dir := t.TempDir()
	writeSet(t, dir)
	measure := func() float64 { return 1 }
	for name, o := range map[string]options{
		"unknown mode":   {mode: "x"},
		"build no graph": {mode: "build", m: 8, efc: 16},
		"missing graph":  {mode: "query", graph: filepath.Join(dir, "none"), efs: []int{10}, ks: []int{10}},
		"k above truth":  {mode: "query", graph: filepath.Join(dir, "none"), efs: []int{200}, ks: []int{101}},
	} {
		o.dataDir, o.set = dir, "t"
		if err := run(o, measure, t.Logf); err == nil {
			t.Errorf("%s: accepted", name)
		}
	}
	if _, err := ints("10,x"); err == nil {
		t.Error("ints accepted 10,x")
	}
	if _, err := ints("0"); err == nil {
		t.Error("ints accepted 0")
	}
}

func TestNoiseBaseline(t *testing.T) {
	dir := t.TempDir()
	script := func(name, body string) string {
		p := filepath.Join(dir, name)
		if err := os.WriteFile(p, []byte("#!/bin/sh\n"+body+"\n"), 0o755); err != nil {
			t.Fatal(err)
		}
		return p
	}
	if v, err := noiseBaseline(script("ok", `[ "$*" = "-n 1 -cpu 2 -q" ] && echo 481234.5`), 2); err != nil || v != 481234.5 {
		t.Fatalf("got %v, %v", v, err)
	}
	for name, body := range map[string]string{"fails": "exit 1", "text": "echo median 5", "zero": "echo 0"} {
		if _, err := noiseBaseline(script(name, body), 0); err == nil {
			t.Errorf("%s: accepted", name)
		}
	}
}

func TestShards(t *testing.T) {
	dir := t.TempDir()
	writeSet(t, dir)
	s, err := bench.LoadANNSet(dir, "t")
	if err != nil {
		t.Fatal(err)
	}
	o := options{dataDir: dir, set: "t", graph: filepath.Join(dir, "g"), m: 8, efc: 64, workers: 1, seed: 1,
		efs: []int{10, 200}, ks: []int{10, 100}, shards: 2}
	// Shards run in goroutines connected by pipes, as processes are by theirs.
	spawn := func(i, n int) (*shardConn, error) {
		so := o
		so.shard, so.shards, so.graph = i, n, o.graph+"."+strconv.Itoa(i)
		reqR, reqW := io.Pipe()
		repR, repW := io.Pipe()
		errc := make(chan error, 1)
		go func() {
			err := serve(s, so, reqR, repW)
			errc <- errors.Join(err, repW.Close())
		}()
		return &shardConn{w: bufio.NewWriter(reqW), r: bufio.NewReader(repR), done: func() error {
			return errors.Join(reqW.Close(), <-errc)
		}}, nil
	}
	var first []point
	for run := range 2 { // the second run reads the graphs the first one saved, given other build flags
		if run == 1 {
			o.m, o.efc = 16, 200
		}
		res, err := shards(s, o, func() float64 { return 1 }, t.Logf, spawn)
		if err != nil {
			t.Fatal(err)
		}
		r := res.(shardRecord)
		if len(r.Shards) != 2 || r.Shards[0].Nodes+r.Shards[1].Nodes != testN || r.Shards[0].Nodes < testN/3 || r.Shards[1].Nodes < testN/3 {
			t.Fatalf("shards %+v", r.Shards)
		}
		if built := r.Shards[0].BuildSeconds > 0; built != (run == 0) {
			t.Fatalf("run %d: build seconds %v", run, r.Shards[0].BuildSeconds)
		}
		if r.M != 8 || r.EfC != 64 {
			t.Fatalf("run %d: M %d and efConstruction %d recorded, the graphs' are 8 and 64", run, r.M, r.EfC)
		}
		var got [][2]int
		for i, p := range r.Points {
			got = append(got, [2]int{p.K, p.Ef})
			if p.M != 8 || p.EfC != 64 {
				t.Errorf("run %d: point labelled M %d and efConstruction %d", run, p.M, p.EfC)
			}
			if p.Ef == 200 && p.Recall < 0.99 {
				t.Errorf("k=%d ef=200: recall %.4f", p.K, p.Recall)
			}
			if run == 1 && p.Recall != first[i].Recall {
				t.Errorf("k=%d ef=%d: recall %.4f from saved graphs, %.4f from built ones", p.K, p.Ef, p.Recall, first[i].Recall)
			}
		}
		if want := [][2]int{{10, 10}, {10, 200}, {100, 200}}; !slices.Equal(got, want) {
			t.Fatalf("points %v, want %v", got, want)
		}
		first = r.Points
	}
}
