// Command annbench measures the hand-written HNSW index (internal/hnsw)
// with the R2 protocol of docs/BENCHMARKS.md, on a set written by
// experiments/m0/ann/prep.py.
//
//	annbench -mode checkgt -set glove                                 verify the ground truth of the first queries
//	annbench -mode build -set glove -M 16 -efc 200 -graph G -noise NB  build with -workers goroutines, save to G
//	annbench -mode query -set glove -graph G -noise NB -ref REF        read G, measure every (k, ef)
//	annbench -mode shards -set glove -M 32 -efc 400 -graph G -noise NB  the same through -shards shard processes
//
// In shards mode, shard i reads its graph from G.i, or builds it with
// -workers / -shards goroutines and saves it there.
//
// NB is a cmd/noisebase binary: every noise baseline is taken by running
// it, so the session reference and the baselines come from the same code
// placement (internal/bench, noise.go).
package main

import (
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"os/exec"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/sHyanorn/recserve/internal/bench"
	"github.com/sHyanorn/recserve/internal/hnsw"
	"github.com/sHyanorn/recserve/internal/vec"
)

type options struct {
	mode, dataDir, set, graph, out string
	noise                          string
	m, efc, workers                int
	seed                           uint64
	efs, ks                        []int
	nq, check, cpu                 int
	shards, shard                  int
	ref                            float64
}

func main() {
	var o options
	var efs, ks string
	flag.StringVar(&o.mode, "mode", "query", "checkgt, build or query")
	flag.StringVar(&o.dataDir, "data", "/home/user/data/m0/ann", "directory written by prep.py")
	flag.StringVar(&o.set, "set", "synth", "dataset: glove or synth")
	flag.StringVar(&o.graph, "graph", "", "graph file: written by build, read by query")
	flag.StringVar(&o.out, "out", "", "JSON result file")
	flag.IntVar(&o.m, "M", 16, "build: M")
	flag.IntVar(&o.efc, "efc", 200, "build: efConstruction")
	flag.IntVar(&o.workers, "workers", 4, "build: inserting goroutines; 1 builds reproducibly")
	flag.Uint64Var(&o.seed, "seed", 1, "build: seed of the level draw")
	flag.StringVar(&efs, "ef", "10,20,40,80,120,200,400,800", "query: ef values; those below k are skipped")
	flag.StringVar(&ks, "k", "10,100", "query: k values")
	flag.IntVar(&o.nq, "nq", 0, "query: use the first nq queries (0: all)")
	flag.IntVar(&o.check, "check", 100, "checkgt: number of queries to verify")
	flag.IntVar(&o.shards, "shards", 2, "shards, serve: number of shards")
	flag.IntVar(&o.shard, "shard", 0, "serve: the shard this process serves")
	flag.StringVar(&o.noise, "noise", "", "build, query: cmd/noisebase binary that takes the noise baselines")
	flag.IntVar(&o.cpu, "cpu", 0, "CPU the noise baseline is pinned to")
	flag.Float64Var(&o.ref, "ref", 0, "noise reference in ns per pass (0 disables flagging)")
	flag.Parse()
	var err error
	if o.efs, err = ints(efs); err != nil {
		log.Fatal(err)
	}
	if o.ks, err = ints(ks); err != nil {
		log.Fatal(err)
	}
	if o.noise == "" && o.mode != "checkgt" && o.mode != "serve" {
		log.Fatal("-noise is required")
	}
	measure := func() float64 {
		v, err := noiseBaseline(o.noise, o.cpu)
		if err != nil {
			log.Fatal(err)
		}
		return v
	}
	if err := run(o, measure, log.Printf); err != nil {
		log.Fatal(err)
	}
}

// noiseBaseline runs the noisebase binary once and returns its baseline.
func noiseBaseline(bin string, cpu int) (float64, error) {
	cmd := exec.Command(bin, "-n", "1", "-cpu", strconv.Itoa(cpu), "-q")
	cmd.Stderr = os.Stderr // its reason for failing, such as a CPU it cannot pin to
	out, err := cmd.Output()
	if err != nil {
		return 0, fmt.Errorf("noise baseline: %w", err)
	}
	v, err := strconv.ParseFloat(strings.TrimSpace(string(out)), 64)
	if err != nil || v <= 0 {
		return 0, fmt.Errorf("noise baseline: output %q", out)
	}
	return v, nil
}

func ints(s string) ([]int, error) {
	var v []int
	for _, f := range strings.Split(s, ",") {
		n, err := strconv.Atoi(strings.TrimSpace(f))
		if err != nil || n < 1 {
			return nil, fmt.Errorf("%q is not a list of positive integers", s)
		}
		v = append(v, n)
	}
	return v, nil
}

// run loads the set and runs the mode; measure takes a noise baseline and
// logf reports progress.
func run(o options, measure func() float64, logf func(string, ...any)) error {
	s, err := bench.LoadANNSet(o.dataDir, o.set)
	if err != nil {
		return err
	}
	logf("%s: %d base, %d queries, dim %d", o.set, s.NBase(), s.NQuery(), s.Dim)
	var res any
	switch o.mode {
	case "checkgt":
		res, err = checkTruth(s, o, logf)
	case "build":
		res, err = build(s, o, measure, logf)
	case "query":
		res, err = query(s, o, measure, logf)
	case "shards":
		res, err = shards(s, o, measure, logf, spawnProcess(o))
	case "serve":
		err = serve(s, o, os.Stdin, os.Stdout)
	default:
		err = fmt.Errorf("unknown mode %q", o.mode)
	}
	if err != nil || o.out == "" {
		return err
	}
	return bench.WriteJSON(o.out, res)
}

// checkTruth verifies the ground truth of the first o.check queries with
// an exact scan, on every CPU.
func checkTruth(s *bench.ANNSet, o options, logf func(string, ...any)) (any, error) {
	n := min(o.check, s.NQuery())
	errs := make([]error, n)
	var next atomic.Int64
	var wg sync.WaitGroup
	for range runtime.GOMAXPROCS(0) {
		wg.Go(func() {
			for i := int(next.Add(1) - 1); i < n; i = int(next.Add(1) - 1) {
				errs[i] = s.CheckTruth(i)
			}
		})
	}
	wg.Wait()
	if err := errors.Join(errs...); err != nil {
		return nil, err
	}
	logf("ground truth of %d queries verified", n)
	return map[string]any{"env": bench.Env(nil), "set": o.set, "queries_checked": n}, nil
}

type buildRecord struct {
	Env               map[string]string `json:"env"`
	Set               string            `json:"set"`
	Nodes             int               `json:"nodes"`
	M                 int               `json:"M"`
	EfC               int               `json:"ef_construction"`
	Workers           int               `json:"workers"`
	Seed              uint64            `json:"seed"`
	BuildSeconds      float64           `json:"build_seconds"`
	HeapObjectsMiB    float64           `json:"heap_objects_mib"` // with the base vectors
	VectorsMiB        float64           `json:"vectors_mib"`
	UnreachableLayer0 int               `json:"unreachable_layer0"`
	Noise             bench.Noise       `json:"noise"`
}

func build(s *bench.ANNSet, o options, measure func() float64, logf func(string, ...any)) (any, error) {
	if o.graph == "" {
		return nil, errors.New("build needs -graph")
	}
	f, err := os.Create(o.graph) // before the build, which can take an hour
	if err != nil {
		return nil, err
	}
	r, err := buildTo(f, s, o, measure)
	if cerr := f.Close(); err == nil {
		err = cerr
	}
	if err != nil {
		return nil, err
	}
	logf("build M=%d efc=%d workers=%d: %.0f s, heap %.0f MiB, %d unreachable on layer 0, noisy %v",
		o.m, o.efc, o.workers, r.BuildSeconds, r.HeapObjectsMiB, r.UnreachableLayer0, r.Noise.Flagged)
	return r, nil
}

// buildTo builds the index between two noise baselines, checks it and
// writes it to w.
func buildTo(w io.Writer, s *bench.ANNSet, o options, measure func() float64) (buildRecord, error) {
	before := measure()
	start := time.Now()
	ix, err := hnsw.Build(s.Base, s.Dim, hnsw.Config{M: o.m, EfConstruction: o.efc, Seed: o.seed, Workers: o.workers})
	elapsed := time.Since(start)
	after := measure()
	if err != nil {
		return buildRecord{}, err
	}
	if err := ix.Check(); err != nil {
		return buildRecord{}, err
	}
	r := buildRecord{
		Env: env(o), Set: o.set, Nodes: ix.Len(), M: o.m, EfC: o.efc, Workers: o.workers, Seed: o.seed,
		BuildSeconds: elapsed.Seconds(), HeapObjectsMiB: bench.HeapObjectsMiB(), VectorsMiB: float64(4*len(s.Base)) / (1 << 20),
		UnreachableLayer0: unreachable(ix),
		Noise: bench.Noise{Reference: o.ref, Before: before, After: after, Attempt: 1,
			Flagged: bench.Noisy(before, o.ref) || bench.Noisy(after, o.ref)},
	}
	_, err = ix.WriteTo(w)
	return r, err
}

// env is the environment record with the noisebase binary's checksum.
func env(o options) map[string]string {
	extra := map[string]string{"noisebase": o.noise}
	if o.noise != "" {
		extra["noisebase_sha256"] = bench.FileSHA256(o.noise)
	}
	return bench.Env(extra)
}

func unreachable(ix *hnsw.Index) int {
	n := 0
	for _, ok := range ix.Reachable() {
		if !ok {
			n++
		}
	}
	return n
}

type point struct {
	Index   string        `json:"index"`
	M       int           `json:"M"`
	EfC     int           `json:"ef_construction"`
	Ef      int           `json:"ef"`
	K       int           `json:"k"`
	Recall  float64       `json:"recall"`
	Latency bench.Summary `json:"latency"`
	QPS     float64       `json:"qps"`
	GC      bench.GCStats `json:"gc"`
	Noise   bench.Noise   `json:"noise"`
}

type queryRecord struct {
	Env               map[string]string `json:"env"`
	Set               string            `json:"set"`
	Nodes             int               `json:"nodes"`
	Queries           int               `json:"queries"`
	M                 int               `json:"M"`
	EfC               int               `json:"ef_construction"`
	HeapObjectsMiB    float64           `json:"heap_objects_mib"` // after reading, with the base vectors
	UnreachableLayer0 int               `json:"unreachable_layer0"`
	Points            []point           `json:"points"`
}

func query(s *bench.ANNSet, o options, measure func() float64, logf func(string, ...any)) (any, error) {
	nq, err := queryCount(s, o)
	if err != nil {
		return nil, err
	}
	f, err := os.Open(o.graph)
	if err != nil {
		return nil, err
	}
	ix, err := hnsw.Read(f, s.Base, s.Dim)
	if cerr := f.Close(); err == nil {
		err = cerr
	}
	if err != nil {
		return nil, err
	}
	m, efc := ix.Params()
	r := queryRecord{Env: env(o), Set: o.set, Nodes: ix.Len(), Queries: nq, M: m, EfC: efc,
		HeapObjectsMiB: bench.HeapObjectsMiB(), UnreachableLayer0: unreachable(ix)}
	logf("graph M=%d efc=%d: heap %.0f MiB, %d unreachable on layer 0", m, efc, r.HeapObjectsMiB, r.UnreachableLayer0)
	for _, k := range o.ks {
		for _, ef := range o.efs {
			if ef < k {
				continue
			}
			search := func(q []float32) ([]vec.Hit, error) { return ix.Search(q, k, ef, nil), nil }
			got, noise := bench.Bracketed(o.ref, measure, func() pass { return timed(s, nq, k, search) })
			p := point{Index: "recserve/hnsw", M: m, EfC: efc, Ef: ef, K: k, Recall: got.recall,
				Latency: got.latency, QPS: got.qps, GC: got.gc, Noise: noise}
			logPoint(logf, p)
			r.Points = append(r.Points, p)
		}
	}
	return r, nil
}

// queryCount checks the k values and returns the number of queries to use.
func queryCount(s *bench.ANNSet, o options) (int, error) {
	for _, k := range o.ks {
		if k > bench.TruthK {
			return 0, fmt.Errorf("k %d above the %d true neighbors", k, bench.TruthK)
		}
	}
	nq := s.NQuery()
	if o.nq > 0 {
		nq = min(nq, o.nq)
	}
	if nq == 0 {
		return 0, errors.New("no queries")
	}
	return nq, nil
}

func logPoint(logf func(string, ...any), p point) {
	logf("ef=%d k=%d recall=%.4f p50=%.0fus p99=%.0fus qps=%.0f gc=%d max pause <= %.2fms noisy=%v attempt=%d",
		p.Ef, p.K, p.Recall, p.Latency.P50, p.Latency.P99, p.QPS, p.GC.Cycles, p.GC.MaxPauseUpper, p.Noise.Flagged, p.Noise.Attempt)
}

type pass struct {
	latency bench.Summary
	recall  float64
	qps     float64
	gc      bench.GCStats
	err     error
}

// timed answers the first nq queries one at a time on this goroutine with
// search, after a warm pass over the first 1,000, and scores the answers
// once timing ends. GC statistics cover the timed pass only.
func timed(s *bench.ANNSet, nq, k int, search func([]float32) ([]vec.Hit, error)) pass {
	for q := range min(1000, nq) {
		if _, err := search(s.QueryVec(q)); err != nil {
			return pass{err: err}
		}
	}
	lat := make([]time.Duration, nq)
	hits := make([][]vec.Hit, nq)
	g0 := bench.ReadGC()
	start := time.Now()
	for q := range nq {
		t0 := time.Now()
		h, err := search(s.QueryVec(q))
		lat[q] = time.Since(t0)
		if err != nil {
			return pass{err: err}
		}
		hits[q] = h
	}
	elapsed := time.Since(start)
	gc := bench.GCBetween(g0, bench.ReadGC())
	ids := make([]int32, 0, k)
	var sum float64
	for q, h := range hits {
		ids = ids[:0]
		for _, x := range h {
			ids = append(ids, x.ID)
		}
		sum += s.Recall(q, ids, k)
	}
	return pass{latency: bench.Summarize(lat), recall: sum / float64(nq), qps: float64(nq) / elapsed.Seconds(), gc: gc}
}
