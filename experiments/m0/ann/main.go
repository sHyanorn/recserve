// Command ann runs risk check R2 of docs/BENCHMARKS.md: coder/hnsw and exact
// search in pure Go, and the recall of faiss results written by faiss_ref.py.
package main

import (
	"container/heap"
	"flag"
	"fmt"
	"log"
	"math"
	"math/rand"
	"os"
	"path/filepath"
	"runtime"
	"runtime/metrics"
	"strconv"
	"strings"
	"time"

	"github.com/coder/hnsw"

	"github.com/sHyanorn/recserve/experiments/m0/internal/bench"
)

var (
	dataDir = flag.String("data", "/home/user/data/m0/ann", "directory written by prep.py")
	set     = flag.String("set", "synth", "dataset: glove or synth")
	index   = flag.String("index", "hnsw", "hnsw, exact or faiss (recall of faiss_ref.py output)")
	m       = flag.Int("M", 16, "HNSW M")
	efc     = flag.Int("efc", 200, "HNSW efConstruction")
	efs     = flag.String("ef", "10,20,40,80,120,200,400,800", "query-time ef values")
	ks      = flag.String("k", "10,100", "k values")
	nq      = flag.Int("nq", 0, "limit the number of queries (0: all)")
	ref     = flag.Float64("ref", 0, "noise reference in ns per pass (0 disables flagging)")
	out     = flag.String("out", "", "JSON result file")
)

// recallTol absorbs float32 rounding when comparing a returned item's
// similarity with that of the true k-th neighbor.
const recallTol = 1e-5

type data struct {
	dim         int
	base, query []float32
	nBase, nQ   int
	gt          []int32 // nQ × 100 true neighbors, best first
}

func must[T any](v T, err error) T {
	if err != nil {
		log.Fatal(err)
	}
	return v
}

func loadData() *data {
	meta := must(os.ReadFile(filepath.Join(*dataDir, *set+".dim")))
	dim := must(strconv.Atoi(strings.TrimSpace(string(meta))))
	d := &data{dim: dim}
	d.base, d.nBase = must3(bench.ReadF32(filepath.Join(*dataDir, *set+"_base.f32"), dim))
	d.query, d.nQ = must3(bench.ReadF32(filepath.Join(*dataDir, *set+"_query.f32"), dim))
	d.gt = must(bench.ReadI32(filepath.Join(*dataDir, *set+"_gt.i32")))
	if len(d.gt) != d.nQ*100 {
		log.Fatalf("ground truth has %d entries for %d queries", len(d.gt), d.nQ)
	}
	if *nq > 0 && *nq < d.nQ {
		d.nQ = *nq
	}
	return d
}

func must3[A, B any](a A, b B, err error) (A, B) {
	if err != nil {
		log.Fatal(err)
	}
	return a, b
}

func (d *data) vec(i int) []float32  { return d.base[i*d.dim : (i+1)*d.dim] }
func (d *data) qvec(i int) []float32 { return d.query[i*d.dim : (i+1)*d.dim] }

func sim64(a, b []float32) float64 {
	var s float64
	for i, v := range a {
		s += float64(v) * float64(b[i])
	}
	return s
}

// recall is the ann-benchmarks definition: the share of the k returned items
// whose similarity is at least that of the true k-th neighbor.
func (d *data) recall(q int, got []int32, k int) float64 {
	qv := d.qvec(q)
	thr := sim64(qv, d.vec(int(d.gt[q*100+k-1]))) - recallTol
	seen := make(map[int32]bool, len(got))
	n := 0
	for _, id := range got[:min(k, len(got))] {
		if id < 0 || seen[id] { // faiss pads missing results with -1
			continue
		}
		seen[id] = true
		if sim64(qv, d.vec(int(id))) >= thr {
			n++
		}
	}
	return float64(n) / float64(k)
}

type gcStats struct {
	NumGC          uint64  `json:"num_gc"`
	MaxPauseUpper  float64 `json:"max_pause_upper_ms"` // upper bound of the highest histogram bucket hit
	GCCPUFraction  float64 `json:"gc_cpu_fraction"`
	HeapObjectsMiB float64 `json:"heap_objects_mib,omitempty"`
}

var gcSamples = []metrics.Sample{
	{Name: "/sched/pauses/total/gc:seconds"},
	{Name: "/cpu/classes/gc/total:cpu-seconds"},
	{Name: "/cpu/classes/total:cpu-seconds"},
	{Name: "/gc/cycles/total:gc-cycles"},
}

type gcSnap struct {
	pauses       []uint64
	buckets      []float64
	gcCPU, total float64
	cycles       uint64
}

func snapGC() gcSnap {
	s := make([]metrics.Sample, len(gcSamples))
	copy(s, gcSamples)
	metrics.Read(s)
	h := s[0].Value.Float64Histogram()
	return gcSnap{pauses: append([]uint64(nil), h.Counts...), buckets: h.Buckets,
		gcCPU: s[1].Value.Float64(), total: s[2].Value.Float64(), cycles: s[3].Value.Uint64()}
}

func gcDiff(a, b gcSnap) gcStats {
	st := gcStats{NumGC: b.cycles - a.cycles}
	for i := len(b.pauses) - 1; i >= 0; i-- {
		if b.pauses[i] > a.pauses[i] {
			st.MaxPauseUpper = b.buckets[i+1] * 1e3
			break
		}
	}
	if b.total > a.total {
		st.GCCPUFraction = (b.gcCPU - a.gcCPU) / (b.total - a.total)
	}
	return st
}

func heapObjectsMiB() float64 {
	runtime.GC()
	s := []metrics.Sample{{Name: "/memory/classes/heap/objects:bytes"}}
	metrics.Read(s)
	return float64(s[0].Value.Uint64()) / (1 << 20)
}

type point struct {
	Index    string        `json:"index"`
	M        int           `json:"M,omitempty"`
	EfC      int           `json:"ef_construction,omitempty"`
	Ef       int           `json:"ef,omitempty"`
	K        int           `json:"k"`
	Recall   float64       `json:"recall"`
	Latency  bench.Summary `json:"latency"`
	QPS      float64       `json:"qps"`
	GC       gcStats       `json:"gc"`
	Noise    bench.Noise   `json:"noise"`
	Recall10 float64       `json:"recall_at_10,omitempty"` // exact search only: from the same top-100
}

// timed answers every query once after a warm pass over the first 1,000 and
// returns per-query latencies, the mean recall and GC statistics of the timed pass.
func (d *data) timed(k int, search func(q []float32, k int) []int32) (bench.Summary, float64, float64, gcStats, [][]int32) {
	for q := range min(1000, d.nQ) {
		search(d.qvec(q), k)
	}
	lat := make([]time.Duration, d.nQ)
	res := make([][]int32, d.nQ)
	g0 := snapGC()
	start := time.Now()
	for q := range d.nQ {
		t0 := time.Now()
		res[q] = search(d.qvec(q), k)
		lat[q] = time.Since(t0)
	}
	elapsed := time.Since(start)
	gs := gcDiff(g0, snapGC())
	var rsum float64
	for q := range d.nQ {
		rsum += d.recall(q, res[q], k)
	}
	return bench.Summarize(lat), rsum / float64(d.nQ), float64(d.nQ) / elapsed.Seconds(), gs, res
}

type cand struct {
	id  int32
	sim float32
}

type minHeap []cand

func (h minHeap) Len() int           { return len(h) }
func (h minHeap) Less(i, j int) bool { return h[i].sim < h[j].sim }
func (h minHeap) Swap(i, j int)      { h[i], h[j] = h[j], h[i] }
func (h *minHeap) Push(x any)        { *h = append(*h, x.(cand)) }
func (h *minHeap) Pop() any          { o := *h; x := o[len(o)-1]; *h = o[:len(o)-1]; return x }

func (d *data) exact(q []float32, k int) []int32 {
	h := make(minHeap, 0, k+1)
	for i := range d.nBase {
		row := d.vec(i)
		var s float32
		for j, v := range q {
			s += v * row[j]
		}
		if len(h) < k {
			heap.Push(&h, cand{int32(i), s})
		} else if s > h[0].sim {
			h[0] = cand{int32(i), s}
			heap.Fix(&h, 0)
		}
	}
	ids := make([]int32, len(h))
	for i := len(h) - 1; i >= 0; i-- {
		ids[i] = heap.Pop(&h).(cand).id
	}
	return ids
}

func ints(s string) []int {
	var v []int
	for _, f := range strings.Split(s, ",") {
		v = append(v, must(strconv.Atoi(strings.TrimSpace(f))))
	}
	return v
}

func main() {
	flag.Parse()
	d := loadData()
	fmt.Printf("%s: %d base, %d queries, dim %d\n", *set, d.nBase, d.nQ, d.dim)
	res := map[string]any{"env": bench.Env(map[string]string{"set": *set, "index": *index})}
	var points []point
	switch *index {
	case "exact":
		type r struct {
			s    bench.Summary
			rec  float64
			qps  float64
			gc   gcStats
			ids  [][]int32
		}
		got, noise := bench.Bracketed(*ref, 0, func() r {
			s, rec, qps, gs, ids := d.timed(100, d.exact)
			return r{s, rec, qps, gs, ids}
		})
		var r10 float64
		for q := range d.nQ {
			r10 += d.recall(q, got.ids[q], 10)
		}
		p := point{Index: "exact", K: 100, Recall: got.rec, Recall10: r10 / float64(d.nQ), Latency: got.s, QPS: got.qps, GC: got.gc, Noise: noise}
		fmt.Printf("exact k=100 recall=%.4f recall@10=%.4f p50=%.0fus p99=%.0fus noisy=%v\n", p.Recall, p.Recall10, p.Latency.P50, p.Latency.P99, noise.Flagged)
		points = append(points, p)
	case "hnsw":
		g := hnsw.NewGraph[int32]()
		g.M, g.Ml, g.EfSearch = *m, 1/math.Log(float64(*m)), *efc
		g.Distance = hnsw.CosineDistance
		g.Rng = rand.New(rand.NewSource(1))
		start := time.Now()
		for i := range d.nBase {
			g.Add(hnsw.MakeNode(int32(i), d.vec(i)))
			if (i+1)%100000 == 0 {
				fmt.Printf("  added %d in %s\n", i+1, time.Since(start).Round(time.Second))
			}
		}
		build := time.Since(start)
		heapMiB := heapObjectsMiB()
		fmt.Printf("build %s, heap objects %.0f MiB\n", build.Round(time.Second), heapMiB)
		res["build_seconds"], res["heap_objects_mib"] = build.Seconds(), heapMiB
		for _, k := range ints(*ks) {
			for _, ef := range ints(*efs) {
				if ef < k {
					continue
				}
				g.EfSearch = ef
				search := func(q []float32, k int) []int32 {
					nodes := g.Search(q, k)
					ids := make([]int32, len(nodes))
					for i, n := range nodes {
						ids[i] = n.Key
					}
					return ids
				}
				type r struct {
					s   bench.Summary
					rec float64
					qps float64
					gc  gcStats
				}
				got, noise := bench.Bracketed(*ref, 0, func() r {
					s, rec, qps, gs, _ := d.timed(k, search)
					return r{s, rec, qps, gs}
				})
				p := point{Index: "coder/hnsw", M: *m, EfC: *efc, Ef: ef, K: k, Recall: got.rec, Latency: got.s, QPS: got.qps, GC: got.gc, Noise: noise}
				fmt.Printf("hnsw M=%d ef=%d k=%d recall=%.4f p50=%.0fus p99=%.0fus qps=%.0f gcmax=%.2fms gccpu=%.3f noisy=%v\n",
					*m, ef, k, p.Recall, p.Latency.P50, p.Latency.P99, p.QPS, p.GC.MaxPauseUpper, p.GC.GCCPUFraction, noise.Flagged)
				points = append(points, p)
			}
		}
	case "faiss":
		// faiss_ref.py writes, per (k, ef), result ids and latencies; only recall is computed here.
		for _, k := range ints(*ks) {
			for _, ef := range ints(*efs) {
				if ef < k {
					continue
				}
				name := filepath.Join(*dataDir, fmt.Sprintf("faiss_%s_M%d_k%d_ef%d.i32", *set, *m, k, ef))
				ids, err := bench.ReadI32(name)
				if err != nil {
					log.Fatal(err)
				}
				var rsum float64
				for q := range d.nQ {
					rsum += d.recall(q, ids[q*k:(q+1)*k], k)
				}
				p := point{Index: "faiss", M: *m, EfC: *efc, Ef: ef, K: k, Recall: rsum / float64(d.nQ)}
				fmt.Printf("faiss M=%d ef=%d k=%d recall=%.4f\n", *m, ef, k, p.Recall)
				points = append(points, p)
			}
		}
	default:
		log.Fatalf("unknown index %s", *index)
	}
	res["points"] = points
	if *out != "" {
		if err := os.MkdirAll(filepath.Dir(*out), 0o755); err != nil {
			log.Fatal(err)
		}
		if err := bench.WriteJSON(*out, res); err != nil {
			log.Fatal(err)
		}
	}
}
