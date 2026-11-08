// Command onnx runs risk check R1 of docs/BENCHMARKS.md: parity and latency of
// ONNX Runtime called from Go, against a pure-Go reference.
package main

import (
	"flag"
	"fmt"
	"log"
	"math"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	ort "github.com/yalue/onnxruntime_go"

	"github.com/sHyanorn/recserve/experiments/m0/internal/bench"
)

const (
	dim        = 64
	candidates = 500
	feats      = 8
)

var (
	dir     = flag.String("dir", "/home/user/data/m0/onnx", "directory written by export.py")
	lib     = flag.String("lib", "/home/user/data/m0/onnxruntime-linux-x64-1.31.1/lib/libonnxruntime.so", "ONNX Runtime shared library")
	mode    = flag.String("mode", "parity", "parity or latency")
	ref     = flag.Float64("ref", 0, "noise reference in ns per pass (0 disables flagging)")
	out     = flag.String("out", "", "JSON result file")
	dur     = flag.Duration("dur", 20*time.Second, "measured time per configuration")
	warm    = flag.Duration("warm", 5*time.Second, "warmup per configuration")
	concs   = flag.String("conc", "1,2,4,8", "concurrency levels")
	threads = flag.String("threads", "1,0", "intra-op thread settings; 0 is ONNX Runtime's default")
	models  = flag.String("models", "enc_l50,enc_l200,ranker,go_mlp,go_dot", "what to measure")
)

func must[T any](v T, err error) T {
	if err != nil {
		log.Fatal(err)
	}
	return v
}

func load(name string) *bench.Npy { return must(bench.ReadNpy(filepath.Join(*dir, name))) }

func ints(s string) []int {
	var v []int
	for _, f := range strings.Split(s, ",") {
		v = append(v, must(strconv.Atoi(strings.TrimSpace(f))))
	}
	return v
}

// caller runs one inference on case i and returns its output.
type caller interface {
	call(i int) []float32
}

// ortCaller owns the input and output tensors of one goroutine; the session is shared.
type ortCaller struct {
	sess    *ort.DynamicAdvancedSession
	ins     []ort.Value
	out     *ort.Tensor[float32]
	fill    func(i int)
	outSize int
}

func (c *ortCaller) call(i int) []float32 {
	c.fill(i)
	if err := c.sess.Run(c.ins, []ort.Value{c.out}); err != nil {
		log.Fatal(err)
	}
	return c.out.GetData()
}

type model struct {
	name    string
	cases   int
	want    []float32 // Python onnxruntime outputs, cases × outSize
	outSize int
	newCall func(sess *ort.DynamicAdvancedSession) caller
	file    string
	inNames []string
}

func encoder(length int) model {
	in, want := load(fmt.Sprintf("enc_l%d_in.npy", length)), load(fmt.Sprintf("enc_l%d_out.npy", length))
	return model{
		name: fmt.Sprintf("enc_l%d", length), cases: in.Shape[0], want: want.F32, outSize: dim,
		file: fmt.Sprintf("enc_l%d.onnx", length), inNames: []string{"item_seq"},
		newCall: func(sess *ort.DynamicAdvancedSession) caller {
			x := must(ort.NewEmptyTensor[int64](ort.NewShape(1, int64(length))))
			o := must(ort.NewEmptyTensor[float32](ort.NewShape(1, dim)))
			return &ortCaller{sess: sess, ins: []ort.Value{x}, out: o, outSize: dim,
				fill: func(i int) { copy(x.GetData(), in.I64[i*length:(i+1)*length]) }}
		},
	}
}

type rankerData struct{ user, items, feats, want *bench.Npy }

func loadRanker() rankerData {
	return rankerData{load("ranker_user.npy"), load("ranker_items.npy"), load("ranker_feats.npy"), load("ranker_out.npy")}
}

func ranker(d rankerData) model {
	return model{
		name: "ranker", cases: d.user.Shape[0], want: d.want.F32, outSize: candidates,
		file: "ranker.onnx", inNames: []string{"user", "items", "feats"},
		newCall: func(sess *ort.DynamicAdvancedSession) caller {
			u := must(ort.NewEmptyTensor[float32](ort.NewShape(1, dim)))
			it := must(ort.NewEmptyTensor[float32](ort.NewShape(1, candidates, dim)))
			f := must(ort.NewEmptyTensor[float32](ort.NewShape(1, candidates, feats)))
			o := must(ort.NewEmptyTensor[float32](ort.NewShape(1, candidates)))
			return &ortCaller{sess: sess, ins: []ort.Value{u, it, f}, out: o, outSize: candidates,
				fill: func(i int) {
					copy(u.GetData(), d.user.F32[i*dim:(i+1)*dim])
					copy(it.GetData(), d.items.F32[i*candidates*dim:(i+1)*candidates*dim])
					copy(f.GetData(), d.feats.F32[i*candidates*feats:(i+1)*candidates*feats])
				}}
		},
	}
}

// goMLP is the ranker MLP in plain Go, with the same arithmetic per candidate.
type goMLP struct {
	d      rankerData
	w, b   [3]*bench.Npy
	x      [2*dim + feats]float32
	h1, h2 []float32
	out    []float32
}

func newGoMLP(d rankerData) *goMLP {
	m := &goMLP{d: d, h1: make([]float32, 128), h2: make([]float32, 64), out: make([]float32, candidates)}
	for i := range 3 {
		m.w[i], m.b[i] = load(fmt.Sprintf("ranker_w%d.npy", i)), load(fmt.Sprintf("ranker_b%d.npy", i))
	}
	return m
}

func dense(w, b *bench.Npy, x, y []float32, relu bool) {
	in := w.Shape[1]
	for o := range y {
		row := w.F32[o*in : (o+1)*in]
		s := b.F32[o]
		for j, v := range x {
			s += row[j] * v
		}
		if relu && s < 0 {
			s = 0
		}
		y[o] = s
	}
}

func (m *goMLP) call(i int) []float32 {
	u := m.d.user.F32[i*dim : (i+1)*dim]
	var y [1]float32
	for c := range candidates {
		copy(m.x[:dim], u)
		copy(m.x[dim:2*dim], m.d.items.F32[(i*candidates+c)*dim:(i*candidates+c+1)*dim])
		copy(m.x[2*dim:], m.d.feats.F32[(i*candidates+c)*feats:(i*candidates+c+1)*feats])
		dense(m.w[0], m.b[0], m.x[:], m.h1, true)
		dense(m.w[1], m.b[1], m.h1, m.h2, true)
		dense(m.w[2], m.b[2], m.h2, y[:], false)
		m.out[c] = y[0]
	}
	return m.out
}

// goDot scores candidates by dot product with the user vector.
type goDot struct {
	d   rankerData
	out []float32
}

func (m *goDot) call(i int) []float32 {
	u := m.d.user.F32[i*dim : (i+1)*dim]
	for c := range candidates {
		row := m.d.items.F32[(i*candidates+c)*dim : (i*candidates+c+1)*dim]
		var s float32
		for j, v := range u {
			s += v * row[j]
		}
		m.out[c] = s
	}
	return m.out
}

func newSession(m model, threads int) *ort.DynamicAdvancedSession {
	opts := must(ort.NewSessionOptions())
	defer opts.Destroy()
	if threads > 0 {
		if err := opts.SetIntraOpNumThreads(threads); err != nil {
			log.Fatal(err)
		}
	}
	return must(ort.NewDynamicAdvancedSession(filepath.Join(*dir, m.file), m.inNames, []string{outName(m)}, opts))
}

func outName(m model) string {
	if m.name == "ranker" {
		return "scores"
	}
	return "user_emb"
}

func maxDiff(c caller, cases, size int, want []float32) float64 {
	var d float64
	for i := range cases {
		got := c.call(i)
		for j, v := range got {
			d = max(d, math.Abs(float64(v)-float64(want[i*size+j])))
		}
	}
	return d
}

type latencyResult struct {
	Model   string        `json:"model"`
	Threads string        `json:"intra_op_threads"`
	Conc    int           `json:"concurrency"`
	Calls   float64       `json:"calls_per_s"`
	Latency bench.Summary `json:"latency"`
	Noise   bench.Noise   `json:"noise"`
}

// measure runs conc goroutines, each with its own caller, for warm+dur and
// records every call that started after the warmup.
func measure(newCaller func() caller, cases, conc int) ([]time.Duration, float64) {
	callers := make([]caller, conc)
	for g := range callers {
		callers[g] = newCaller()
	}
	start := time.Now()
	measureFrom, end := start.Add(*warm), start.Add(*warm+*dur)
	per := make([][]time.Duration, conc)
	var wg sync.WaitGroup
	for g := range conc {
		wg.Go(func() {
			lat := make([]time.Duration, 0, 1<<16)
			for i := g; ; i++ {
				t0 := time.Now()
				if !t0.Before(end) {
					break
				}
				callers[g].call(i % cases)
				if !t0.Before(measureFrom) {
					lat = append(lat, time.Since(t0))
				}
			}
			per[g] = lat
		})
	}
	wg.Wait()
	var all []time.Duration
	for _, l := range per {
		all = append(all, l...)
	}
	return all, float64(len(all)) / dur.Seconds()
}

func main() {
	flag.Parse()
	ort.SetSharedLibraryPath(*lib)
	if err := ort.InitializeEnvironment(); err != nil {
		log.Fatal(err)
	}
	defer ort.DestroyEnvironment() //nolint:errcheck
	rd := loadRanker()
	onnxModels := map[string]model{"enc_l50": encoder(50), "enc_l200": encoder(200), "ranker": ranker(rd)}
	env := bench.Env(map[string]string{"onnxruntime": ort.GetVersion()})

	switch *mode {
	case "parity":
		res := map[string]any{"env": env}
		for _, name := range []string{"enc_l50", "enc_l200", "ranker"} {
			m := onnxModels[name]
			sess := newSession(m, 0)
			d := maxDiff(m.newCall(sess), m.cases, m.outSize, m.want)
			sess.Destroy() //nolint:errcheck
			fmt.Printf("%s: max abs diff Go vs Python onnxruntime %.3g\n", name, d)
			res[name] = d
		}
		d := maxDiff(newGoMLP(rd), rd.user.Shape[0], candidates, rd.want.F32)
		fmt.Printf("go_mlp: max abs diff vs Python onnxruntime %.3g\n", d)
		res["go_mlp"] = d
		write(res)
	case "latency":
		var res []latencyResult
		for _, name := range strings.Split(*models, ",") {
			for _, conc := range ints(*concs) {
				if m, ok := onnxModels[name]; ok {
					for _, th := range ints(*threads) {
						sess := newSession(m, th)
						r := run(name, threadName(th), conc, func() caller { return m.newCall(sess) }, m.cases)
						sess.Destroy() //nolint:errcheck
						res = append(res, r)
					}
					continue
				}
				var nc func() caller
				switch name {
				case "go_mlp":
					nc = func() caller { return newGoMLP(rd) }
				case "go_dot":
					nc = func() caller { return &goDot{d: rd, out: make([]float32, candidates)} }
				default:
					log.Fatalf("unknown model %s", name)
				}
				res = append(res, run(name, "go", conc, nc, rd.user.Shape[0]))
			}
		}
		write(map[string]any{"env": env, "results": res})
	default:
		log.Fatalf("unknown mode %s", *mode)
	}
}

func threadName(t int) string {
	if t == 0 {
		return "default"
	}
	return strconv.Itoa(t)
}

func run(name, threads string, conc int, nc func() caller, cases int) latencyResult {
	type r struct {
		lat   []time.Duration
		calls float64
	}
	got, noise := bench.Bracketed(*ref, 0, func() r { l, c := measure(nc, cases, conc); return r{l, c} })
	s := bench.Summarize(got.lat)
	fmt.Printf("%-8s threads=%-7s conc=%d calls/s=%8.0f p50=%7.0fus p99=%7.0fus p999=%7.0fus noisy=%v\n",
		name, threads, conc, got.calls, s.P50, s.P99, s.P999, noise.Flagged)
	return latencyResult{Model: name, Threads: threads, Conc: conc, Calls: got.calls, Latency: s, Noise: noise}
}

func write(v any) {
	if *out == "" {
		return
	}
	if err := os.MkdirAll(filepath.Dir(*out), 0o755); err != nil {
		log.Fatal(err)
	}
	if err := bench.WriteJSON(*out, v); err != nil {
		log.Fatal(err)
	}
}
