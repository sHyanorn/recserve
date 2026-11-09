package recommend

import (
	"bufio"
	"encoding/json"
	"fmt"
	"math/rand/v2"
	"os"
	"path/filepath"
	"testing"

	"github.com/sHyanorn/recserve/internal/bundle"
	"github.com/sHyanorn/recserve/internal/vec"
)

// rankCase is the input of rank for one request.
type rankCase struct {
	results [][]vec.Hit
	uv      []float32
	seen    map[int32]bool
}

func newRankCase(p *Pipeline, hist []int32) rankCase {
	seen := make(map[int32]bool, len(hist))
	for _, id := range hist {
		seen[id] = true
	}
	uv := p.Bundle.UserVector(hist)
	var results [][]vec.Hit
	for _, src := range p.sources(hist, uv, seen) {
		results = append(results, src())
	}
	return rankCase{results, uv, seen}
}

func benchRank(b *testing.B, p *Pipeline, cases []rankCase) {
	for _, k := range []int{20, 100} {
		b.Run(fmt.Sprint("k=", k), func(b *testing.B) {
			b.ReportAllocs()
			i := 0
			for b.Loop() {
				c := cases[i%len(cases)]
				p.rank(c.results, c.uv, c.seen, k)
				i++
			}
			var cands int
			for _, c := range cases {
				cands += len(merge(c.results))
			}
			b.ReportMetric(float64(cands)/float64(len(cases)), "candidates")
		})
	}
}

// BenchmarkRank times the stages after retrieval (merge, scoring, seen
// filter, MMR) on synthetic data shaped like the Video_Games bundle:
// 25,600 items of 64 dimensions, 50 co-occurring items per item, a
// history of 50 items.
func BenchmarkRank(b *testing.B) {
	const n, dim = 25600, 64
	r := rand.New(rand.NewPCG(1, 2))
	data := make([]float32, (n+1)*dim)
	for i := dim; i < len(data); i++ {
		data[i] = float32(r.NormFloat64() * 0.1)
	}
	m := matrix(b, dim, data)
	cooc := make([]int32, (n+1)*bundle.CoocWidth)
	for i := bundle.CoocWidth; i < len(cooc); i++ {
		cooc[i] = int32(1 + r.IntN(n))
	}
	popular := make([]int32, 500)
	for i := range popular {
		popular[i] = int32(1 + r.IntN(n))
	}
	bd := bundle.New(bundle.Manifest{ModelVersion: "b", ItemCount: n, Dim: dim, MaxSeqLen: 50}, make([]string, n+1), m, cooc, popular)
	p := &Pipeline{Bundle: bd}
	cases := make([]rankCase, 20)
	for i := range cases {
		hist := make([]int32, 50)
		for j := range hist {
			hist[j] = int32(1 + r.IntN(n))
		}
		cases[i] = newRankCase(p, hist)
	}
	benchRank(b, p, cases)
}

// BenchmarkRankRealData runs BenchmarkRank's measurement on the bundle in
// RECSERVE_BUNDLE, with the histories of its golden cases.
func BenchmarkRankRealData(b *testing.B) {
	dir := os.Getenv("RECSERVE_BUNDLE")
	if dir == "" {
		b.Skip("RECSERVE_BUNDLE not set")
	}
	bd, err := bundle.Load(dir, bundle.Options{})
	if err != nil {
		b.Fatal(err)
	}
	f, err := os.Open(filepath.Join(dir, "golden.jsonl"))
	if err != nil {
		b.Fatal(err)
	}
	defer f.Close() //nolint:errcheck // read only
	p := &Pipeline{Bundle: bd}
	var cases []rankCase
	sc := bufio.NewScanner(f)
	sc.Buffer(nil, 1<<20)
	for sc.Scan() {
		var g struct {
			History []int32 `json:"history"`
		}
		if err := json.Unmarshal(sc.Bytes(), &g); err != nil {
			b.Fatal(err)
		}
		cases = append(cases, newRankCase(p, g.History))
	}
	if err := sc.Err(); err != nil {
		b.Fatal(err)
	}
	benchRank(b, p, cases)
}
