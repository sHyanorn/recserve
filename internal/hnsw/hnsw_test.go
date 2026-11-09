package hnsw

import (
	"bytes"
	"errors"
	"fmt"
	"math"
	"math/rand/v2"
	"slices"
	"sync"
	"testing"

	"github.com/sHyanorn/recserve/internal/vec"
)

// clustered returns n L2-normalized rows of dim values: centers from a
// standard normal, each row a random center plus N(0, sigma²) noise.
func clustered(seed uint64, n, dim, centers int, sigma float64) []float32 {
	r := rand.New(rand.NewPCG(seed, 1))
	c := make([]float64, centers*dim)
	for i := range c {
		c[i] = r.NormFloat64()
	}
	out := make([]float32, n*dim)
	for i := range n {
		k := r.IntN(centers)
		var norm float64
		row := make([]float64, dim)
		for j := range row {
			row[j] = c[k*dim+j] + sigma*r.NormFloat64()
			norm += row[j] * row[j]
		}
		for j, v := range row {
			out[i*dim+j] = float32(v / math.Sqrt(norm))
		}
	}
	return out
}

type dataset struct {
	data, queries []float32
	dim           int
	kth           *sync.Map // thresholds: key {k, filtered} → per query the k-th true similarity
}

func newDataset(seed uint64, n, nq, dim, centers int, sigma float64) dataset {
	all := clustered(seed, n+nq, dim, centers, sigma)
	return dataset{data: all[:n*dim], queries: all[n*dim:], dim: dim, kth: &sync.Map{}}
}

func (d dataset) query(i int) []float32 { return d.queries[i*d.dim : (i+1)*d.dim] }
func (d dataset) nq() int               { return len(d.queries) / d.dim }
func (d dataset) row(i int32) []float32 { return d.data[int(i)*d.dim : (int(i)+1)*d.dim] }

// oddIDs is the one filter the tests use.
func oddIDs(id int32) bool { return id%2 == 1 }

// thresholds returns, per query, the k-th highest float64 inner product
// over the rows, leaving out odd IDs if filtered.
func (d dataset) thresholds(k int, filtered bool) []float64 {
	type key struct {
		k        int
		filtered bool
	}
	if v, ok := d.kth.Load(key{k, filtered}); ok {
		return v.([]float64)
	}
	out := make([]float64, d.nq())
	sims := make([]float64, 0, len(d.data)/d.dim)
	for i := range out {
		sims = sims[:0]
		for id := range int32(len(d.data) / d.dim) {
			if !filtered || !oddIDs(id) {
				sims = append(sims, vec.Dot(d.query(i), d.row(id)))
			}
		}
		slices.Sort(sims)
		out[i] = sims[max(0, len(sims)-k)]
	}
	d.kth.Store(key{k, filtered}, out)
	return out
}

// recall is the ann-benchmarks definition: the share of the k returned
// items at least as similar as the true k-th neighbor, less 1e-5.
func (d dataset) recall(ix *Index, k, ef int, filtered bool) float64 {
	var skip func(int32) bool
	if filtered {
		skip = oddIDs
	}
	thr := d.thresholds(k, filtered)
	var sum float64
	for i := range d.nq() {
		q := d.query(i)
		n := 0
		for _, h := range ix.Search(q, k, ef, skip) {
			if vec.Dot(q, d.row(h.ID)) >= thr[i]-1e-5 {
				n++
			}
		}
		sum += float64(n) / float64(k)
	}
	return sum / float64(d.nq())
}

// reachable returns the number of nodes reachable on layer 0 from the entry point.
func reachable(ix *Index) int {
	n := 0
	for _, v := range reachableFrom(ix) {
		if v {
			n++
		}
	}
	return n
}

// reachableFrom marks the nodes reachable on layer 0 from the entry point.
func reachableFrom(ix *Index) []bool {
	seen := make([]bool, ix.n)
	if ix.n == 0 {
		return seen
	}
	queue := []int32{ix.entry}
	seen[ix.entry] = true
	s := ix.newSearcher()
	for len(queue) > 0 {
		i := queue[0]
		queue = queue[1:]
		for _, j := range ix.neighbors(s, i, 0) {
			if !seen[j] {
				seen[j] = true
				queue = append(queue, j)
			}
		}
	}
	return seen
}

func maxDegree0(ix *Index) int {
	most := 0
	for i := range int32(ix.n) {
		most = max(most, int(ix.list(i, 0)[0]))
	}
	return most
}

func mustBuild(t testing.TB, d dataset, cfg Config, mut mutation) *Index {
	t.Helper()
	ix, err := build(d.data, d.dim, cfg, mut)
	if err != nil {
		t.Fatal(err)
	}
	return ix
}

// The test datasets: tight clusters, where neighbor selection decides
// whether clusters stay connected, and a looser set for recall.
var (
	tight = newDataset(1, 3000, 100, 8, 60, 0.05)
	loose = newDataset(2, 10000, 200, 32, 100, 0.4)
)

var (
	tightCfg = Config{M: 4, EfConstruction: 32, Seed: 1}
	looseCfg = Config{M: 12, EfConstruction: 100, Seed: 1}
)

// The standard builds, shared by the tests that only read them.
var (
	tightIx = sync.OnceValue(func() *Index { ix, _ := build(tight.data, tight.dim, tightCfg, mutation{}); return ix })
	looseIx = sync.OnceValue(func() *Index { ix, _ := build(loose.data, loose.dim, looseCfg, mutation{}); return ix })
)

const (
	looseK, looseEf = 10, 40
	minRecall       = 0.95 // see TestRecall
	// Pruning can remove every link to a node, so a few nodes may be
	// unreachable (also in hnswlib and faiss); measured: 3 of 3,000 on
	// tight, 0 to 16 of 10,000 on loose.
	minReachable = 0.995
)

// checks are the properties every build must have; each returns an error
// if the index lacks it. TestMutantsAreCaught runs them on broken builds.
func checks(tightIx, looseIx *Index) []error {
	var errs []error
	for name, ix := range map[string]*Index{"tight": tightIx, "loose": looseIx} {
		if err := ix.Check(); err != nil {
			errs = append(errs, fmt.Errorf("%s: %w", name, err))
		}
		if n := reachable(ix); float64(n) < minReachable*float64(ix.n) {
			errs = append(errs, fmt.Errorf("%s: %d of %d nodes reachable on layer 0", name, n, ix.n))
		}
	}
	if got := maxDegree0(looseIx); got <= looseCfg.M || got > 2*looseCfg.M {
		errs = append(errs, fmt.Errorf("loose: most layer-0 links %d, want in (M, 2M] = (%d, %d]", got, looseCfg.M, 2*looseCfg.M))
	}
	if r := loose.recall(looseIx, looseK, looseEf, false); r < minRecall {
		errs = append(errs, fmt.Errorf("loose: recall@%d at ef=%d is %.4f, want >= %.2f", looseK, looseEf, r, minRecall))
	}
	if r := tight.recall(tightIx, 10, 10, false); r < minRecall {
		errs = append(errs, fmt.Errorf("tight: recall@10 at ef=10 is %.4f, want >= %.2f", r, minRecall))
	}
	return errs
}

func TestBuildHasAllProperties(t *testing.T) {
	if errs := checks(tightIx(), looseIx()); len(errs) > 0 {
		t.Fatal(errors.Join(errs...))
	}
}

// Each broken variant of D024 must fail at least one of the checks.
func TestMutantsAreCaught(t *testing.T) {
	for name, mut := range map[string]mutation{
		"nearest instead of Algorithm 4": {simpleSelect: true},
		"layer 0 capped at M":            {layer0M: true},
		"entry point never moves":        {frozenEntry: true},
	} {
		errs := checks(mustBuild(t, tight, tightCfg, mut), mustBuild(t, loose, looseCfg, mut))
		if len(errs) == 0 {
			t.Errorf("%s: passed every check", name)
		}
		t.Logf("%s: %v", name, errors.Join(errs...))
	}
}

// exactAmong returns the top k of ids by the float32 inner product with q,
// ties to the lower ID.
func exactAmong(ix *Index, q []float32, ids []int32, k int) []vec.Hit {
	all := make([]vec.Hit, 0, len(ids))
	for _, id := range ids {
		all = append(all, vec.Hit{ID: id, Score: float64(vec.Dot32(q, ix.vec(id)))})
	}
	slices.SortFunc(all, func(a, b vec.Hit) int {
		if vec.Better(a, b) {
			return -1
		}
		return 1
	})
	return all[:min(k, len(all))]
}

// With ef at least the node count, layer 0 is searched exhaustively from
// the entry point, so the result is the exact top k among the reachable
// nodes. On random unit vectors every node is reachable and recall is 1.
func TestExhaustiveSearchIsExact(t *testing.T) {
	uniform := newDataset(9, 2000, 50, 8, 2050, 0) // one center per row: uniform on the sphere
	u := mustBuild(t, uniform, Config{M: 8, EfConstruction: 64, Seed: 1}, mutation{})
	if n := reachable(u); n != u.n {
		t.Fatalf("precondition: %d of %d nodes reachable", n, u.n)
	}
	if got := uniform.recall(u, 10, u.n, false); got != 1 {
		t.Fatalf("recall@10 with ef = n is %v, want 1", got)
	}
	for name, c := range map[string]struct {
		d   dataset
		cfg Config
	}{"uniform": {uniform, Config{M: 8, EfConstruction: 64, Seed: 1}}, "tight": {tight, tightCfg}} {
		ix := mustBuild(t, c.d, c.cfg, mutation{})
		var ids []int32
		for i := range int32(ix.n) {
			if reachableFrom(ix)[i] {
				ids = append(ids, i)
			}
		}
		for i := range c.d.nq() {
			q := c.d.query(i)
			if got, want := ix.Search(q, 10, ix.n, nil), exactAmong(ix, q, ids, 10); !slices.Equal(got, want) {
				t.Fatalf("%s query %d: got %v, want %v", name, i, got, want)
			}
		}
	}
}

// The recall floor of the checks was set from measurements on these
// datasets with margin: loose 1.000 at ef=40, tight 0.968 at ef=10.
func TestRecall(t *testing.T) {
	ix := looseIx()
	for _, ef := range []int{10, 20, 40, 80} {
		t.Logf("loose recall@%d ef=%d: %.4f", looseK, ef, loose.recall(ix, looseK, ef, false))
	}
	t.Logf("tight recall@10 ef=10: %.4f", tight.recall(tightIx(), 10, 10, false))
}

func TestFilteredSearch(t *testing.T) {
	ix := looseIx()
	for i := range loose.nq() {
		for _, h := range ix.Search(loose.query(i), 10, 40, oddIDs) {
			if oddIDs(h.ID) {
				t.Fatalf("query %d returned filtered node %d", i, h.ID)
			}
		}
	}
	if r := loose.recall(ix, 10, 40, true); r < minRecall {
		t.Fatalf("filtered recall %.4f, want >= %.2f", r, minRecall)
	}
	all := func(int32) bool { return true }
	if got := ix.Search(loose.query(0), 10, 40, all); len(got) != 0 {
		t.Fatalf("everything filtered, got %v", got)
	}
}

// A parallel build gives a different graph; its recall must stay close to
// the reproducible single-threaded build's.
func TestParallelBuild(t *testing.T) {
	serial := looseIx()
	cfg := looseCfg
	cfg.Workers = 4
	par := mustBuild(t, loose, cfg, mutation{})
	if err := par.Check(); err != nil {
		t.Fatal(err)
	}
	if n := reachable(par); float64(n) < minReachable*float64(par.n) {
		t.Fatalf("%d of %d nodes reachable", n, par.n)
	}
	// Small builds with many workers make concurrent inserts meet often.
	for seed := range uint64(20) {
		d := newDataset(seed, 1500, 1, 8, 30, 0.1)
		ix := mustBuild(t, d, Config{M: 4, EfConstruction: 24, Seed: seed, Workers: 8}, mutation{})
		if err := ix.Check(); err != nil {
			t.Fatalf("seed %d: %v", seed, err)
		}
	}
	for _, ef := range []int{10, 40} {
		rs, rp := loose.recall(serial, looseK, ef, false), loose.recall(par, looseK, ef, false)
		t.Logf("ef=%d recall serial %.4f parallel %.4f", ef, rs, rp)
		if math.Abs(rs-rp) > 0.01 {
			t.Errorf("ef=%d: serial %.4f and parallel %.4f differ by more than 0.01", ef, rs, rp)
		}
	}
}

func TestSerialBuildIsReproducible(t *testing.T) {
	a, b := tightIx(), mustBuild(t, tight, tightCfg, mutation{})
	if !slices.Equal(a.links0, b.links0) || !slices.Equal(a.upper, b.upper) || a.entry != b.entry {
		t.Fatal("two builds with the same seed differ")
	}
}

// P(level >= 1) is 1/M with mL = 1/ln M.
func TestLevelDistribution(t *testing.T) {
	const n, m = 200000, 16
	above := 0
	for _, l := range drawLevels(n, m, 7) {
		if l > 0 {
			above++
		}
	}
	if got, want := float64(above)/n, 1.0/m; math.Abs(got-want) > 0.005 {
		t.Fatalf("share above layer 0 %.4f, want %.4f", got, want)
	}
}

func TestSmallAndDegenerate(t *testing.T) {
	cfg := Config{M: 4, EfConstruction: 16, Seed: 3}
	empty, err := Build(nil, 4, cfg)
	if err != nil || empty.Search([]float32{1, 0, 0, 0}, 5, 10, nil) != nil || empty.Check() != nil {
		t.Fatalf("empty index: %v", err)
	}
	one, _ := Build([]float32{0, 1}, 2, cfg)
	if got := one.Search([]float32{1, 0}, 5, 10, nil); len(got) != 1 || got[0].ID != 0 {
		t.Fatalf("one node: %v", got)
	}
	// 3 distinct vectors, 100 copies each. Ties go to the lower ID, so most
	// copies get no links from the others and cannot be found; the index
	// stays valid and returns distinct nodes of the best vector.
	data := make([]float32, 0, 600)
	for i := range 300 {
		data = append(data, float32(i%3), 1)
	}
	dup, _ := Build(data, 2, cfg)
	if err := dup.Check(); err != nil {
		t.Fatal(err)
	}
	got := dup.Search([]float32{1, 0}, 20, 50, nil)
	ids := map[int32]bool{}
	for _, h := range got {
		ids[h.ID] = true
		if h.Score != 2 {
			t.Fatalf("duplicates: %v", got)
		}
	}
	if len(got) == 0 || len(ids) != len(got) {
		t.Fatalf("duplicates: got %d results, %d distinct", len(got), len(ids))
	}
	for _, bad := range []struct {
		data []float32
		dim  int
		cfg  Config
	}{{[]float32{1, 2, 3}, 2, cfg}, {[]float32{1, 2}, 0, cfg}, {[]float32{1, 2}, 2, Config{M: 1, EfConstruction: 4}}} {
		if _, err := Build(bad.data, bad.dim, bad.cfg); err == nil {
			t.Errorf("Build(%v, %d, %+v) accepted", bad.data, bad.dim, bad.cfg)
		}
	}
}

func TestSaveAndRead(t *testing.T) {
	ix := looseIx()
	var buf bytes.Buffer
	if _, err := ix.WriteTo(&buf); err != nil {
		t.Fatal(err)
	}
	saved := buf.Bytes()
	got, err := Read(bytes.NewReader(saved), loose.data, loose.dim)
	if err != nil {
		t.Fatal(err)
	}
	for i := range loose.nq() {
		q := loose.query(i)
		if a, b := ix.Search(q, 10, 40, nil), got.Search(q, 10, 40, nil); !slices.Equal(a, b) {
			t.Fatalf("query %d: %v after reading, %v before", i, b, a)
		}
	}
	corrupt := slices.Clone(saved)
	corrupt[len(fileMagic)+8*8+100] = 200 // a level above the cap
	for name, b := range map[string][]byte{
		"truncated": saved[:len(saved)-4],
		"trailing":  append(slices.Clone(saved), 0),
		"corrupt":   corrupt,
		"magic":     append([]byte("x"), saved[1:]...),
	} {
		if _, err := Read(bytes.NewReader(b), loose.data, loose.dim); err == nil {
			t.Errorf("%s file accepted", name)
		}
	}
	if _, err := Read(bytes.NewReader(saved), loose.data[:len(loose.data)-loose.dim], loose.dim); err == nil {
		t.Error("data of another size accepted")
	}
}
