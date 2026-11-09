// Package hnsw is a hierarchical navigable small world graph (Malkov and
// Yashunin, arXiv:1603.09320) over float32 vectors, searched by inner
// product. docs/DECISIONS.md D016 and D024 record the design.
//
// Node i is row i of the vector array the index is built over. Neighbor
// lists live in flat int32 arrays, so the garbage collector has no pointers
// to scan.
package hnsw

import (
	"errors"
	"fmt"
	"math"
	"math/rand/v2"
	"slices"
	"sync"
	"sync/atomic"

	"github.com/sHyanorn/recserve/internal/vec"
)

// Config sets the build parameters.
type Config struct {
	M              int    // links per node on layers 1 and up; 2M on layer 0
	EfConstruction int    // result list size of the searches that insert a node
	Seed           uint64 // seed of the level draw
	Workers        int    // goroutines inserting nodes; 1 or less builds reproducibly
}

// maxLevel caps a node's level; with M >= 2 a level above 30 has
// probability below 2^-30 per node.
const maxLevel = 30

// Index is an HNSW graph. It is safe for concurrent searches once built.
type Index struct {
	dim, n   int
	data     []float32 // n rows of dim values, the caller's
	m, m0    int       // most links per node on layers 1 and up, and on layer 0
	efc      int
	levels   []uint8
	links0   []int32 // per node 1+m0 slots: the count, then the links
	upperOff []int32 // per node the offset in upper of its layer-1 list, -1 below layer 1
	upper    []int32 // per node and layer 1..level, 1+m slots: the count, then the links
	entry    int32
	top      int // level of the entry point

	locks  []sync.Mutex // per node, during a parallel build only
	global sync.Mutex   // during a parallel build: guards entry and top
	pool   sync.Pool    // *searcher
	mut    mutation
}

// mutation breaks the algorithm on purpose, for tests that must catch it.
type mutation struct {
	simpleSelect bool // keep the nearest candidates instead of Algorithm 4
	layer0M      bool // cap layer 0 at M links instead of 2M
	frozenEntry  bool // never move the entry point
}

// item is a node and its distance to the current base point: the negative
// inner product, so smaller is closer.
type item struct {
	d  float32
	id int32
}

func closer(a, b item) bool { return a.d < b.d || (a.d == b.d && a.id < b.id) }

func cmpItems(a, b item) int {
	if closer(a, b) {
		return -1
	}
	if closer(b, a) {
		return 1
	}
	return 0
}

// Build indexes the rows of data, dim values each.
func Build(data []float32, dim int, cfg Config) (*Index, error) {
	return build(data, dim, cfg, mutation{})
}

func build(data []float32, dim int, cfg Config, mut mutation) (*Index, error) {
	if dim <= 0 || len(data)%dim != 0 {
		return nil, fmt.Errorf("hnsw: %d values do not form rows of %d", len(data), dim)
	}
	if cfg.M < 2 || cfg.EfConstruction < 1 {
		return nil, errors.New("hnsw: M must be at least 2 and EfConstruction at least 1")
	}
	n := len(data) / dim
	if n > math.MaxInt32 {
		return nil, fmt.Errorf("hnsw: %d rows exceed int32 IDs", n)
	}
	x := &Index{dim: dim, n: n, data: data, m: cfg.M, m0: 2 * cfg.M, efc: max(cfg.EfConstruction, cfg.M), mut: mut}
	if mut.layer0M {
		x.m0 = x.m
	}
	x.levels = drawLevels(n, cfg.M, cfg.Seed)
	if err := x.allocate(); err != nil {
		return nil, err
	}
	x.pool.New = func() any { return x.newSearcher() }
	if n == 0 {
		return x, nil
	}
	x.entry, x.top = 0, int(x.levels[0])
	workers := max(1, cfg.Workers)
	if workers > 1 {
		x.locks = make([]sync.Mutex, n)
	}
	var next atomic.Int64
	next.Store(1)
	var wg sync.WaitGroup
	for range workers {
		wg.Go(func() {
			s := x.newSearcher()
			for i := next.Add(1) - 1; i < int64(n); i = next.Add(1) - 1 {
				x.insert(s, int32(i))
			}
		})
	}
	wg.Wait()
	x.locks = nil
	return x, nil
}

// drawLevels draws each node's level as floor(-ln U / ln M), U uniform on (0, 1].
func drawLevels(n, m int, seed uint64) []uint8 {
	r := rand.New(rand.NewPCG(seed, 0x9e3779b97f4a7c15))
	mult := 1 / math.Log(float64(m))
	out := make([]uint8, n)
	for i := range out {
		out[i] = uint8(min(math.Floor(-math.Log(1-r.Float64())*mult), maxLevel))
	}
	return out
}

// allocate sizes the link arrays from the levels.
func (x *Index) allocate() error {
	x.links0 = make([]int32, x.n*(1+x.m0))
	x.upperOff = make([]int32, x.n)
	total := 0
	for i, l := range x.levels {
		if l == 0 {
			x.upperOff[i] = -1
			continue
		}
		if total > math.MaxInt32 {
			return errors.New("hnsw: upper layers exceed int32 offsets")
		}
		x.upperOff[i] = int32(total)
		total += int(l) * (1 + x.m)
	}
	x.upper = make([]int32, total)
	return nil
}

// Len returns the number of nodes.
func (x *Index) Len() int { return x.n }

func (x *Index) vec(i int32) []float32 { return x.data[int(i)*x.dim : (int(i)+1)*x.dim] }

func (x *Index) dist(q []float32, i int32) float32 { return -vec.Dot32(q, x.vec(i)) }

func (x *Index) capacity(layer int) int {
	if layer == 0 {
		return x.m0
	}
	return x.m
}

// list returns node i's slots on layer: the count, then capacity(layer) links.
func (x *Index) list(i int32, layer int) []int32 {
	if layer == 0 {
		s := int(i) * (1 + x.m0)
		return x.links0[s : s+1+x.m0]
	}
	s := int(x.upperOff[i]) + (layer-1)*(1+x.m)
	return x.upper[s : s+1+x.m]
}

// neighbors returns node i's links on layer. During a parallel build it
// copies them under the node's lock into s.nb.
func (x *Index) neighbors(s *searcher, i int32, layer int) []int32 {
	if x.locks == nil {
		l := x.list(i, layer)
		return l[1 : 1+l[0]]
	}
	x.locks[i].Lock()
	l := x.list(i, layer)
	s.nb = append(s.nb[:0], l[1:1+l[0]]...)
	x.locks[i].Unlock()
	return s.nb
}

// searcher holds one goroutine's scratch space.
type searcher struct {
	visited []uint16 // visited[i] == epoch: node i was visited in this search
	epoch   uint16
	cand    []item // min-heap: nearest unexpanded candidate on top
	res     []item // max-heap: farthest result on top
	nb      []int32
	eps     []item // entry points of the next layer search
	w       []item // results of a layer search, nearest first
	sel     []item
	ov      []item // overflow candidates of a neighbor's list
	ovSel   []item
	ids     []int32
	ids2    []int32
}

func (x *Index) newSearcher() *searcher { return &searcher{visited: make([]uint16, x.n)} }

func (s *searcher) nextEpoch() {
	s.epoch++
	if s.epoch == 0 {
		clear(s.visited)
		s.epoch = 1
	}
}

// searchLayer is Algorithm 2: it leaves in s.res the up to ef nodes nearest
// to q on layer found from the entry points eps. Nodes for which skip
// returns true are traversed but not kept. It stops when the nearest
// unexpanded candidate is farther than the farthest result and the results
// are full (hnswlib's rule; without skip the results cannot be short then).
func (x *Index) searchLayer(s *searcher, q []float32, eps []item, ef, layer int, skip func(int32) bool) {
	s.nextEpoch()
	s.cand, s.res = s.cand[:0], s.res[:0]
	keep := func(it item) {
		if skip == nil || !skip(it.id) {
			s.res = pushFar(s.res, it)
			if len(s.res) > ef {
				s.res = popFar(s.res)
			}
		}
	}
	for _, e := range eps {
		if s.visited[e.id] != s.epoch {
			s.visited[e.id] = s.epoch
			s.cand = pushNear(s.cand, e)
			keep(e)
		}
	}
	bound := float32(math.Inf(1)) // distance of the farthest result
	if len(s.res) > 0 {
		bound = s.res[0].d
	}
	for len(s.cand) > 0 {
		c := s.cand[0]
		if c.d > bound && (len(s.res) == ef || skip == nil) {
			break
		}
		s.cand = popNear(s.cand)
		for _, j := range x.neighbors(s, c.id, layer) {
			if s.visited[j] == s.epoch {
				continue
			}
			s.visited[j] = s.epoch
			if d := x.dist(q, j); len(s.res) < ef || d < bound {
				it := item{d, j}
				s.cand = pushNear(s.cand, it)
				keep(it)
				if len(s.res) > 0 {
					bound = s.res[0].d
				}
			}
		}
	}
}

// greedy walks from cur to the nearest neighbor of q on each layer from
// down to above, ef = 1 in Algorithm 2's terms (hnswlib's descent).
func (x *Index) greedy(s *searcher, q []float32, cur item, from, above int) item {
	for layer := from; layer > above; layer-- {
		for changed := true; changed; {
			changed = false
			for _, j := range x.neighbors(s, cur.id, layer) {
				if d := x.dist(q, j); d < cur.d {
					cur, changed = item{d, j}, true
				}
			}
		}
	}
	return cur
}

// selectNeighbors is Algorithm 4 without its two options: going through
// cands nearest first (distances to the base point), it keeps a candidate
// unless a kept one is strictly closer to it than the base point is, until
// limit are kept.
func (x *Index) selectNeighbors(cands []item, limit int, out []item) []item {
	out = out[:0]
	for _, c := range cands {
		if len(out) == limit {
			break
		}
		good := true
		if !x.mut.simpleSelect {
			cv := x.vec(c.id)
			for _, r := range out {
				if -vec.Dot32(cv, x.vec(r.id)) < c.d {
					good = false
					break
				}
			}
		}
		if good {
			out = append(out, c)
		}
	}
	return out
}

// insert is Algorithm 1 for node i.
func (x *Index) insert(s *searcher, i int32) {
	q := x.vec(i)
	level := int(x.levels[i])
	held := false
	if x.locks != nil {
		x.global.Lock()
		held = true
	}
	entry, top := x.entry, x.top
	if held && level <= top {
		x.global.Unlock()
		held = false
	}
	// A node above the top level keeps the global lock until it is the entry point (hnswlib).
	cur := x.greedy(s, q, item{x.dist(q, entry), entry}, top, level)
	s.eps = append(s.eps[:0], cur)
	for layer := min(level, top); layer >= 0; layer-- {
		x.searchLayer(s, q, s.eps, x.efc, layer, nil)
		s.w = sortedNear(s.w, s.res)
		// In a parallel build a node inserted at the same time may already
		// link to i, so i can find itself (faiss skips it the same way).
		s.w = slices.DeleteFunc(s.w, func(it item) bool { return it.id == i })
		s.sel = x.selectNeighbors(s.w, x.m, s.sel)
		s.ids = s.ids[:0]
		for _, e := range s.sel {
			s.ids = append(s.ids, e.id)
		}
		x.connect(s, i, layer, s.ids)
		for _, e := range s.sel {
			x.connect(s, e.id, layer, []int32{i})
		}
		s.eps = append(s.eps[:0], s.w...) // ep ← W
	}
	if level > top && !x.mut.frozenEntry {
		x.entry, x.top = i, level
	}
	if held {
		x.global.Unlock()
	}
}

// connect adds links from e to the nodes in add on layer, skipping those
// already linked. A list that would overflow selects again from its links
// and the new ones with Algorithm 4. In a serial build a new node's list is
// empty when it is connected; in a parallel build other nodes may have
// linked to it already, and two nodes may select each other.
func (x *Index) connect(s *searcher, e int32, layer int, add []int32) {
	if x.locks != nil {
		x.locks[e].Lock()
		defer x.locks[e].Unlock()
	}
	l := x.list(e, layer)
	limit := x.capacity(layer)
	s.ids2 = s.ids2[:0]
	for _, j := range add {
		if j != e && !slices.Contains(l[1:1+l[0]], j) {
			if int(l[0]) < limit {
				l[1+l[0]] = j
				l[0]++
			} else {
				s.ids2 = append(s.ids2, j)
			}
		}
	}
	if len(s.ids2) == 0 {
		return
	}
	ev := x.vec(e)
	s.ov = s.ov[:0]
	for _, j := range append(s.ids2, l[1:1+l[0]]...) {
		s.ov = append(s.ov, item{-vec.Dot32(ev, x.vec(j)), j})
	}
	slices.SortFunc(s.ov, cmpItems)
	s.ovSel = x.selectNeighbors(s.ov, limit, s.ovSel)
	l[0] = int32(len(s.ovSel))
	for k, it := range s.ovSel {
		l[1+k] = it.id
	}
}

// sortedNear copies h into dst, nearest first.
func sortedNear(dst, h []item) []item {
	dst = append(dst[:0], h...)
	slices.SortFunc(dst, cmpItems)
	return dst
}

// Search returns the k nodes with the highest inner product with q, best
// first, ties to the lower ID, searching layer 0 with max(ef, k) results.
// Nodes for which skip returns true are not returned; skip may be nil.
// q must have the index's dimension.
func (x *Index) Search(q []float32, k, ef int, skip func(int32) bool) []vec.Hit {
	if x.n == 0 || k <= 0 {
		return nil
	}
	s := x.pool.Get().(*searcher)
	defer x.pool.Put(s)
	cur := x.greedy(s, q, item{x.dist(q, x.entry), x.entry}, x.top, 0)
	x.searchLayer(s, q, append(s.eps[:0], cur), max(ef, k), 0, skip)
	s.w = sortedNear(s.w, s.res)
	out := make([]vec.Hit, min(k, len(s.w)))
	for j := range out {
		out[j] = vec.Hit{ID: s.w[j].id, Score: float64(-s.w[j].d)}
	}
	return out
}

// Check verifies the graph's invariants: list sizes and offsets, links to
// existing nodes on layers they live on, no self or repeated links, and the
// entry point on the top layer.
func (x *Index) Check() error {
	if len(x.levels) != x.n || len(x.links0) != x.n*(1+x.m0) || len(x.upperOff) != x.n {
		return errors.New("hnsw: array sizes do not match the node count")
	}
	off := 0
	for i, l := range x.levels {
		want := int32(-1)
		if l > 0 {
			want = int32(off)
			off += int(l) * (1 + x.m)
		}
		if l > maxLevel || x.upperOff[i] != want {
			return fmt.Errorf("hnsw: node %d: level %d or upper offset %d is wrong", i, l, x.upperOff[i])
		}
	}
	if len(x.upper) != off {
		return errors.New("hnsw: upper layer array size does not match the levels")
	}
	if x.n == 0 {
		return nil
	}
	if x.entry < 0 || int(x.entry) >= x.n || int(x.levels[x.entry]) != x.top || x.top != int(slices.Max(x.levels)) {
		return fmt.Errorf("hnsw: entry point %d at level %d is not on the top layer", x.entry, x.top)
	}
	stamp := make([]int, x.n)
	round := 0
	for i := range int32(x.n) {
		for layer := 0; layer <= int(x.levels[i]); layer++ {
			round++
			l := x.list(i, layer)
			if l[0] < 0 || int(l[0]) > x.capacity(layer) {
				return fmt.Errorf("hnsw: node %d layer %d holds %d links", i, layer, l[0])
			}
			for _, j := range l[1 : 1+l[0]] {
				switch {
				case j < 0 || int(j) >= x.n:
					return fmt.Errorf("hnsw: node %d layer %d links to %d", i, layer, j)
				case j == i || stamp[j] == round:
					return fmt.Errorf("hnsw: node %d layer %d repeats or links to itself", i, layer)
				case int(x.levels[j]) < layer:
					return fmt.Errorf("hnsw: node %d layer %d links to %d, which is not on that layer", i, layer, j)
				}
				stamp[j] = round
			}
		}
	}
	return nil
}

// Reachable marks the nodes reachable on layer 0 from the entry point: the
// nodes a search can return. Pruning can remove every link to a node, so a
// few may be unreachable, as in hnswlib and faiss.
func (x *Index) Reachable() []bool {
	seen := make([]bool, x.n)
	if x.n == 0 {
		return seen
	}
	queue := []int32{x.entry}
	seen[x.entry] = true
	for h := 0; h < len(queue); h++ {
		l := x.list(queue[h], 0)
		for _, j := range l[1 : 1+l[0]] {
			if !seen[j] {
				seen[j] = true
				queue = append(queue, j)
			}
		}
	}
	return seen
}

func pushNear(h []item, it item) []item {
	h = append(h, it)
	for i := len(h) - 1; i > 0; {
		p := (i - 1) / 2
		if !closer(h[i], h[p]) {
			break
		}
		h[i], h[p] = h[p], h[i]
		i = p
	}
	return h
}

func popNear(h []item) []item {
	n := len(h) - 1
	h[0] = h[n]
	h = h[:n]
	for i := 0; ; {
		c := 2*i + 1
		if c >= n {
			break
		}
		if c+1 < n && closer(h[c+1], h[c]) {
			c++
		}
		if !closer(h[c], h[i]) {
			break
		}
		h[i], h[c] = h[c], h[i]
		i = c
	}
	return h
}

func pushFar(h []item, it item) []item {
	h = append(h, it)
	for i := len(h) - 1; i > 0; {
		p := (i - 1) / 2
		if !closer(h[p], h[i]) {
			break
		}
		h[i], h[p] = h[p], h[i]
		i = p
	}
	return h
}

func popFar(h []item) []item {
	n := len(h) - 1
	h[0] = h[n]
	h = h[:n]
	for i := 0; ; {
		c := 2*i + 1
		if c >= n {
			break
		}
		if c+1 < n && closer(h[c], h[c+1]) {
			c++
		}
		if !closer(h[i], h[c]) {
			break
		}
		h[i], h[c] = h[c], h[i]
		i = c
	}
	return h
}
