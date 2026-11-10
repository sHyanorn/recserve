package shard

import (
	"context"
	"fmt"
	"sync"

	"github.com/sHyanorn/recserve/internal/hnsw"
	"github.com/sHyanorn/recserve/internal/vec"
)

// Mode is how a shard searches its vectors.
type Mode string

// The modes.
const (
	Exact Mode = "exact" // every vector, by inner product
	HNSW  Mode = "hnsw"  // an HNSW index over the shard's vectors
)

// Config sets up a Set.
type Config struct {
	Shards int
	Mode   Mode
	HNSW   hnsw.Config // build parameters, HNSW mode only
	Ef     int         // search list size, HNSW mode only
}

// DefaultConfig is two exact shards; the HNSW settings are those of
// docs/BENCHMARKS.md, used when Mode is HNSW.
var DefaultConfig = Config{Shards: 2, Mode: Exact, HNSW: hnsw.Config{M: 16, EfConstruction: 200, Seed: 1}, Ef: 400}

// checkEvery is how many rows an exact search scans between checks of its
// context.
const checkEvery = 1024

// Part is one shard: the items Of assigns to it, in ID order, each with its
// vector. Items whose vector is zero are left out: their inner product with
// any query is 0, so their rank would depend on the other items' scores and
// their IDs, not on the query.
type Part struct {
	ids    []int32 // global IDs
	rows   vec.Matrix
	index  *hnsw.Index
	zeroes int
}

func (cfg Config) validate() error {
	if cfg.Shards < 1 {
		return fmt.Errorf("%d shards", cfg.Shards)
	}
	if cfg.Mode != Exact && cfg.Mode != HNSW {
		return fmt.Errorf("shard mode %q", cfg.Mode)
	}
	return nil
}

// BuildPart copies shard i of cfg.Shards out of the rows of m from 1 up
// (row 0 is padding) and, in HNSW mode, indexes it. The same m, cfg and i
// give the same part, graph included, as long as cfg.HNSW.Workers is at
// most 1.
func BuildPart(m vec.Matrix, cfg Config, i int) (*Part, error) {
	if err := cfg.validate(); err != nil {
		return nil, err
	}
	if i < 0 || i >= cfg.Shards {
		return nil, fmt.Errorf("shard %d of %d", i, cfg.Shards)
	}
	p := &Part{}
	var data []float32
	for id := int32(1); int(id) < m.Rows(); id++ {
		if Of(id, cfg.Shards) != i {
			continue
		}
		row := m.Row(id)
		if zero(row) {
			p.zeroes++
			continue
		}
		p.ids = append(p.ids, id)
		data = append(data, row...)
	}
	var err error
	if p.rows, err = vec.NewMatrix(m.Dim, data); err != nil {
		return nil, err
	}
	if cfg.Mode == HNSW && len(p.ids) > 0 {
		if p.index, err = hnsw.Build(data, m.Dim, cfg.HNSW); err != nil {
			return nil, fmt.Errorf("shard %d: %w", i, err)
		}
	}
	return p, nil
}

func zero(v []float32) bool {
	for _, x := range v {
		if x != 0 {
			return false
		}
	}
	return true
}

// Dim returns the dimension of the part's vectors.
func (p *Part) Dim() int { return p.rows.Dim }

// Len returns the number of items in the part.
func (p *Part) Len() int { return len(p.ids) }

// Search returns the part's k items with the highest inner product with q,
// best first, ties to the lower ID, leaving out the IDs in seen. Scores are
// computed as vec.Dot does. A part built in exact mode scans every vector,
// checks ctx every checkEvery rows and returns ctx's error once it has
// ended; one built in HNSW mode searches its graph with the search list
// size ef and checks ctx only before it starts.
func (p *Part) Search(ctx context.Context, q []float32, k, ef int, seen map[int32]bool) ([]vec.Hit, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	t := vec.NewTop(k)
	if p.index != nil {
		for _, h := range p.index.Search(q, k, ef, func(l int32) bool { return seen[p.ids[l]] }) {
			t.Push(vec.Hit{ID: p.ids[h.ID], Score: vec.Dot(q, p.rows.Row(h.ID))})
		}
		return t.Hits(), nil
	}
	for l, id := range p.ids {
		if l%checkEvery == 0 && l > 0 {
			if err := ctx.Err(); err != nil {
				return nil, err
			}
		}
		if !seen[id] {
			t.Push(vec.Hit{ID: id, Score: vec.Dot(q, p.rows.Row(int32(l)))})
		}
	}
	return t.Hits(), nil
}

// Set holds the item vectors split into shards by Of, in this process.
type Set struct {
	cfg   Config
	parts []*Part
}

// Build splits the rows of m from 1 up (row 0 is padding) into
// cfg.Shards shards and, in HNSW mode, indexes each.
func Build(m vec.Matrix, cfg Config) (*Set, error) {
	if err := cfg.validate(); err != nil {
		return nil, err
	}
	s := &Set{cfg: cfg, parts: make([]*Part, cfg.Shards)}
	for i := range s.parts {
		var err error
		if s.parts[i], err = BuildPart(m, cfg, i); err != nil {
			return nil, err
		}
	}
	return s, nil
}

// WithEf returns a Set that shares s's shards and searches them with the
// HNSW search list size ef.
func (s *Set) WithEf(ef int) *Set {
	c := *s
	c.cfg.Ef = ef
	return &c
}

// Config returns the settings the set was built and is searched with.
func (s *Set) Config() Config { return s.cfg }

// Zeroes returns the number of items left out for a zero vector.
func (s *Set) Zeroes() int {
	n := 0
	for _, p := range s.parts {
		n += p.zeroes
	}
	return n
}

// Search searches shard i as Part.Search does, with the set's search list
// size.
func (s *Set) Search(ctx context.Context, i int, q []float32, k int, seen map[int32]bool) ([]vec.Hit, error) {
	return s.parts[i].Search(ctx, q, k, s.cfg.Ef, seen)
}

// Merge returns the best k of the shards' results, best first, ties to the
// lower ID. With every shard's top k it equals a top k over all items.
func Merge(results [][]vec.Hit, k int) []vec.Hit {
	t := vec.NewTop(k)
	for _, hits := range results {
		for _, h := range hits {
			t.Push(h)
		}
	}
	return t.Hits()
}

// SearchAll searches every shard in parallel and merges their top k; it
// fails if any shard does.
func (s *Set) SearchAll(ctx context.Context, q []float32, k int, seen map[int32]bool) ([]vec.Hit, error) {
	results := make([][]vec.Hit, len(s.parts))
	errs := make([]error, len(s.parts))
	var wg sync.WaitGroup
	for i := range s.parts {
		wg.Go(func() { results[i], errs[i] = s.Search(ctx, i, q, k, seen) })
	}
	wg.Wait()
	for i, err := range errs {
		if err != nil {
			return nil, fmt.Errorf("shard %d: %w", i, err)
		}
	}
	return Merge(results, k), nil
}
