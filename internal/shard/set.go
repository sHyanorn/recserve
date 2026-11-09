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

// Set holds the item vectors split into shards by Of. Items whose vector is
// zero are left out: their inner product with any query is 0, so their rank
// would depend on the other items' scores and their IDs, not on the query.
type Set struct {
	cfg    Config
	parts  []part
	dim    int
	zeroes int
}

// part is one shard: the global IDs of its items and their vectors, copied
// row by row in ID order.
type part struct {
	ids   []int32
	rows  vec.Matrix
	index *hnsw.Index
}

// Build splits the rows of m from 1 up (row 0 is padding) into
// cfg.Shards shards and, in HNSW mode, indexes each.
func Build(m vec.Matrix, cfg Config) (*Set, error) {
	if cfg.Shards < 1 {
		return nil, fmt.Errorf("%d shards", cfg.Shards)
	}
	if cfg.Mode != Exact && cfg.Mode != HNSW {
		return nil, fmt.Errorf("shard mode %q", cfg.Mode)
	}
	s := &Set{cfg: cfg, parts: make([]part, cfg.Shards), dim: m.Dim}
	data := make([][]float32, cfg.Shards)
	for id := int32(1); int(id) < m.Rows(); id++ {
		row := m.Row(id)
		if zero(row) {
			s.zeroes++
			continue
		}
		p := Of(id, cfg.Shards)
		s.parts[p].ids = append(s.parts[p].ids, id)
		data[p] = append(data[p], row...)
	}
	for i := range s.parts {
		p := &s.parts[i]
		var err error
		if p.rows, err = vec.NewMatrix(m.Dim, data[i]); err != nil {
			return nil, err
		}
		if cfg.Mode == HNSW && len(p.ids) > 0 {
			if p.index, err = hnsw.Build(data[i], m.Dim, cfg.HNSW); err != nil {
				return nil, fmt.Errorf("shard %d: %w", i, err)
			}
		}
	}
	return s, nil
}

func zero(v []float32) bool {
	for _, x := range v {
		if x != 0 {
			return false
		}
	}
	return true
}

// WithEf returns a Set that shares s's shards and searches them with the
// HNSW search list size ef.
func (s *Set) WithEf(ef int) *Set {
	c := *s
	c.cfg.Ef = ef
	return &c
}

// Shards returns the number of shards.
func (s *Set) Shards() int { return len(s.parts) }

// Zeroes returns the number of items left out for a zero vector.
func (s *Set) Zeroes() int { return s.zeroes }

// Search returns shard i's k items with the highest inner product with q,
// best first, ties to the lower ID, leaving out IDs for which skip returns
// true. Scores are computed as vec.Dot does. An exact search checks ctx
// every checkEvery rows and returns ctx's error once it has ended; an HNSW
// search checks it only before it starts.
func (s *Set) Search(ctx context.Context, i int, q []float32, k int, skip func(int32) bool) ([]vec.Hit, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	p := &s.parts[i]
	if p.index != nil {
		local := p.index.Search(q, k, s.cfg.Ef, func(l int32) bool { return skip != nil && skip(p.ids[l]) })
		t := vec.NewTop(k)
		for _, h := range local {
			id := p.ids[h.ID]
			t.Push(vec.Hit{ID: id, Score: vec.Dot(q, p.rows.Row(h.ID))})
		}
		return t.Hits(), nil
	}
	t := vec.NewTop(k)
	for l, id := range p.ids {
		if l%checkEvery == 0 && l > 0 {
			if err := ctx.Err(); err != nil {
				return nil, err
			}
		}
		if skip == nil || !skip(id) {
			t.Push(vec.Hit{ID: id, Score: vec.Dot(q, p.rows.Row(int32(l)))})
		}
	}
	return t.Hits(), nil
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
func (s *Set) SearchAll(ctx context.Context, q []float32, k int, skip func(int32) bool) ([]vec.Hit, error) {
	results := make([][]vec.Hit, len(s.parts))
	errs := make([]error, len(s.parts))
	var wg sync.WaitGroup
	for i := range s.parts {
		wg.Go(func() { results[i], errs[i] = s.Search(ctx, i, q, k, skip) })
	}
	wg.Wait()
	for i, err := range errs {
		if err != nil {
			return nil, fmt.Errorf("shard %d: %w", i, err)
		}
	}
	return Merge(results, k), nil
}
