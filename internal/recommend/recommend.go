// Package recommend runs the request path: history lookup, retrieval from
// several sources in parallel, scoring, ranking with the bundle's ranker,
// and re-ranking with the seen-item filter, deduplication and MMR.
package recommend

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"math"
	"slices"
	"sync"

	"github.com/sHyanorn/recserve/internal/bundle"
	"github.com/sHyanorn/recserve/internal/history"
	"github.com/sHyanorn/recserve/internal/shard"
	"github.com/sHyanorn/recserve/internal/vec"
)

// Defaults for the request path.
const (
	MaxK          = 100
	VectorTop     = 200 // candidates from vector retrieval
	CoocSeeds     = 10  // most recent history items used as co-occurrence seeds
	PopularTop    = 100 // candidates from the popularity list
	MMRLambda     = 0.7 // weight of relevance against diversity
	SourceVector  = "vector"
	SourceCooc    = "cooc"
	SourcePopular = "popular"
)

// Degradation reasons reported in a response.
const (
	RankerTimeout = "ranker_timeout"
	RankerError   = "ranker_error"
)

// The sources' positions in the results of a request.
const (
	srcVector = iota
	srcCooc
	srcPopular
)

// Request is one recommendation request.
type Request struct {
	User    string
	K       int
	TS      int64    // only events strictly before TS are used
	Exclude []string // raw item IDs never to return
}

// Item is one recommended item.
type Item struct {
	ID    string  `json:"id"`
	Score float64 `json:"score"`
}

// Response is the result of a request.
type Response struct {
	Items        []Item   `json:"items"`
	ModelVersion string   `json:"model_version"`
	Degraded     []string `json:"degraded"` // what was skipped or fell back; empty if nothing
	// Dropped counts the candidates left out because the ranker takes at
	// most bundle.RankerRows; they are the ones with the lowest retrieval
	// scores.
	Dropped int `json:"-"`
}

// Ranker scores rows of candidate features, bundle.RankerFeatures in order,
// one score per row; higher is better.
type Ranker interface {
	Rank(ctx context.Context, features []float32) ([]float32, error)
}

// Pipeline answers requests from one bundle and a history store.
type Pipeline struct {
	Bundle  *bundle.Bundle
	Shards  *shard.Set // the bundle's item vectors, for vector retrieval
	History history.Store

	rankWith       Ranker // test only: replaces the bundle's ranker
	skipSeenFilter bool   // test only: the broken variant of property 2
}

// New returns a pipeline over b and h, with b's vectors split into shards
// by cfg.
func New(b *bundle.Bundle, h history.Store, cfg shard.Config) (*Pipeline, error) {
	s, err := shard.Build(b.Vectors, cfg)
	if err != nil {
		return nil, err
	}
	return &Pipeline{Bundle: b, Shards: s, History: h}, nil
}

func (p *Pipeline) ranker() Ranker {
	if p.rankWith != nil {
		return p.rankWith
	}
	if p.Bundle.Ranker != nil {
		return p.Bundle.Ranker
	}
	return nil
}

type candidate struct {
	id    int32
	score float64 // source score, used when there is no user vector
}

// Recommend answers a request.
func (p *Pipeline) Recommend(ctx context.Context, req Request) (Response, error) {
	if req.K <= 0 || req.K > MaxK {
		return Response{}, fmt.Errorf("k must be in 1..%d", MaxK)
	}
	b := p.Bundle
	hist, err := p.History.Before(ctx, req.User, req.TS, 0)
	if err != nil {
		return Response{}, fmt.Errorf("history: %w", err)
	}
	seen := make(map[int32]bool, len(hist)+len(req.Exclude))
	for _, id := range hist {
		seen[id] = true
	}
	for _, raw := range req.Exclude {
		if id, ok := b.Lookup(raw); ok {
			seen[id] = true
		}
	}
	uv, err := b.UserVector(ctx, hist)
	if err != nil {
		return Response{}, fmt.Errorf("user vector: %w", err)
	}

	sources := p.sources(ctx, hist, uv, seen)
	results := make([][]vec.Hit, len(sources))
	errs := make([]error, len(sources))
	var wg sync.WaitGroup
	for i, src := range sources {
		wg.Go(func() { results[i], errs[i] = src() })
	}
	wg.Wait()
	if err := ctx.Err(); err != nil {
		return Response{}, err
	}
	if err := errors.Join(errs...); err != nil {
		return Response{}, err
	}
	resp := Response{ModelVersion: b.ModelVersion, Degraded: []string{}}
	scored := p.retrievalScores(results, uv, seen)
	if rk := p.ranker(); rk != nil && len(scored) > 0 {
		ranked, dropped, err := p.rank(ctx, rk, scored, results, hist, uv)
		switch {
		case err == nil:
			scored, resp.Dropped = ranked, dropped
		case errors.Is(ctx.Err(), context.Canceled):
			return Response{}, err
		case errors.Is(err, context.DeadlineExceeded):
			resp.Degraded = append(resp.Degraded, RankerTimeout)
		default:
			resp.Degraded = append(resp.Degraded, RankerError)
		}
	}
	picked := mmr(b.Vectors, b.InvNorms, scored, req.K, MMRLambda)
	resp.Items = make([]Item, len(picked))
	for i, h := range picked {
		resp.Items[i] = Item{ID: b.IDs[h.ID], Score: h.Score}
	}
	return resp, nil
}

// retrievalScores merges the sources' results and drops seen items. A
// candidate's score is its dot product with the user vector, or, without
// one, its best source score.
func (p *Pipeline) retrievalScores(results [][]vec.Hit, uv []float32, seen map[int32]bool) []vec.Hit {
	b := p.Bundle
	cands := merge(results)
	scored := make([]vec.Hit, 0, len(cands))
	for _, c := range cands {
		s := c.score
		if uv != nil {
			s = vec.Dot(uv, b.Vectors.Row(c.id))
		}
		scored = append(scored, vec.Hit{ID: c.id, Score: s})
	}
	if !p.skipSeenFilter {
		scored = slices.DeleteFunc(scored, func(h vec.Hit) bool { return seen[h.ID] })
	}
	return scored
}

// rank scores the candidates with rk: the bundle.RankerRows best by
// retrieval score, whose scores become rk's; the rest are dropped, and
// their number returned.
func (p *Pipeline) rank(ctx context.Context, rk Ranker, scored []vec.Hit, results [][]vec.Hit, hist []int32, uv []float32) ([]vec.Hit, int, error) {
	cands := slices.Clone(scored)
	slices.SortFunc(cands, func(a, b vec.Hit) int { // vec.Better first
		return cmp.Or(cmp.Compare(b.Score, a.Score), cmp.Compare(a.ID, b.ID))
	})
	dropped := max(0, len(cands)-bundle.RankerRows)
	cands = cands[:len(cands)-dropped]
	scores, err := rk.Rank(ctx, p.features(cands, results, hist, uv))
	if err != nil {
		return nil, 0, err
	}
	if len(scores) != len(cands) {
		return nil, 0, fmt.Errorf("ranker returned %d scores for %d candidates", len(scores), len(cands))
	}
	for i, s := range scores {
		cands[i].Score = float64(s)
	}
	return cands, dropped, nil
}

// features returns the ranker's input for cands, one row per candidate,
// bundle.RankerFeatures in order (docs/SPEC.md §8). Each value is computed
// in float64 and rounded to float32.
func (p *Pipeline) features(cands []vec.Hit, results [][]vec.Hit, hist []int32, uv []float32) []float32 {
	b := p.Bundle
	cooc, popular := best(results[srcCooc]), best(results[srcPopular])
	var last []float32
	var lastNorm float64
	if len(hist) > 0 {
		last = b.Vectors.Row(hist[len(hist)-1])
		lastNorm = math.Sqrt(vec.Dot(last, last))
	}
	histLen := math.Log1p(float64(len(hist)))
	nf := len(bundle.RankerFeatures)
	out := make([]float32, len(cands)*nf)
	for i, c := range cands {
		row := b.Vectors.Row(c.ID)
		norm := math.Sqrt(vec.Dot(row, row))
		var vs, cos float64
		if uv != nil {
			vs = vec.Dot(uv, row)
		}
		if norm > 0 && lastNorm > 0 {
			cos = vec.Dot(row, last) / (norm * lastNorm)
		}
		f := out[i*nf : (i+1)*nf]
		f[0], f[1], f[2] = float32(vs), float32(cooc[c.ID]), float32(popular[c.ID])
		f[3], f[4], f[5] = float32(norm), float32(cos), float32(histLen)
	}
	return out
}

// best returns each ID's highest score among hits.
func best(hits []vec.Hit) map[int32]float64 {
	out := make(map[int32]float64, len(hits))
	for _, h := range hits {
		if s, ok := out[h.ID]; !ok || h.Score > s {
			out[h.ID] = h.Score
		}
	}
	return out
}

// sources returns the retrieval sources; each returns its hits, best first.
func (p *Pipeline) sources(ctx context.Context, hist []int32, uv []float32, seen map[int32]bool) []func() ([]vec.Hit, error) {
	b := p.Bundle
	return []func() ([]vec.Hit, error){ // in the order of srcVector, srcCooc, srcPopular
		func() ([]vec.Hit, error) { // vector
			if uv == nil {
				return nil, nil
			}
			return p.Shards.SearchAll(ctx, uv, VectorTop, func(id int32) bool { return seen[id] })
		},
		func() ([]vec.Hit, error) { // co-occurrence: seeds weighted by recency, neighbors by rank
			var out []vec.Hit
			for i := 0; i < CoocSeeds && i < len(hist); i++ {
				w := 1 / float64(i+1)
				for r, id := range b.CoocRow(hist[len(hist)-1-i]) {
					out = append(out, vec.Hit{ID: id, Score: w / float64(r+1)})
				}
			}
			return out, nil
		},
		func() ([]vec.Hit, error) { // popularity
			pop := b.Popular[:min(PopularTop, len(b.Popular))]
			out := make([]vec.Hit, len(pop))
			for r, id := range pop {
				out[r] = vec.Hit{ID: id, Score: 1 / float64(r+1)}
			}
			return out, nil
		},
	}
}

// merge returns the union of the sources' hits, each ID once with its best
// source score, ordered by ID.
func merge(results [][]vec.Hit) []candidate {
	top := best(slices.Concat(results...))
	out := make([]candidate, 0, len(top))
	for id, s := range top {
		out = append(out, candidate{id, s})
	}
	slices.SortFunc(out, func(a, b candidate) int { return cmp.Compare(a.id, b.id) })
	return out
}

// cosine is the cosine similarity of rows a and b of m, given the inverse
// row norms; 0 if either row is zero.
func cosine(m vec.Matrix, inv []float32, a, b int32) float64 {
	if inv[a] == 0 || inv[b] == 0 {
		return 0
	}
	return float64(vec.Dot32(m.Row(a), m.Row(b)) * inv[a] * inv[b])
}

// mmr picks k hits by maximal marginal relevance: relevance is the score
// scaled to [0, 1] over the candidates, redundancy the highest cosine
// similarity to an already picked item (0 before any pick); inv holds the
// inverse row norms of m. Ties go to the lower ID.
//
// Redundancy only grows as items are picked, so a value computed in an
// earlier round bounds the current one from above. The candidates sit in a
// heap ordered by that bound; the top is brought up to date against the
// items picked since its last update, and is picked once it is up to date
// and still on top. This picks what recomputing every value each round
// would, with far fewer similarity computations.
func mmr(m vec.Matrix, inv []float32, hits []vec.Hit, k int, lambda float64) []vec.Hit {
	if len(hits) == 0 || k <= 0 {
		return nil
	}
	lo, hi := math.Inf(1), math.Inf(-1)
	for _, h := range hits {
		lo, hi = min(lo, h.Score), max(hi, h.Score)
	}
	rel := func(s float64) float64 {
		if hi == lo {
			return 1
		}
		return (s - lo) / (hi - lo)
	}
	value := func(e *mmrEntry) float64 { return lambda*rel(e.hit.Score) - (1-lambda)*e.redundancy }
	h := make(mmrHeap, len(hits))
	for i, hit := range hits {
		h[i] = mmrEntry{hit: hit}
		h[i].value = value(&h[i])
	}
	h.init()
	out := make([]vec.Hit, 0, min(k, len(hits)))
	for len(out) < k && len(h) > 0 {
		e := &h[0]
		if e.upTo == len(out) {
			out = append(out, e.hit)
			h = h.popTop()
			continue
		}
		for _, p := range out[e.upTo:] {
			e.redundancy = max(e.redundancy, cosine(m, inv, e.hit.ID, p.ID))
		}
		e.upTo = len(out)
		e.value = value(e)
		h.down(0)
	}
	return out
}

type mmrEntry struct {
	hit        vec.Hit
	redundancy float64 // highest similarity to the first upTo picked items
	upTo       int
	value      float64 // MMR value as of upTo picks: an upper bound on the current one
}

// mmrHeap is a max-heap by value, then by lower ID.
type mmrHeap []mmrEntry

func (h mmrHeap) less(i, j int) bool {
	return h[i].value > h[j].value || (h[i].value == h[j].value && h[i].hit.ID < h[j].hit.ID)
}

func (h mmrHeap) init() {
	for i := len(h)/2 - 1; i >= 0; i-- {
		h.down(i)
	}
}

func (h mmrHeap) down(i int) {
	for {
		c := 2*i + 1
		if c >= len(h) {
			return
		}
		if r := c + 1; r < len(h) && h.less(r, c) {
			c = r
		}
		if !h.less(c, i) {
			return
		}
		h[i], h[c] = h[c], h[i]
		i = c
	}
}

func (h mmrHeap) popTop() mmrHeap {
	n := len(h) - 1
	h[0] = h[n]
	h = h[:n]
	h.down(0)
	return h
}
