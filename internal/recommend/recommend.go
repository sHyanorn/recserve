// Package recommend runs the request path: history lookup, retrieval from
// several sources in parallel, scoring, and re-ranking with the seen-item
// filter, deduplication and MMR.
package recommend

import (
	"cmp"
	"context"
	"fmt"
	"math"
	"slices"
	"sync"

	"github.com/sHyanorn/recserve/internal/bundle"
	"github.com/sHyanorn/recserve/internal/history"
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
	Items        []Item `json:"items"`
	ModelVersion string `json:"model_version"`
}

// Pipeline answers requests from one bundle and a history store.
type Pipeline struct {
	Bundle  *bundle.Bundle
	History history.Store

	skipSeenFilter bool // test only: the broken variant of property 2
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
	uv := b.UserVector(hist)

	cands, err := p.retrieve(ctx, hist, uv, seen)
	if err != nil {
		return Response{}, err
	}
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
	picked := mmr(b.Vectors, scored, req.K, MMRLambda)
	resp := Response{Items: make([]Item, len(picked)), ModelVersion: b.ModelVersion}
	for i, h := range picked {
		resp.Items[i] = Item{ID: b.IDs[h.ID], Score: h.Score}
	}
	return resp, nil
}

// retrieve runs the sources in parallel and returns their union, each ID
// once, keeping the best source score.
func (p *Pipeline) retrieve(ctx context.Context, hist []int32, uv []float32, seen map[int32]bool) ([]candidate, error) {
	b := p.Bundle
	sources := []func() []vec.Hit{
		func() []vec.Hit { // vector
			if uv == nil {
				return nil
			}
			return b.Vectors.TopK(uv, VectorTop, func(id int32) bool { return seen[id] })
		},
		func() []vec.Hit { // co-occurrence: seeds weighted by recency, neighbors by rank
			var out []vec.Hit
			for i := 0; i < CoocSeeds && i < len(hist); i++ {
				w := 1 / float64(i+1)
				for r, id := range b.CoocRow(hist[len(hist)-1-i]) {
					out = append(out, vec.Hit{ID: id, Score: w / float64(r+1)})
				}
			}
			return out
		},
		func() []vec.Hit { // popularity
			pop := b.Popular[:min(PopularTop, len(b.Popular))]
			out := make([]vec.Hit, len(pop))
			for r, id := range pop {
				out[r] = vec.Hit{ID: id, Score: 1 / float64(r+1)}
			}
			return out
		},
	}
	results := make([][]vec.Hit, len(sources))
	var wg sync.WaitGroup
	for i, src := range sources {
		wg.Go(func() { results[i] = src() })
	}
	wg.Wait()
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	best := map[int32]float64{}
	for _, hits := range results {
		for _, h := range hits {
			if s, ok := best[h.ID]; !ok || h.Score > s {
				best[h.ID] = h.Score
			}
		}
	}
	out := make([]candidate, 0, len(best))
	for id, s := range best {
		out = append(out, candidate{id, s})
	}
	slices.SortFunc(out, func(a, b candidate) int { return cmp.Compare(a.id, b.id) })
	return out, nil
}

// mmr picks k hits by maximal marginal relevance: relevance is the score
// scaled to [0, 1] over the candidates, redundancy the highest cosine
// similarity to an already picked item. Ties go to the lower ID.
func mmr(m vec.Matrix, hits []vec.Hit, k int, lambda float64) []vec.Hit {
	if len(hits) == 0 {
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
	norms := make([]float64, len(hits))
	for i, h := range hits {
		norms[i] = math.Sqrt(vec.Dot(m.Row(h.ID), m.Row(h.ID)))
	}
	redundancy := make([]float64, len(hits)) // highest similarity to a picked item
	used := make([]bool, len(hits))
	out := make([]vec.Hit, 0, min(k, len(hits)))
	for len(out) < k && len(out) < len(hits) {
		bi, bv := -1, math.Inf(-1)
		for i, h := range hits {
			if used[i] {
				continue
			}
			v := lambda*rel(h.Score) - (1-lambda)*redundancy[i]
			if v > bv || (v == bv && h.ID < hits[bi].ID) {
				bi, bv = i, v
			}
		}
		used[bi] = true
		out = append(out, hits[bi])
		for i, h := range hits {
			if used[i] || norms[i] == 0 || norms[bi] == 0 {
				continue
			}
			sim := vec.Dot(m.Row(h.ID), m.Row(hits[bi].ID)) / (norms[i] * norms[bi])
			redundancy[i] = max(redundancy[i], sim)
		}
	}
	return out
}
