package bundle

import (
	"math"

	"github.com/sHyanorn/recserve/internal/vec"
)

// The sizes of the retrieval sources that the ranker features depend on
// (docs/SPEC.md §8).
const (
	CoocSeeds  = 10  // latest history entries used as co-occurrence seeds
	PopularTop = 100 // most popular items retrieved
)

// CoocHits returns the co-occurrence candidates of hist, oldest first: the
// item at position r of the row of the seed s entries from the latest
// scores 1 / ((s + 1)(r + 1)). An item may appear more than once.
func (b *Bundle) CoocHits(hist []int32) []vec.Hit {
	var out []vec.Hit
	for s := 0; s < CoocSeeds && s < len(hist); s++ {
		for r, id := range b.CoocRow(hist[len(hist)-1-s]) {
			out = append(out, vec.Hit{ID: id, Score: 1 / float64((s+1)*(r+1))})
		}
	}
	return out
}

// PopularHits returns the PopularTop most popular items, the one at rank r
// scoring 1 / (r + 1).
func (b *Bundle) PopularHits() []vec.Hit {
	pop := b.Popular[:min(PopularTop, len(b.Popular))]
	out := make([]vec.Hit, len(pop))
	for r, id := range pop {
		out[r] = vec.Hit{ID: id, Score: 1 / float64(r+1)}
	}
	return out
}

// BestScores returns each ID's highest score among hits.
func BestScores(hits []vec.Hit) map[int32]float64 {
	out := make(map[int32]float64, len(hits))
	for _, h := range hits {
		if s, ok := out[h.ID]; !ok || h.Score > s {
			out[h.ID] = h.Score
		}
	}
	return out
}

// Features returns the ranker's input for cands, one row per candidate,
// the columns of RankerFeatures (docs/SPEC.md §8). hist is the history,
// oldest first, uv the user vector or nil, and cooc and popular hold each
// candidate's best score from those sources (BestScores), empty for a
// source that was skipped. Each value is computed in float64 and rounded
// to float32.
func (b *Bundle) Features(cands, hist []int32, uv []float32, cooc, popular map[int32]float64) []float32 {
	var last []float32
	var lastNorm float64
	if len(hist) > 0 {
		last = b.Vectors.Row(hist[len(hist)-1])
		lastNorm = math.Sqrt(vec.Dot(last, last))
	}
	histLen := math.Log1p(float64(len(hist)))
	nf := len(RankerFeatures)
	out := make([]float32, len(cands)*nf)
	for i, id := range cands {
		row := b.Vectors.Row(id)
		norm := math.Sqrt(vec.Dot(row, row))
		var vs, cos float64
		if uv != nil {
			vs = vec.Dot(uv, row)
		}
		if norm > 0 && lastNorm > 0 {
			cos = vec.Dot(row, last) / (norm * lastNorm)
		}
		f := out[i*nf : (i+1)*nf]
		f[0], f[1], f[2] = float32(vs), float32(cooc[id]), float32(popular[id])
		f[3], f[4], f[5] = float32(norm), float32(cos), float32(histLen)
	}
	return out
}
