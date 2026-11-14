package bundle

import (
	"math"
	"time"

	"github.com/sHyanorn/recserve/internal/vec"
)

// The sizes of the retrieval sources that the ranker features depend on
// (docs/SPEC.md §8).
const (
	CoocSeeds  = 10  // latest history entries used as co-occurrence seeds
	PopularTop = 100 // most popular items retrieved
)

// CoocHits returns the co-occurrence candidates of hist, oldest first, from
// the bundle's table (CoocHitsOf).
func (b *Bundle) CoocHits(hist []int32) []vec.Hit {
	return CoocHitsOf(hist, b.CoocRow)
}

// CoocHitsOf returns the co-occurrence candidates of hist, oldest first,
// given each item's co-occurring items, best first: the item at position r
// of the row of the seed s entries from the latest scores
// 1 / ((s + 1)(r + 1)). An item may appear more than once.
func CoocHitsOf(hist []int32, row func(id int32) []int32) []vec.Hit {
	var out []vec.Hit
	for s := 0; s < CoocSeeds && s < len(hist); s++ {
		for r, id := range row(hist[len(hist)-1-s]) {
			out = append(out, vec.Hit{ID: id, Score: 1 / float64((s+1)*(r+1))})
		}
	}
	return out
}

// PopularHits returns the PopularTop most popular items of the bundle's
// list (PopularHitsOf).
func (b *Bundle) PopularHits() []vec.Hit {
	return PopularHitsOf(b.Popular)
}

// PopularHitsOf returns the first PopularTop items of popular, best first,
// the one at rank r scoring 1 / (r + 1).
func PopularHitsOf(popular []int32) []vec.Hit {
	pop := popular[:min(PopularTop, len(popular))]
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

// Recency is what a request's RecencyFeatures read from the stream state at
// its time TS (D060): an item's events in the window before TS, and the time
// of its first event before TS, false if it has none.
type Recency struct {
	TS    int64
	Count func(id int32) int
	First func(id int32) (int64, bool)
}

// ageUnit is item_age's unit, a day in milliseconds; a test changes it to
// show that golden cases catch another unit.
var ageUnit = float64(24 * time.Hour / time.Millisecond)

// Features returns the ranker's input for cands, one row per candidate,
// the columns of the bundle's ranker_features: RankerFeatures (docs/SPEC.md
// §8), then RecencyFeatures if the bundle's ranker takes them, which rec
// must then give. hist is the history, oldest first, uv the user vector or
// nil, and cooc and popular hold each candidate's best score from those
// sources (BestScores), empty for a source that was skipped. Each value is
// computed in float64 and rounded to float32.
func (b *Bundle) Features(cands, hist []int32, uv []float32, cooc, popular map[int32]float64, rec *Recency) []float32 {
	var last []float32
	var lastNorm float64
	if len(hist) > 0 {
		last = b.Vectors.Row(hist[len(hist)-1])
		lastNorm = math.Sqrt(vec.Dot(last, last))
	}
	histLen := math.Log1p(float64(len(hist)))
	nf := b.featureCount()
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
		if nf > len(RankerFeatures) {
			var age float64
			if first, ok := rec.First(id); ok {
				age = math.Log1p(float64(rec.TS-first) / ageUnit)
			}
			f[6], f[7] = float32(math.Log1p(float64(rec.Count(id)))), float32(age)
		}
	}
	return out
}
