package recommend

import (
	"context"
	"fmt"
	"math/rand/v2"
	"slices"
	"testing"

	"github.com/sHyanorn/recserve/internal/shard"
)

// checkProperty6 runs the pipeline's vector retrieval in exact mode over
// catalogs larger than VectorTop and compares it with single-node exact
// search over the nonzero, unseen rows, item by item. broken makes each
// shard return only its share of VectorTop.
func checkProperty6(seed uint64, cases int, broken bool) error {
	r := rand.New(rand.NewPCG(seed, 6))
	ctx := context.Background()
	for c := range cases {
		p, err := randomPipeline(r, VectorTop+1+r.IntN(3*VectorTop), 0)
		if err != nil {
			return err
		}
		m := p.Bundle.Vectors
		if p.Shards, err = shard.Build(m, shard.Config{Shards: 1 + r.IntN(7), Mode: shard.Exact}); err != nil {
			return err
		}
		p.shardShare = broken
		uv := make([]float32, m.Dim)
		for j := range uv {
			uv[j] = float32(r.IntN(7) - 3)
		}
		seen := map[int32]bool{}
		for range r.IntN(50) {
			seen[int32(1+r.IntN(p.Bundle.ItemCount))] = true
		}
		var d degradation
		results, cov, err := p.retrieve(ctx, nil, uv, seen, &d)
		if err != nil {
			return err
		}
		if cov.Answered != cov.Shards || len(d.reasons) > 0 {
			return fmt.Errorf("case %d: degraded %v, coverage %+v", c, d.list(), *cov)
		}
		want := m.TopK(uv, VectorTop, func(id int32) bool { return seen[id] || isZero(m.Row(id)) })
		if got := results[srcVector]; !slices.Equal(got, want) {
			return fmt.Errorf("case %d: %d items, %d shards:\n got %v\nwant %v", c, p.Bundle.ItemCount, cov.Shards, got, want)
		}
	}
	return nil
}

func isZero(row []float32) bool {
	return !slices.ContainsFunc(row, func(x float32) bool { return x != 0 })
}

// Property 6 on the request path: the shards' merged top VectorTop equals
// single-node exact top VectorTop; ties break by internal ID ascending.
func TestProperty6PipelineShards(t *testing.T) {
	if err := checkProperty6(1, 100, false); err != nil {
		t.Fatal(err)
	}
}

// The broken variant asks each shard for ceil(VectorTop/n) items.
func TestProperty6PipelineCatchesBrokenVariant(t *testing.T) {
	if err := checkProperty6(1, 100, true); err == nil {
		t.Fatal("the check passed shards that return ceil(VectorTop/n) items each")
	}
}
