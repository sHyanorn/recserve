package recommend

import (
	"context"
	"fmt"
	"math/rand/v2"
	"slices"
	"testing"

	"github.com/sHyanorn/recserve/internal/shard"
	"github.com/sHyanorn/recserve/internal/vec"
)

// checkProperty6 runs the pipeline's vector retrieval in exact mode over
// catalogs larger than VectorTop and compares it with single-node exact
// search over the nonzero, unseen rows, item by item. build returns the
// shards of a catalog; broken makes each shard return only its share of
// VectorTop.
func checkProperty6(seed uint64, cases int, broken bool, build func(m vec.Matrix, cfg shard.Config) (Shards, error)) error {
	r := rand.New(rand.NewPCG(seed, 6))
	ctx := context.Background()
	for c := range cases {
		p, err := randomPipeline(r, VectorTop+1+r.IntN(3*VectorTop), 0)
		if err != nil {
			return err
		}
		m := p.Bundle.Vectors
		if p.Shards, err = build(m, shard.Config{Shards: 1 + r.IntN(7), Mode: shard.Exact}); err != nil {
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

func localShards(m vec.Matrix, cfg shard.Config) (Shards, error) { return shard.Build(m, cfg) }

func isZero(row []float32) bool {
	return !slices.ContainsFunc(row, func(x float32) bool { return x != 0 })
}

// Property 6 on the request path: the shards' merged top VectorTop equals
// single-node exact top VectorTop; ties break by internal ID ascending.
func TestProperty6PipelineShards(t *testing.T) {
	if err := checkProperty6(1, 100, false, localShards); err != nil {
		t.Fatal(err)
	}
}

// The broken variant asks each shard for ceil(VectorTop/n) items.
func TestProperty6PipelineCatchesBrokenVariant(t *testing.T) {
	if err := checkProperty6(1, 100, true, localShards); err == nil {
		t.Fatal("the check passed shards that return ceil(VectorTop/n) items each")
	}
}

// strayShards answers shard 0's searches with one hit more, for an ID the
// bundle does not hold: what a replica returns that was given another
// bundle under the version's name.
type strayShards struct {
	Shards
	id int32
}

func (s strayShards) Search(ctx context.Context, i int, q []float32, k int, seen map[int32]bool) ([]vec.Hit, error) {
	hits, err := s.Shards.Search(ctx, i, q, k, seen)
	if i == 0 && err == nil {
		hits = append([]vec.Hit{{ID: s.id, Score: 1e9}}, hits...)
	}
	return hits, err
}

// A shard that returns an item outside the bundle counts as one that
// failed: the response is the one without that shard, and the error is kept.
func TestShardReturningUnknownItemIsSkipped(t *testing.T) {
	r := rand.New(rand.NewPCG(14, 14))
	local, err := randomPipeline(r, 300, 3)
	if err != nil {
		t.Fatal(err)
	}
	skipped := &fault{faults: []stageFault{{stageVector, 0, false}}, reason: ShardsPartial}
	for _, id := range []int32{-1, 0, int32(local.Bundle.ItemCount) + 1} {
		p := *local
		p.Shards = strayShards{local.Shards, id}
		searched := 0
		for range 20 {
			req := Request{User: fmt.Sprint("u", r.IntN(3)), K: 1 + r.IntN(MaxK), TS: int64(r.IntN(110))}
			got, err := p.Recommend(context.Background(), req)
			if err != nil {
				t.Fatal(err)
			}
			if got.Coverage == nil {
				continue
			}
			searched++
			if err := sameResponse(got, want(t, local, skipped, req)); err != nil {
				t.Fatalf("item %d, %+v: %v", id, req, err)
			}
			if len(got.Failures) != 1 || got.Failures[0].Reason != ShardsPartial {
				t.Fatalf("item %d: failures %v, want the shard's", id, got.Failures)
			}
		}
		if searched == 0 {
			t.Fatal("no request had a user vector")
		}
	}
}
