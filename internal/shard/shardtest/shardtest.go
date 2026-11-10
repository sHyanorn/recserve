// Package shardtest holds the checks that every way of searching the
// shards must pass: in this process and through annshard replicas.
package shardtest

import (
	"context"
	"fmt"
	"math/rand/v2"
	"slices"

	"github.com/sHyanorn/recserve/internal/shard"
	"github.com/sHyanorn/recserve/internal/vec"
)

// Searcher searches one shard of a set, as shard.Set does.
type Searcher interface {
	Search(ctx context.Context, i int, q []float32, k int, seen map[int32]bool) ([]vec.Hit, error)
}

// RandomMatrix returns n rows of dim small integers after a zero padding
// row; about one row in ten is zero, and scores tie often.
func RandomMatrix(r *rand.Rand, n, dim int) vec.Matrix {
	data := make([]float32, (n+1)*dim)
	for i := 1; i <= n; i++ {
		if r.IntN(10) == 0 {
			continue
		}
		for j := range dim {
			data[i*dim+j] = float32(r.IntN(5) - 2)
		}
	}
	m, err := vec.NewMatrix(dim, data)
	if err != nil {
		panic(err)
	}
	return m
}

// CheckProperty6 compares scatter-gather in exact mode with single-node
// exact search over the nonzero rows, item by item, on random catalogs,
// shard counts, queries, seen sets and k. build returns the shards of m
// split by cfg; perShard is how many items each shard is asked for a top k.
func CheckProperty6(seed uint64, cases int, perShard func(k, n int) int, build func(m vec.Matrix, cfg shard.Config) (Searcher, error)) error {
	r := rand.New(rand.NewPCG(seed, 6))
	for c := range cases {
		n, dim := 1+r.IntN(300), 1+r.IntN(4)
		m := RandomMatrix(r, n, dim)
		cfg := shard.Config{Shards: 1 + r.IntN(7), Mode: shard.Exact}
		s, err := build(m, cfg)
		if err != nil {
			return err
		}
		q := make([]float32, dim)
		for j := range q {
			q[j] = float32(r.IntN(5) - 2)
		}
		seen := map[int32]bool{}
		for range r.IntN(20) {
			seen[int32(1+r.IntN(n))] = true
		}
		k := 1 + r.IntN(n+10)
		want := m.TopK(q, k, func(id int32) bool {
			return seen[id] || !slices.ContainsFunc(m.Row(id), func(x float32) bool { return x != 0 })
		})
		var results [][]vec.Hit
		for i := range cfg.Shards {
			hits, err := s.Search(context.Background(), i, q, perShard(k, cfg.Shards), seen)
			if err != nil {
				return fmt.Errorf("case %d: shard %d: %w", c, i, err)
			}
			results = append(results, hits)
		}
		if got := shard.Merge(results, k); !slices.Equal(got, want) {
			return fmt.Errorf("case %d: %d shards, k %d:\n got %v\nwant %v", c, cfg.Shards, k, got, want)
		}
	}
	return nil
}

// All asks each shard for the whole top k; Share, the broken variant of
// property 6, for ceil(k/n) items only.
func All(k, _ int) int   { return k }
func Share(k, n int) int { return (k + n - 1) / n }
