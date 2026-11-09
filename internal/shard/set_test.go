package shard

import (
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"slices"
	"testing"

	"github.com/sHyanorn/recserve/internal/vec"
)

// randomMatrix returns n rows of dim small integers after a zero padding
// row; about one row in ten is zero, and scores tie often.
func randomMatrix(r *rand.Rand, n, dim int) vec.Matrix {
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

// gather searches every shard for perShard(k) items and merges the top k.
func gather(s *Set, q []float32, k int, skip func(int32) bool, perShard func(k, n int) int) ([]vec.Hit, error) {
	var results [][]vec.Hit
	for i := range s.Shards() {
		hits, err := s.Search(context.Background(), i, q, perShard(k, s.Shards()), skip)
		if err != nil {
			return nil, err
		}
		results = append(results, hits)
	}
	return Merge(results, k), nil
}

// checkProperty6 compares scatter-gather in exact mode with single-node
// exact search over the nonzero rows, item by item; perShard is how many
// items each shard returns for a top k.
func checkProperty6(seed uint64, cases int, perShard func(k, n int) int) error {
	r := rand.New(rand.NewPCG(seed, 6))
	for c := range cases {
		n, dim := 1+r.IntN(300), 1+r.IntN(4)
		m := randomMatrix(r, n, dim)
		s, err := Build(m, Config{Shards: 1 + r.IntN(7), Mode: Exact})
		if err != nil {
			return err
		}
		q := make([]float32, dim)
		for j := range q {
			q[j] = float32(r.IntN(5) - 2)
		}
		skipped := map[int32]bool{}
		for range r.IntN(20) {
			skipped[int32(1+r.IntN(n))] = true
		}
		skip := func(id int32) bool { return skipped[id] }
		k := 1 + r.IntN(n+10)
		want := m.TopK(q, k, func(id int32) bool { return skip(id) || zero(m.Row(id)) })
		got, err := gather(s, q, k, skip, perShard)
		if err != nil {
			return err
		}
		if !slices.Equal(got, want) {
			return fmt.Errorf("case %d: %d shards, k %d:\n got %v\nwant %v", c, s.Shards(), k, got, want)
		}
	}
	return nil
}

// Property 6: scatter-gather top-k in exact mode equals single-node exact
// top-k item by item; ties break by internal ID ascending.
func TestProperty6ScatterGatherEqualsSingleNode(t *testing.T) {
	if err := checkProperty6(1, 500, func(k, _ int) int { return k }); err != nil {
		t.Fatal(err)
	}
}

// The broken variant asks each shard for its share of k only.
func TestProperty6CatchesBrokenVariant(t *testing.T) {
	if err := checkProperty6(1, 500, func(k, n int) int { return (k + n - 1) / n }); err == nil {
		t.Fatal("the check passed shards that return ceil(k/n) items each")
	}
}

func TestSearchAll(t *testing.T) {
	r := rand.New(rand.NewPCG(2, 2))
	m := randomMatrix(r, 200, 3)
	s, err := Build(m, DefaultConfig)
	if err != nil {
		t.Fatal(err)
	}
	q := []float32{1, -2, 1}
	got, err := s.SearchAll(context.Background(), q, 50, nil)
	if err != nil {
		t.Fatal(err)
	}
	if want := m.TopK(q, 50, func(id int32) bool { return zero(m.Row(id)) }); !slices.Equal(got, want) {
		t.Fatalf("got %v\nwant %v", got, want)
	}
}

func TestZeroRowsLeftOut(t *testing.T) {
	m, err := vec.NewMatrix(2, []float32{0, 0, 1, 0, 0, 0, -1, 0, 0, 0})
	if err != nil {
		t.Fatal(err)
	}
	s, err := Build(m, Config{Shards: 2, Mode: Exact})
	if err != nil {
		t.Fatal(err)
	}
	if s.Zeroes() != 2 {
		t.Fatalf("%d zero rows, want 2", s.Zeroes())
	}
	// Item 3 scores -1; items 2 and 4, at 0, would rank above it.
	got, err := s.SearchAll(context.Background(), []float32{1, 0}, 3, nil)
	if err != nil {
		t.Fatal(err)
	}
	if want := []vec.Hit{{ID: 1, Score: 1}, {ID: 3, Score: -1}}; !slices.Equal(got, want) {
		t.Fatalf("got %v, want %v", got, want)
	}
}

// An exact search whose context ends stops within checkEvery rows.
func TestSearchStopsWhenContextEnds(t *testing.T) {
	const n, at = 10000, 3000
	r := rand.New(rand.NewPCG(3, 3))
	data := make([]float32, (n+1)*2)
	for i := 2; i < len(data); i++ {
		data[i] = float32(r.NormFloat64())
	}
	m, err := vec.NewMatrix(2, data)
	if err != nil {
		t.Fatal(err)
	}
	s, err := Build(m, Config{Shards: 1, Mode: Exact})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	scanned := 0
	_, err = s.Search(ctx, 0, []float32{1, 1}, 10, func(int32) bool {
		scanned++
		if scanned == at {
			cancel()
		}
		return false
	})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("err = %v, want context.Canceled", err)
	}
	if scanned > at+checkEvery {
		t.Fatalf("scanned %d rows after the context ended at row %d", scanned-at, at)
	}
	if _, err := s.Search(ctx, 0, []float32{1, 1}, 10, nil); !errors.Is(err, context.Canceled) {
		t.Fatalf("search with an ended context: %v", err)
	}
}

// In HNSW mode on well-separated data every shard finds its exact top k,
// with the scores exact search gives.
func TestHNSWMode(t *testing.T) {
	const n, dim = 2000, 8
	r := rand.New(rand.NewPCG(4, 4))
	data := make([]float32, (n+1)*dim)
	for i := dim; i < len(data); i++ {
		data[i] = float32(r.NormFloat64())
	}
	m, err := vec.NewMatrix(dim, data)
	if err != nil {
		t.Fatal(err)
	}
	cfg := DefaultConfig
	cfg.Mode, cfg.Ef = HNSW, 200
	s, err := Build(m, cfg)
	if err != nil {
		t.Fatal(err)
	}
	q := m.Row(17)
	skip := func(id int32) bool { return id == 17 }
	got, err := s.SearchAll(context.Background(), q, 10, skip)
	if err != nil {
		t.Fatal(err)
	}
	if want := m.TopK(q, 10, skip); !slices.Equal(got, want) {
		t.Fatalf("got %v\nwant %v", got, want)
	}
}

func TestBuildRejects(t *testing.T) {
	m, err := vec.NewMatrix(1, []float32{0, 1})
	if err != nil {
		t.Fatal(err)
	}
	for _, cfg := range []Config{{Shards: 0, Mode: Exact}, {Shards: 1, Mode: "ivf"}} {
		if _, err := Build(m, cfg); err == nil {
			t.Errorf("%+v accepted", cfg)
		}
	}
}
