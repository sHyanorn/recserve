package shard_test

import (
	"context"
	"errors"
	"math/rand/v2"
	"slices"
	"testing"

	"github.com/sHyanorn/recserve/internal/shard"
	"github.com/sHyanorn/recserve/internal/shard/shardtest"
	"github.com/sHyanorn/recserve/internal/vec"
)

// checkEvery is how many rows an exact search scans between checks of its
// context (D028).
const checkEvery = 1024

func isZero(row []float32) bool {
	return !slices.ContainsFunc(row, func(x float32) bool { return x != 0 })
}

func local(m vec.Matrix, cfg shard.Config) (shardtest.Searcher, error) { return shard.Build(m, cfg) }

// Property 6: scatter-gather top-k in exact mode equals single-node exact
// top-k item by item; ties break by internal ID ascending.
func TestProperty6ScatterGatherEqualsSingleNode(t *testing.T) {
	if err := shardtest.CheckProperty6(1, 500, shardtest.All, local); err != nil {
		t.Fatal(err)
	}
}

// The broken variant asks each shard for its share of k only.
func TestProperty6CatchesBrokenVariant(t *testing.T) {
	if err := shardtest.CheckProperty6(1, 500, shardtest.Share, local); err == nil {
		t.Fatal("the check passed shards that return ceil(k/n) items each")
	}
}

func TestSearchAll(t *testing.T) {
	r := rand.New(rand.NewPCG(2, 2))
	m := shardtest.RandomMatrix(r, 200, 3)
	s, err := shard.Build(m, shard.DefaultConfig)
	if err != nil {
		t.Fatal(err)
	}
	q := []float32{1, -2, 1}
	got, err := s.SearchAll(context.Background(), q, 50, nil)
	if err != nil {
		t.Fatal(err)
	}
	if want := m.TopK(q, 50, func(id int32) bool { return isZero(m.Row(id)) }); !slices.Equal(got, want) {
		t.Fatalf("got %v\nwant %v", got, want)
	}
}

func TestZeroRowsLeftOut(t *testing.T) {
	m, err := vec.NewMatrix(2, []float32{0, 0, 1, 0, 0, 0, -1, 0, 0, 0})
	if err != nil {
		t.Fatal(err)
	}
	s, err := shard.Build(m, shard.Config{Shards: 2, Mode: shard.Exact})
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

// polled is a context that counts the checks of its state and has ended,
// cancelled, from check number end on.
type polled struct {
	context.Context
	checks, end int
}

func (c *polled) Err() error {
	c.checks++
	if c.checks >= c.end {
		return context.Canceled
	}
	return nil
}

// An exact search checks its context at least every checkEvery rows and
// stops at the first check that finds it ended.
func TestSearchStopsWhenContextEnds(t *testing.T) {
	const n = 10000
	r := rand.New(rand.NewPCG(3, 3))
	data := make([]float32, (n+1)*2)
	for i := 2; i < len(data); i++ {
		data[i] = float32(r.NormFloat64())
	}
	m, err := vec.NewMatrix(2, data)
	if err != nil {
		t.Fatal(err)
	}
	s, err := shard.Build(m, shard.Config{Shards: 1, Mode: shard.Exact})
	if err != nil {
		t.Fatal(err)
	}
	open := &polled{Context: context.Background(), end: n}
	if _, err := s.Search(open, 0, []float32{1, 1}, 10, nil); err != nil {
		t.Fatal(err)
	}
	if open.checks < n/checkEvery {
		t.Fatalf("%d checks of the context over %d rows, want one every %d", open.checks, n, checkEvery)
	}
	ends := &polled{Context: context.Background(), end: 4}
	if _, err := s.Search(ends, 0, []float32{1, 1}, 10, nil); !errors.Is(err, context.Canceled) {
		t.Fatalf("err = %v, want context.Canceled", err)
	}
	if ends.checks != ends.end {
		t.Fatalf("%d checks of a context that ended at check %d", ends.checks, ends.end)
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
	cfg := shard.DefaultConfig
	cfg.Mode, cfg.Ef = shard.HNSW, 200
	s, err := shard.Build(m, cfg)
	if err != nil {
		t.Fatal(err)
	}
	q := m.Row(17)
	got, err := s.SearchAll(context.Background(), q, 10, map[int32]bool{17: true})
	if err != nil {
		t.Fatal(err)
	}
	if want := m.TopK(q, 10, func(id int32) bool { return id == 17 }); !slices.Equal(got, want) {
		t.Fatalf("got %v\nwant %v", got, want)
	}
}

func TestBuildRejects(t *testing.T) {
	m, err := vec.NewMatrix(1, []float32{0, 1})
	if err != nil {
		t.Fatal(err)
	}
	for _, cfg := range []shard.Config{{Shards: 0, Mode: shard.Exact}, {Shards: 1, Mode: "ivf"}} {
		if _, err := shard.Build(m, cfg); err == nil {
			t.Errorf("%+v accepted", cfg)
		}
	}
	for _, i := range []int{-1, 2} {
		if _, err := shard.BuildPart(m, shard.Config{Shards: 2, Mode: shard.Exact}, i); err == nil {
			t.Errorf("shard %d of 2 accepted", i)
		}
	}
}
