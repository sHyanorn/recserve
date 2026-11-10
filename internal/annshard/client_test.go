package annshard_test

import (
	"context"
	"errors"
	"math/rand/v2"
	"slices"
	"testing"
	"time"

	"github.com/sHyanorn/recserve/internal/annshard"
	"github.com/sHyanorn/recserve/internal/annshard/annshardtest"
	"github.com/sHyanorn/recserve/internal/hnsw"
	"github.com/sHyanorn/recserve/internal/infratest"
	"github.com/sHyanorn/recserve/internal/shard"
	"github.com/sHyanorn/recserve/internal/shard/shardtest"
	"github.com/sHyanorn/recserve/internal/vec"
)

// remote returns a build function for shardtest that puts each catalog
// on replicas.
func remote(t *testing.T) func(m vec.Matrix, cfg shard.Config) (shardtest.Searcher, error) {
	build := annshardtest.Remote(t, 7)
	return func(m vec.Matrix, cfg shard.Config) (shardtest.Searcher, error) { return build(m, cfg) }
}

// Property 6 over gRPC: the replicas' merged top k equals single-node exact
// top k item by item.
func TestProperty6OverGRPC(t *testing.T) {
	if err := shardtest.CheckProperty6(1, 200, shardtest.All, remote(t)); err != nil {
		t.Fatal(err)
	}
}

func TestProperty6OverGRPCCatchesBrokenVariant(t *testing.T) {
	if err := shardtest.CheckProperty6(1, 200, shardtest.Share, remote(t)); err == nil {
		t.Fatal("the check passed shards that return ceil(k/n) items each")
	}
}

// In HNSW mode the replicas build the graphs the gateway would, and a
// search through the client equals the in-process one hit by hit, with the
// set's search list size.
func TestHNSWEqualsInProcess(t *testing.T) {
	r := rand.New(rand.NewPCG(2, 2))
	const n, dim = 600, 3
	m := shardtest.RandomMatrix(r, n, dim)
	cfg := shard.Config{Shards: 3, Mode: shard.HNSW, HNSW: hnsw.Config{M: 8, EfConstruction: 50, Seed: 7}, Ef: 40}
	local, err := shard.Build(m, cfg)
	if err != nil {
		t.Fatal(err)
	}
	s, err := remote(t)(m, cfg)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	for c := range 60 {
		q := []float32{float32(r.IntN(5) - 2), float32(r.IntN(5) - 2), float32(r.IntN(5) - 2)}
		k := 1 + r.IntN(100)
		seen := map[int32]bool{}
		for range r.IntN(40) {
			seen[int32(1+r.IntN(n))] = true
		}
		for i := range cfg.Shards {
			want, err := local.Search(ctx, i, q, k, seen)
			if err != nil {
				t.Fatal(err)
			}
			got, err := s.Search(ctx, i, q, k, seen)
			if err != nil {
				t.Fatal(err)
			}
			if !slices.Equal(got, want) {
				t.Fatalf("case %d, shard %d, k %d:\n got %v\nwant %v", c, i, k, got, want)
			}
		}
	}
}

// A search that its context ends returns the context's error, as an
// in-process search does; a replica that is gone is another error.
func TestSearchErrors(t *testing.T) {
	r := rand.New(rand.NewPCG(3, 3))
	root := t.TempDir()
	annshardtest.WriteVectors(t, root, "v1", shardtest.RandomMatrix(r, 50, 2))
	proxy := infratest.NewProxy(t, annshardtest.Start(t, root))
	c := annshardtest.Dial(t, [][]string{{proxy.Addr()}}, annshard.Options{})
	s, err := c.Prepare(context.Background(), "v1", 50, shard.Config{Shards: 1, Mode: shard.Exact})
	if err != nil {
		t.Fatal(err)
	}
	search := func(ctx context.Context) error {
		_, err := s.Search(ctx, 0, []float32{1, 1}, 5, nil)
		return err
	}
	if err := search(context.Background()); err != nil {
		t.Fatal(err)
	}

	proxy.Blackhole()
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	start := time.Now()
	if err := search(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("silent replica: err = %v, want context.DeadlineExceeded", err)
	}
	if d := time.Since(start); d > 5*time.Second {
		t.Fatalf("a search with 50 ms returned after %v", d)
	}
	ctx, cancel = context.WithCancel(context.Background())
	time.AfterFunc(20*time.Millisecond, cancel)
	if err := search(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled search: err = %v, want context.Canceled", err)
	}

	proxy.Refuse()
	ctx, cancel = context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := search(ctx); err == nil || errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) {
		t.Fatalf("replica gone: err = %v, want an error that is not the context's", err)
	}
}

func TestPrepareErrors(t *testing.T) {
	r := rand.New(rand.NewPCG(4, 4))
	m := shardtest.RandomMatrix(r, 50, 2)
	rootA, rootB := t.TempDir(), t.TempDir()
	annshardtest.WriteVectors(t, rootA, "v1", m)
	annshardtest.WriteVectors(t, rootA, "v2", m)
	annshardtest.WriteVectors(t, rootB, "v1", m)
	c := annshardtest.Dial(t, [][]string{{annshardtest.Start(t, rootA)}, {annshardtest.Start(t, rootB)}}, annshard.Options{})
	ctx := context.Background()
	cfg := shard.Config{Shards: 2, Mode: shard.Exact}
	if _, err := c.Prepare(ctx, "v1", 50, cfg); err != nil {
		t.Fatal(err)
	}
	// Shard 1's replica lacks v2.
	if _, err := c.Prepare(ctx, "v2", 50, cfg); err == nil {
		t.Fatal("a version one shard lacks was prepared")
	}
	for _, bad := range []shard.Config{{Shards: 3, Mode: shard.Exact}, {Shards: 2, Mode: "ivf"}} {
		if _, err := c.Prepare(ctx, "v1", 50, bad); err == nil {
			t.Errorf("%+v accepted", bad)
		}
	}
}

// No shard may be left without a replica that holds the version.
func TestPrepareNeedsAReplicaPerShard(t *testing.T) {
	r := rand.New(rand.NewPCG(8, 8))
	m := shardtest.RandomMatrix(r, 100, 2)
	full, empty := t.TempDir(), t.TempDir()
	annshardtest.WriteVectors(t, full, "v1", m)
	has, lacks := func() string { return annshardtest.Start(t, full) }, func() string { return annshardtest.Start(t, empty) }
	cfg := shard.Config{Shards: 2, Mode: shard.Exact}
	ctx := context.Background()
	if _, err := annshardtest.Dial(t, [][]string{{has(), lacks()}, {lacks(), has()}}, annshard.Options{}).Prepare(ctx, "v1", 100, cfg); err != nil {
		t.Fatal(err)
	}
	if _, err := annshardtest.Dial(t, [][]string{{has(), has()}, {lacks(), lacks()}}, annshard.Options{}).Prepare(ctx, "v1", 100, cfg); err == nil {
		t.Fatal("prepared with a shard none of whose replicas holds the version")
	}
}

func TestParseAddrs(t *testing.T) {
	got, err := annshard.ParseAddrs("a:1,b:1;c:1")
	if want := [][]string{{"a:1", "b:1"}, {"c:1"}}; err != nil || !slices.EqualFunc(got, want, slices.Equal) {
		t.Fatalf("got %v, %v", got, err)
	}
	for _, bad := range []string{"", "a:1;", "a:1,,b:1", ";a:1"} {
		if _, err := annshard.ParseAddrs(bad); err == nil {
			t.Errorf("%q accepted", bad)
		}
	}
}
