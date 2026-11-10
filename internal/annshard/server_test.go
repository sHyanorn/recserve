package annshard_test

import (
	"context"
	"math/rand/v2"
	"os"
	"path/filepath"
	"slices"
	"sync"
	"testing"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"

	"github.com/sHyanorn/recserve/internal/annshard/annshardpb"
	"github.com/sHyanorn/recserve/internal/annshard/annshardtest"
	"github.com/sHyanorn/recserve/internal/hnsw"
	"github.com/sHyanorn/recserve/internal/shard"
	"github.com/sHyanorn/recserve/internal/shard/shardtest"
	"github.com/sHyanorn/recserve/internal/vec"
)

// replica starts a replica over root and returns a client of it.
func replica(t *testing.T, root string) annshardpb.AnnShardClient {
	t.Helper()
	conn, err := grpc.NewClient(annshardtest.Start(t, root), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { conn.Close() }) //nolint:errcheck // the test is over
	return annshardpb.NewAnnShardClient(conn)
}

func exactIndex(version string, i, n int) *annshardpb.Index {
	return &annshardpb.Index{Version: version, Shard: int32(i), Shards: int32(n), Mode: annshardpb.Mode_MODE_EXACT}
}

func wantCode(t *testing.T, err error, want codes.Code) {
	t.Helper()
	if status.Code(err) != want {
		t.Fatalf("err = %v, want code %v", err, want)
	}
}

// A replica's answer equals the search of the same shard built in this
// process, item by item and score by score, in both modes.
func TestSearchEqualsInProcess(t *testing.T) {
	r := rand.New(rand.NewPCG(1, 1))
	const n, dim, shards = 400, 3, 3
	m := shardtest.RandomMatrix(r, n, dim)
	root := t.TempDir()
	annshardtest.WriteVectors(t, root, "v1", m)
	c := replica(t, root)
	ctx := context.Background()
	for _, cfg := range []shard.Config{
		{Shards: shards, Mode: shard.Exact},
		{Shards: shards, Mode: shard.HNSW, HNSW: hnsw.Config{M: 8, EfConstruction: 50, Seed: 7}, Ef: 60},
	} {
		for i := range shards {
			ix := exactIndex("v1", i, shards)
			if cfg.Mode == shard.HNSW {
				ix.Mode, ix.HnswM, ix.HnswEfConstruction, ix.HnswSeed = annshardpb.Mode_MODE_HNSW, 8, 50, 7
			}
			if _, err := c.Prepare(ctx, &annshardpb.PrepareRequest{Index: ix}); err != nil {
				t.Fatal(err)
			}
			local, err := shard.BuildPart(m, cfg, i)
			if err != nil {
				t.Fatal(err)
			}
			for range 30 {
				q := []float32{float32(r.IntN(5) - 2), float32(r.IntN(5) - 2), float32(r.IntN(5) - 2)}
				k := 1 + r.IntN(n)
				seen := map[int32]bool{}
				var exclude []int32
				for range r.IntN(30) {
					id := int32(1 + r.IntN(n))
					seen[id] = true
					exclude = append(exclude, id)
				}
				want, err := local.Search(ctx, q, k, cfg.Ef, seen)
				if err != nil {
					t.Fatal(err)
				}
				got, err := c.Search(ctx, &annshardpb.SearchRequest{Index: ix, Query: q, K: int32(k), Exclude: exclude, Ef: int32(cfg.Ef)})
				if err != nil {
					t.Fatal(err)
				}
				hits := make([]vec.Hit, len(got.GetIds()))
				for j, id := range got.GetIds() {
					hits[j] = vec.Hit{ID: id, Score: got.GetScores()[j]}
				}
				if len(got.GetScores()) != len(hits) || !slices.Equal(hits, want) {
					t.Fatalf("%s shard %d, k %d:\n got %v\nwant %v", cfg.Mode, i, k, hits, want)
				}
			}
		}
	}
}

// A replica answers only from the index a request names: not before it is
// prepared, and not from another version, shard or split it holds.
func TestSearchNeedsTheNamedIndex(t *testing.T) {
	r := rand.New(rand.NewPCG(2, 2))
	root := t.TempDir()
	annshardtest.WriteVectors(t, root, "v1", shardtest.RandomMatrix(r, 50, 2))
	annshardtest.WriteVectors(t, root, "v2", shardtest.RandomMatrix(r, 50, 2))
	c := replica(t, root)
	ctx := context.Background()
	search := func(ix *annshardpb.Index) error {
		_, err := c.Search(ctx, &annshardpb.SearchRequest{Index: ix, Query: []float32{1, 1}, K: 5})
		return err
	}
	held := exactIndex("v1", 0, 2)
	wantCode(t, search(held), codes.FailedPrecondition)
	if _, err := c.Prepare(ctx, &annshardpb.PrepareRequest{Index: held}); err != nil {
		t.Fatal(err)
	}
	if err := search(held); err != nil {
		t.Fatal(err)
	}
	hnsw := exactIndex("v1", 0, 2)
	hnsw.Mode, hnsw.HnswM, hnsw.HnswEfConstruction = annshardpb.Mode_MODE_HNSW, 8, 50
	for _, other := range []*annshardpb.Index{exactIndex("v2", 0, 2), exactIndex("v1", 1, 2), exactIndex("v1", 0, 3), hnsw} {
		wantCode(t, search(other), codes.FailedPrecondition)
	}
}

func TestPrepare(t *testing.T) {
	r := rand.New(rand.NewPCG(3, 3))
	root := t.TempDir()
	m := shardtest.RandomMatrix(r, 50, 2)
	annshardtest.WriteVectors(t, root, "v1", m)
	c := replica(t, root)
	ctx := context.Background()
	prepare := func(ix *annshardpb.Index) error {
		_, err := c.Prepare(ctx, &annshardpb.PrepareRequest{Index: ix})
		return err
	}

	// Concurrent and repeated requests for one index all succeed.
	errs := make([]error, 8)
	var wg sync.WaitGroup
	for i := range errs {
		wg.Go(func() { errs[i] = prepare(exactIndex("v1", 0, 1)) })
	}
	wg.Wait()
	for _, err := range append(errs, prepare(exactIndex("v1", 0, 1))) {
		if err != nil {
			t.Fatal(err)
		}
	}

	wantCode(t, prepare(exactIndex("v9", 0, 1)), codes.NotFound)

	// A directory that holds another version than its name says.
	annshardtest.WriteVectors(t, root, "v2", m)
	if err := os.Rename(filepath.Join(root, "v2"), filepath.Join(root, "v3")); err != nil {
		t.Fatal(err)
	}
	wantCode(t, prepare(exactIndex("v3", 0, 1)), codes.FailedPrecondition)

	// Vectors that are not the ones the manifest lists.
	annshardtest.WriteVectors(t, root, "v4", m)
	if err := os.WriteFile(filepath.Join(root, "v4", "items.npy"), []byte("tampered"), 0o644); err != nil {
		t.Fatal(err)
	}
	wantCode(t, prepare(exactIndex("v4", 0, 1)), codes.FailedPrecondition)

	// A failed build is tried again by the next request.
	annshardtest.WriteVectors(t, root, "v4", m)
	if err := prepare(exactIndex("v4", 0, 1)); err != nil {
		t.Fatal(err)
	}
}

func TestInvalidRequests(t *testing.T) {
	r := rand.New(rand.NewPCG(4, 4))
	root := t.TempDir()
	annshardtest.WriteVectors(t, root, "v1", shardtest.RandomMatrix(r, 50, 2))
	// A version must not lead out of the bundle root, here to a bundle
	// beside it in the test's temporary directory.
	annshardtest.WriteVectors(t, filepath.Dir(root), "outside", shardtest.RandomMatrix(r, 50, 2))
	c := replica(t, root)
	ctx := context.Background()
	edit := func(f func(*annshardpb.Index)) *annshardpb.Index {
		ix := exactIndex("v1", 0, 2)
		f(ix)
		return ix
	}
	for name, ix := range map[string]*annshardpb.Index{
		"no index":        nil,
		"no version":      edit(func(ix *annshardpb.Index) { ix.Version = "" }),
		"parent":          edit(func(ix *annshardpb.Index) { ix.Version = ".." }),
		"path":            edit(func(ix *annshardpb.Index) { ix.Version = "../outside" }),
		"shard too high":  edit(func(ix *annshardpb.Index) { ix.Shard = 2 }),
		"negative shard":  edit(func(ix *annshardpb.Index) { ix.Shard = -1 }),
		"no shards":       edit(func(ix *annshardpb.Index) { ix.Shards = 0 }),
		"no mode":         edit(func(ix *annshardpb.Index) { ix.Mode = annshardpb.Mode_MODE_UNSPECIFIED }),
		"HNSW parameters": edit(func(ix *annshardpb.Index) { ix.Mode, ix.HnswM = annshardpb.Mode_MODE_HNSW, 1 }),
	} {
		_, err := c.Prepare(ctx, &annshardpb.PrepareRequest{Index: ix})
		if status.Code(err) != codes.InvalidArgument {
			t.Errorf("prepare, %s: err = %v", name, err)
		}
	}
	ix := exactIndex("v1", 0, 2)
	if _, err := c.Prepare(ctx, &annshardpb.PrepareRequest{Index: ix}); err != nil {
		t.Fatal(err)
	}
	for name, req := range map[string]*annshardpb.SearchRequest{
		"no index":        {Query: []float32{1, 1}, K: 5},
		"query too short": {Index: ix, Query: []float32{1}, K: 5},
		"query too long":  {Index: ix, Query: []float32{1, 1, 1}, K: 5},
		"k 0":             {Index: ix, Query: []float32{1, 1}},
		"negative k":      {Index: ix, Query: []float32{1, 1}, K: -1},
	} {
		_, err := c.Search(ctx, req)
		if status.Code(err) != codes.InvalidArgument {
			t.Errorf("search, %s: err = %v", name, err)
		}
	}
	// k beyond the shard's items returns them all.
	got, err := c.Search(ctx, &annshardpb.SearchRequest{Index: ix, Query: []float32{1, 1}, K: 1 << 30})
	if err != nil || len(got.GetIds()) == 0 || len(got.GetIds()) > 50 {
		t.Fatalf("k = 2^30: %d items, %v", len(got.GetIds()), err)
	}
}
