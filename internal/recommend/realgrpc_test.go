package recommend

import (
	"context"
	"os"
	"path/filepath"
	"slices"
	"testing"
	"time"

	"github.com/sHyanorn/recserve/internal/annshard"
	"github.com/sHyanorn/recserve/internal/annshard/annshardtest"
	"github.com/sHyanorn/recserve/internal/shard"
)

// TestRealDataShardsOverGRPC runs on the real bundle (see TestRealDataSmoke)
// with its vectors on two shards of two replicas each, which load them from
// the bundle's directory: in exact and in HNSW mode, every shard search
// through a replica returns the hits of the in-process shard, and the
// pipeline's response to each request equals the in-process pipeline's,
// item by item and score by score. It reports the latency of a shard search
// in the process and through a replica, over loopback, one search at a time.
func TestRealDataShardsOverGRPC(t *testing.T) {
	rd := loadRealData(t)
	qs := realQueries(t, rd)
	ctx := context.Background()
	// A replica finds a version in the directory of that name under its root.
	dir, err := filepath.Abs(rd.dir)
	if err != nil {
		t.Fatal(err)
	}
	root := t.TempDir()
	if err := os.Symlink(dir, filepath.Join(root, rd.bundle.ModelVersion)); err != nil {
		t.Fatal(err)
	}
	client := annshardtest.Dial(t, annshardtest.StartAll(t, root, 2, 2), annshard.Options{})

	for _, mode := range []shard.Mode{shard.Exact, shard.HNSW} {
		cfg := shard.DefaultConfig
		cfg.Mode = mode
		local, err := shard.Build(rd.bundle.Vectors, cfg)
		if err != nil {
			t.Fatal(err)
		}
		start := time.Now()
		remote, err := client.Prepare(ctx, rd.bundle.ModelVersion, rd.bundle.ItemCount, cfg)
		if err != nil {
			t.Fatal(err)
		}
		prepared := time.Since(start)
		pl, pr := New(rd.bundle, rd.store, local), New(rd.bundle, rd.store, remote)
		for i, q := range qs {
			want, err := pl.Recommend(ctx, q.req)
			if err != nil {
				t.Fatal(err)
			}
			got, err := pr.Recommend(ctx, q.req)
			if err != nil {
				t.Fatal(err)
			}
			if err := sameResponse(got, want); err != nil {
				t.Fatalf("%s, request %d: %v", mode, i, err)
			}
		}

		var inProcess, replica []time.Duration
		for i, q := range qs {
			if q.uv == nil {
				continue
			}
			for s := range cfg.Shards {
				start := time.Now()
				want, err := local.Search(ctx, s, q.uv, VectorTop, q.seen)
				inProcess = append(inProcess, time.Since(start))
				if err != nil {
					t.Fatal(err)
				}
				start = time.Now()
				got, err := remote.Search(ctx, s, q.uv, VectorTop, q.seen)
				replica = append(replica, time.Since(start))
				if err != nil {
					t.Fatal(err)
				}
				if !slices.Equal(got, want) {
					t.Fatalf("%s, request %d, shard %d: the replica's hits differ from the in-process shard's", mode, i, s)
				}
			}
		}
		t.Logf("%s (replicas prepared in %v): %d responses and %d shard searches equal; shard search in process %s; through a replica %s",
			mode, prepared.Round(time.Millisecond), len(qs), len(replica), summary(inProcess), summary(replica))
	}
}
