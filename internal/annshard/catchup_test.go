package annshard_test

import (
	"context"
	"math/rand/v2"
	"slices"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/sHyanorn/recserve/internal/annshard"
	"github.com/sHyanorn/recserve/internal/annshard/annshardpb"
	"github.com/sHyanorn/recserve/internal/annshard/annshardtest"
	"github.com/sHyanorn/recserve/internal/shard"
	"github.com/sHyanorn/recserve/internal/shard/shardtest"
	"github.com/sHyanorn/recserve/internal/vec"
)

// holds reports whether the replica at addr answers a search of shard 0 of
// one, for version, in exact mode.
func holds(t *testing.T, addr, version string) bool {
	t.Helper()
	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close() //nolint:errcheck // a probe
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	_, err = annshardpb.NewAnnShardClient(conn).Search(ctx, &annshardpb.SearchRequest{Index: exactIndex(version, 0, 1), Query: []float32{1, 1}, K: 1})
	return err == nil
}

// searchUntil searches through s, requiring every answer to equal the
// in-process one, until done returns true; it fails after 30 s.
func searchUntil(t *testing.T, s *annshard.Shards, m vec.Matrix, done func() bool) {
	t.Helper()
	local, err := shard.Build(m, s.Config())
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	for deadline := time.Now().Add(30 * time.Second); ; time.Sleep(5 * time.Millisecond) {
		q := []float32{1, float32(time.Now().UnixNano() % 7)}
		got, err := s.Search(ctx, 0, q, 10, nil)
		if err != nil {
			t.Fatal(err)
		}
		want, err := local.Search(ctx, 0, q, 10, nil)
		if err != nil {
			t.Fatal(err)
		}
		if !slices.Equal(got, want) {
			t.Fatalf("got %v\nwant %v", got, want)
		}
		if done() {
			return
		}
		if time.Now().After(deadline) {
			t.Fatal("still waiting after 30 s")
		}
	}
}

// A version is prepared once every shard has a replica that holds it. A
// replica that could not prepare it costs no search, and holds it soon
// after it can: the first search that finds it lacking has it prepare.
func TestReplicaCatchesUp(t *testing.T) {
	r := rand.New(rand.NewPCG(6, 6))
	m := shardtest.RandomMatrix(r, 100, 2)
	full, late := t.TempDir(), t.TempDir()
	v1 := annshardtest.WriteVectors(t, full, m)
	behind := annshardtest.Start(t, late)
	c := annshardtest.Dial(t, [][]string{{behind, annshardtest.Start(t, full)}}, annshard.Options{})
	s, err := c.Prepare(context.Background(), v1, 100, shard.Config{Shards: 1, Mode: shard.Exact})
	if err != nil {
		t.Fatal(err)
	}
	if holds(t, behind, v1) {
		t.Fatal("the replica without the bundle holds its index")
	}
	annshardtest.WriteVectors(t, late, m)
	searchUntil(t, s, m, func() bool { return holds(t, behind, v1) })
}

// The client tells a replica which version the gateway serves, when it
// prepares a version and when it has a replica catch up, and the replica
// keeps that version although another was named later (D043).
func TestReplicasKeepTheServingVersion(t *testing.T) {
	r := rand.New(rand.NewPCG(17, 17))
	matrices := make([]vec.Matrix, 4)
	for i := range matrices {
		matrices[i] = shardtest.RandomMatrix(r, 100, 2)
	}
	rootA, rootB := t.TempDir(), t.TempDir()
	v1, v2 := annshardtest.WriteVectors(t, rootB, matrices[0]), annshardtest.WriteVectors(t, rootB, matrices[1])
	v3, v4 := annshardtest.WriteVectors(t, rootA, matrices[2]), annshardtest.WriteVectors(t, rootA, matrices[3])
	annshardtest.WriteVectors(t, rootA, matrices[0])
	a := annshardtest.Start(t, rootA)
	c := annshardtest.Dial(t, [][]string{{a, annshardtest.Start(t, rootB)}}, annshard.Options{Serving: func() string { return v1 }})
	ctx := context.Background()
	cfg := shard.Config{Shards: 1, Mode: shard.Exact}
	var s2 *annshard.Shards
	for _, v := range []string{v1, v3, v2} { // a holds v1 and v3, v3 named last
		s, err := c.Prepare(ctx, v, 100, cfg)
		if err != nil {
			t.Fatal(err)
		}
		s2 = s
	}
	// Catching up on v2, a has three versions and drops v3, not v1.
	annshardtest.WriteVectors(t, rootA, matrices[1])
	searchUntil(t, s2, matrices[1], func() bool { return holds(t, a, v2) })
	if holds(t, a, v3) || !holds(t, a, v1) {
		t.Fatal("catching up, the replica dropped the version served")
	}
	// Preparing v4, it drops v2, which the probe below names last, not v1.
	if !holds(t, a, v2) {
		t.Fatal("the replica lost v2")
	}
	if _, err := c.Prepare(ctx, v4, 100, cfg); err != nil {
		t.Fatal(err)
	}
	if holds(t, a, v2) || !holds(t, a, v1) {
		t.Fatal("preparing a version, the replica dropped the version served")
	}
}

// A replica that restarts has no index. The searches go on, answered by
// the other replica, and the restarted one is prepared again by the first
// search that reaches it.
func TestReplicaRestartsEmpty(t *testing.T) {
	r := rand.New(rand.NewPCG(7, 7))
	m := shardtest.RandomMatrix(r, 100, 2)
	root := t.TempDir()
	v1 := annshardtest.WriteVectors(t, root, m)
	addr, stop := annshardtest.StartOn(t, "127.0.0.1:0", root)
	c := annshardtest.Dial(t, [][]string{{addr, annshardtest.Start(t, root)}}, annshard.Options{})
	s, err := c.Prepare(context.Background(), v1, 100, shard.Config{Shards: 1, Mode: shard.Exact})
	if err != nil {
		t.Fatal(err)
	}
	if !holds(t, addr, v1) {
		t.Fatal("the replica does not hold the index it prepared")
	}
	stop()
	down := true
	searchUntil(t, s, m, func() bool {
		if down { // a few searches with the replica gone, then it is back, empty
			annshardtest.StartOn(t, addr, root)
			down = false
			return false
		}
		return holds(t, addr, v1)
	})
}
