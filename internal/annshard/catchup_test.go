package annshard_test

import (
	"context"
	"math/rand/v2"
	"os"
	"path/filepath"
	"slices"
	"sync/atomic"
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
	c := annshardtest.Dial(t, [][]string{{behind, annshardtest.Start(t, full)}}, annshard.Options{Config: shard.Config{Shards: 1, Mode: shard.Exact}})
	s, err := c.Prepare(context.Background(), v1, 100)
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
	cfg := shard.Config{Shards: 1, Mode: shard.Exact}
	c := annshardtest.Dial(t, [][]string{{a, annshardtest.Start(t, rootB)}}, annshard.Options{Config: cfg, Serving: func() (string, time.Time) { return v1, time.Time{} }})
	ctx := context.Background()
	var s2 *annshard.Shards
	for _, v := range []string{v1, v3, v2} { // a holds v1 and v3, v3 named last
		s, err := c.Prepare(ctx, v, 100)
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
	if _, err := c.Prepare(ctx, v4, 100); err != nil {
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
	c := annshardtest.Dial(t, [][]string{{addr, annshardtest.Start(t, root)}}, annshard.Options{Config: shard.Config{Shards: 1, Mode: shard.Exact}})
	s, err := c.Prepare(context.Background(), v1, 100)
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

// waitReady fails the test unless the replica at addr is ready within 20 s,
// time for a build and a few attempts to reconnect, which gRPC spaces from
// 1 s up to 5 s apart.
func waitReady(t *testing.T, addr string) {
	t.Helper()
	for deadline := time.Now().Add(20 * time.Second); !annshardtest.Ready(t, addr); time.Sleep(10 * time.Millisecond) {
		if time.Now().After(deadline) {
			t.Fatalf("replica %s not ready after 20 s", addr)
		}
	}
}

// A replica that was down when a version was prepared, or that restarts,
// is prepared with the version served as soon as the client's connection
// to it is up, with no search to find it lacking (D044).
func TestReplicaCatchesUpOnConnecting(t *testing.T) {
	r := rand.New(rand.NewPCG(9, 9))
	root := t.TempDir()
	v1 := annshardtest.WriteVectors(t, root, shardtest.RandomMatrix(r, 100, 2))
	a, stopA := annshardtest.StartOn(t, "127.0.0.1:0", root)
	b, stopB := annshardtest.StartOn(t, "127.0.0.1:0", root)
	stopB()
	c := annshardtest.Dial(t, [][]string{{a, b}}, annshard.Options{Config: shard.Config{Shards: 1, Mode: shard.Exact}, Serving: func() (string, time.Time) { return v1, time.Time{} }})
	if _, err := c.Prepare(context.Background(), v1, 100); err != nil {
		t.Fatal(err)
	}
	annshardtest.StartOn(t, b, root)
	waitReady(t, b)
	stopA()
	annshardtest.StartOn(t, a, root)
	waitReady(t, a)
}

// The first version a client prepares, with none served yet, is the one
// that every replica is then ready with.
func TestFirstVersionMakesReady(t *testing.T) {
	r := rand.New(rand.NewPCG(10, 10))
	root := t.TempDir()
	v1 := annshardtest.WriteVectors(t, root, shardtest.RandomMatrix(r, 100, 2))
	addrs := annshardtest.StartAll(t, root, 2, 2)
	c := annshardtest.Dial(t, addrs, annshard.Options{Config: shard.Config{Shards: 2, Mode: shard.Exact}})
	if _, err := c.Prepare(context.Background(), v1, 100); err != nil {
		t.Fatal(err)
	}
	for _, g := range addrs {
		for _, addr := range g {
			if !annshardtest.Ready(t, addr) {
				t.Fatalf("replica %s is not ready", addr)
			}
		}
	}
}

// A replica whose connection stays up follows the version served, with no
// search: it is prepared with each new one, and asked again after a failure,
// as for a version whose directory arrives late (D044).
func TestReplicaFollowsTheVersionServed(t *testing.T) {
	r := rand.New(rand.NewPCG(11, 11))
	root, elsewhere := t.TempDir(), t.TempDir()
	v1 := annshardtest.WriteVectors(t, root, shardtest.RandomMatrix(r, 100, 2))
	v2 := annshardtest.WriteVectors(t, elsewhere, shardtest.RandomMatrix(r, 100, 2))
	addr := annshardtest.Start(t, root)
	var served atomic.Pointer[string]
	served.Store(new(string))
	c := annshardtest.Dial(t, [][]string{{addr}}, annshard.Options{Config: shard.Config{Shards: 1, Mode: shard.Exact}, Serving: func() (string, time.Time) { return *served.Load(), time.Time{} }})
	if _, err := c.Prepare(context.Background(), v1, 100); err != nil {
		t.Fatal(err)
	}
	served.Store(&v1)
	if !annshardtest.Ready(t, addr) {
		t.Fatal("not ready on the first version")
	}
	served.Store(&v2)
	for deadline := time.Now().Add(20 * time.Second); annshardtest.Ready(t, addr); time.Sleep(10 * time.Millisecond) {
		if time.Now().After(deadline) {
			t.Fatal("still ready 20 s after the version served became one it cannot build")
		}
	}
	if err := os.Rename(filepath.Join(elsewhere, v2), filepath.Join(root, v2)); err != nil {
		t.Fatal(err)
	}
	waitReady(t, addr)
}
