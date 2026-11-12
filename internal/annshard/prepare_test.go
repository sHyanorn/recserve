package annshard_test

import (
	"bytes"
	"context"
	"math/rand/v2"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"

	"github.com/sHyanorn/recserve/internal/annshard"
	"github.com/sHyanorn/recserve/internal/annshard/annshardpb"
	"github.com/sHyanorn/recserve/internal/annshard/annshardtest"
	"github.com/sHyanorn/recserve/internal/shard"
	"github.com/sHyanorn/recserve/internal/shard/shardtest"
)

// Prepare waits for a replica whose Prepare does not return until its
// context ends, then fails if the replica's shard has no other, and leaves
// none of its calls running.
func TestPrepareFailsWhenItsContextEnds(t *testing.T) {
	r := rand.New(rand.NewPCG(10, 10))
	root := t.TempDir()
	v := annshardtest.WriteVectors(t, root, shardtest.RandomMatrix(r, 50, 2))
	arrived := make(chan struct{}, 1)
	stuck := grpc.UnaryInterceptor(func(ctx context.Context, req any, _ *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		if _, ok := req.(*annshardpb.PrepareRequest); !ok {
			return handler(ctx, req)
		}
		select {
		case arrived <- struct{}{}:
		default:
		}
		<-ctx.Done()
		return nil, status.FromContextError(ctx.Err()).Err()
	})
	c := annshardtest.Dial(t, [][]string{{annshardtest.Start(t, root, stuck)}}, annshard.Options{Config: shard.Config{Shards: 1, Mode: shard.Exact}, PrepareTimeout: 2 * time.Minute})
	const timeout = 200 * time.Millisecond
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	start := time.Now()
	if _, err := c.Prepare(ctx, v, 50); err == nil {
		t.Fatal("prepared with the shard's only replica stuck")
	}
	if took := time.Since(start); took < timeout || took > time.Second {
		t.Fatalf("the Prepare whose context ended after %v took %v", timeout, took)
	}
	select {
	case <-arrived:
	case <-time.After(5 * time.Second):
		t.Fatal("no Prepare arrived")
	}
	buf := make([]byte, 1<<20)
	if stacks := buf[:runtime.Stack(buf, true)]; bytes.Contains(stacks, []byte("annshard.(*Client).Prepare")) {
		t.Fatalf("a call of Prepare runs after it returned:\n%s", stacks)
	}
}

// namings counts, by version, the Prepares that reach a replica naming it
// served.
type namings struct {
	mu sync.Mutex
	n  map[string]int
}

func (x *namings) option() grpc.ServerOption {
	return grpc.UnaryInterceptor(func(ctx context.Context, req any, _ *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		if p, ok := req.(*annshardpb.PrepareRequest); ok && p.GetServingVersion() != "" {
			x.mu.Lock()
			x.n[p.GetServingVersion()]++
			x.mu.Unlock()
		}
		return handler(ctx, req)
	})
}

func (x *namings) count(v string) int {
	x.mu.Lock()
	defer x.mu.Unlock()
	return x.n[v]
}

// holdsShard reports whether the replica at addr holds shard i of n of
// version.
func holdsShard(t *testing.T, addr, version string, i, n int) bool {
	t.Helper()
	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close() //nolint:errcheck // a probe
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	_, err = annshardpb.NewAnnShardClient(conn).Search(ctx, &annshardpb.SearchRequest{Index: exactIndex(version, i, n), Query: []float32{1, 1}, K: 1})
	return err == nil
}

// Two gateways share two shards of two replicas. Gateway B serves v1;
// gateway A prepares v2, activates it and stops. B's swap to v3, naming v1
// served, leaves every replica v1, which B goes on serving until it
// activates v3, although no request of B's named v1 since A's v2 (D057).
func TestSwapKeepsTheVersionServedAfterAnotherGatewayStops(t *testing.T) {
	r := rand.New(rand.NewPCG(15, 15))
	root := t.TempDir()
	write := func() string { return annshardtest.WriteVectors(t, root, shardtest.RandomMatrix(r, 50, 2)) }
	v1, v2, v3 := write(), write(), write()
	const shards, replicas = 2, 2
	addrs := make([][]string, shards)
	var seen []*namings
	for i := range addrs {
		for range replicas {
			x := &namings{n: map[string]int{}}
			seen = append(seen, x)
			addrs[i] = append(addrs[i], annshardtest.Start(t, root, x.option()))
		}
	}
	cfg := shard.Config{Shards: shards, Mode: shard.Exact}
	ctx := context.Background()
	serving := func(p *atomic.Pointer[string]) func() string {
		return func() string {
			if v := p.Load(); v != nil {
				return *v
			}
			return ""
		}
	}
	waitNamed := func(v string, n int) { // n Prepares naming v served at every replica
		t.Helper()
		for deadline := time.Now().Add(10 * time.Second); ; time.Sleep(10 * time.Millisecond) {
			all := true
			for _, x := range seen {
				all = all && x.count(v) >= n
			}
			if all {
				return
			}
			if time.Now().After(deadline) {
				t.Fatalf("not every replica had %d Prepares naming %s served within 10 s", n, v)
			}
		}
	}
	search := func(s *annshard.Shards) error {
		for i := range shards {
			if _, err := s.Search(ctx, i, []float32{1, 1}, 5, nil); err != nil {
				return err
			}
		}
		return nil
	}

	var bServes, aServes atomic.Pointer[string]
	b := annshardtest.Dial(t, addrs, annshard.Options{Config: cfg, Serving: serving(&bServes)})
	s1, err := b.Prepare(ctx, v1, 50)
	if err != nil {
		t.Fatal(err)
	}
	bServes.Store(&v1)
	waitNamed(v1, 2) // B's Prepare and its watch's

	a, err := annshard.Dial(addrs, annshard.Options{Config: cfg, Serving: serving(&aServes)})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := a.Prepare(ctx, v2, 50); err != nil {
		t.Fatal(err)
	}
	aServes.Store(&v2)
	waitNamed(v2, 2) // A's Prepare and its watch's
	if err := a.Close(); err != nil {
		t.Fatal(err)
	}

	if _, err := b.Prepare(ctx, v3, 50); err != nil {
		t.Fatal(err)
	}
	for i, g := range addrs {
		for j, addr := range g {
			if !holdsShard(t, addr, v1, i, shards) {
				t.Errorf("replica %d of shard %d lacks v1, which B serves, once B prepared v3", j, i)
			}
		}
	}
	if err := search(s1); err != nil {
		t.Errorf("B's search of v1 once B prepared v3: %v", err)
	}
}
