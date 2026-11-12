package annshard_test

import (
	"bytes"
	"context"
	"errors"
	"math/rand/v2"
	"runtime"
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

// held is a Prepare that a replica's interceptor keeps from it.
type held struct {
	req     *annshardpb.PrepareRequest
	release chan struct{} // closed by the test to let it through
	done    chan struct{} // closed once the replica has answered it
}

// holder keeps the Prepares that keep returns true for from a replica until
// the test releases them, or until their caller gives up. Each is sent on
// arrived as it arrives.
type holder struct {
	keep    func(*annshardpb.PrepareRequest) bool
	arrived chan *held
}

func newHolder(keep func(*annshardpb.PrepareRequest) bool) *holder {
	return &holder{keep: keep, arrived: make(chan *held, 100)}
}

// option returns the server option that installs the holder.
func (h *holder) option() grpc.ServerOption {
	return grpc.UnaryInterceptor(func(ctx context.Context, req any, _ *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		p, ok := req.(*annshardpb.PrepareRequest)
		if !ok || !h.keep(p) {
			return handler(ctx, req)
		}
		x := &held{req: p, release: make(chan struct{}), done: make(chan struct{})}
		defer close(x.done)
		h.arrived <- x
		select {
		case <-x.release:
		case <-ctx.Done():
			return nil, status.FromContextError(ctx.Err()).Err()
		}
		return handler(ctx, req)
	})
}

// next returns the next Prepare the holder keeps, failing after 5 s.
func (h *holder) next(t *testing.T) *held {
	t.Helper()
	select {
	case x := <-h.arrived:
		return x
	case <-time.After(5 * time.Second):
		t.Fatal("no Prepare arrived")
		return nil
	}
}

// holdsShard reports whether the replica at addr answers a search of shard
// i of n of version, in exact mode.
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

// servedAt is a version the test's gateway serves, and since when.
type servedAt struct {
	version string
	since   time.Time
}

// Prepare returns once every shard has a replica that holds the version,
// and the Prepare a replica receives after a later one cannot make it drop
// the version served (D057). The second replica of each shard keeps its
// Prepares of v2 until the gateway has swapped to v2 and on to v3; the one
// sent during the swap to v2, which names v1 served, then reaches it first.
// Ordered by arrival alone, it would make v1 the version served, and the
// replica would drop v3 to make room for v2.
func TestPrepareReturnsWithAReplicaPerShard(t *testing.T) {
	r := rand.New(rand.NewPCG(9, 9))
	root := t.TempDir()
	write := func() string { return annshardtest.WriteVectors(t, root, shardtest.RandomMatrix(r, 50, 2)) }
	v1, v2, v3 := write(), write(), write()
	const n = 2
	late := make([]*holder, n)
	addrs := make([][]string, n)
	for i := range n {
		late[i] = newHolder(func(p *annshardpb.PrepareRequest) bool { return p.GetIndex().GetVersion() == v2 })
		addrs[i] = []string{annshardtest.Start(t, root), annshardtest.Start(t, root, late[i].option())}
	}
	var served atomic.Pointer[servedAt]
	serving := func() (string, time.Time) {
		if s := served.Load(); s != nil {
			return s.version, s.since
		}
		return "", time.Time{}
	}
	activate := func(v string) { served.Store(&servedAt{v, time.Now()}) }
	c := annshardtest.Dial(t, addrs, annshard.Options{Config: shard.Config{Shards: n, Mode: shard.Exact}, PrepareTimeout: 2 * time.Minute, Serving: serving})
	ctx := context.Background()
	prepare := func(v string) *annshard.Shards {
		t.Helper()
		start := time.Now()
		ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
		defer cancel()
		s, err := c.Prepare(ctx, v, 50)
		if err != nil {
			t.Fatal(err)
		}
		if took := time.Since(start); took > time.Second {
			t.Fatalf("the Prepare of %s took %v", v, took)
		}
		return s
	}
	// searched checks that every shard has a replica that answers searches
	// of the version s names.
	searched := func(s *annshard.Shards) {
		t.Helper()
		for range 5 {
			for i := range n {
				if _, err := s.Search(ctx, i, []float32{1, 1}, 5, nil); err != nil {
					t.Fatalf("shard %d: %v", i, err)
				}
			}
		}
	}
	waitHolds := func(v string, i int) {
		t.Helper()
		for deadline := time.Now().Add(10 * time.Second); !holdsShard(t, addrs[i][1], v, i, n); time.Sleep(10 * time.Millisecond) {
			if time.Now().After(deadline) {
				t.Fatalf("the second replica of shard %d lacks %s after 10 s", i, v)
			}
		}
	}

	searched(prepare(v1))
	activate(v1)

	s2 := prepare(v2) // while the second replicas keep it
	activate(v2)
	swap, watch := make([]*held, n), make([]*held, n)
	for i := range n {
		swap[i] = late[i].next(t)
		watch[i] = late[i].next(t) // the client's watch tells it v2 is served
		if got := swap[i].req.GetServingVersion(); got != v1 {
			t.Fatalf("shard %d: the swap's Prepare names %q served, want %s", i, got, v1)
		}
	}
	// The searches find the second replicas lacking v2; while the swap's
	// Prepare to one is in flight, catch-up sends it no other.
	searched(s2)
	select {
	case x := <-late[0].arrived:
		t.Fatalf("a Prepare of v2 naming %q served was sent while the swap's was in flight", x.req.GetServingVersion())
	case x := <-late[1].arrived:
		t.Fatalf("a Prepare of v2 naming %q served was sent while the swap's was in flight", x.req.GetServingVersion())
	case <-time.After(300 * time.Millisecond):
	}

	s3 := prepare(v3)
	for i := range n {
		waitHolds(v3, i)
	}
	activate(v3)
	searched(s3)

	for i := range n {
		close(swap[i].release)
		<-swap[i].done
		if !holdsShard(t, addrs[i][1], v3, i, n) {
			t.Fatalf("shard %d: the swap's late Prepare of v2 had the second replica drop v3, the version served", i)
		}
	}
	searched(s3)
	for i := range n {
		close(watch[i].release)
		<-watch[i].done
		waitHolds(v3, i)
	}
	searched(s3)
}

// Prepare fails as soon as every replica of a shard has failed, whatever
// the other shards' replicas are doing, and when its context ends first.
// The calls it leaves running end at Close, which waits for them.
func TestPrepareFails(t *testing.T) {
	r := rand.New(rand.NewPCG(10, 10))
	root, empty := t.TempDir(), t.TempDir()
	v := annshardtest.WriteVectors(t, root, shardtest.RandomMatrix(r, 50, 2))
	stuck := newHolder(func(*annshardpb.PrepareRequest) bool { return true })
	stuckAddr := annshardtest.Start(t, root, stuck.option())
	// A call that ended with its context returns 100 ms later, so that one
	// Close does not wait for still runs after it.
	var ended atomic.Int32
	slow := grpc.WithUnaryInterceptor(func(ctx context.Context, method string, req, reply any, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
		err := invoker(ctx, method, req, reply, cc, opts...)
		if ctx.Err() != nil {
			time.Sleep(100 * time.Millisecond)
			ended.Add(1)
		}
		return err
	})
	dial := func(addrs [][]string) *annshard.Client {
		t.Helper()
		c, err := annshard.Dial(addrs, annshard.Options{Config: shard.Config{Shards: len(addrs), Mode: shard.Exact}, PrepareTimeout: 2 * time.Minute, Dial: []grpc.DialOption{slow}})
		if err != nil {
			t.Fatal(err)
		}
		return c
	}

	c1 := dial([][]string{{stuckAddr}, {annshardtest.Start(t, empty), annshardtest.Start(t, empty)}})
	start := time.Now()
	if _, err := c1.Prepare(context.Background(), v, 50); err == nil {
		t.Fatal("prepared with a shard none of whose replicas holds the version")
	} else if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) {
		t.Fatalf("err = %v, want the replicas' errors", err)
	}
	if took := time.Since(start); took > time.Second {
		t.Fatalf("the failed Prepare took %v", took)
	}
	stuck.next(t)

	c2 := dial([][]string{{stuckAddr}})
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	start = time.Now()
	if _, err := c2.Prepare(ctx, v, 50); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("err = %v, want %v", err, context.DeadlineExceeded)
	}
	if took := time.Since(start); took > time.Second {
		t.Fatalf("the Prepare whose context ended took %v", took)
	}
	stuck.next(t)

	for _, c := range []*annshard.Client{c1, c2} {
		if err := c.Close(); err != nil {
			t.Fatal(err)
		}
	}
	if got := ended.Load(); got != 2 {
		t.Fatalf("%d of the 2 calls left running had ended when Close returned", got)
	}
	buf := make([]byte, 1<<20)
	if stacks := buf[:runtime.Stack(buf, true)]; bytes.Contains(stacks, []byte("annshard.(*Client).Prepare")) {
		t.Fatalf("a call of Prepare runs after Close:\n%s", stacks)
	}
}
