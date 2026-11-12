package annshard_test

import (
	"bytes"
	"context"
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

// held is a Prepare that a holder keeps from the replica's handler.
type held struct {
	req     *annshardpb.PrepareRequest
	release chan struct{} // closed by the test to let it through
	done    chan struct{} // closed once the replica has answered it
}

// holder keeps the Prepares that keep selects until the test releases
// them. A paused holder also keeps those whose caller has given up and
// then runs them, as a replica that was stopped does once it runs again.
type holder struct {
	keep    func(*annshardpb.PrepareRequest) bool
	paused  bool
	arrived chan *held
	resume  chan struct{} // closed by resumeAll
}

func newHolder(keep func(*annshardpb.PrepareRequest) bool, paused bool) *holder {
	return &holder{keep: keep, paused: paused, arrived: make(chan *held, 100), resume: make(chan struct{})}
}

func (h *holder) option() grpc.ServerOption {
	return grpc.UnaryInterceptor(func(ctx context.Context, req any, _ *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		p, ok := req.(*annshardpb.PrepareRequest)
		if !ok || !h.keep(p) {
			return handler(ctx, req)
		}
		x := &held{req: p, release: make(chan struct{}), done: make(chan struct{})}
		defer close(x.done)
		h.arrived <- x
		cancelled := ctx.Done()
		if h.paused {
			cancelled = nil
		}
		select {
		case <-x.release:
		case <-h.resume:
		case <-cancelled:
			return nil, status.FromContextError(ctx.Err()).Err()
		}
		return handler(ctx, req)
	})
}

// resumeAll lets every Prepare kept and to come through; a test calls it
// before its replica stops.
func (h *holder) resumeAll() { close(h.resume) }

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

type servedAt struct {
	version string
	since   time.Time
}

// A replica that was stopped while the gateway swapped to v2 and that runs
// the swap's Prepare of v2, naming v1 served, only after the client gave
// it up and swapped on to v3, keeps v3, the version served (D057 point 4).
// Prepare waits for the stopped replica until its context ends, and goes
// on with the shard's other replica.
func TestLatePrepareThroughTheClient(t *testing.T) {
	r := rand.New(rand.NewPCG(9, 9))
	root := t.TempDir()
	write := func() string { return annshardtest.WriteVectors(t, root, shardtest.RandomMatrix(r, 50, 2)) }
	v1, v2, v3 := write(), write(), write()
	late := newHolder(func(p *annshardpb.PrepareRequest) bool { return p.GetIndex().GetVersion() == v2 }, true)
	b := annshardtest.Start(t, root, late.option())
	t.Cleanup(late.resumeAll)
	addrs := [][]string{{annshardtest.Start(t, root), b}}
	var served atomic.Pointer[servedAt]
	serving := func() (string, time.Time) {
		if s := served.Load(); s != nil {
			return s.version, s.since
		}
		return "", time.Time{}
	}
	activate := func(v string) { served.Store(&servedAt{v, time.Now()}) }
	const timeout = 300 * time.Millisecond
	c := annshardtest.Dial(t, addrs, annshard.Options{Config: shard.Config{Shards: 1, Mode: shard.Exact}, PrepareTimeout: timeout, Serving: serving})
	prepare := func(v string) *annshard.Shards {
		t.Helper()
		ctx, cancel := context.WithTimeout(context.Background(), timeout)
		defer cancel()
		s, err := c.Prepare(ctx, v, 50)
		if err != nil {
			t.Fatal(err)
		}
		return s
	}
	searched := func(s *annshard.Shards) {
		t.Helper()
		for range 5 {
			if _, err := s.Search(context.Background(), 0, []float32{1, 1}, 5, nil); err != nil {
				t.Fatal(err)
			}
		}
	}
	waitHolds := func(v string) {
		t.Helper()
		for deadline := time.Now().Add(10 * time.Second); !holdsShard(t, b, v, 0, 1); time.Sleep(10 * time.Millisecond) {
			if time.Now().After(deadline) {
				t.Fatalf("the second replica lacks %s after 10 s", v)
			}
		}
	}

	searched(prepare(v1))
	activate(v1)

	start := time.Now()
	s2 := prepare(v2) // the second replica runs none of its Prepares of v2
	if took := time.Since(start); took < timeout {
		t.Fatalf("the Prepare of v2 returned after %v, before the second replica's call ended", took)
	}
	swap := late.next(t)
	if got := swap.req.GetServingVersion(); got != v1 {
		t.Fatalf("the swap's Prepare names %q served, want %s", got, v1)
	}
	activate(v2)
	searched(s2)

	s3 := prepare(v3)
	activate(v3)
	searched(s3)

	close(swap.release)
	<-swap.done
	waitHolds(v2) // the late Prepare's build, which goes on as its caller has gone
	if !holdsShard(t, b, v3, 0, 1) {
		t.Fatal("the late Prepare of v2 had the second replica drop v3, the version served")
	}
	searched(s3)
}

// Prepare waits for a replica whose Prepare does not return until its
// context ends, then fails if the replica's shard has no other, and leaves
// none of its calls running.
func TestPrepareFailsWhenItsContextEnds(t *testing.T) {
	r := rand.New(rand.NewPCG(10, 10))
	root := t.TempDir()
	v := annshardtest.WriteVectors(t, root, shardtest.RandomMatrix(r, 50, 2))
	stuck := newHolder(func(*annshardpb.PrepareRequest) bool { return true }, false)
	c := annshardtest.Dial(t, [][]string{{annshardtest.Start(t, root, stuck.option())}}, annshard.Options{Config: shard.Config{Shards: 1, Mode: shard.Exact}, PrepareTimeout: 2 * time.Minute})
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
	stuck.next(t)
	buf := make([]byte, 1<<20)
	if stacks := buf[:runtime.Stack(buf, true)]; bytes.Contains(stacks, []byte("annshard.(*Client).Prepare")) {
		t.Fatalf("a call of Prepare runs after it returned:\n%s", stacks)
	}
}

// A gateway that starts, whether its Prepare succeeds or not, and stops
// before activating a version leaves a replica the version that another
// gateway serves: the replica stays ready, and that gateway's next swap,
// naming its version served, keeps it (D057 point 1).
func TestStartingGatewayLeavesTheVersionServed(t *testing.T) {
	r := rand.New(rand.NewPCG(11, 11))
	root, elsewhere := t.TempDir(), t.TempDir()
	write := func(root string) string { return annshardtest.WriteVectors(t, root, shardtest.RandomMatrix(r, 50, 2)) }
	vA, vB, v3, missing := write(root), write(root), write(root), write(elsewhere)
	addr := annshardtest.Start(t, root)
	cfg := shard.Config{Shards: 1, Mode: shard.Exact}
	ctx := context.Background()
	var served atomic.Pointer[servedAt]
	a := annshardtest.Dial(t, [][]string{{addr}}, annshard.Options{Config: cfg, Serving: func() (string, time.Time) {
		if s := served.Load(); s != nil {
			return s.version, s.since
		}
		return "", time.Time{}
	}})
	sA, err := a.Prepare(ctx, vA, 50)
	if err != nil {
		t.Fatal(err)
	}
	served.Store(&servedAt{vA, time.Now()})
	time.Sleep(1500 * time.Millisecond) // the watch has sent the replica vA's time
	for _, v := range []string{vB, missing} { // gateways that start and stop
		b, err := annshard.Dial([][]string{{addr}}, annshard.Options{Config: cfg})
		if err != nil {
			t.Fatal(err)
		}
		_, err = b.Prepare(ctx, v, 50)
		if (err == nil) != (v == vB) {
			t.Fatalf("prepare %s: %v", v, err)
		}
		if err := b.Close(); err != nil {
			t.Fatal(err)
		}
		if !annshardtest.Ready(t, addr) {
			t.Fatalf("not ready once a gateway that prepared %s stopped", v)
		}
	}
	if _, err := a.Prepare(ctx, v3, 50); err != nil {
		t.Fatal(err)
	}
	if _, err := sA.Search(ctx, 0, []float32{1, 1}, 5, nil); err != nil {
		t.Fatalf("the swap to v3 dropped vA, which the gateway serves: %v", err)
	}
}
