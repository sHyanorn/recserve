package recommend

import (
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"sync/atomic"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/sHyanorn/recserve/internal/annshard"
	"github.com/sHyanorn/recserve/internal/annshard/annshardpb"
	"github.com/sHyanorn/recserve/internal/annshard/annshardtest"
	"github.com/sHyanorn/recserve/internal/infratest"
	"github.com/sHyanorn/recserve/internal/shard"
	"github.com/sHyanorn/recserve/internal/vec"
)

// remoteShards returns a build function that puts each catalog on annshard
// replicas, one per shard.
func remoteShards(t *testing.T) func(m vec.Matrix, cfg shard.Config) (Shards, error) {
	build := annshardtest.Remote(t, 7)
	return func(m vec.Matrix, cfg shard.Config) (Shards, error) { return build(m, cfg) }
}

// Property 6 on the request path over gRPC.
func TestProperty6PipelineOverGRPC(t *testing.T) {
	if err := checkProperty6(1, 100, false, remoteShards(t)); err != nil {
		t.Fatal(err)
	}
}

func TestProperty6PipelineOverGRPCCatchesBrokenVariant(t *testing.T) {
	if err := checkProperty6(1, 100, true, remoteShards(t)); err == nil {
		t.Fatal("the check passed shards that return ceil(VectorTop/n) items each")
	}
}

// The shard degradations of D029 with the faults in the network, between
// the gateway and two shards of one replica each: a replica that refuses
// connections fails its shard, one that does not answer times it out at
// the shard budget, and the response is the one the in-process shards give
// with those shards skipped. Without a fault it is the in-process response.
func TestShardFaultsOverGRPC(t *testing.T) {
	const budget = 100 * time.Millisecond
	r := rand.New(rand.NewPCG(21, 21))
	local, err := randomPipeline(r, 300, 3)
	if err != nil {
		t.Fatal(err)
	}
	local.rankWith = linearRanker
	root := t.TempDir()
	annshardtest.WriteVectors(t, root, "v1", local.Bundle.Vectors)
	proxies := []*infratest.Proxy{infratest.NewProxy(t, annshardtest.Start(t, root)), infratest.NewProxy(t, annshardtest.Start(t, root))}
	client := annshardtest.Dial(t, [][]string{{proxies[0].Addr()}, {proxies[1].Addr()}}, annshard.Options{})
	remote, err := client.Prepare(context.Background(), "v1", local.Shards.Config())
	if err != nil {
		t.Fatal(err)
	}
	p := *local
	p.Shards, p.Budgets = remote, Budgets{Shard: budget}

	healthy := &fault{name: "no fault"}
	var cases []*fault
	for _, f := range faults {
		if f.faults[0].stage == stageVector {
			cases = append(cases, healthy, f)
		}
	}
	// What each proxy does now: Pass closes the open connections, so a proxy
	// is only told what changes.
	const pass, refuse, blackhole = 0, 1, 2
	state := make([]int, len(proxies))
	for _, f := range append(cases, healthy) {
		for i, px := range proxies {
			next := pass
			if f.fails(stageVector, i) {
				next = refuse
				if f.faults[min(i, len(f.faults)-1)].timeout {
					next = blackhole
				}
			}
			if next != state[i] {
				[]func(){px.Pass, px.Refuse, px.Blackhole}[next]()
				state[i] = next
			}
		}
		if f == healthy {
			// The client connects again within its backoff.
			deadline := time.Now().Add(20 * time.Second)
			for i := 0; i < len(proxies); {
				if _, err := remote.Search(context.Background(), i, make([]float32, local.Bundle.Dim), 1, nil); err == nil {
					i++
				} else if time.Now().After(deadline) {
					t.Fatalf("shard %d did not come back: %v", i, err)
				}
			}
		}
		var reported int
		for range 4 {
			req := Request{User: fmt.Sprint("u", r.IntN(4)), K: 1 + r.IntN(MaxK), TS: int64(r.IntN(110)),
				Exclude: []string{fmt.Sprint("i", 1+r.IntN(local.Bundle.ItemCount))}}
			start := time.Now()
			got, err := p.Recommend(context.Background(), req)
			if err != nil {
				t.Fatal(err)
			}
			if d := time.Since(start); d > budget+5*time.Second {
				t.Fatalf("%s: answered after %v with a shard budget of %v", f.name, d, budget)
			}
			if err := sameResponse(got, want(t, local, f, req)); err != nil {
				t.Fatalf("%s, %+v: %v", f.name, req, err)
			}
			if len(got.Degraded) > 0 {
				reported++
			}
			for _, fl := range got.Failures {
				if errors.Is(fl.Err, context.DeadlineExceeded) {
					t.Fatalf("%s: a timeout kept as a failure: %v", f.name, fl.Err)
				}
			}
		}
		if (reported > 0) != (f != healthy) {
			t.Fatalf("%s: %d of 4 responses degraded", f.name, reported)
		}
	}
}

// replicated is a catalog on two shards of two replicas each. The first
// replica of a shard can be made faulty: it is behind a proxy, and fails
// its searches while failing is set.
type replicated struct {
	addrs   [][]string
	proxies []*infratest.Proxy // of each shard's first replica
	failing atomic.Bool
}

func replicate(t *testing.T, m vec.Matrix) *replicated {
	t.Helper()
	root := t.TempDir()
	annshardtest.WriteVectors(t, root, "v1", m)
	c := &replicated{}
	fail := grpc.UnaryInterceptor(func(ctx context.Context, req any, _ *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		if _, ok := req.(*annshardpb.SearchRequest); ok && c.failing.Load() {
			return nil, status.Error(codes.Internal, "injected")
		}
		return handler(ctx, req)
	})
	for range 2 {
		px := infratest.NewProxy(t, annshardtest.Start(t, root, fail))
		c.proxies = append(c.proxies, px)
		c.addrs = append(c.addrs, []string{px.Addr(), annshardtest.Start(t, root)})
	}
	return c
}

// The ways one replica of every shard is lost in property 12.
var replicaFaults = map[string]func(*replicated){
	"refuses connections": func(c *replicated) {
		for _, px := range c.proxies {
			px.Refuse()
		}
	},
	"does not answer": func(c *replicated) {
		for _, px := range c.proxies {
			px.Blackhole()
		}
	},
	"answers with an error": func(c *replicated) { c.failing.Store(true) },
}

// checkProperty12 loses one replica of each of two shards and compares 10
// responses with those of in-process shards: the same items, every shard
// answered, nothing degraded. dial returns the client of the replicas at
// addrs[shard][replica]. Ten searches of a shard stay within its hedge
// budget also if every one of them needs a hedge.
func checkProperty12(t *testing.T, lose func(*replicated), dial func(addrs [][]string) *annshard.Client) error {
	r := rand.New(rand.NewPCG(12, 12))
	local, err := randomPipeline(r, 300, 3)
	if err != nil {
		return err
	}
	local.rankWith = linearRanker
	c := replicate(t, local.Bundle.Vectors)
	remote, err := dial(c.addrs).Prepare(context.Background(), "v1", local.Shards.Config())
	if err != nil {
		return err
	}
	p := *local
	p.Shards, p.Budgets = remote, Budgets{Shard: 5 * time.Second}
	lose(c)
	for range 10 {
		req := Request{User: fmt.Sprint("u", r.IntN(4)), K: 1 + r.IntN(MaxK), TS: int64(r.IntN(110)),
			Exclude: []string{fmt.Sprint("i", 1+r.IntN(local.Bundle.ItemCount))}}
		got, err := p.Recommend(context.Background(), req)
		if err != nil {
			return err
		}
		if err := sameResponse(got, want(t, local, &fault{}, req)); err != nil {
			return fmt.Errorf("%+v: %w", req, err)
		}
	}
	return nil
}

// Property 12: with one replica of every shard refusing connections, not
// answering or answering with errors, every response is the healthy one.
func TestProperty12OneReplicaLostPerShard(t *testing.T) {
	for name, lose := range replicaFaults {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			err := checkProperty12(t, lose, func(addrs [][]string) *annshard.Client {
				return annshardtest.Dial(t, addrs, annshard.Options{HedgeDelay: 20 * time.Millisecond})
			})
			if err != nil {
				t.Fatal(err)
			}
		})
	}
}

// The broken variant knows one replica of each shard, the one that is lost.
func TestProperty12CatchesBrokenVariant(t *testing.T) {
	for name, lose := range replicaFaults {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			err := checkProperty12(t, lose, func(addrs [][]string) *annshard.Client {
				return annshardtest.Dial(t, [][]string{addrs[0][:1], addrs[1][:1]}, annshard.Options{HedgeDelay: 20 * time.Millisecond})
			})
			if err == nil {
				t.Fatal("the check passed a client that asks one replica per shard")
			}
		})
	}
}

// A shard whose replicas all run past the shard budget is left out: the
// response comes back at the budget, reports shards_partial and one of two
// shards answered, and holds what the in-process shards give without it.
func TestSlowShardOverGRPC(t *testing.T) {
	const budget = 100 * time.Millisecond
	r := rand.New(rand.NewPCG(22, 22))
	local, err := randomPipeline(r, 300, 3)
	if err != nil {
		t.Fatal(err)
	}
	local.rankWith = linearRanker
	root := t.TempDir()
	annshardtest.WriteVectors(t, root, "v1", local.Bundle.Vectors)
	slow := []*infratest.Proxy{infratest.NewProxy(t, annshardtest.Start(t, root)), infratest.NewProxy(t, annshardtest.Start(t, root))}
	addrs := [][]string{{slow[0].Addr(), slow[1].Addr()}, {annshardtest.Start(t, root), annshardtest.Start(t, root)}}
	remote, err := annshardtest.Dial(t, addrs, annshard.Options{HedgeDelay: 5 * time.Millisecond}).Prepare(context.Background(), "v1", local.Shards.Config())
	if err != nil {
		t.Fatal(err)
	}
	p := *local
	p.Shards, p.Budgets = remote, Budgets{Shard: budget}
	for _, px := range slow {
		px.Blackhole()
	}
	shard0 := &fault{faults: []stageFault{{stageVector, 0, true}}, reason: ShardsPartial}
	var partial int
	for range 6 {
		req := Request{User: fmt.Sprint("u", r.IntN(4)), K: 1 + r.IntN(MaxK), TS: int64(r.IntN(110))}
		start := time.Now()
		got, err := p.Recommend(context.Background(), req)
		if err != nil {
			t.Fatal(err)
		}
		if d := time.Since(start); d > budget+5*time.Second {
			t.Fatalf("answered after %v with a shard budget of %v", d, budget)
		}
		if err := sameResponse(got, want(t, local, shard0, req)); err != nil {
			t.Fatalf("%+v: %v", req, err)
		}
		if got.Coverage != nil {
			partial++
			if len(got.Failures) > 0 {
				t.Fatalf("a timeout kept as a failure: %v", got.Failures)
			}
		}
	}
	if partial == 0 {
		t.Fatal("no request had a user vector")
	}
}
