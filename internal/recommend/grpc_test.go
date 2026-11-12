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
	"github.com/sHyanorn/recserve/internal/bundle"
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
	version := annshardtest.WriteVectors(t, root, local.Bundle.Vectors)
	proxies := []*infratest.Proxy{infratest.NewProxy(t, annshardtest.Start(t, root)), infratest.NewProxy(t, annshardtest.Start(t, root))}
	client := annshardtest.Dial(t, [][]string{{proxies[0].Addr()}, {proxies[1].Addr()}}, annshard.Options{Config: local.Shards.Config()})
	remote, err := client.Prepare(context.Background(), version, local.Bundle.ItemCount)
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
	version string
	addrs   [][]string
	proxies []*infratest.Proxy // of each shard's first replica
	failing atomic.Bool
}

func replicate(t *testing.T, m vec.Matrix) *replicated {
	t.Helper()
	root := t.TempDir()
	c := &replicated{version: annshardtest.WriteVectors(t, root, m)}
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

// checkProperty12 loses one replica of each of two shards, once every
// replica holds the version, and compares 10 responses with those of
// in-process shards: the same items, every shard answered, nothing
// degraded. dial returns the client of the replicas at
// addrs[shard][replica], with the shards configured as cfg. Ten searches of
// a shard stay within its hedge budget also if every one of them needs a
// hedge.
func checkProperty12(t *testing.T, lose func(*replicated), dial func(addrs [][]string, cfg shard.Config) *annshard.Client) error {
	r := rand.New(rand.NewPCG(12, 12))
	local, err := randomPipeline(r, 300, 3)
	if err != nil {
		return err
	}
	local.rankWith = linearRanker
	c := replicate(t, local.Bundle.Vectors)
	remote, err := annshardtest.PrepareAll(t, dial(c.addrs, local.Shards.Config()), c.version, local.Bundle.ItemCount)
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
			err := checkProperty12(t, lose, func(addrs [][]string, cfg shard.Config) *annshard.Client {
				return annshardtest.Dial(t, addrs, annshard.Options{Config: cfg, HedgeDelay: 20 * time.Millisecond})
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
			err := checkProperty12(t, lose, func(addrs [][]string, cfg shard.Config) *annshard.Client {
				return annshardtest.Dial(t, [][]string{addrs[0][:1], addrs[1][:1]}, annshard.Options{Config: cfg, HedgeDelay: 20 * time.Millisecond})
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
	version := annshardtest.WriteVectors(t, root, local.Bundle.Vectors)
	slow := []*infratest.Proxy{infratest.NewProxy(t, annshardtest.Start(t, root)), infratest.NewProxy(t, annshardtest.Start(t, root))}
	addrs := [][]string{{slow[0].Addr(), slow[1].Addr()}, {annshardtest.Start(t, root), annshardtest.Start(t, root)}}
	remote, err := annshardtest.Dial(t, addrs, annshard.Options{Config: local.Shards.Config(), HedgeDelay: 5 * time.Millisecond}).Prepare(context.Background(), version, local.Bundle.ItemCount)
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

// swapReplicas is two shards of two replicas each for the swap tests. The
// replicas know a test bundle's vectors by the version their files give.
type swapReplicas struct {
	roots    [][]string // the bundle root of each replica, by shard
	addrs    [][]string
	versions map[string]string // by the test bundle's version
}

// startSwapReplicas starts the replicas, each over a bundle root of its
// own and with the server options opts returns for it.
func startSwapReplicas(t *testing.T, opts func() []grpc.ServerOption) *swapReplicas {
	t.Helper()
	c := &swapReplicas{roots: make([][]string, 2), addrs: make([][]string, 2), versions: map[string]string{}}
	for i := range c.roots {
		for range 2 {
			root := t.TempDir()
			c.roots[i] = append(c.roots[i], root)
			c.addrs[i] = append(c.addrs[i], annshardtest.Start(t, root, opts()...))
		}
	}
	return c
}

// give writes the vectors of version v, of items items, to the root of
// replica j of shard i.
func (c *swapReplicas) give(t *testing.T, i, j int, v string, items int) {
	t.Helper()
	c.versions[v] = annshardtest.WriteVectors(t, c.roots[i][j], swapBundleOf(t, v, nil, items).Vectors)
}

// prepare returns the function that prepares a bundle's version on the
// replicas.
func (c *swapReplicas) prepare(t *testing.T) func(*bundle.Bundle) (Shards, error) {
	client := annshardtest.Dial(t, c.addrs, annshard.Options{Config: shard.DefaultConfig, HedgeDelay: 5 * time.Millisecond})
	return func(b *bundle.Bundle) (Shards, error) {
		return client.Prepare(context.Background(), c.versions[b.ModelVersion], b.ItemCount)
	}
}

// property4Items is the catalog size of property 4 over gRPC: well above
// VectorTop, so that a shard's candidates depend on the vectors it searched
// and a replica that searched another version's changes the response.
const property4Items = 2000

func checkProperty4OverGRPC(t *testing.T, opts func() []grpc.ServerOption) (map[string]int, error) {
	c := startSwapReplicas(t, opts)
	for i := range c.roots {
		for j := range c.roots[i] {
			c.give(t, i, j, "a", property4Items)
			c.give(t, i, j, "b", property4Items)
		}
	}
	st := newSwapTestOn(t, nil, "", property4Items, c.prepare(t))
	defer st.live.Close() //nolint:errcheck // the bundles have nothing to close
	return checkProperty4(st, 8, 60)
}

// Property 4 over gRPC: with the vectors on two shards of two replicas
// each, every response during concurrent requests and repeated swaps
// recomputes from the vectors of the version it names.
func TestProperty4SwapUnderLoadOverGRPC(t *testing.T) {
	served, err := checkProperty4OverGRPC(t, func() []grpc.ServerOption { return nil })
	if err != nil {
		t.Fatal(err)
	}
	if served["a"] == 0 || served["b"] == 0 {
		t.Fatalf("responses per version: %v", served)
	}
	t.Logf("responses per version: %v", served)
}

// The broken variant is a replica that ignores the version a search names
// and answers from the version it prepared last: the request held across
// the first swap then merges one shard of each version.
func TestProperty4OverGRPCCatchesBrokenVariant(t *testing.T) {
	_, err := checkProperty4OverGRPC(t, func() []grpc.ServerOption {
		var latest atomic.Pointer[string]
		return []grpc.ServerOption{grpc.UnaryInterceptor(func(ctx context.Context, req any, _ *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
			switch r := req.(type) {
			case *annshardpb.PrepareRequest:
				resp, err := handler(ctx, req)
				if err == nil {
					v := r.GetIndex().GetVersion()
					latest.Store(&v)
				}
				return resp, err
			case *annshardpb.SearchRequest:
				if v := latest.Load(); v != nil {
					r.Index.Version = *v
				}
			}
			return handler(ctx, req)
		})}
	})
	if err == nil {
		t.Fatal("the check passed replicas that answer from the version they prepared last")
	}
	t.Logf("broken variant caught: %.300v", err)
}

// A swap is rejected while a shard has no replica that can prepare the new
// version, and the active version goes on serving. Once every shard has
// one, the swap goes through, also with a replica of the other shard still
// lacking the version, and that replica costs no response.
func TestSwapNeedsAReplicaPerShard(t *testing.T) {
	c := startSwapReplicas(t, func() []grpc.ServerOption { return nil })
	for i := range c.roots {
		for j := range c.roots[i] {
			c.give(t, i, j, "a", swapItems)
		}
	}
	c.give(t, 0, 0, "b", swapItems) // shard 0: one replica of two; shard 1: none
	st := newSwapTestOn(t, nil, "", swapItems, c.prepare(t))
	defer st.live.Close() //nolint:errcheck // the bundles have nothing to close
	serve := func(version string) {
		t.Helper()
		r := rand.New(rand.NewPCG(31, 31))
		for range 30 {
			req := Request{User: fmt.Sprint("u", r.IntN(swapUsers)), K: 1 + r.IntN(10), TS: int64(r.IntN(110))}
			resp, err := st.live.Recommend(context.Background(), req)
			if err != nil {
				t.Fatal(err)
			}
			if resp.ModelVersion != version {
				t.Fatalf("served by version %s, want %s", resp.ModelVersion, version)
			}
			if err := st.check(req, resp); err != nil {
				t.Fatal(err)
			}
		}
	}
	if _, err := st.live.Swap(st.prepare("b")); err == nil {
		t.Fatal("swapped to a version no replica of shard 1 holds")
	}
	serve("a")
	c.give(t, 1, 1, "b", swapItems)
	if _, err := st.live.Swap(st.prepare("b")); err != nil {
		t.Fatal(err)
	}
	serve("b")
}
