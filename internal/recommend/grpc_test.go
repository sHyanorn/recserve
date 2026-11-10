package recommend

import (
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"testing"
	"time"

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
	client := annshardtest.Dial(t, [][]string{{proxies[0].Addr()}, {proxies[1].Addr()}})
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
