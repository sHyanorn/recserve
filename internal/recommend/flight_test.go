package recommend

import (
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"reflect"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/sHyanorn/recserve/internal/cache"
)

// flightRun sends n requests for the same response to one pipeline with an
// empty cache at the same time. A computation is held at the encoder until
// all n have joined the requests on their key; then the context of the
// first one's request, the leader, is cancelled if cancelLeader. It
// returns the responses and errors in the order of the requests, the number
// of computations and the leader's index.
func flightRun(t *testing.T, n int, broken, cancelLeader bool) (resps []Response, errs []error, computed, leader int) {
	t.Helper()
	p, err := randomPipeline(rand.New(rand.NewPCG(11, 11)), 40, 1)
	if err != nil {
		t.Fatal(err)
	}
	p.Cache = cache.New[Response](10, time.Hour)
	p.noFlight = broken
	type index struct{}
	ctxs, cancels := make([]context.Context, n), make([]context.CancelFunc, n)
	for i := range n {
		ctxs[i], cancels[i] = context.WithCancel(context.WithValue(context.Background(), index{}, i))
		defer cancels[i]()
	}
	var joined, computations atomic.Int32
	all := make(chan struct{})
	p.join = func() {
		if joined.Add(1) == int32(n) {
			close(all)
		}
	}
	leader = -1
	p.stall = func(ctx context.Context, stage string, _ int) error {
		if stage != stageEncoder {
			return nil
		}
		first := computations.Add(1) == 1
		<-all // no response reaches the cache before every request has missed it
		if !first {
			return nil
		}
		leader = ctx.Value(index{}).(int)
		if cancelLeader {
			cancels[leader]()
			<-ctx.Done()
		}
		return ctx.Err()
	}
	resps, errs = make([]Response, n), make([]error, n)
	var wg sync.WaitGroup
	for i := range n {
		wg.Go(func() { resps[i], errs[i] = p.Recommend(ctxs[i], Request{User: "u0", K: 5, TS: 50}) })
	}
	wg.Wait()
	return resps, errs, int(computations.Load()), leader
}

// checkProperty11 returns an error unless n concurrent identical requests
// that miss the cache compute the response once and all get it.
func checkProperty11(t *testing.T, broken bool) error {
	const n = 16
	resps, errs, computed, leader := flightRun(t, n, broken, false)
	if computed != 1 {
		return fmt.Errorf("%d requests computed the response %d times", n, computed)
	}
	for i, resp := range resps {
		if errs[i] != nil {
			return fmt.Errorf("request %d: %w", i, errs[i])
		}
		if resp.Cached == nil || *resp.Cached {
			return fmt.Errorf("request %d: not a cache miss", i)
		}
		if len(resp.Items) != 5 || !reflect.DeepEqual(resp.Items, resps[leader].Items) || resp.ModelVersion != resps[leader].ModelVersion {
			return fmt.Errorf("request %d got %+v, the leader %+v", i, resp, resps[leader])
		}
	}
	return nil
}

func TestProperty11ConcurrentMissesComputeOnce(t *testing.T) {
	if err := checkProperty11(t, false); err != nil {
		t.Fatal(err)
	}
}

func TestProperty11CatchesBrokenVariant(t *testing.T) {
	if err := checkProperty11(t, true); err == nil {
		t.Fatal("requests that bypass the singleflight passed")
	} else {
		t.Log("caught:", err)
	}
}

// A leader whose client goes away fails alone: the requests that waited for
// it start over, one of them computes, and all of them are answered.
func TestFlightLeaderCancelled(t *testing.T) {
	const n = 8
	resps, errs, computed, leader := flightRun(t, n, false, true)
	if computed != 2 {
		t.Fatalf("computed %d times, want 2: the leader and its successor", computed)
	}
	var want []Item
	for i, resp := range resps {
		if i == leader {
			if !errors.Is(errs[i], context.Canceled) {
				t.Fatalf("the leader: %v", errs[i])
			}
			continue
		}
		if errs[i] != nil {
			t.Fatalf("request %d: %v", i, errs[i])
		}
		if want == nil {
			want = resp.Items
		}
		if len(resp.Items) != 5 || !reflect.DeepEqual(resp.Items, want) {
			t.Fatalf("request %d got %v, want %v", i, resp.Items, want)
		}
	}
}
