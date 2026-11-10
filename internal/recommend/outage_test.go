package recommend

import (
	"context"
	"fmt"
	"math/rand/v2"
	"slices"
	"testing"
	"time"

	"github.com/sHyanorn/recserve/internal/breaker"
	"github.com/sHyanorn/recserve/internal/cache"
	"github.com/sHyanorn/recserve/internal/history"
	"github.com/sHyanorn/recserve/internal/infratest"
)

// checkProperty10 takes Valkey away from a pipeline whose history store and
// cache are on it, behind one circuit breaker, by a proxy that refuses
// connections or accepts them and never answers (blackhole). A user's
// stored history equals the recent items its requests carry, so a response
// computed from the recent items has the cache key of a healthy one.
//
// Valkey first comes back in the middle of a request, after the history
// read failed and before the response could be stored. During the outage
// that follows, every request must be answered within its bound from the
// recent items alone and report history_unavailable. Once Valkey is back, a
// request with that history must never hit the cache, and is soon answered
// in full. broken stores degraded responses.
func checkProperty10(t *testing.T, blackhole, broken bool) error {
	t.Helper()
	const (
		budget   = 100 * time.Millisecond
		openFor  = time.Second // longer than the requests of the outage take, stalls included
		bound    = time.Second // far below the client's own timeouts of 5 and 10 s
		requests = 20
	)
	ctx := context.Background()
	proxy := infratest.NewProxy(t, infratest.ValkeyAddr(t))
	take := proxy.Refuse
	if blackhole {
		take = proxy.Blackhole
	}
	store := history.NewValkey(infratest.ValkeyClient(t, proxy.Addr()))
	vc, err := cache.NewValkey[Response](infratest.ValkeyClient(t, proxy.Addr()), time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	p, err := randomPipeline(rand.New(rand.NewPCG(10, 10)), 40, 0)
	if err != nil {
		t.Fatal(err)
	}
	p.Bundle.Popular = []int32{1, 2, 3, 4, 5, 6, 7, 8}
	user := fmt.Sprintf("p10-%016x", rand.Uint64())
	recent := []string{"i3", "i7", "i1", "i9"}
	events := make([]history.Event, len(recent))
	for i, item := range recent {
		events[i] = history.Event{ID: uint64(i), User: user, Item: item, TS: int64(i)}
	}
	if err := store.Append(ctx, events); err != nil {
		t.Fatal(err)
	}
	healthy := Request{User: user, K: 5, TS: 10}
	degraded := Request{User: user, K: 5, TS: 10, Recent: recent}
	ref := *p
	ref.History = storeWith(t, events...)
	want, err := ref.Recommend(ctx, healthy)
	if err != nil {
		t.Fatal(err)
	}
	if len(want.Items) != 5 {
		t.Fatalf("the reference response holds %d items", len(want.Items))
	}

	br := breaker.New(5, openFor, nil)
	p.History = breaker.Store{Store: store, Breaker: br}
	p.Budgets.History = budget
	p.cacheDegraded = broken
	fromRecent := func() (time.Duration, error) {
		start := time.Now()
		resp, err := p.Recommend(ctx, degraded)
		took := time.Since(start)
		switch {
		case err != nil:
			return took, err
		case !slices.Equal(resp.Degraded, []string{HistoryUnavailable}):
			return took, fmt.Errorf("without Valkey: degraded %v", resp.Degraded)
		case !slices.Equal(resp.Items, want.Items):
			return took, fmt.Errorf("without Valkey: items %v, from the recent items alone %v", resp.Items, want.Items)
		case took > bound:
			return took, fmt.Errorf("without Valkey a request took %v", took)
		}
		return took, nil
	}

	// back lets the clients through again and waits until each has
	// replaced its connections: it holds several and takes one at random
	// for a call, and a call on a connection closed since would fail once
	// more and hide what the cache holds.
	sc := salted{vc, rand.Uint64()}
	back := func() {
		proxy.Pass()
		for ok := 0; ok < 50; ok++ {
			_, herr := store.Before(ctx, user, healthy.TS, 0)
			_, _, cerr := vc.Get(ctx, cache.Key{})
			if herr != nil || cerr != nil {
				ok = 0
				time.Sleep(time.Millisecond)
			}
		}
	}
	p.Cache = breaker.Cache[Response]{Cache: sc, Breaker: br}
	take()
	p.stall = func(_ context.Context, stage string, _ int) error {
		if stage != stageEncoder {
			return nil
		}
		// The history read has failed, and Valkey is back before the
		// response could be stored.
		back()
		return nil
	}
	if _, err := fromRecent(); err != nil {
		return err
	}
	p.stall = nil
	take()
	var took time.Duration
	for range requests {
		if took, err = fromRecent(); err != nil {
			return err
		}
	}
	if blackhole && took > budget/2 {
		return fmt.Errorf("request %d of the outage took %v: it waited for Valkey through the breaker", requests, took)
	}

	back()
	for deadline := time.Now().Add(10 * time.Second); ; time.Sleep(openFor / 10) {
		resp, err := p.Recommend(ctx, healthy)
		switch {
		case err != nil:
			return err
		case resp.Cached != nil && *resp.Cached:
			return fmt.Errorf("with Valkey back, the cache holds a response of the outage, degraded %v", resp.Degraded)
		case len(resp.Degraded) == 0:
			if !slices.Equal(resp.Items, want.Items) {
				return fmt.Errorf("with Valkey back: items %v, want %v", resp.Items, want.Items)
			}
			return nil
		case time.Now().After(deadline):
			return fmt.Errorf("still degraded after Valkey came back: %v (%v)", resp.Degraded, resp.Failures)
		}
	}
}

func TestProperty10ValkeyUnavailable(t *testing.T) {
	for _, blackhole := range []bool{false, true} {
		t.Run(fmt.Sprintf("blackhole=%v", blackhole), func(t *testing.T) {
			if err := checkProperty10(t, blackhole, false); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestProperty10CatchesBrokenVariant(t *testing.T) {
	for _, blackhole := range []bool{false, true} {
		t.Run(fmt.Sprintf("blackhole=%v", blackhole), func(t *testing.T) {
			if err := checkProperty10(t, blackhole, true); err == nil {
				t.Fatal("a pipeline that caches degraded responses passed")
			} else {
				t.Log("caught:", err)
			}
		})
	}
}
