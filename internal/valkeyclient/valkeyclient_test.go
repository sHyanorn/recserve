package valkeyclient_test

import (
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"testing"
	"time"

	"github.com/valkey-io/valkey-go"

	"github.com/sHyanorn/recserve/internal/breaker"
	"github.com/sHyanorn/recserve/internal/cache"
	"github.com/sHyanorn/recserve/internal/history"
	"github.com/sHyanorn/recserve/internal/infratest"
	"github.com/sHyanorn/recserve/internal/valkeyclient"
)

// checkSlowSetup runs what a gateway request asks of Valkey, a history read
// and a cache read within one budget and a cache write within another,
// through the gateway's circuit breaker, against a healthy server on which
// opening a connection takes longer than the budget. At the start, and
// again after every connection was closed, requests may fail while
// connections open; the breaker must never change its state, a request
// must soon succeed, and from then on every request must.
func checkSlowSetup(t *testing.T, client func(addr string) (*valkeyclient.Client, error)) error {
	t.Helper()
	const (
		budget   = 250 * time.Millisecond // long against a stall of the test
		setup    = 600 * time.Millisecond
		warmUp   = 10 * time.Second
		requests = 50
	)
	proxy := infratest.NewProxy(t, infratest.ValkeyAddr(t))
	proxy.Slow(setup)
	var clients [2]*valkeyclient.Client
	for i := range clients {
		c, err := client(proxy.Addr())
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(c.Close)
		clients[i] = c
	}
	vc, err := cache.NewValkey[string](clients[1], time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	var changes []string
	br := breaker.New(5, time.Second, func(from, to string) { changes = append(changes, from+">"+to) })
	store := breaker.Store{Store: history.NewValkey(clients[0]), Breaker: br}
	results := breaker.Cache[string]{Cache: vc, Breaker: br}
	user := fmt.Sprintf("slow-setup-%016x", rand.Uint64())
	key := cache.Key{0: byte(rand.Uint32()), 1: byte(rand.Uint32()), 2: byte(rand.Uint32()), 3: byte(rand.Uint32())}

	request := func() error {
		ctx, cancel := context.WithTimeout(context.Background(), budget)
		defer cancel()
		if _, err := store.Before(ctx, user, 10, 0); err != nil {
			return fmt.Errorf("history read: %w", err)
		}
		if _, _, err := results.Get(ctx, key); err != nil {
			return fmt.Errorf("cache read: %w", err)
		}
		ctx, cancel = context.WithTimeout(context.Background(), budget)
		defer cancel()
		if err := results.Put(ctx, key, "v"); err != nil {
			return fmt.Errorf("cache write: %w", err)
		}
		return nil
	}
	phase := func(name string) error {
		start := time.Now()
		for i := 0; i < requests; i++ {
			err := request()
			switch {
			case len(changes) > 0:
				return fmt.Errorf("%s the breaker went %v with Valkey healthy; last error: %w", name, changes, err)
			case err == nil:
			case i > 0:
				return fmt.Errorf("%s, request %d after the first that succeeded: %w", name, i, err)
			case time.Since(start) > warmUp:
				return fmt.Errorf("%s no request succeeded within %v: %w", name, warmUp, err)
			default:
				i-- // still warming up
				time.Sleep(time.Millisecond)
			}
		}
		t.Logf("%s the first request succeeded after %v", name, time.Since(start).Round(time.Millisecond))
		return nil
	}
	if err := phase("at the start"); err != nil {
		return err
	}
	proxy.Pass() // closes every connection
	return phase("after every connection was closed")
}

func TestSlowSetupLeavesBreakerClosed(t *testing.T) {
	if err := checkSlowSetup(t, valkeyclient.New); err != nil {
		t.Fatal(err)
	}
}

func TestSlowSetupCatchesDialingInCalls(t *testing.T) {
	if err := checkSlowSetup(t, valkeyclient.NewDialingInCalls); err == nil {
		t.Fatal("a client that opens its connections within the calls passed")
	} else {
		t.Log("caught:", err)
	}
}

// With the server gone a call fails at once, with an error that is not
// ErrConnecting, once the attempt to connect again has failed; with the
// server back, calls succeed again.
func TestServerGoneAndBack(t *testing.T) {
	proxy := infratest.NewProxy(t, infratest.ValkeyAddr(t))
	c := infratest.ValkeyClient(t, proxy.Addr())
	ping := func() (time.Duration, error) {
		ctx, cancel := context.WithTimeout(context.Background(), 250*time.Millisecond)
		defer cancel()
		start := time.Now()
		err := c.Do(ctx, func(v valkey.Client) error { return v.Do(ctx, v.B().Ping().Build()).Error() })
		return time.Since(start), err
	}
	// until polls ping until it gives the wanted outcome.
	until := func(what string, ok func(time.Duration, error) bool) {
		t.Helper()
		for deadline := time.Now().Add(10 * time.Second); ; time.Sleep(time.Millisecond) {
			took, err := ping()
			if ok(took, err) {
				return
			}
			if time.Now().After(deadline) {
				t.Fatalf("%s: still %v after %v", what, err, took)
			}
		}
	}
	if _, err := ping(); err != nil {
		t.Fatal(err)
	}
	proxy.Refuse()
	until("with the server gone", func(took time.Duration, err error) bool {
		return err != nil && !errors.Is(err, valkeyclient.ErrConnecting) && took < 50*time.Millisecond
	})
	proxy.Pass()
	until("with the server back", func(_ time.Duration, err error) bool { return err == nil })

	c.Close()
	if _, err := ping(); !errors.Is(err, valkeyclient.ErrClosed) {
		t.Fatalf("after Close: %v", err)
	}
}
