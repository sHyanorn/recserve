package breaker

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"testing"
	"time"

	"github.com/sHyanorn/recserve/internal/cache"
	"github.com/sHyanorn/recserve/internal/history"
	"github.com/sHyanorn/recserve/internal/valkeyclient"
)

// server stands for Valkey behind a store and a cache: it counts the calls
// that reach it and fails them with err.
type server struct {
	calls int
	err   error
}

func (s *server) Append(context.Context, []history.Event) error { s.calls++; return s.err }

func (s *server) Before(context.Context, string, int64, int) ([]string, error) {
	s.calls++
	return []string{"a"}, s.err
}

func (s *server) Get(context.Context, cache.Key) (int, bool, error) {
	s.calls++
	return 7, s.err == nil, s.err
}

func (s *server) Put(context.Context, cache.Key, int) error { s.calls++; return s.err }

func TestBreaker(t *testing.T) {
	ctx := context.Background()
	down := errors.New("down")
	const openFor = 300 * time.Millisecond // long against a stall of the test between two calls
	var changes []string
	srv := &server{}
	b := New(3, openFor, func(from, to string) { changes = append(changes, from+">"+to) })
	store, c := Store{srv, b}, Cache[int]{srv, b}

	if items, err := store.Before(ctx, "u", 1, 0); err != nil || len(items) != 1 {
		t.Fatalf("closed: %v, %v", items, err)
	}
	if v, ok, err := c.Get(ctx, cache.Key{}); err != nil || !ok || v != 7 {
		t.Fatalf("closed: %v, %v, %v", v, ok, err)
	}

	// Cancelled calls are no failures, nor are calls that ended waiting
	// for a first connection, and a success between failures starts the
	// count again.
	for _, excluded := range []error{context.Canceled, fmt.Errorf("%w: %w", valkeyclient.ErrConnecting, context.DeadlineExceeded)} {
		srv.err = excluded
		for range 5 {
			if _, err := store.Before(ctx, "u", 1, 0); !errors.Is(err, excluded) {
				t.Fatal(err)
			}
		}
	}
	srv.err = down
	for range 2 {
		if err := c.Put(ctx, cache.Key{}, 1); !errors.Is(err, down) {
			t.Fatal(err)
		}
	}
	srv.err = nil
	if err := c.Put(ctx, cache.Key{}, 1); err != nil {
		t.Fatal(err)
	}

	// Three failures in a row, of the store and the cache together, open it.
	srv.err = down
	if _, err := store.Before(ctx, "u", 1, 0); !errors.Is(err, down) {
		t.Fatal(err)
	}
	if _, _, err := c.Get(ctx, cache.Key{}); !errors.Is(err, down) {
		t.Fatal(err)
	}
	if err := c.Put(ctx, cache.Key{}, 1); !errors.Is(err, down) {
		t.Fatal(err)
	}
	reached := srv.calls
	if _, err := store.Before(ctx, "u", 1, 0); !errors.Is(err, ErrOpen) {
		t.Fatalf("open: %v", err)
	}
	if _, _, err := c.Get(ctx, cache.Key{}); !errors.Is(err, ErrOpen) {
		t.Fatalf("open: %v", err)
	}
	if err := c.Put(ctx, cache.Key{}, 1); !errors.Is(err, ErrOpen) {
		t.Fatalf("open: %v", err)
	}
	if srv.calls != reached {
		t.Fatalf("%d calls reached the server through the open breaker", srv.calls-reached)
	}
	// An append does not go through the breaker.
	if err := store.Append(ctx, nil); !errors.Is(err, down) {
		t.Fatal(err)
	}

	// After the open time one call tries the server; a failure opens the
	// breaker again, a success closes it.
	time.Sleep(openFor + 10*time.Millisecond)
	if _, err := store.Before(ctx, "u", 1, 0); !errors.Is(err, down) {
		t.Fatalf("the trial call: %v", err)
	}
	if _, err := store.Before(ctx, "u", 1, 0); !errors.Is(err, ErrOpen) {
		t.Fatalf("after a failed trial: %v", err)
	}
	time.Sleep(openFor + 10*time.Millisecond)
	srv.err = nil
	for range 2 {
		if _, err := store.Before(ctx, "u", 1, 0); err != nil {
			t.Fatalf("after the server is back: %v", err)
		}
	}
	want := []string{"closed>open", "open>half-open", "half-open>open", "open>half-open", "half-open>closed"}
	if !slices.Equal(changes, want) {
		t.Fatalf("state changes %v, want %v", changes, want)
	}
}
