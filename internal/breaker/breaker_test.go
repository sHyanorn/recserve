package breaker

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"

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
	b := New(Settings{Window: time.Hour, MinCalls: 6, FailureRate: 0.5, Open: openFor}, func(from, to string) { changes = append(changes, from+">"+to) })
	store, c := Store{srv, b}, Cache[int]{srv, b}

	if items, err := store.Before(ctx, "u", 1, 0); err != nil || len(items) != 1 {
		t.Fatalf("closed: %v, %v", items, err)
	}
	if v, ok, err := c.Get(ctx, cache.Key{}); err != nil || !ok || v != 7 {
		t.Fatalf("closed: %v, %v, %v", v, ok, err)
	}

	// Cancelled calls are no failures, nor are calls that ended waiting
	// for a first connection: they count as neither, or the breaker would
	// open before the call below that opens it, or after.
	for _, excluded := range []error{context.Canceled, fmt.Errorf("%w: %w", valkeyclient.ErrConnecting, context.DeadlineExceeded)} {
		srv.err = excluded
		for range 5 {
			if _, err := store.Before(ctx, "u", 1, 0); !errors.Is(err, excluded) {
				t.Fatal(err)
			}
		}
	}
	// With fewer calls ended than MinCalls, failures do not open it.
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

	// The failure that makes 6 calls ended, 3 of them failed, of the store
	// and the cache together, opens it.
	srv.err = down
	if _, err := store.Before(ctx, "u", 1, 0); !errors.Is(err, down) {
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

// metrics returns the values b's collectors report, by metric name and
// labels, as "name,label=value,...".
func metrics(t *testing.T, b *Breaker) map[string]float64 {
	t.Helper()
	reg := prometheus.NewPedanticRegistry()
	reg.MustRegister(b.Collectors()...)
	families, err := reg.Gather()
	if err != nil {
		t.Fatal(err)
	}
	m := map[string]float64{}
	for _, f := range families {
		for _, x := range f.GetMetric() {
			key := f.GetName()
			for _, l := range x.GetLabel() {
				key += "," + l.GetName() + "=" + l.GetValue()
			}
			m[key] = x.GetCounter().GetValue() + x.GetGauge().GetValue()
		}
	}
	return m
}

func TestMetrics(t *testing.T) {
	ctx := context.Background()
	const openFor = 300 * time.Millisecond
	srv := &server{}
	b := New(Settings{Window: time.Hour, MinCalls: 6, FailureRate: 0.5, Open: openFor}, nil)
	store, c := Store{srv, b}, Cache[int]{srv, b}
	state := func() string {
		t.Helper()
		var in []string
		for k, v := range metrics(t, b) {
			if s, ok := strings.CutPrefix(k, "recserve_valkey_breaker_state,state="); ok && v == 1 {
				in = append(in, s)
			}
		}
		if len(in) != 1 {
			t.Fatalf("breaker in states %v, want one", in)
		}
		return in[0]
	}
	if s := state(); s != "closed" {
		t.Fatalf("state %s, want closed", s)
	}

	history := func() error { _, err := store.Before(ctx, "u", 1, 0); return err }
	get := func() error { _, _, err := c.Get(ctx, cache.Key{}); return err }
	put := func() error { return c.Put(ctx, cache.Key{}, 1) }
	timeout := fmt.Errorf("read: %w", context.DeadlineExceeded)
	connecting := fmt.Errorf("%w: %w", valkeyclient.ErrConnecting, context.DeadlineExceeded)
	down := errors.New("down")
	type step struct {
		call      func() error
		err, want error // the server's error, and the call's
	}
	run := func(steps ...step) {
		t.Helper()
		for i, s := range steps {
			srv.err = s.err
			if err := s.call(); !errors.Is(err, s.want) || (s.want == nil) != (err == nil) {
				t.Fatalf("step %d: %v, want %v", i, err, s.want)
			}
		}
	}
	run(step{history, nil, nil}, step{get, nil, nil}, step{put, nil, nil},
		step{get, context.Canceled, context.Canceled}, step{history, connecting, connecting},
		step{history, timeout, timeout}, step{history, timeout, timeout}, step{put, down, down})
	if s := state(); s != "open" {
		t.Fatalf("after 3 failures of 6 calls: state %s, want open", s)
	}
	run(step{history, nil, ErrOpen})
	time.Sleep(openFor + 10*time.Millisecond)
	if s := state(); s != "half-open" {
		t.Fatalf("after the open time: state %s, want half-open", s)
	}
	run(step{get, nil, nil})
	if s := state(); s != "closed" {
		t.Fatalf("after a trial that succeeded: state %s, want closed", s)
	}

	calls := map[string]float64{}
	for k, v := range metrics(t, b) {
		if s, ok := strings.CutPrefix(k, "recserve_valkey_calls_total,"); ok {
			calls[s] = v
		}
	}
	want := map[string]float64{
		"call=history,result=ok":          1,
		"call=history,result=connecting":  1,
		"call=history,result=timeout":     2,
		"call=history,result=refused":     1,
		"call=cache_get,result=ok":        2,
		"call=cache_get,result=cancelled": 1,
		"call=cache_put,result=ok":        1,
		"call=cache_put,result=error":     1,
	}
	if !maps.Equal(calls, want) {
		t.Fatalf("calls %v, want %v", calls, want)
	}
}

// TestStateMetricOneAtATime scrapes the state metric while another
// goroutine moves the breaker between closed, open and half-open: every
// scrape must report exactly one state as 1.
func TestStateMetricOneAtATime(t *testing.T) {
	b := New(Settings{Window: time.Hour, MinCalls: 1, FailureRate: 0.5, Open: time.Microsecond}, nil)
	reg := prometheus.NewPedanticRegistry()
	reg.MustRegister(b.Collectors()...)
	down := errors.New("down")
	stop := make(chan struct{})
	done := make(chan struct{})
	go func() {
		defer close(done)
		for {
			select {
			case <-stop:
				return
			default:
			}
			// The first call opens it; the second closes it once half-open.
			if err := b.do("history", func() error { return down }); !errors.Is(err, down) && !errors.Is(err, ErrOpen) {
				t.Error(err)
				return
			}
			if err := b.do("history", func() error { return nil }); err != nil && !errors.Is(err, ErrOpen) {
				t.Error(err)
				return
			}
		}
	}()
	defer func() { close(stop); <-done }()
	bad := map[int]int{} // scrapes by the number of states reported as 1
	for range 20000 {
		families, err := reg.Gather()
		if err != nil {
			t.Fatal(err)
		}
		in := 0
		for _, f := range families {
			if f.GetName() != "recserve_valkey_breaker_state" {
				continue
			}
			if len(f.GetMetric()) != 3 {
				t.Fatalf("%d state series, want 3", len(f.GetMetric()))
			}
			for _, m := range f.GetMetric() {
				if m.GetGauge().GetValue() == 1 {
					in++
				}
			}
		}
		if in != 1 {
			bad[in]++
		}
	}
	if len(bad) > 0 {
		t.Fatalf("scrapes by the number of states reported as 1, other than one: %v", bad)
	}
}
