// Package breaker puts the gateway's calls to Valkey behind one circuit
// breaker (docs/DECISIONS.md D037): once enough of its calls in a window
// failed it is open, and a call fails at once with ErrOpen, without
// reaching the server, until a later call is let through to try the server
// again.
package breaker

import (
	"context"
	"errors"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/sony/gobreaker/v2"

	"github.com/sHyanorn/recserve/internal/cache"
	"github.com/sHyanorn/recserve/internal/history"
	"github.com/sHyanorn/recserve/internal/valkeyclient"
)

// ErrOpen is the error of a call the breaker did not let through.
var ErrOpen = errors.New("circuit breaker open")

// Breaker is a circuit breaker shared by the calls made through it.
type Breaker struct {
	cb    *gobreaker.CircuitBreaker[struct{}]
	calls *prometheus.CounterVec
}

// Settings say when a closed breaker opens and how long it stays open.
type Settings struct {
	// Window is the fixed window in which calls are counted: the counts
	// start again when it ends, and when the breaker closes.
	Window time.Duration
	// MinCalls is how many calls must have ended in the window, failed or
	// succeeded, before a failure can open the breaker.
	MinCalls uint32
	// FailureRate is the share of those calls at or above which a failure
	// opens it.
	FailureRate float64
	// Open is how long it stays open before it lets a call try the server.
	Open time.Duration
}

// Gateway is the gateway's settings (docs/DECISIONS.md D037, revision of
// M7): a window of 1 s, as Sentinel's statIntervalMs; at least 100 calls,
// resilience4j's minimumNumberOfCalls; half of them failed, the failure
// rate of resilience4j and Hystrix; open for 1 s.
var Gateway = Settings{Window: time.Second, MinCalls: 100, FailureRate: 0.5, Open: time.Second}

// New returns a closed breaker with the given settings. It opens on a
// failure that leaves at least s.MinCalls calls ended in the current window
// and at least s.FailureRate of them failed, and stays open for s.Open;
// then one call at a time is let through, and the first that succeeds
// closes it, while one that fails opens it again. A call whose context was
// cancelled counts as neither, nor does one that ended waiting for a first
// connection (valkeyclient.ErrConnecting): neither says that the server
// fails. A call that ends in a later window than it began, or after the
// breaker changed state, is not counted. onChange, if not nil, is called
// with the states of every change.
func New(s Settings, onChange func(from, to string)) *Breaker {
	return &Breaker{cb: gobreaker.NewCircuitBreaker[struct{}](gobreaker.Settings{
		MaxRequests: 1,
		Interval:    s.Window,
		Timeout:     s.Open,
		ReadyToTrip: func(c gobreaker.Counts) bool { return trips(c, s) },
		IsExcluded: func(err error) bool {
			return errors.Is(err, context.Canceled) || errors.Is(err, valkeyclient.ErrConnecting)
		},
		OnStateChange: func(_ string, from, to gobreaker.State) {
			if onChange != nil {
				onChange(from.String(), to.String())
			}
		},
	}), calls: prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "recserve_valkey_calls_total",
		Help: "Valkey calls through the circuit breaker, by call (history, cache_get, cache_put) and result: ok, timeout (the call's budget ended), error, cancelled, connecting (no connection within the budget), or refused (the breaker was open).",
	}, []string{"call", "result"})}
}

// trips says whether counts c open a breaker with settings s.
func trips(c gobreaker.Counts, s Settings) bool {
	ended := c.TotalSuccesses + c.TotalFailures
	return ended >= s.MinCalls && float64(c.TotalFailures) >= s.FailureRate*float64(ended)
}

// Collectors returns the breaker's metrics: its calls by result, and its
// state, 1 for the state it is in and 0 for the others.
func (b *Breaker) Collectors() []prometheus.Collector {
	cs := []prometheus.Collector{b.calls}
	for _, s := range []gobreaker.State{gobreaker.StateClosed, gobreaker.StateHalfOpen, gobreaker.StateOpen} {
		cs = append(cs, prometheus.NewGaugeFunc(prometheus.GaugeOpts{
			Name:        "recserve_valkey_breaker_state",
			Help:        "State of the circuit breaker on Valkey: 1 for the state it is in (closed, half-open or open), 0 for the others.",
			ConstLabels: prometheus.Labels{"state": s.String()},
		}, func() float64 {
			if b.cb.State() == s {
				return 1
			}
			return 0
		}))
	}
	return cs
}

// do runs call, named name in the metrics, through the breaker.
func (b *Breaker) do(name string, call func() error) error {
	_, err := b.cb.Execute(func() (struct{}, error) { return struct{}{}, call() })
	if errors.Is(err, gobreaker.ErrOpenState) || errors.Is(err, gobreaker.ErrTooManyRequests) {
		err = ErrOpen
	}
	b.calls.WithLabelValues(name, result(err)).Inc()
	return err
}

// result names the outcome of a call for the metrics.
func result(err error) string {
	switch {
	case err == nil:
		return "ok"
	case errors.Is(err, ErrOpen):
		return "refused"
	case errors.Is(err, context.Canceled):
		return "cancelled"
	case errors.Is(err, valkeyclient.ErrConnecting):
		return "connecting"
	case errors.Is(err, context.DeadlineExceeded):
		return "timeout"
	}
	return "error"
}

// Store is a history store whose reads go through a breaker. Appends go
// straight to the store.
type Store struct {
	history.Store
	Breaker *Breaker
}

// Before implements history.Store.
func (s Store) Before(ctx context.Context, user string, ts int64, n int) (items []string, err error) {
	err = s.Breaker.do("history", func() error {
		items, err = s.Store.Before(ctx, user, ts, n)
		return err
	})
	return items, err
}

// Cache is a cache whose reads and writes go through a breaker.
type Cache[V any] struct {
	Cache interface {
		Get(ctx context.Context, k cache.Key) (V, bool, error)
		Put(ctx context.Context, k cache.Key, val V) error
	}
	Breaker *Breaker
}

// Get returns the value under k, if the cache holds one.
func (c Cache[V]) Get(ctx context.Context, k cache.Key) (val V, ok bool, err error) {
	err = c.Breaker.do("cache_get", func() error {
		val, ok, err = c.Cache.Get(ctx, k)
		return err
	})
	return val, ok, err
}

// Put stores val under k.
func (c Cache[V]) Put(ctx context.Context, k cache.Key, val V) error {
	return c.Breaker.do("cache_put", func() error { return c.Cache.Put(ctx, k, val) })
}
