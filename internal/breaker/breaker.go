// Package breaker puts the gateway's calls to Valkey behind one circuit
// breaker (docs/DECISIONS.md D037): after a run of failed calls it is open,
// and a call fails at once with ErrOpen, without reaching the server, until
// a later call is let through to try the server again.
package breaker

import (
	"context"
	"errors"
	"time"

	"github.com/sony/gobreaker/v2"

	"github.com/sHyanorn/recserve/internal/cache"
	"github.com/sHyanorn/recserve/internal/history"
	"github.com/sHyanorn/recserve/internal/valkeyclient"
)

// ErrOpen is the error of a call the breaker did not let through.
var ErrOpen = errors.New("circuit breaker open")

// Breaker is a circuit breaker shared by the calls made through it.
type Breaker struct {
	cb *gobreaker.CircuitBreaker[struct{}]
}

// New returns a closed breaker. It opens after the given number of failures
// in a row and stays open for the given time; then one call at a time is
// let through, and the first that succeeds closes it, while one that fails
// opens it again. A call whose context was cancelled counts as neither, nor
// does one that ended waiting for a first connection
// (valkeyclient.ErrConnecting): neither says that the server fails.
// onChange, if not nil, is called with the states of every change.
func New(failures uint32, open time.Duration, onChange func(from, to string)) *Breaker {
	return &Breaker{gobreaker.NewCircuitBreaker[struct{}](gobreaker.Settings{
		MaxRequests: 1,
		Timeout:     open,
		ReadyToTrip: func(c gobreaker.Counts) bool { return c.ConsecutiveFailures >= failures },
		IsExcluded: func(err error) bool {
			return errors.Is(err, context.Canceled) || errors.Is(err, valkeyclient.ErrConnecting)
		},
		OnStateChange: func(_ string, from, to gobreaker.State) {
			if onChange != nil {
				onChange(from.String(), to.String())
			}
		},
	})}
}

func (b *Breaker) do(call func() error) error {
	_, err := b.cb.Execute(func() (struct{}, error) { return struct{}{}, call() })
	if errors.Is(err, gobreaker.ErrOpenState) || errors.Is(err, gobreaker.ErrTooManyRequests) {
		return ErrOpen
	}
	return err
}

// Store is a history store whose reads go through a breaker. Appends go
// straight to the store.
type Store struct {
	history.Store
	Breaker *Breaker
}

// Before implements history.Store.
func (s Store) Before(ctx context.Context, user string, ts int64, n int) (items []string, err error) {
	err = s.Breaker.do(func() error {
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
	err = c.Breaker.do(func() error {
		val, ok, err = c.Cache.Get(ctx, k)
		return err
	})
	return val, ok, err
}

// Put stores val under k.
func (c Cache[V]) Put(ctx context.Context, k cache.Key, val V) error {
	return c.Breaker.do(func() error { return c.Cache.Put(ctx, k, val) })
}
