package cache

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math/rand/v2"
	"time"

	"github.com/valkey-io/valkey-go"
)

// Valkey is a cache on one Valkey server, shared by every process that
// uses the server (docs/DECISIONS.md D036). An entry is the string
// c:<key in hex> holding the value as JSON, set with the time to live
// changed by up to Jitter either way. The cache has no size of its own: the
// server evicts entries when its memory is full.
type Valkey[V any] struct {
	c      valkey.Client
	ttl    time.Duration
	prefix string
}

// NewValkey connects to the Valkey server at addr; it fails if the server
// does not answer. Entries live for ttl, at least a millisecond.
func NewValkey[V any](addr string, ttl time.Duration) (*Valkey[V], error) {
	if ttl < time.Millisecond {
		return nil, fmt.Errorf("cache time to live %v: Valkey takes none below 1ms", ttl)
	}
	c, err := valkey.NewClient(valkey.ClientOption{ // the options of the history store (D038)
		InitAddress:       []string{addr},
		ForceSingleClient: true,
		DisableCache:      true,
		DisableRetry:      true,
	})
	if err != nil {
		return nil, fmt.Errorf("valkey %s: %w", addr, err)
	}
	return &Valkey[V]{c: c, ttl: ttl, prefix: "c:"}, nil
}

// Close closes the connections to the server.
func (v *Valkey[V]) Close() { v.c.Close() }

func (v *Valkey[V]) key(k Key) string { return v.prefix + hex.EncodeToString(k[:]) }

// Get returns the value under k, if the server holds one.
func (v *Valkey[V]) Get(ctx context.Context, k Key) (V, bool, error) {
	var zero V
	b, err := v.c.Do(ctx, v.c.B().Get().Key(v.key(k)).Build()).AsBytes()
	if valkey.IsValkeyNil(err) {
		return zero, false, nil
	}
	if err != nil {
		return zero, false, fmt.Errorf("valkey get: %w", err)
	}
	var val V
	if err := json.Unmarshal(b, &val); err != nil {
		return zero, false, fmt.Errorf("cache entry: %w", err)
	}
	return val, true, nil
}

// Put stores val under k.
func (v *Valkey[V]) Put(ctx context.Context, k Key, val V) error {
	b, err := json.Marshal(val)
	if err != nil {
		return fmt.Errorf("cache entry: %w", err)
	}
	ttl := max(time.Duration(float64(v.ttl)*(1+Jitter*(2*rand.Float64()-1))), time.Millisecond)
	if err := v.c.Do(ctx, v.c.B().Set().Key(v.key(k)).Value(valkey.BinaryString(b)).Px(ttl).Build()).Error(); err != nil {
		return fmt.Errorf("valkey set: %w", err)
	}
	return nil
}
