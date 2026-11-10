// Package cache keeps recent results for a time to live with jitter, so
// that entries written together do not expire together: Cache in process
// memory, at most a fixed number of entries, the least recently used
// evicted first; Valkey on a server that several processes share.
package cache

import (
	"container/list"
	"context"
	"math/rand/v2"
	"sync"
	"time"
)

// Key identifies an entry: a digest of everything the value depends on.
type Key [32]byte

// Jitter is the largest relative change to the time to live of an entry.
const Jitter = 0.1

// Cache is a bounded LRU cache with expiring entries, safe for concurrent
// use.
type Cache[V any] struct {
	mu      sync.Mutex
	size    int
	ttl     time.Duration
	entries map[Key]*list.Element
	order   *list.List // of *entry[V], most recently used first

	now  func() time.Time
	rand func() float64 // in [0, 1)
}

type entry[V any] struct {
	key     Key
	val     V
	expires time.Time
}

// New returns a cache of at most size entries, each kept for ttl, changed
// by up to Jitter either way. A cache of size 0 or less stores nothing.
func New[V any](size int, ttl time.Duration) *Cache[V] {
	size = max(size, 0)
	return &Cache[V]{
		size: size, ttl: ttl, entries: make(map[Key]*list.Element, size), order: list.New(),
		now: time.Now, rand: rand.Float64,
	}
}

// Get returns the value under k, if there is one that has not expired. The
// error is always nil; it is there for the callers that also use Valkey.
func (c *Cache[V]) Get(_ context.Context, k Key) (V, bool, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	el, ok := c.entries[k]
	if !ok {
		var zero V
		return zero, false, nil
	}
	e := el.Value.(*entry[V])
	if !c.now().Before(e.expires) {
		c.remove(el)
		var zero V
		return zero, false, nil
	}
	c.order.MoveToFront(el)
	return e.val, true, nil
}

// Put stores v under k, evicting the least recently used entry if the
// cache is full. The error is always nil.
func (c *Cache[V]) Put(_ context.Context, k Key, v V) error {
	if c.size == 0 {
		return nil
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	ttl := time.Duration(float64(c.ttl) * (1 + Jitter*(2*c.rand()-1)))
	e := &entry[V]{key: k, val: v, expires: c.now().Add(ttl)}
	if el, ok := c.entries[k]; ok {
		el.Value = e
		c.order.MoveToFront(el)
		return nil
	}
	if c.order.Len() >= c.size {
		c.remove(c.order.Back())
	}
	c.entries[k] = c.order.PushFront(e)
	return nil
}

// Len returns the number of entries, expired ones included until they are
// read or evicted.
func (c *Cache[V]) Len() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.order.Len()
}

func (c *Cache[V]) remove(el *list.Element) {
	delete(c.entries, el.Value.(*entry[V]).key)
	c.order.Remove(el)
}
