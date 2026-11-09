package cache

import (
	"fmt"
	"sync"
	"testing"
	"time"
)

func key(i int) Key { return Key{byte(i), byte(i >> 8)} }

// testCache returns a cache whose clock the test sets and whose jitter
// draws come from draw.
func testCache(size int, ttl time.Duration, draw *float64) (*Cache[int], *time.Time) {
	c := New[int](size, ttl)
	now := time.Unix(0, 0)
	c.now = func() time.Time { return now }
	c.rand = func() float64 { return *draw }
	return c, &now
}

func TestEvictsLeastRecentlyUsed(t *testing.T) {
	draw := 0.5
	c, _ := testCache(3, time.Hour, &draw)
	for i := range 3 {
		c.Put(key(i), i)
	}
	c.Get(key(0)) // 1 is now the least recently used
	c.Put(key(3), 3)
	if _, ok := c.Get(key(1)); ok {
		t.Fatal("1 not evicted")
	}
	for _, i := range []int{0, 2, 3} {
		if v, ok := c.Get(key(i)); !ok || v != i {
			t.Fatalf("Get(%d) = %d, %v", i, v, ok)
		}
	}
	c.Put(key(2), 20) // replacing an entry evicts nothing
	if v, _ := c.Get(key(2)); v != 20 || c.Len() != 3 {
		t.Fatalf("replaced value %d, %d entries", v, c.Len())
	}
}

// An entry lives for the time to live scaled by 1 + Jitter*(2u-1), u the
// draw: from 0.9 to 1.1 times it.
func TestExpiresWithJitter(t *testing.T) {
	const ttl = 100 * time.Second
	for _, c := range []struct {
		draw float64
		life time.Duration
	}{{0, 90 * time.Second}, {0.5, ttl}, {0.999999, 110 * time.Second}} {
		draw := c.draw
		cc, now := testCache(10, ttl, &draw)
		cc.Put(key(1), 1)
		*now = now.Add(c.life - time.Second)
		if _, ok := cc.Get(key(1)); !ok {
			t.Fatalf("draw %v: expired before %v", c.draw, c.life)
		}
		*now = now.Add(time.Second)
		if _, ok := cc.Get(key(1)); ok {
			t.Fatalf("draw %v: alive at %v", c.draw, c.life)
		}
		if cc.Len() != 0 {
			t.Fatalf("draw %v: expired entry kept", c.draw)
		}
	}
}

func TestConcurrentUse(t *testing.T) {
	c := New[int](64, time.Minute)
	var wg sync.WaitGroup
	for g := range 8 {
		wg.Go(func() {
			for i := range 10000 {
				k := key((g*7 + i) % 200)
				if v, ok := c.Get(k); ok && v != int(k[0])|int(k[1])<<8 {
					panic(fmt.Sprint("wrong value ", v, " for ", k))
				}
				c.Put(k, int(k[0])|int(k[1])<<8)
			}
		})
	}
	wg.Wait()
	if c.Len() != 64 {
		t.Fatalf("%d entries, want 64", c.Len())
	}
}

func TestNoSizeStoresNothing(t *testing.T) {
	for _, size := range []int{0, -1} {
		c := New[int](size, time.Hour)
		c.Put(key(1), 1)
		if _, ok := c.Get(key(1)); ok || c.Len() != 0 {
			t.Fatalf("size %d: stored an entry", size)
		}
	}
}
