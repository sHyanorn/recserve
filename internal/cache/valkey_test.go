package cache

import (
	"context"
	"fmt"
	"math/rand/v2"
	"testing"
	"time"

	"github.com/sHyanorn/recserve/internal/infratest"
)

type value struct {
	Name   string    `json:"name"`
	Scores []float64 `json:"scores"`
}

// testValkey returns a cache on the test server under a prefix of its own.
func testValkey(t *testing.T, ttl time.Duration) *Valkey[value] {
	t.Helper()
	v, err := NewValkey[value](infratest.ValkeyAddr(t), ttl)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(v.Close)
	v.prefix = fmt.Sprintf("test-%016x:", rand.Uint64())
	return v
}

// A value comes back as it was stored, every float64 bit for bit, to any
// client of the server; a key never stored is a miss.
func TestValkeyRoundTrip(t *testing.T) {
	a := testValkey(t, time.Minute)
	b := testValkey(t, time.Minute)
	b.prefix = a.prefix
	r := rand.New(rand.NewPCG(1, 0))
	want := value{Name: "v", Scores: []float64{0, 1, 0.1, 1.0 / 3, -2.5e-300, 1.7976931348623157e308, 5e-324}}
	for range 1000 {
		want.Scores = append(want.Scores, r.NormFloat64()*r.ExpFloat64())
	}
	if _, ok := get(b, key(1)); ok {
		t.Fatal("hit before anything was stored")
	}
	put(a, key(1), want)
	got, ok := get(b, key(1))
	if !ok || got.Name != want.Name || len(got.Scores) != len(want.Scores) {
		t.Fatalf("got %v, %q and %d scores", ok, got.Name, len(got.Scores))
	}
	for i, s := range want.Scores {
		if got.Scores[i] != s {
			t.Fatalf("score %d: stored %v, read %v", i, s, got.Scores[i])
		}
	}
	if _, ok := get(b, key(2)); ok {
		t.Fatal("hit under another key")
	}
}

// Entries are set with the time to live changed by up to Jitter either way,
// not all with the same one, and are gone after it.
func TestValkeyExpiresWithJitter(t *testing.T) {
	const ttl = 100 * time.Second
	v := testValkey(t, ttl)
	lo, hi := time.Duration(1<<62), time.Duration(0)
	for i := range 200 {
		put(v, key(i), value{})
		ms, err := v.c.Do(context.Background(), v.c.B().Pttl().Key(v.key(key(i))).Build()).AsInt64()
		if err != nil {
			t.Fatal(err)
		}
		left := time.Duration(ms) * time.Millisecond
		lo, hi = min(lo, left), max(hi, left)
	}
	// A time to live is read a moment after it is set.
	if lo < 89*time.Second || hi > 110*time.Second || hi-lo < 10*time.Second {
		t.Fatalf("times to live from %v to %v, want them spread over 90s to 110s", lo, hi)
	}

	short := testValkey(t, 200*time.Millisecond)
	put(short, key(1), value{Name: "v"})
	if _, ok := get(short, key(1)); !ok {
		t.Fatal("miss right after the put")
	}
	time.Sleep(450 * time.Millisecond)
	if _, ok := get(short, key(1)); ok {
		t.Fatal("alive after twice the time to live")
	}
}

// An entry that is not the value's JSON is an error, not a hit; so is a
// call whose context has ended.
func TestValkeyErrors(t *testing.T) {
	v := testValkey(t, time.Minute)
	ctx := context.Background()
	if err := v.c.Do(ctx, v.c.B().Set().Key(v.key(key(1))).Value("not JSON").Px(time.Minute).Build()).Error(); err != nil {
		t.Fatal(err)
	}
	if _, ok, err := v.Get(ctx, key(1)); err == nil || ok {
		t.Fatalf("a malformed entry gave ok %v, err %v", ok, err)
	}
	ended, cancel := context.WithCancel(ctx)
	cancel()
	if _, ok, err := v.Get(ended, key(2)); err == nil || ok {
		t.Fatalf("a get under an ended context gave ok %v, err %v", ok, err)
	}
	if err := v.Put(ended, key(2), value{}); err == nil {
		t.Fatal("a put under an ended context gave no error")
	}
	if _, err := NewValkey[value](infratest.ValkeyAddr(t), 0); err == nil {
		t.Fatal("a time to live of 0 was accepted")
	}
}
