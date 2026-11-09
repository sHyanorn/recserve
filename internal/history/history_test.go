package history

import (
	"context"
	"fmt"
	"math/rand/v2"
	"slices"
	"sync"
	"testing"
)

// naiveBefore is the reference for property 1: the last n distinct events
// with TS < ts (all of them if n <= 0).
func naiveBefore(events []Event, ts int64, n int) []int32 {
	var all []Event
	seen := map[uint64]bool{}
	for _, e := range events {
		if !seen[e.ID] {
			seen[e.ID] = true
			all = append(all, e)
		}
	}
	slices.SortFunc(all, order)
	var out []int32
	for _, e := range all {
		if e.TS < ts {
			out = append(out, e.Item)
		}
	}
	if n <= 0 {
		return out
	}
	return out[max(0, len(out)-n):]
}

// checkProperty1 drives a store with random events, repeated deliveries and
// queries, and returns the first disagreement with naiveBefore.
func checkProperty1(newStore func() *Memory, seed uint64, cases int) error {
	r := rand.New(rand.NewPCG(seed, 0))
	ctx := context.Background()
	for c := range cases {
		s := newStore()
		var sent []Event
		nEvents := r.IntN(600)
		for range nEvents {
			var e Event
			if len(sent) > 0 && r.IntN(5) == 0 {
				e = sent[r.IntN(len(sent))] // redelivery
			} else {
				ts := int64(r.IntN(50)) // few distinct times, so ties are common
				item := int32(1 + r.IntN(30))
				e = Event{ID: EventID("u", fmt.Sprint(item), ts, "view"), User: "u", Item: item, TS: ts}
			}
			sent = append(sent, e)
			if err := s.Append(ctx, []Event{e}); err != nil {
				return err
			}
		}
		for range 20 {
			ts, n := int64(r.IntN(52)), r.IntN(610)-5
			got, err := s.Before(ctx, "u", ts, n)
			if err != nil {
				return err
			}
			if want := naiveBefore(sent, ts, n); !slices.Equal(got, want) && len(got)+len(want) > 0 {
				return fmt.Errorf("case %d: Before(ts=%d, n=%d) = %v, want %v", c, ts, n, got, want)
			}
		}
	}
	return nil
}

func TestProperty1PointInTime(t *testing.T) {
	if err := checkProperty1(NewMemory, 1, 300); err != nil {
		t.Fatal(err)
	}
}

func TestProperty1CatchesBrokenVariant(t *testing.T) {
	broken := func() *Memory { m := NewMemory(); m.inclusive = true; return m }
	if err := checkProperty1(broken, 1, 300); err == nil {
		t.Fatal("the check passed a store that returns events at the request time")
	}
}

func TestUsersAreSeparate(t *testing.T) {
	s, ctx := NewMemory(), context.Background()
	_ = s.Append(ctx, []Event{{ID: 1, User: "a", Item: 1, TS: 1}, {ID: 2, User: "b", Item: 2, TS: 1}})
	if got, _ := s.Before(ctx, "a", 2, 10); !slices.Equal(got, []int32{1}) {
		t.Fatalf("a: %v", got)
	}
	if got, _ := s.Before(ctx, "c", 2, 10); len(got) != 0 {
		t.Fatalf("c: %v", got)
	}
}

func TestConcurrentAppendAndRead(t *testing.T) {
	s, ctx := NewMemory(), context.Background()
	var wg sync.WaitGroup
	for w := range 8 {
		wg.Go(func() {
			for i := range 500 {
				u := fmt.Sprint("u", i%10)
				_ = s.Append(ctx, []Event{{ID: uint64(w*1000 + i), User: u, Item: int32(i), TS: int64(i)}})
				_, _ = s.Before(ctx, u, int64(i), 50)
			}
		})
	}
	wg.Wait()
	for i := range 10 {
		got, _ := s.Before(ctx, fmt.Sprint("u", i), 1<<40, 0)
		if len(got) != 8*50 {
			t.Fatalf("user %d has %d events, want %d", i, len(got), 8*50)
		}
	}
}

func TestEventID(t *testing.T) {
	// The ID must not change across processes and releases: stored histories depend on it.
	if got := EventID("u", "i", 1, "view"); got != 0xd6d78860cf2db18 {
		t.Fatalf("EventID changed: %#x", got)
	}
	if EventID("ab", "c", 1, "") == EventID("a", "bc", 1, "") {
		t.Fatal("field boundaries collide")
	}
}
