package history

import (
	"context"
	"fmt"
	"math"
	"math/rand/v2"
	"slices"
	"sync"
	"testing"
)

// naiveBefore is the reference for property 1: the last n distinct events
// with TS < ts (all of them if n <= 0).
func naiveBefore(events []Event, ts int64, n int) []string {
	var all []Event
	seen := map[uint64]bool{}
	for _, e := range events {
		if !seen[e.ID] {
			seen[e.ID] = true
			all = append(all, e)
		}
	}
	slices.SortFunc(all, order)
	var out []string
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
func checkProperty1(newStore func() Store, seed uint64, cases int) error {
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
				item := fmt.Sprint("i", 1+r.IntN(30))
				e = Event{ID: EventID("u", item, ts, "view"), User: "u", Item: item, TS: ts}
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
			if want := naiveBefore(sent, ts, n); !slices.Equal(got, want) {
				return fmt.Errorf("case %d: Before(ts=%d, n=%d) = %v, want %v", c, ts, n, got, want)
			}
		}
	}
	return nil
}

// checkProperty8 stores a random set of events once in one store and, in
// another, with repeats, shuffled and in batches of random size. It returns
// the first difference between the two at any user, time and n.
func checkProperty8(newStore func() Store, seed uint64, cases int) error {
	r := rand.New(rand.NewPCG(seed, 0))
	ctx := context.Background()
	const times = 20
	for c := range cases {
		users := 1 + r.IntN(4)
		var events []Event
		seen := map[uint64]bool{}
		for range r.IntN(200) {
			user, item, ts := fmt.Sprint("u", r.IntN(users)), fmt.Sprint("i", 1+r.IntN(30)), int64(r.IntN(times))
			kind := []string{"view", "buy"}[r.IntN(2)]
			if e := (Event{ID: EventID(user, item, ts, kind), User: user, Item: item, TS: ts}); !seen[e.ID] {
				seen[e.ID] = true
				events = append(events, e)
			}
		}
		once, many := newStore(), newStore()
		if err := once.Append(ctx, events); err != nil {
			return err
		}
		deliveries := slices.Clone(events)
		for _, e := range events {
			for range r.IntN(3) {
				deliveries = append(deliveries, e)
			}
		}
		r.Shuffle(len(deliveries), func(i, j int) { deliveries[i], deliveries[j] = deliveries[j], deliveries[i] })
		for len(deliveries) > 0 {
			k := 1 + r.IntN(min(len(deliveries), 20))
			if err := many.Append(ctx, deliveries[:k]); err != nil {
				return err
			}
			deliveries = deliveries[k:]
		}
		for u := range users {
			user := fmt.Sprint("u", u)
			for ts := range int64(times + 2) {
				for _, n := range []int{0, 1, 7} {
					want, err := once.Before(ctx, user, ts, n)
					if err != nil {
						return err
					}
					got, err := many.Before(ctx, user, ts, n)
					if err != nil {
						return err
					}
					if !slices.Equal(got, want) {
						return fmt.Errorf("case %d: Before(%s, ts=%d, n=%d) = %v after repeated, shuffled delivery, %v after one", c, user, ts, n, got, want)
					}
				}
			}
		}
	}
	return nil
}

// inclusive is the broken variant of property 1: its upper bound is closed,
// so it also returns the events at the request time.
type inclusive struct{ Store }

func (s inclusive) Before(ctx context.Context, user string, ts int64, n int) ([]string, error) {
	return s.Store.Before(ctx, user, ts+1, n)
}

// bySequence is the broken variant of property 8: it stores each event
// under its delivery number in place of its ID, so a repeated delivery is
// stored again.
type bySequence struct {
	Store
	n *uint64
}

func (s bySequence) Append(ctx context.Context, events []Event) error {
	events = slices.Clone(events)
	for i := range events {
		*s.n++
		events[i].ID = *s.n
	}
	return s.Store.Append(ctx, events)
}

// byArrival is a second broken variant of property 8: it stores each event
// once, but under the number of its first delivery, so events at one time
// are ordered by arrival.
type byArrival struct {
	Store
	first map[uint64]uint64
}

func (s byArrival) Append(ctx context.Context, events []Event) error {
	events = slices.Clone(events)
	for i, e := range events {
		if _, ok := s.first[e.ID]; !ok {
			s.first[e.ID] = uint64(len(s.first))
		}
		events[i].ID = s.first[e.ID]
	}
	return s.Store.Append(ctx, events)
}

// forEachStore runs a contract test against every Store implementation.
// newStore returns an empty store on each call; cases is the number of
// random cases a property check should run, fewer where an operation is a
// network round trip.
func forEachStore(t *testing.T, f func(t *testing.T, newStore func() Store, cases int)) {
	t.Run("memory", func(t *testing.T) { f(t, func() Store { return NewMemory() }, 300) })
	t.Run("valkey", func(t *testing.T) { f(t, valkeyStores(t), 40) })
}

func TestProperty1PointInTime(t *testing.T) {
	forEachStore(t, func(t *testing.T, newStore func() Store, cases int) {
		if err := checkProperty1(newStore, 1, cases); err != nil {
			t.Fatal(err)
		}
	})
}

func TestProperty1CatchesBrokenVariant(t *testing.T) {
	forEachStore(t, func(t *testing.T, newStore func() Store, cases int) {
		if err := checkProperty1(func() Store { return inclusive{newStore()} }, 1, cases); err == nil {
			t.Fatal("the check passed a store that returns events at the request time")
		}
	})
}

func TestProperty8RedeliveryAndOrder(t *testing.T) {
	forEachStore(t, func(t *testing.T, newStore func() Store, cases int) {
		if err := checkProperty8(newStore, 1, cases); err != nil {
			t.Fatal(err)
		}
	})
}

func TestProperty8CatchesBrokenVariant(t *testing.T) {
	forEachStore(t, func(t *testing.T, newStore func() Store, cases int) {
		if err := checkProperty8(func() Store { return bySequence{newStore(), new(uint64)} }, 1, cases); err == nil {
			t.Fatal("the check passed a store that stores a repeated delivery again")
		}
		if err := checkProperty8(func() Store { return byArrival{newStore(), map[uint64]uint64{}} }, 1, cases); err == nil {
			t.Fatal("the check passed a store that orders events at one time by arrival")
		}
	})
}

// Events at the same time are ordered by ID as a number, in every store:
// IDs whose decimal or unpadded form sorts otherwise as text.
func TestSameTimeOrderIsByEventID(t *testing.T) {
	ids := []uint64{2, 10, 0x100, 0x0a00000000000000, 1 << 63, math.MaxUint64}
	forEachStore(t, func(t *testing.T, newStore func() Store, _ int) {
		s, ctx := newStore(), context.Background()
		var want []string
		for i, id := range ids {
			want = append(want, fmt.Sprint("i", i))
			if i > 0 && ids[i-1] >= id {
				t.Fatal("ids must ascend")
			}
		}
		for _, i := range rand.Perm(len(ids)) {
			if err := s.Append(ctx, []Event{{ID: ids[i], User: "u", Item: want[i], TS: 5}}); err != nil {
				t.Fatal(err)
			}
		}
		for n, want := range map[int][]string{0: want, 2: want[len(want)-2:]} {
			if got, err := s.Before(ctx, "u", 6, n); err != nil || !slices.Equal(got, want) {
				t.Fatalf("Before(n=%d) = %v, %v, want %v", n, got, err, want)
			}
		}
	})
}

// The largest event time is stored exactly, and a request time beyond it
// sees every event.
func TestMaxTS(t *testing.T) {
	forEachStore(t, func(t *testing.T, newStore func() Store, _ int) {
		s, ctx := newStore(), context.Background()
		if err := s.Append(ctx, []Event{{ID: 1, User: "u", Item: "a", TS: MaxTS - 1}, {ID: 2, User: "u", Item: "b", TS: MaxTS}}); err != nil {
			t.Fatal(err)
		}
		for ts, want := range map[int64][]string{MaxTS - 1: nil, MaxTS: {"a"}, MaxTS + 1: {"a", "b"}, math.MaxInt64: {"a", "b"}} {
			if got, err := s.Before(ctx, "u", ts, 0); err != nil || !slices.Equal(got, want) {
				t.Fatalf("Before(ts=%d) = %v, %v, want %v", ts, got, err, want)
			}
		}
	})
}

// One Append of many users and of one long history, as a preload sends it,
// stores every event.
func TestLargeBatch(t *testing.T) {
	forEachStore(t, func(t *testing.T, newStore func() Store, _ int) {
		s, ctx := newStore(), context.Background()
		var events []Event
		for i := range 2500 {
			ts := int64(1 + i%700)
			events = append(events, Event{ID: EventID("long", fmt.Sprint(i), ts, ""), User: "long", Item: fmt.Sprint(i), TS: ts})
		}
		for u := range 350 {
			user := fmt.Sprint("u", u)
			events = append(events, Event{ID: EventID(user, "a", 3, ""), User: user, Item: "a", TS: 3})
		}
		if err := s.Append(ctx, events); err != nil {
			t.Fatal(err)
		}
		if got, err := s.Before(ctx, "long", 701, 0); err != nil || !slices.Equal(got, naiveBefore(events[:2500], 701, 0)) {
			t.Fatalf("the long history has %d events, %v", len(got), err)
		}
		for u := range 350 {
			if got, err := s.Before(ctx, fmt.Sprint("u", u), 4, 0); err != nil || !slices.Equal(got, []string{"a"}) {
				t.Fatalf("user %d: %v, %v", u, got, err)
			}
		}
	})
}

// An event keeps its first time: storing its ID again with another time
// changes nothing, in every store.
func TestStoredEventIsNotChanged(t *testing.T) {
	forEachStore(t, func(t *testing.T, newStore func() Store, _ int) {
		s, ctx := newStore(), context.Background()
		for _, ts := range []int64{5, 1, 9} {
			if err := s.Append(ctx, []Event{{ID: 7, User: "u", Item: "a", TS: ts}}); err != nil {
				t.Fatal(err)
			}
		}
		for ts, want := range map[int64][]string{5: nil, 6: {"a"}, 10: {"a"}} {
			if got, err := s.Before(ctx, "u", ts, 0); err != nil || !slices.Equal(got, want) {
				t.Fatalf("Before(ts=%d) = %v, %v, want %v", ts, got, err, want)
			}
		}
	})
}

func TestUsersAreSeparate(t *testing.T) {
	forEachStore(t, func(t *testing.T, newStore func() Store, _ int) {
		s, ctx := newStore(), context.Background()
		if err := s.Append(ctx, []Event{{ID: 1, User: "a", Item: "x", TS: 1}, {ID: 2, User: "b", Item: "y", TS: 1}}); err != nil {
			t.Fatal(err)
		}
		for user, want := range map[string][]string{"a": {"x"}, "c": nil} {
			got, err := s.Before(ctx, user, 2, 10)
			if err != nil || !slices.Equal(got, want) {
				t.Fatalf("%s: %v, %v", user, got, err)
			}
		}
	})
}

func TestConcurrentAppendAndRead(t *testing.T) {
	forEachStore(t, func(t *testing.T, newStore func() Store, _ int) {
		s, ctx := newStore(), context.Background()
		var wg sync.WaitGroup
		for w := range 8 {
			wg.Go(func() {
				for i := range 500 {
					u := fmt.Sprint("u", i%10)
					if err := s.Append(ctx, []Event{{ID: uint64(w*1000 + i), User: u, Item: fmt.Sprint(i), TS: int64(i)}}); err != nil {
						t.Error(err)
						return
					}
					if _, err := s.Before(ctx, u, int64(i), 50); err != nil {
						t.Error(err)
						return
					}
				}
			})
		}
		wg.Wait()
		for i := range 10 {
			got, err := s.Before(ctx, fmt.Sprint("u", i), 1<<40, 0)
			if err != nil {
				t.Fatal(err)
			}
			if len(got) != 8*50 {
				t.Fatalf("user %d has %d events, want %d", i, len(got), 8*50)
			}
		}
	})
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
