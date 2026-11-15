package fresh

import (
	"cmp"
	"context"
	"fmt"
	"math/rand/v2"
	"slices"
	"strconv"
	"testing"

	"github.com/sHyanorn/recserve/internal/dataset"
	"github.com/sHyanorn/recserve/internal/history"
)

func event(user, item string, ts int64) history.Event {
	return history.Event{ID: history.EventID(user, item, ts, dataset.Kind), User: user, Item: item, TS: ts}
}

// checkFeed sends a feed random events, many at one time: each user's in
// time order, those at one time in any order, the users' interleaved at
// random, and one in ten sent again later. A state held from the start,
// and one built from the events kept at the end, must read as a state
// given the events by Events through a history that holds them. broken
// sets a feed's broken variant.
func checkFeed(seed uint64, cases int, broken func(*Feed)) error {
	r := rand.New(rand.NewPCG(seed, 0))
	ctx := context.Background()
	for c := range cases {
		byUser := make([][]history.Event, 1+r.IntN(4))
		var rows []dataset.Row
		var stored []history.Event
		ids := map[uint64]bool{}
		for range r.IntN(120) {
			u := r.IntN(len(byUser))
			row := dataset.Row{User: fmt.Sprint("u", u), Item: strconv.Itoa(1 + r.IntN(testItems+1)), TS: int64(r.IntN(30))}
			e := event(row.User, row.Item, row.TS)
			if !ids[e.ID] {
				ids[e.ID] = true
				rows, stored = append(rows, row), append(stored, e)
				byUser[u] = append(byUser[u], e)
			}
		}
		for _, evs := range byUser {
			r.Shuffle(len(evs), func(i, j int) { evs[i], evs[j] = evs[j], evs[i] })
			slices.SortStableFunc(evs, func(a, b history.Event) int { return cmp.Compare(a.TS, b.TS) })
		}
		var sent []history.Event
		for left := len(stored); left > 0; left-- {
			u := r.IntN(len(byUser))
			for len(byUser[u]) == 0 {
				u = (u + 1) % len(byUser)
			}
			sent = append(sent, byUser[u][0])
			byUser[u] = byUser[u][1:]
		}
		for i := range sent {
			if r.IntN(10) == 0 {
				sent = slices.Insert(sent, i+1+r.IntN(len(sent)-i), sent[i])
			}
		}

		f := NewFeed()
		broken(f)
		held, release := f.State("held", testItems, lookup)
		for len(sent) > 0 {
			n := min(len(sent), 1+r.IntN(5))
			f.Add(sent[:n]...)
			sent = sent[n:]
		}
		built, _ := f.State("built", testItems, lookup)
		release()

		h := history.NewMemory()
		if err := h.Append(ctx, stored); err != nil {
			return err
		}
		events, err := Events(ctx, h, lookup, rows)
		if err != nil {
			return err
		}
		want := New(testItems)
		want.Add(events...)
		times := []int64{-1, 31, int64(r.IntN(32))}
		for _, row := range rows {
			times = append(times, row.TS)
		}
		for _, ts := range times {
			w := []int64{1, 3, 7, 40}[r.IntN(4)]
			if err := compare(held, want, ts, w); err != nil {
				return fmt.Errorf("case %d, the state held while the events arrived: %w", c, err)
			}
			if err := compare(built, want, ts, w); err != nil {
				return fmt.Errorf("case %d, the state built from the events kept: %w", c, err)
			}
		}
	}
	return nil
}

func TestFeedPairsAsEvents(t *testing.T) {
	if err := checkFeed(1, 300, func(*Feed) {}); err != nil {
		t.Fatal(err)
	}
}

func TestFeedCatchesBrokenVariants(t *testing.T) {
	if err := checkFeed(1, 300, func(f *Feed) { f.repeat = true }); err == nil {
		t.Error("the check passed a feed that keeps an event taken twice")
	}
	if err := checkFeed(1, 300, func(f *Feed) { f.sameTime = true }); err == nil {
		t.Error("the check passed a feed that pairs events at one time")
	}
}

// An event taken after a later event of its user is not paired with that
// later event in the states held, while a state built afterwards from the
// events kept pairs them as Events does.
func TestFeedLateEvent(t *testing.T) {
	f := NewFeed()
	held, _ := f.State("held", testItems, lookup)
	if added, skipped := f.Add(event("u", "1", 10), event("u", "2", 5), event("u", "2", 5)); added != 2 || skipped != 1 {
		t.Fatalf("Add = %d added, %d skipped; want 2 and 1", added, skipped)
	}
	if got := held.Row(1, 11, 5); len(got) != 0 {
		t.Errorf("held Row(1, 11) = %v, want none: the event at 5 arrived after the one at 10", got)
	}
	built, _ := f.State("built", testItems, lookup)
	if got := built.Row(1, 11, 5); !slices.Equal(got, []int32{2}) {
		t.Errorf("built Row(1, 11) = %v, want [2]", got)
	}
	if got := built.Count(2, 0, 11); got != 1 {
		t.Errorf("built Count(2) = %d, want 1: the event sent twice is kept once", got)
	}
}

// Holders of one key share a state; an event reaches every state held; a
// state is dropped when its last holder releases it, and built anew from
// every event kept when asked for again.
func TestFeedStates(t *testing.T) {
	shifted := func(raw string) (int32, bool) { // other internal IDs
		id, ok := lookup(raw)
		return testItems + 1 - id, ok
	}
	f := NewFeed()
	f.Add(event("u", "1", 1), event("u", "2", 2))
	a, releaseA := f.State("a", testItems, lookup)
	a2, releaseA2 := f.State("a", testItems, lookup)
	b, releaseB := f.State("b", testItems, shifted)
	if a != a2 || a == b {
		t.Fatal("holders of one key do not share a state, or two keys share one")
	}
	releaseA()
	releaseA() // a second call releases nothing
	f.Add(event("u", "3", 3))
	if a.Count(3, 0, 4) != 1 || b.Count(testItems-2, 0, 4) != 1 {
		t.Fatal("an event did not reach every state held")
	}
	if got := b.Row(testItems+1-3, 4, 5); !slices.Equal(got, []int32{testItems + 1 - 2, testItems + 1 - 1}) {
		t.Errorf("b Row(3) = %v, want items 2 and 1 in b's IDs", got)
	}
	releaseA2()
	releaseB()
	f.Add(event("u", "4", 4))
	if a.Count(4, 0, 5) != 0 {
		t.Error("an event reached a state no longer held")
	}
	again, _ := f.State("a", testItems, lookup)
	if again == a || again.Count(4, 0, 5) != 1 || again.Count(1, 0, 5) != 1 {
		t.Error("a state asked for again is not built anew from every event kept")
	}
	if f.Len() != 4 {
		t.Errorf("Len = %d, want 4", f.Len())
	}
}
