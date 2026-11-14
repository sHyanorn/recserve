// Package fresh keeps state from the event stream that a request reads at
// its own time: each item's events, the most popular items over a window
// and co-occurrence pairs made as events arrive (docs/DECISIONS.md D059).
// Every read takes the request time ts and uses only events before it, as
// history.Store.Before does, so a state given every event of a data set
// answers a request as a state given only the events before it would.
package fresh

import (
	"cmp"
	"context"
	"slices"
	"sync"
	"sync/atomic"

	"github.com/sHyanorn/recserve/internal/dataset"
	"github.com/sHyanorn/recserve/internal/history"
)

// PairSpan is the number of a user's latest events an event is paired with.
const PairSpan = 5

// Event is an event as the state takes it, in internal item IDs: its item,
// its time, and the items of the user's at most PairSpan latest events
// before it.
type Event struct {
	Item int32
	TS   int64
	Prev []int32
}

// stamped is an item at a time: an event, or the partner of a pair.
type stamped struct {
	ts   int64
	item int32
}

// State holds the events given to it. It is safe for concurrent use. An
// event given twice counts twice: removing repeated deliveries is the
// caller's.
type State struct {
	mu     sync.RWMutex
	times  [][]int64   // item i's event times, ascending
	pairs  [][]stamped // item i's pairs: each partner at the pair's time, by time
	events []stamped   // every event, by time
	// popular is the last popular list read, kept while no event is added.
	popular atomic.Pointer[popularList]
}

type popularList struct {
	lo, hi, n int // the window's events, events[lo:hi], and the list's length
	ids       []int32
}

// New returns an empty state for the internal IDs 1 to items.
func New(items int) *State {
	return &State{times: make([][]int64, items+1), pairs: make([][]stamped, items+1)}
}

// Add adds events, in any order. An event pairs its item with each of its
// previous items other than itself, in the rows of both, at the event's
// time.
func (s *State) Add(events ...Event) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, e := range events {
		s.times[e.Item] = insert(s.times[e.Item], e.TS, func(t int64) int64 { return t })
		s.events = insert(s.events, stamped{e.TS, e.Item}, stampedTS)
		for _, p := range e.Prev {
			if p != e.Item {
				s.pairs[e.Item] = insert(s.pairs[e.Item], stamped{e.TS, p}, stampedTS)
				s.pairs[p] = insert(s.pairs[p], stamped{e.TS, e.Item}, stampedTS)
			}
		}
	}
	s.popular.Store(nil)
}

func stampedTS(s stamped) int64 { return s.ts }

// insert puts x into list, kept ascending by ts, after the elements of
// equal ts: at the end when x is not earlier than the last.
func insert[T any](list []T, x T, ts func(T) int64) []T {
	t := ts(x)
	i, _ := slices.BinarySearchFunc(list, t, func(e T, t int64) int {
		if ts(e) <= t {
			return -1
		}
		return 1
	})
	return slices.Insert(list, i, x)
}

// before returns the number of elements of list with ts below t.
func before[T any](list []T, t int64, ts func(T) int64) int {
	i, _ := slices.BinarySearchFunc(list, t, func(e T, t int64) int {
		if ts(e) < t {
			return -1
		}
		return 1
	})
	return i
}

// Count returns the number of item's events in [from, to).
func (s *State) Count(item int32, from, to int64) int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	id := func(t int64) int64 { return t }
	return max(0, before(s.times[item], to, id)-before(s.times[item], from, id))
}

// First returns the time of item's first event before ts, and false if it
// has none.
func (s *State) First(item int32, ts int64) (int64, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if t := s.times[item]; len(t) > 0 && t[0] < ts {
		return t[0], true
	}
	return 0, false
}

// Popular returns the at most n items with the most events in [ts - w, ts),
// at least one each, ties to the lower ID.
func (s *State) Popular(ts, w int64, n int) []int32 {
	s.mu.RLock()
	defer s.mu.RUnlock()
	lo, hi := before(s.events, ts-w, stampedTS), before(s.events, ts, stampedTS)
	if p := s.popular.Load(); p != nil && p.lo == lo && p.hi == hi && p.n == n {
		return p.ids
	}
	counts := map[int32]int{}
	for _, e := range s.events[lo:hi] {
		counts[e.item]++
	}
	ids := top(counts, n)
	s.popular.Store(&popularList{lo, hi, n, ids})
	return ids
}

// Row returns the at most n items paired with item most often before ts,
// ties to the lower ID.
func (s *State) Row(item int32, ts int64, n int) []int32 {
	s.mu.RLock()
	defer s.mu.RUnlock()
	pairs := s.pairs[item]
	counts := map[int32]int{}
	for _, p := range pairs[:before(pairs, ts, stampedTS)] {
		counts[p.item]++
	}
	return top(counts, n)
}

// top returns the at most n IDs with the highest counts, ties to the lower ID.
func top(counts map[int32]int, n int) []int32 {
	ids := make([]int32, 0, len(counts))
	for id := range counts {
		ids = append(ids, id)
	}
	slices.SortFunc(ids, func(a, b int32) int { return cmp.Or(cmp.Compare(counts[b], counts[a]), cmp.Compare(a, b)) })
	return ids[:min(n, len(ids))]
}

// Events returns rows as the state takes them, each with the items of the
// user's at most PairSpan latest events before it, as h reads them; h must
// hold the rows. Rows and previous items that lookup does not know are
// left out.
func Events(ctx context.Context, h history.Store, lookup func(raw string) (int32, bool), rows []dataset.Row) ([]Event, error) {
	out := make([]Event, 0, len(rows))
	for _, r := range rows {
		item, ok := lookup(r.Item)
		if !ok {
			continue
		}
		raw, err := h.Before(ctx, r.User, r.TS, PairSpan)
		if err != nil {
			return nil, err
		}
		e := Event{Item: item, TS: r.TS}
		for _, p := range raw {
			if id, ok := lookup(p); ok {
				e.Prev = append(e.Prev, id)
			}
		}
		out = append(out, e)
	}
	return out, nil
}
