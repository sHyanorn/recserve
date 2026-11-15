package fresh

import (
	"cmp"
	"slices"
	"strings"
	"sync"

	"github.com/sHyanorn/recserve/internal/history"
)

// Feed keeps every event a gateway takes, by user, and the states built
// from them in the internal IDs of the bundles in use (D062). It is safe
// for concurrent use.
type Feed struct {
	mu     sync.Mutex
	users  map[string]*user
	items  map[string]string // each raw item ID once, shared by its events
	states map[string]*held  // by the SHA-256 of a bundle's items.ids

	repeat   bool // test only: the broken variant that keeps an event taken again
	sameTime bool // test only: the broken variant that pairs an event with those at its time
}

// user is a user's events, by time, then event ID.
type user struct{ evs []kept }

// kept is an event as a feed keeps it.
type kept struct {
	ts   int64
	id   uint64
	item string
}

func order(a, b kept) int { return cmp.Or(cmp.Compare(a.ts, b.ts), cmp.Compare(a.id, b.id)) }

// held is a state and the number of its holders.
type held struct {
	state  *State
	lookup func(raw string) (int32, bool)
	refs   int
}

// NewFeed returns a feed without events.
func NewFeed() *Feed {
	return &Feed{users: map[string]*user{}, items: map[string]string{}, states: map[string]*held{}}
}

// Add takes events. One taken before is skipped, as the history store
// skips it. Each other one is kept and added to every state held, paired
// with the items of its user's at most PairSpan latest events before its
// time among those taken so far. It returns the numbers of events added
// and skipped.
func (f *Feed) Add(events ...history.Event) (added, skipped int) {
	f.mu.Lock()
	defer f.mu.Unlock()
	batches := make(map[*held][]Event, len(f.states))
	for _, e := range events {
		u := f.users[e.User]
		if u == nil {
			// A key of its own: the event's strings may share a larger one.
			u = &user{}
			f.users[strings.Clone(e.User)] = u
		}
		k := kept{ts: e.TS, id: e.ID}
		i, found := slices.BinarySearchFunc(u.evs, k, order)
		if found && !f.repeat {
			skipped++
			continue
		}
		if k.item = f.items[e.Item]; k.item == "" {
			k.item = strings.Clone(e.Item)
			f.items[k.item] = k.item
		}
		u.evs = slices.Insert(u.evs, i, k)
		added++
		for _, h := range f.states {
			if se, ok := f.paired(u.evs, i, h.lookup); ok {
				batches[h] = append(batches[h], se)
			}
		}
	}
	for h, b := range batches {
		h.state.Add(b...)
	}
	return added, skipped
}

// paired returns evs[i] as a state takes it, with the items of the at most
// PairSpan events before its time that precede it in evs: the event that
// history.Store.Before and Events give for a history holding evs. It is
// false if lookup does not know the event's item.
func (f *Feed) paired(evs []kept, i int, lookup func(raw string) (int32, bool)) (Event, bool) {
	item, ok := lookup(evs[i].item)
	if !ok {
		return Event{}, false
	}
	j := i
	for j > 0 && evs[j-1].ts == evs[i].ts && !f.sameTime {
		j--
	}
	e := Event{Item: item, TS: evs[i].ts}
	for _, p := range evs[max(0, j-PairSpan):j] {
		if id, ok := lookup(p.item); ok {
			e.Prev = append(e.Prev, id)
		}
	}
	return e, true
}

// State returns the state for the bundles whose items.ids have the SHA-256
// key, in the internal IDs 1 to items that lookup gives, and the function
// that releases it. A state that is not held is built from every event
// kept, each paired as Events pairs it; the feed adds every event it takes
// later to each state until its last holder releases it.
func (f *Feed) State(key string, items int, lookup func(raw string) (int32, bool)) (*State, func()) {
	f.mu.Lock()
	defer f.mu.Unlock()
	h := f.states[key]
	if h == nil {
		h = &held{state: New(items), lookup: lookup}
		var all []Event
		for _, u := range f.users {
			for i := range u.evs {
				if e, ok := f.paired(u.evs, i, lookup); ok {
					all = append(all, e)
				}
			}
		}
		// In time order every event goes to the end of the state's lists.
		slices.SortStableFunc(all, func(a, b Event) int { return cmp.Compare(a.TS, b.TS) })
		h.state.Add(all...)
		f.states[key] = h
	}
	h.refs++
	var once sync.Once
	return h.state, func() {
		once.Do(func() {
			f.mu.Lock()
			defer f.mu.Unlock()
			if h.refs--; h.refs == 0 && f.states[key] == h {
				delete(f.states, key)
			}
		})
	}
}

// Len returns the number of events kept.
func (f *Feed) Len() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	n := 0
	for _, u := range f.users {
		n += len(u.evs)
	}
	return n
}
