// Package history stores each user's recent events and answers
// point-in-time queries: the items a user interacted with strictly before a
// given time (docs/SPEC.md §7, property 1).
package history

import (
	"cmp"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"hash/maphash"
	"slices"
	"strconv"
	"sync"
)

// Event is one user event. ID identifies the event: storing the same ID
// twice stores it once. Item is the raw item ID, so a stored history does
// not depend on any bundle's internal IDs.
type Event struct {
	ID   uint64
	User string
	Item string
	TS   int64 // milliseconds since the Unix epoch
}

// MaxTS is the largest event time /v1/events accepts, 2^53 ms: a Valkey
// sorted set scores an event by its time as a float64, which is exact up to
// there.
const MaxTS = 1 << 53

// EventID derives an event's ID from its fields, so an identical event sent twice is one event.
func EventID(user, item string, ts int64, kind string) uint64 {
	h := sha256.New()
	var n [4]byte
	for _, s := range []string{user, item, strconv.FormatInt(ts, 10), kind} {
		binary.LittleEndian.PutUint32(n[:], uint32(len(s)))
		h.Write(n[:])
		h.Write([]byte(s))
	}
	return binary.LittleEndian.Uint64(h.Sum(nil))
}

// Store keeps user histories.
type Store interface {
	// Append stores events; events already stored are ignored.
	Append(ctx context.Context, events []Event) error
	// Before returns the raw item IDs of the user's last n stored events
	// with TS < ts, oldest first; all of them if n <= 0.
	Before(ctx context.Context, user string, ts int64, n int) ([]string, error)
}

// order sorts events by time, then by ID.
func order(a, b Event) int {
	if c := cmp.Compare(a.TS, b.TS); c != 0 {
		return c
	}
	return cmp.Compare(a.ID, b.ID)
}

const shards = 64

// Memory is an in-process Store.
type Memory struct {
	seed   maphash.Seed
	shards [shards]struct {
		sync.RWMutex
		users map[string][]Event // sorted by order
	}
}

// NewMemory returns an empty in-process store.
func NewMemory() *Memory {
	m := &Memory{seed: maphash.MakeSeed()}
	for i := range m.shards {
		m.shards[i].users = map[string][]Event{}
	}
	return m
}

func (m *Memory) shard(user string) int { return int(maphash.String(m.seed, user) % shards) }

// Append implements Store.
func (m *Memory) Append(_ context.Context, events []Event) error {
	for _, e := range events {
		s := &m.shards[m.shard(e.User)]
		s.Lock()
		evs := s.users[e.User]
		i, found := slices.BinarySearchFunc(evs, e, order)
		if !found && !slices.ContainsFunc(evs, func(x Event) bool { return x.ID == e.ID }) {
			s.users[e.User] = slices.Insert(evs, i, e)
		}
		s.Unlock()
	}
	return nil
}

// Before implements Store.
func (m *Memory) Before(_ context.Context, user string, ts int64, n int) ([]string, error) {
	s := &m.shards[m.shard(user)]
	s.RLock()
	defer s.RUnlock()
	evs := s.users[user]
	end, _ := slices.BinarySearchFunc(evs, ts, func(e Event, t int64) int {
		if e.TS < t {
			return -1
		}
		return 1
	})
	start := 0
	if n > 0 {
		start = max(0, end-n)
	}
	out := make([]string, 0, end-start)
	for _, e := range evs[start:end] {
		out = append(out, e.Item)
	}
	return out, nil
}
