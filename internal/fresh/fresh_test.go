package fresh

import (
	"cmp"
	"context"
	"fmt"
	"math"
	"math/rand/v2"
	"slices"
	"strconv"
	"testing"

	"github.com/sHyanorn/recserve/internal/dataset"
	"github.com/sHyanorn/recserve/internal/history"
)

// reader is what a request reads from a state.
type reader interface {
	Count(item int32, from, to int64) int
	First(item int32, ts int64) (int64, bool)
	Popular(ts, w int64, n int) []int32
	Row(item int32, ts int64, n int) []int32
}

// inclusive is a broken variant of the state: its reads also see the
// events at the request time.
type inclusive struct{ *State }

func (s inclusive) Count(item int32, from, to int64) int     { return s.State.Count(item, from, to+1) }
func (s inclusive) First(item int32, ts int64) (int64, bool) { return s.State.First(item, ts+1) }
func (s inclusive) Popular(ts, w int64, n int) []int32       { return s.State.Popular(ts+1, w+1, n) }
func (s inclusive) Row(item int32, ts int64, n int) []int32  { return s.State.Row(item, ts+1, n) }

// unbounded is a broken variant of the history that previous items are
// read from: it returns the user's latest events, whatever their time, so
// an event is paired with later ones.
type unbounded struct{ history.Store }

func (h unbounded) Before(ctx context.Context, user string, _ int64, n int) ([]string, error) {
	return h.Store.Before(ctx, user, math.MaxInt64, n)
}

const testItems = 12

// lookup knows the items "1" to "12"; the tests also send "13".
func lookup(raw string) (int32, bool) {
	id, err := strconv.Atoi(raw)
	return int32(id), err == nil && id >= 1 && id <= testItems
}

// naive computes a request's reads at ts from the events, without a State.
type naive struct {
	events []history.Event // as stored: by time, then event ID
}

func (v naive) Count(item int32, from, to int64) int {
	n := 0
	for _, e := range v.events {
		if id, ok := lookup(e.Item); ok && id == item && from <= e.TS && e.TS < to {
			n++
		}
	}
	return n
}

func (v naive) First(item int32, ts int64) (int64, bool) {
	for _, e := range v.events {
		if id, ok := lookup(e.Item); ok && id == item && e.TS < ts {
			return e.TS, true
		}
	}
	return 0, false
}

func (v naive) Popular(ts, w int64, n int) []int32 {
	counts := map[int32]int{}
	for _, e := range v.events {
		if id, ok := lookup(e.Item); ok && ts-w <= e.TS && e.TS < ts {
			counts[id]++
		}
	}
	return ranked(counts, n)
}

func (v naive) Row(item int32, ts int64, n int) []int32 {
	counts := map[int32]int{}
	for k, e := range v.events {
		id, ok := lookup(e.Item)
		if !ok || e.TS >= ts {
			continue
		}
		// The user's latest PairSpan events before e, known items or not,
		// counted back from e.
		var latest []int32
		seen := 0
		for j := k - 1; j >= 0 && seen < PairSpan; j-- {
			p := v.events[j]
			if p.User != e.User || p.TS >= e.TS {
				continue
			}
			seen++
			if pid, ok := lookup(p.Item); ok {
				latest = append(latest, pid)
			}
		}
		for _, p := range latest {
			if p == id {
				continue
			}
			if id == item {
				counts[p]++
			}
			if p == item {
				counts[id]++
			}
		}
	}
	return ranked(counts, n)
}

// ranked returns the at most n IDs with the highest counts, ties to the
// lower ID, written apart from top.
func ranked(counts map[int32]int, n int) []int32 {
	type kv struct {
		id int32
		n  int
	}
	var all []kv
	for id, c := range counts {
		all = append(all, kv{id, c})
	}
	slices.SortFunc(all, func(a, b kv) int { return cmp.Or(b.n-a.n, int(a.id-b.id)) })
	out := []int32{}
	for _, x := range all[:min(n, len(all))] {
		out = append(out, x.id)
	}
	return out
}

// compare returns the first read on which got and want differ at ts.
func compare(got, want reader, ts, w int64) error {
	if g, x := got.Popular(ts, w, 5), want.Popular(ts, w, 5); !slices.Equal(g, x) {
		return fmt.Errorf("Popular(ts=%d, w=%d) = %v, want %v", ts, w, g, x)
	}
	for item := int32(1); item <= testItems; item++ {
		if g, x := got.Count(item, ts-w, ts), want.Count(item, ts-w, ts); g != x {
			return fmt.Errorf("Count(%d, [%d, %d)) = %d, want %d", item, ts-w, ts, g, x)
		}
		g, gok := got.First(item, ts)
		x, xok := want.First(item, ts)
		if g != x || gok != xok {
			return fmt.Errorf("First(%d, %d) = %d %v, want %d %v", item, ts, g, gok, x, xok)
		}
		if g, x := got.Row(item, ts, 4), want.Row(item, ts, 4); !slices.Equal(g, x) {
			return fmt.Errorf("Row(%d, %d) = %v, want %v", item, ts, g, x)
		}
	}
	return nil
}

// checkPointInTime gives a state random events, many at one time, and at
// every event's time and at other times compares its reads, through read,
// with those of a state given only the events before that time and with a
// naive count. Previous items come from prev(h), h holding the events.
func checkPointInTime(seed uint64, cases int, read func(*State) reader, prev func(history.Store) history.Store) error {
	r := rand.New(rand.NewPCG(seed, 0))
	ctx := context.Background()
	for c := range cases {
		users := 1 + r.IntN(4)
		var rows []dataset.Row
		var stored []history.Event
		ids := map[uint64]bool{}
		for range r.IntN(120) {
			row := dataset.Row{User: fmt.Sprint("u", r.IntN(users)), Item: strconv.Itoa(1 + r.IntN(testItems+1)), TS: int64(r.IntN(30))}
			e := history.Event{ID: history.EventID(row.User, row.Item, row.TS, dataset.Kind), User: row.User, Item: row.Item, TS: row.TS}
			if !ids[e.ID] {
				ids[e.ID] = true
				rows, stored = append(rows, row), append(stored, e)
			}
		}
		h := history.NewMemory()
		if err := h.Append(ctx, stored); err != nil {
			return err
		}
		events, err := Events(ctx, prev(h), lookup, rows)
		if err != nil {
			return err
		}
		full := New(testItems)
		shuffled := slices.Clone(events)
		r.Shuffle(len(shuffled), func(i, j int) { shuffled[i], shuffled[j] = shuffled[j], shuffled[i] })
		full.Add(shuffled...)
		slices.SortFunc(stored, func(a, b history.Event) int { return cmp.Or(cmp.Compare(a.TS, b.TS), cmp.Compare(a.ID, b.ID)) })
		times := []int64{-1, 31, int64(r.IntN(32))}
		for _, row := range rows {
			times = append(times, row.TS)
		}
		for _, ts := range times {
			w := []int64{1, 3, 7, 40}[r.IntN(4)]
			prefix := New(testItems)
			for _, e := range events {
				if e.TS < ts {
					prefix.Add(e)
				}
			}
			if err := compare(read(full), prefix, ts, w); err != nil {
				return fmt.Errorf("case %d, against the state of the earlier events: %w", c, err)
			}
			if err := compare(read(full), naive{stored}, ts, w); err != nil {
				return fmt.Errorf("case %d, against a naive count: %w", c, err)
			}
		}
	}
	return nil
}

func plain(s *State) reader                     { return s }
func sameHistory(h history.Store) history.Store { return h }

func TestPointInTime(t *testing.T) {
	if err := checkPointInTime(1, 300, plain, sameHistory); err != nil {
		t.Fatal(err)
	}
}

func TestPointInTimeCatchesBrokenVariants(t *testing.T) {
	if err := checkPointInTime(1, 300, func(s *State) reader { return inclusive{s} }, sameHistory); err == nil {
		t.Error("the check passed reads that see the events at the request time")
	}
	if err := checkPointInTime(1, 300, plain, func(h history.Store) history.Store { return unbounded{h} }); err == nil {
		t.Error("the check passed pairs made with the user's later events")
	}
}

// The window is [ts - w, ts): an event at ts - w counts, one at ts does
// not, and neither does a pair made at ts.
func TestReadBounds(t *testing.T) {
	s := New(3)
	s.Add(Event{Item: 1, TS: 89}, Event{Item: 1, TS: 90}, Event{Item: 1, TS: 99}, Event{Item: 2, TS: 100, Prev: []int32{1, 2}}, Event{Item: 1, TS: 100})
	if got := s.Count(1, 90, 100); got != 2 {
		t.Errorf("Count(1, [90, 100)) = %d, want 2", got)
	}
	if got := s.Popular(100, 10, 3); !slices.Equal(got, []int32{1}) {
		t.Errorf("Popular(100, 10) = %v, want [1]", got)
	}
	if got := s.Popular(101, 10, 3); !slices.Equal(got, []int32{1, 2}) {
		t.Errorf("Popular(101, 10) = %v, want [1 2]", got)
	}
	if got := s.Row(1, 100, 5); len(got) != 0 {
		t.Errorf("Row(1, 100) = %v, want none: the pair is made at 100", got)
	}
	if got := s.Row(1, 101, 5); !slices.Equal(got, []int32{2}) {
		t.Errorf("Row(1, 101) = %v, want [2]", got)
	}
	if got := s.Row(2, 101, 5); !slices.Equal(got, []int32{1}) {
		t.Errorf("Row(2, 101) = %v, want [1], the item not paired with itself", got)
	}
	if _, ok := s.First(2, 100); ok {
		t.Error("First(2, 100) found the event at 100")
	}
	if got, ok := s.First(1, 100); !ok || got != 89 {
		t.Errorf("First(1, 100) = %d %v, want 89", got, ok)
	}
}

// Ties go to the lower ID, and a list holds only items with a count.
func TestTiesToLowerID(t *testing.T) {
	s := New(4)
	s.Add(Event{Item: 3, TS: 1}, Event{Item: 2, TS: 2}, Event{Item: 4, TS: 3, Prev: []int32{3, 2}}, Event{Item: 4, TS: 4})
	if got := s.Popular(10, 10, 3); !slices.Equal(got, []int32{4, 2, 3}) {
		t.Errorf("Popular = %v, want [4 2 3]", got)
	}
	if got := s.Row(4, 10, 5); !slices.Equal(got, []int32{2, 3}) {
		t.Errorf("Row(4) = %v, want [2 3]", got)
	}
}
