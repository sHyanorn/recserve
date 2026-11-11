package main

import (
	"context"
	"strings"
	"testing"

	"github.com/sHyanorn/recserve/internal/dataset"
	"github.com/sHyanorn/recserve/internal/history"
)

func TestCheckHistories(t *testing.T) {
	ctx := context.Background()
	preload := [][]dataset.Row{{{User: "u", Item: "a", TS: 1}, {User: "v", Item: "b", TS: 2}}}
	// Out of time order: request 0 writes the earliest event, w's.
	split := []dataset.Row{{User: "u", Item: "c", TS: 20}, {User: "w", Item: "d", TS: 10}}
	samples := []sample{
		{i: 0, events: true, status: 202},
		{i: 1, status: 200},
		{i: 2, events: true, status: 202}, // the split again from its start: w's event once more
		{i: 3, events: true, status: 500}, // u's event, not accepted
	}
	stored := func(extra ...history.Event) history.Store {
		m := history.NewMemory()
		evs := []history.Event{ev("u", "a", 1), ev("v", "b", 2), ev("w", "d", 10)}
		if err := m.Append(ctx, append(evs, extra...)); err != nil {
			t.Fatal(err)
		}
		return m
	}
	check := func(s history.Store) string {
		got, err := checkHistories(ctx, s, preload, split, samples)
		if err != nil {
			t.Fatal(err)
		}
		return got
	}
	if got := check(stored()); got != "ok" {
		t.Fatalf("matching store: %s", got)
	}
	missing := history.NewMemory()
	if err := missing.Append(ctx, []history.Event{ev("u", "a", 1), ev("v", "b", 2)}); err != nil {
		t.Fatal(err)
	}
	if got := check(missing); !strings.HasPrefix(got, "1 of 3 users differ, the first w") {
		t.Fatalf("an accepted event missing: %s", got)
	}
	if got := check(stored(ev("u", "c", 20))); !strings.HasPrefix(got, "1 of 3 users differ, the first u") {
		t.Fatalf("an event stored without its 202: %s", got)
	}
}

func ev(user, item string, ts int64) history.Event {
	return history.Event{ID: history.EventID(user, item, ts, dataset.Kind), User: user, Item: item, TS: ts}
}
