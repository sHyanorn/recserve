package main

import (
	"cmp"
	"context"
	"fmt"
	"maps"
	"net/http"
	"slices"
	"strings"

	"github.com/sHyanorn/recserve/internal/dataset"
	"github.com/sHyanorn/recserve/internal/history"
	"github.com/sHyanorn/recserve/internal/valkeyclient"
)

// histories reads the files and checks the histories on the Valkey server at addr.
func histories(addr, preload, split, samples string) (string, error) {
	var pre [][]dataset.Row
	for _, p := range strings.Split(preload, ",") {
		rows, err := dataset.Read(p)
		if err != nil {
			return "", err
		}
		pre = append(pre, rows)
	}
	rows, err := dataset.Read(split)
	if err != nil {
		return "", err
	}
	s, err := readSamples(samples)
	if err != nil {
		return "", err
	}
	c, err := valkeyclient.New(addr)
	if err != nil {
		return "", err
	}
	defer c.Close()
	return checkHistories(context.Background(), history.NewValkey(c), pre, rows, s)
}

// checkHistories checks property 9 after a run with writes: every user's
// history in store equals the one built from the preloaded events and the
// events the gateway accepted (202). Request i of loadgen's writes scenario
// sends the (i mod n)-th event of the split in time order, as loadgen
// orders it. Users who were written to, accepted or not, are checked as well
// as the preloaded ones, so an event stored without its 202 shows. It returns
// "ok" or what differs.
func checkHistories(ctx context.Context, store history.Store, preload [][]dataset.Row, split []dataset.Row, samples []sample) (string, error) {
	want := history.NewMemory()
	users := map[string]bool{}
	event := func(r dataset.Row) history.Event {
		users[r.User] = true
		return history.Event{ID: history.EventID(r.User, r.Item, r.TS, dataset.Kind), User: r.User, Item: r.Item, TS: r.TS}
	}
	var evs []history.Event
	for _, rows := range preload {
		for _, r := range rows {
			evs = append(evs, event(r))
		}
	}
	ordered := slices.Clone(split)
	slices.SortStableFunc(ordered, func(a, b dataset.Row) int { return cmp.Compare(a.TS, b.TS) })
	for _, s := range samples {
		if !s.events {
			continue
		}
		e := event(ordered[s.i%len(ordered)])
		if s.status == http.StatusAccepted {
			evs = append(evs, e)
		}
	}
	if err := want.Append(ctx, evs); err != nil {
		return "", err
	}
	var differ []string
	for _, u := range slices.Sorted(maps.Keys(users)) {
		w, err := want.Before(ctx, u, history.MaxTS, 0)
		if err != nil {
			return "", err
		}
		g, err := store.Before(ctx, u, history.MaxTS, 0)
		if err != nil {
			return "", fmt.Errorf("history of %s: %w", u, err)
		}
		if !slices.Equal(w, g) {
			differ = append(differ, u)
		}
	}
	if len(differ) > 0 {
		return fmt.Sprintf("%d of %d users differ, the first %s", len(differ), len(users), differ[0]), nil
	}
	return "ok", nil
}
