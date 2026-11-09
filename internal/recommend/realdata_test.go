package recommend

import (
	"context"
	"encoding/json"
	"errors"
	"math/rand/v2"
	"os"
	"path/filepath"
	"slices"
	"testing"

	"github.com/sHyanorn/recserve/internal/bundle"
	"github.com/sHyanorn/recserve/internal/dataset"
	"github.com/sHyanorn/recserve/internal/history"
)

// TestRealDataSmoke runs when RECSERVE_DATA (the directory of make data) and
// RECSERVE_BUNDLE (a bundle built from it) are set. It loads the train and
// valid splits, which precede the test split, into the store, asks for 20
// items for 1,000 random test users at their first test event, and checks
// property 2 against everything they rated before it.
// It also checks that the bundle is rejected when its cutoff reaches the test period.
func TestRealDataSmoke(t *testing.T) {
	data, dir := os.Getenv("RECSERVE_DATA"), os.Getenv("RECSERVE_BUNDLE")
	if data == "" || dir == "" {
		t.Skip("RECSERVE_DATA and RECSERVE_BUNDLE not set")
	}
	test, err := dataset.Read(filepath.Join(data, "Video_Games.test.csv.gz"))
	if err != nil {
		t.Fatal(err)
	}
	first := dataset.FirstTS(test)
	b, err := bundle.Load(dir, bundle.Options{FirstTestTS: first})
	if err != nil {
		t.Fatal(err)
	}
	var train []dataset.Row
	for _, name := range []string{"train", "valid"} {
		rows, err := dataset.Read(filepath.Join(data, "Video_Games."+name+".csv.gz"))
		if err != nil {
			t.Fatal(err)
		}
		train = append(train, rows...)
	}
	store := history.NewMemory()
	trained := map[string]map[string]bool{}
	var evs []history.Event
	for _, r := range train {
		if r.TS >= first {
			t.Fatalf("event at %d is not before the first test event %d", r.TS, first)
		}
		if trained[r.User] == nil {
			trained[r.User] = map[string]bool{}
		}
		trained[r.User][r.Item] = true
		id, ok := b.Lookup(r.Item)
		if !ok {
			t.Fatalf("item %s not in the bundle", r.Item)
		}
		evs = append(evs, history.Event{ID: history.EventID(r.User, r.Item, r.TS, dataset.Kind), User: r.User, Item: id, TS: r.TS})
	}
	if err := store.Append(context.Background(), evs); err != nil {
		t.Fatal(err)
	}

	firstTest := map[string]int64{}
	for _, r := range test {
		if ts, ok := firstTest[r.User]; !ok || r.TS < ts {
			firstTest[r.User] = r.TS
		}
	}
	users := make([]string, 0, len(firstTest))
	for u := range firstTest {
		users = append(users, u)
	}
	slices.Sort(users)
	rand.New(rand.NewPCG(1, 0)).Shuffle(len(users), func(i, j int) { users[i], users[j] = users[j], users[i] })
	users = users[:1000]

	p := &Pipeline{Bundle: b, History: store}
	var withHistory, items, long int
	for _, u := range users {
		resp, err := p.Recommend(context.Background(), Request{User: u, K: 20, TS: firstTest[u]})
		if err != nil {
			t.Fatal(err)
		}
		if len(resp.Items) != 20 {
			t.Fatalf("%s: %d items", u, len(resp.Items))
		}
		if len(trained[u]) > 0 {
			withHistory++
		}
		if len(trained[u]) > 200 {
			long++
		}
		got := map[string]bool{}
		for _, it := range resp.Items {
			if trained[u][it.ID] {
				t.Errorf("%s: recommended %s, rated before the request", u, it.ID)
			}
			if got[it.ID] {
				t.Errorf("%s: %s twice", u, it.ID)
			}
			got[it.ID] = true
			items++
		}
	}
	t.Logf("%d users (%d with history, %d with over 200 items), %d items, none seen or repeated",
		len(users), withHistory, long, items)

	// A copy of the bundle whose cutoff is the first test event must be rejected.
	bad := t.TempDir()
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	for _, e := range entries {
		raw, err := os.ReadFile(filepath.Join(dir, e.Name()))
		if err != nil {
			t.Fatal(err)
		}
		if e.Name() == "manifest.json" {
			var m map[string]any
			if err := json.Unmarshal(raw, &m); err != nil {
				t.Fatal(err)
			}
			m["train_cutoff_ts"] = first
			if raw, err = json.Marshal(m); err != nil {
				t.Fatal(err)
			}
		}
		if err := os.WriteFile(filepath.Join(bad, e.Name()), raw, 0o644); err != nil {
			t.Fatal(err)
		}
	}
	_, err = bundle.Load(bad, bundle.Options{FirstTestTS: first})
	if !errors.Is(err, bundle.ErrCutoff) {
		t.Fatalf("bundle with train_cutoff_ts = first test event: err = %v, want ErrCutoff", err)
	}
	t.Logf("rejected: %v", err)
}
