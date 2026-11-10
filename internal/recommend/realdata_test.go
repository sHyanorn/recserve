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
// A bundle with an ONNX user encoder needs RECSERVE_ORT_LIB as well.
func TestRealDataSmoke(t *testing.T) {
	rd := loadRealData(t)
	b, store, first := rd.bundle, rd.store, rd.first
	trained := map[string]map[string]bool{}
	for _, r := range rd.train {
		if trained[r.User] == nil {
			trained[r.User] = map[string]bool{}
		}
		trained[r.User][r.Item] = true
	}
	test, dir := rd.test, rd.dir

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

	p := newPipeline(t, b, store)
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

type realData struct {
	dir         string
	bundle      *bundle.Bundle
	store       *history.Memory
	train, test []dataset.Row // train holds the train and valid splits
	first       int64         // the first test event's time
}

// loadRealData loads the bundle at RECSERVE_BUNDLE, checked against the
// first test event, and a store holding the train and valid splits of
// RECSERVE_DATA, which precede the test split. Without both variables the
// test is skipped.
func loadRealData(t *testing.T) realData {
	t.Helper()
	data, dir := os.Getenv("RECSERVE_DATA"), os.Getenv("RECSERVE_BUNDLE")
	if data == "" || dir == "" {
		t.Skip("RECSERVE_DATA and RECSERVE_BUNDLE not set")
	}
	rd := realData{dir: dir, store: history.NewMemory()}
	var err error
	if rd.test, err = dataset.Read(filepath.Join(data, "Video_Games.test.csv.gz")); err != nil {
		t.Fatal(err)
	}
	rd.first = dataset.FirstTS(rd.test)
	initORTIfSet(t)
	if rd.bundle, err = bundle.Load(dir, bundle.Options{FirstTestTS: rd.first}); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := rd.bundle.Close(); err != nil {
			t.Error(err)
		}
	})
	for _, name := range []string{"train", "valid"} {
		rows, err := dataset.Read(filepath.Join(data, "Video_Games."+name+".csv.gz"))
		if err != nil {
			t.Fatal(err)
		}
		rd.train = append(rd.train, rows...)
	}
	evs := make([]history.Event, 0, len(rd.train))
	for _, r := range rd.train {
		if r.TS >= rd.first {
			t.Fatalf("event at %d is not before the first test event %d", r.TS, rd.first)
		}
		if _, ok := rd.bundle.Lookup(r.Item); !ok {
			t.Fatalf("item %s not in the bundle", r.Item)
		}
		evs = append(evs, history.Event{ID: history.EventID(r.User, r.Item, r.TS, dataset.Kind), User: r.User, Item: r.Item, TS: r.TS})
	}
	if err := rd.store.Append(context.Background(), evs); err != nil {
		t.Fatal(err)
	}
	return rd
}
