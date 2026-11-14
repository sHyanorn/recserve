package recommend

import (
	"encoding/json"
	"fmt"
	"math/rand/v2"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"

	"github.com/sHyanorn/recserve/internal/bundle"
	"github.com/sHyanorn/recserve/internal/dataset"
)

// rebuilt is a bundle that build.py --cutoff rebuilt from every event
// before at, and a pipeline over it.
type rebuilt struct {
	at int64
	p  *Pipeline
}

// rebuildTime reads the time a bundle was rebuilt at, build.cutoff in its
// manifest, and fails unless every event the bundle used is before it.
func rebuildTime(b *bundle.Bundle) (int64, error) {
	raw, err := os.ReadFile(filepath.Join(b.Dir, "manifest.json"))
	if err != nil {
		return 0, err
	}
	var m struct {
		Build struct {
			Cutoff *int64 `json:"cutoff"`
		} `json:"build"`
	}
	if err := json.Unmarshal(raw, &m); err != nil {
		return 0, err
	}
	if m.Build.Cutoff == nil {
		return 0, fmt.Errorf("bundle %s: no build.cutoff", b.ModelVersion)
	}
	if b.TrainCutoffTS >= *m.Build.Cutoff {
		return 0, fmt.Errorf("bundle %s: train_cutoff_ts %d, rebuilt at %d", b.ModelVersion, b.TrainCutoffTS, *m.Build.Cutoff)
	}
	return *m.Build.Cutoff, nil
}

// latestAtOrBefore returns the index of the last of n rebuild times,
// ascending, given by at, that is at or before ts; -1 if none is.
func latestAtOrBefore(n int, at func(int) int64, ts int64) int {
	return sort.Search(n, func(i int) bool { return at(i) > ts }) - 1
}

// rebuiltRef is a rebuilt bundle by its rebuild time, opened only for the
// requests it answers; open returns the bundle and how to close it.
type rebuiltRef struct {
	at   int64
	open func() (*bundle.Bundle, func() error, error)
}

// openDir opens the bundle in dir, for a rebuiltRef.
func openDir(dir string) func() (*bundle.Bundle, func() error, error) {
	return func() (*bundle.Bundle, func() error, error) {
		b, err := bundle.Load(dir, bundle.Options{})
		if err != nil {
			return nil, nil, err
		}
		return b, b.Close, nil
	}
}

// periodic answers each request with the bundle rebuilt latest at or
// before its time.
type periodic struct {
	bundles []rebuilt // ascending by at
	// choose, test only, replaces that choice: the broken variant.
	choose func(ts int64) int
}

// pipeline returns the pipeline for a request at ts. It fails unless the
// bundle chosen was rebuilt at or before ts and used only events before
// ts, as read from the bundle itself.
func (pr periodic) pipeline(ts int64) (*Pipeline, error) {
	k := latestAtOrBefore(len(pr.bundles), func(i int) int64 { return pr.bundles[i].at }, ts)
	if pr.choose != nil {
		k = pr.choose(ts)
	}
	if k < 0 {
		return nil, fmt.Errorf("no bundle rebuilt at or before %d", ts)
	}
	b := pr.bundles[k]
	if b.at > ts || b.p.Bundle.TrainCutoffTS >= ts {
		return nil, fmt.Errorf("request at %d answered by bundle %s, rebuilt at %d from events up to %d", ts, b.p.Bundle.ModelVersion, b.at, b.p.Bundle.TrainCutoffTS)
	}
	return b.p, nil
}

// rebuiltSet returns pipelines over copies of p's bundle rebuilt at 10, 20
// and 30, each from events up to a millisecond before.
func rebuiltSet(p *Pipeline) periodic {
	var pr periodic
	for _, at := range []int64{10, 20, 30} {
		m := p.Bundle.Manifest
		m.ModelVersion, m.TrainCutoffTS = fmt.Sprint("r", at), at-1
		q := *p
		q.Bundle = bundle.New(m, p.Bundle.IDs, p.Bundle.Vectors, p.Bundle.Cooc, p.Bundle.Popular)
		pr.bundles = append(pr.bundles, rebuilt{at, &q})
	}
	return pr
}

// A request is answered by the bundle rebuilt latest at or before its
// time, one rebuilt at that very time included; before the first there is
// none. Choosing the nearest bundle instead, later ones included, fails
// the check; so does a bundle whose events reach its request's time.
func TestPeriodicChoosesTheLatestBefore(t *testing.T) {
	p, err := randomPipeline(rand.New(rand.NewPCG(23, 23)), 20, 5)
	if err != nil {
		t.Fatal(err)
	}
	pr := rebuiltSet(p)
	for _, c := range []struct {
		ts   int64
		want string
	}{{10, "r10"}, {19, "r10"}, {20, "r20"}, {29, "r20"}, {30, "r30"}, {99, "r30"}} {
		q, err := pr.pipeline(c.ts)
		if err != nil || q.Bundle.ModelVersion != c.want {
			t.Errorf("at %d: %v, %v; want %s", c.ts, q, err, c.want)
		}
	}
	if _, err := pr.pipeline(9); err == nil {
		t.Error("at 9, before every rebuild: no error")
	}

	broken := pr
	broken.choose = func(ts int64) int { return min(2, max(0, int((ts-5)/10))) } // the nearest
	if _, err := broken.pipeline(26); err == nil || !strings.Contains(err.Error(), "rebuilt at 30") {
		t.Errorf("the nearest bundle at 26: %v, want the check to fail", err)
	}
	events := []dataset.Row{{User: "u1", Item: "i1", TS: 12}, {User: "u2", Item: "i2", TS: 26}}
	if _, _, err := runConditionOn(broken.pipeline, qualityCondition{name: "R"}, events); err == nil {
		t.Error("a run with the nearest bundle passed")
	}
	if _, _, err := runConditionOn(pr.pipeline, qualityCondition{name: "R"}, events); err != nil {
		t.Errorf("a run with the latest bundle before: %v", err)
	}

	pr.bundles[1].p.Bundle.TrainCutoffTS = 20 // an event at the rebuild time
	if _, err := pr.pipeline(20); err == nil {
		t.Error("a bundle with an event at the request's time answered it")
	}
}

// rebuildTime reads build.cutoff, and rejects a bundle without one or with
// an event at or after it.
func TestRebuildTime(t *testing.T) {
	dir := t.TempDir()
	b := &bundle.Bundle{Dir: dir, Manifest: bundle.Manifest{ModelVersion: "r", TrainCutoffTS: 99}}
	for _, c := range []struct {
		manifest string
		want     int64
		err      bool
	}{
		{`{"build": {"cutoff": 100}}`, 100, false},
		{`{"build": {"cutoff": 99}}`, 0, true},
		{`{"build": {}}`, 0, true},
	} {
		if err := os.WriteFile(filepath.Join(dir, "manifest.json"), []byte(c.manifest), 0o644); err != nil {
			t.Fatal(err)
		}
		at, err := rebuildTime(b)
		if at != c.want || (err != nil) != c.err {
			t.Errorf("%s: %d, %v", c.manifest, at, err)
		}
	}
}
