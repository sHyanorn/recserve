package recommend

import (
	"cmp"
	"compress/gzip"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"maps"
	"math/rand/v2"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/sHyanorn/recserve/internal/bundle"
	"github.com/sHyanorn/recserve/internal/dataset"
	"github.com/sHyanorn/recserve/internal/fresh"
	"github.com/sHyanorn/recserve/internal/history"
	"github.com/sHyanorn/recserve/internal/onnx/onnxtest"
	"github.com/sHyanorn/recserve/internal/shard"
)

// The evaluation of the stream state, docs/registration/m8-freshness.md:
// the conditions (section 7), the tables (section 8) and the checks of
// validity (section 10), run by a test read (section 9, openTestRead).

// testStart is the test split's start, in both categories (section 2).
const testStart = 1658002729837

// conditionsHash is the SHA-256 of a read's rows, one line each: its name
// and the versions of the bundles it runs on.
func conditionsHash(rows [][]string) string {
	h := sha256.New()
	for _, r := range rows {
		h.Write([]byte(strings.Join(r, " ") + "\n"))
	}
	return hex.EncodeToString(h.Sum(nil))
}

// freshness is what section 7's conditions run on: the bundles B1, B1s6
// and B1s8, those rebuilt every 7 and every 30 days, and those rebuilt
// every 30 days with B1s8's ranker, ascending by rebuild time and opened
// one at a time; the store and the stream state of every event of the
// run, and the window chosen on valid.
type freshness struct {
	b1, s6, s8     *bundle.Bundle
	r7, r30, r30s8 []rebuiltRef
	store          history.Store
	state          *fresh.State
	window         int64
	// rank, test only, replaces each bundle's ranker.
	rank func(b *bundle.Bundle) Ranker
}

// pipeline returns a pipeline over b on the exact shards of TestRealDataQuality's
// healthy condition, with the stream state if stream.
func (f freshness) pipeline(b *bundle.Bundle, stream bool) (*Pipeline, error) {
	p, err := newLocal(b, f.store, shard.DefaultConfig)
	if err != nil {
		return nil, err
	}
	if f.rank != nil {
		p.rankWith = f.rank(b)
	}
	if stream {
		p.Fresh, p.FreshWindow = f.state, f.window
	}
	return p, nil
}

// periodic returns the pipelines over bundles, each answering the requests
// from its rebuild time to the next one's, all built at once: the
// reference that runPeriodic's ranks are tested against.
func (f freshness) periodic(bundles []*bundle.Bundle, stream bool) (periodic, error) {
	var pr periodic
	for _, b := range bundles {
		at, err := rebuildTime(b)
		if err != nil {
			return pr, err
		}
		p, err := f.pipeline(b, stream)
		if err != nil {
			return pr, err
		}
		pr.bundles = append(pr.bundles, rebuilt{at, p})
	}
	slices.SortFunc(pr.bundles, func(a, b rebuilt) int { return cmp.Compare(a.at, b.at) })
	return pr, nil
}

// runPeriodic runs condition name on events, each answered by the bundle
// of refs rebuilt latest at or before its time. It opens one bundle at a
// time, for the events it answers, and closes it after them; each request
// is checked as periodic checks it.
func (f freshness) runPeriodic(name string, refs []rebuiltRef, stream bool, events []dataset.Row) ([]int, error) {
	groups := make([][]int, len(refs))
	for i, e := range events {
		k := latestAtOrBefore(len(refs), func(j int) int64 { return refs[j].at }, e.TS)
		if k < 0 {
			return nil, fmt.Errorf("event %d: no bundle rebuilt at or before %d", i, e.TS)
		}
		groups[k] = append(groups[k], i)
	}
	ranks := make([]int, len(events))
	for k, idx := range groups {
		if len(idx) > 0 {
			if err := f.runRebuilt(name, refs[k], stream, events, idx, ranks); err != nil {
				return nil, err
			}
		}
	}
	return ranks, nil
}

// runRebuilt opens ref, answers the events at idx with it, writing their
// ranks at idx in ranks, and closes it.
func (f freshness) runRebuilt(name string, ref rebuiltRef, stream bool, events []dataset.Row, idx, ranks []int) (err error) {
	b, closeB, err := ref.open()
	if err != nil {
		return err
	}
	defer func() { err = errors.Join(err, closeB()) }()
	at, err := rebuildTime(b)
	if err != nil {
		return err
	}
	if at != ref.at {
		return fmt.Errorf("bundle %s: rebuilt at %d, opened for %d", b.ModelVersion, at, ref.at)
	}
	p, err := f.pipeline(b, stream)
	if err != nil {
		return err
	}
	group := make([]dataset.Row, len(idx))
	for j, i := range idx {
		group[j] = events[i]
	}
	got, _, err := runConditionOn(periodic{bundles: []rebuilt{{at, p}}}.pipeline, qualityCondition{name: name}, group)
	if err != nil {
		return fmt.Errorf("bundle %s: not valid: %w", b.ModelVersion, err)
	}
	for j, i := range idx {
		ranks[i] = got[j]
	}
	return nil
}

// freshnessCondition is a condition of section 7 and its run, one request
// per event.
type freshnessCondition struct {
	name string
	run  func(events []dataset.Row) ([]int, error)
}

// conditions returns section 7's conditions, in its order. A condition
// builds its pipelines when it runs.
func (f freshness) conditions() []freshnessCondition {
	var out []freshnessCondition
	for _, c := range []struct {
		name   string
		b      *bundle.Bundle
		stream bool
	}{{"F0", f.b1, false}, {"S1", f.b1, true}, {"S2", f.b1, true}, {"S3a", f.s6, true}, {"S3", f.s8, true}} {
		out = append(out, freshnessCondition{c.name, func(events []dataset.Row) ([]int, error) {
			p, err := f.pipeline(c.b, c.stream)
			if err != nil {
				return nil, err
			}
			p.bundleCooc = c.name == "S1" // popularity from the state, co-occurrence from B1
			ranks, _, err := runConditionOn(func(int64) (*Pipeline, error) { return p, nil }, qualityCondition{name: c.name}, events)
			if err != nil {
				return nil, fmt.Errorf("not valid: %w", err)
			}
			return ranks, nil
		}})
	}
	for _, c := range []struct {
		name   string
		refs   []rebuiltRef
		stream bool
	}{{"R7", f.r7, false}, {"R30", f.r30, false}, {"S3+R30", f.r30s8, true}} {
		out = append(out, freshnessCondition{c.name, func(events []dataset.Row) ([]int, error) {
			return f.runPeriodic(c.name, c.refs, c.stream, events)
		}})
	}
	return out
}

// rows runs the conditions, one request per event, then the two
// references: B1's vector reference and the popularity reference.
func (f freshness) rows(events []dataset.Row, logf func(string, ...any)) ([]qualityRow, error) {
	var rows []qualityRow
	for _, c := range f.conditions() {
		start := time.Now()
		ranks, err := c.run(events)
		if err != nil {
			return nil, fmt.Errorf("%s: %w", c.name, err)
		}
		logf("%s: %v", c.name, time.Since(start).Round(time.Second))
		rows = append(rows, qualityRow{c.name, ranks})
	}
	ref, _, err := referenceRanks(f.b1, f.store, events)
	if err != nil {
		return nil, err
	}
	pop, err := popularityRanks(f.b1, f.store, f.state, f.window, events)
	if err != nil {
		return nil, err
	}
	return append(rows, qualityRow{"vector reference", ref}, qualityRow{"popularity reference", pop}), nil
}

// buildOf reads the build record of b's manifest.
func buildOf(b *bundle.Bundle) (manifestBuild, error) {
	raw, err := os.ReadFile(filepath.Join(b.Dir, "manifest.json"))
	if err != nil {
		return manifestBuild{}, err
	}
	var m struct {
		Build manifestBuild `json:"build"`
	}
	return m.Build, json.Unmarshal(raw, &m)
}

type manifestBuild struct {
	ALS    map[string]float64 `json:"als"`
	Ranker struct {
		From string `json:"from"`
	} `json:"ranker"`
	UserEncoder struct {
		Graph          string  `json:"graph"`
		Regularization float64 `json:"regularization"`
	} `json:"user_encoder"`
}

// checkRebuilt requires b to have the items, ALS settings and user
// encoder of base, the ranker of the bundle from and from's stream window
// (section 4), and returns its rebuild time.
func checkRebuilt(b, base, from *bundle.Bundle) (int64, error) {
	want, err := buildOf(base)
	if err != nil {
		return 0, err
	}
	at, err := rebuildTime(b)
	if err != nil {
		return 0, err
	}
	got, err := buildOf(b)
	if err != nil {
		return 0, err
	}
	if !maps.Equal(got.ALS, want.ALS) || got.UserEncoder != want.UserEncoder || b.UserEncoder != base.UserEncoder ||
		b.SHA256["items.ids"] != base.SHA256["items.ids"] || got.Ranker.From != from.ModelVersion ||
		(b.Stream == nil) != (from.Stream == nil) || b.Stream != nil && *b.Stream != *from.Stream {
		return 0, fmt.Errorf("bundle %s: ALS %v, encoder %s %+v, ranker from %q, stream %v; want %v, %s %+v, %q, %v, and %s's items",
			b.ModelVersion, got.ALS, b.UserEncoder, got.UserEncoder, got.Ranker.From, b.Stream,
			want.ALS, base.UserEncoder, want.UserEncoder, from.ModelVersion, from.Stream, base.ModelVersion)
	}
	return at, nil
}

// checkRebuiltDir checks the bundle in dir as checkRebuilt does and closes
// it, so that a read holds one rebuilt bundle at a time, and returns its
// version and rebuild time.
func checkRebuiltDir(dir string, base, from *bundle.Bundle) (string, int64, error) {
	b, closeB, err := openDir(dir)()
	if err != nil {
		return "", 0, err
	}
	at, err := checkRebuilt(b, base, from)
	return b.ModelVersion, at, errors.Join(err, closeB())
}

// rebuildTimes requires the rebuild times of bundles rebuilt every period
// to be T_k = testStart + k period for k = 0, 1, ..., each once, and
// returns the latest.
func rebuildTimes(times []int64, period int64) (int64, error) {
	times = slices.Sorted(slices.Values(times))
	for k, at := range times {
		if at != testStart+int64(k)*period {
			return 0, fmt.Errorf("rebuilt at %v, want %d + k %d for k from 0", times, int64(testStart), period)
		}
	}
	if len(times) == 0 {
		return 0, errors.New("no rebuilt bundle")
	}
	return times[len(times)-1], nil
}

// rebuiltThrough requires the latest rebuild, at latest, to be the last
// T_k not after the latest test event, last.
func rebuiltThrough(latest, period, last int64) error {
	if latest > last || last >= latest+period {
		return fmt.Errorf("the latest rebuild is at %d, the latest test event at %d", latest, last)
	}
	return nil
}

// checkStreamBundles requires B1s6 and B1s8 to be B1, its items, vectors
// and user encoder, with rankers over the six features and the eight, and
// the window chosen on valid (section 4, 6.2).
func checkStreamBundles(b1, s6, s8 *bundle.Bundle, window int64) error {
	for _, c := range []struct {
		b       *bundle.Bundle
		recency bool
	}{{s6, false}, {s8, true}} {
		if c.b.Stream == nil || c.b.Stream.PopularWindowMS != window || c.b.Recency() != c.recency || len(c.b.RankerFeatures) == 0 {
			return fmt.Errorf("bundle %s: stream %v, ranker_features %q; want a window of %d ms", c.b.ModelVersion, c.b.Stream, c.b.RankerFeatures, window)
		}
		for _, name := range []string{"items.ids", "items.npy", "user_encoder.onnx"} {
			if c.b.SHA256[name] != b1.SHA256[name] {
				return fmt.Errorf("bundle %s's %s is not B1's", c.b.ModelVersion, name)
			}
		}
	}
	return nil
}

// committedWindow reads the window chosen on valid from windows.txt, as
// make bench-windows wrote it for category (section 6.2).
func committedWindow(category string) (int64, error) {
	raw, err := os.ReadFile(filepath.Join("../../bench-results/m8", category, "windows.txt"))
	if err != nil {
		return 0, err
	}
	for line := range strings.Lines(string(raw)) {
		if days, ok := strings.CutPrefix(strings.TrimSpace(line), "selected: "); ok {
			d, err := strconv.ParseInt(strings.TrimSuffix(days, "d"), 10, 64)
			return d * day, err
		}
	}
	return 0, errors.New("windows.txt selects no window")
}

// windowCounter is the read of the stream state that checkWindowCounts checks.
type windowCounter interface {
	Count(item int32, from, to int64) int
}

// checkWindowCounts requires, for every event, the window count of its
// item that s gives at its time to equal the number of the item's events
// among all in [ts - w, ts), found by binary search in their sorted times
// (section 10).
func checkWindowCounts(b *bundle.Bundle, s windowCounter, w int64, all, events []dataset.Row) error {
	times := map[string][]int64{}
	for _, r := range all {
		times[r.Item] = append(times[r.Item], r.TS)
	}
	for _, t := range times {
		slices.Sort(t)
	}
	for i, e := range events {
		id, ok := b.Lookup(e.Item)
		if !ok {
			return fmt.Errorf("event %d: item %s is not in bundle %s", i, e.Item, b.ModelVersion)
		}
		lo, _ := slices.BinarySearch(times[e.Item], e.TS-w)
		hi, _ := slices.BinarySearch(times[e.Item], e.TS)
		if got := s.Count(id, e.TS-w, e.TS); got != hi-lo {
			return fmt.Errorf("event %d: item %s's window count %d, by binary search %d", i, e.Item, got, hi-lo)
		}
	}
	return nil
}

// freshnessDiffs are the differences of recall@10 that section 8 reports
// with both intervals, the primary first.
var freshnessDiffs = [][2]string{
	{"S3", "F0"}, {"S1", "F0"}, {"S2", "S1"}, {"S3a", "S2"}, {"S3", "S3a"}, {"S3", "R7"}, {"S3", "R30"},
	{"R7", "F0"}, {"R30", "F0"}, {"S3+R30", "S3"}, {"S3+R30", "R30"},
}

// strataTable returns each row's recall@20, NDCG@10 and recall@10, then
// its recall@10 on the events whose item has a train event (trained) and
// on the others (section 8).
func strataTable(rows []qualityRow, trained []bool) string {
	var in, out int
	for _, t := range trained {
		if t {
			in++
		} else {
			out++
		}
	}
	var sb strings.Builder
	fmt.Fprintf(&sb, "| Row | Recall@10 | Recall@20 | NDCG@10 | Recall@10, item with train events (%d) | Recall@10, others (%d) |\n|---|---:|---:|---:|---:|---:|\n", in, out)
	for _, r := range rows {
		var sums [5]float64
		for i, rank := range r.ranks {
			sums[0] += recallAtRank(rank, 10)
			sums[1] += recallAtRank(rank, 20)
			sums[2] += ndcgAtRank(rank, 10)
			if trained[i] {
				sums[3] += recallAtRank(rank, 10)
			} else {
				sums[4] += recallAtRank(rank, 10)
			}
		}
		n := float64(len(r.ranks))
		fmt.Fprintf(&sb, "| %s | %.4f | %.4f | %.4f | %.4f | %.4f |\n", r.name, sums[0]/n, sums[1]/n, sums[2]/n, sums[3]/float64(in), sums[4]/float64(out))
	}
	return sb.String()
}

// diffTable returns each of freshnessDiffs in rows with the 95% intervals
// of the paired bootstraps over users and over events, and if h1, the
// reading of H1 from the first (section 8).
func diffTable(rows []qualityRow, users []string, h1 bool) (string, error) {
	recall := map[string][]float64{}
	for _, r := range rows {
		v := make([]float64, len(r.ranks))
		for i, rank := range r.ranks {
			v[i] = recallAtRank(rank, 10)
		}
		recall[r.name] = v
	}
	var sb strings.Builder
	sb.WriteString("| Δ Recall@10 | Mean | 95% CI, users | 95% CI, events |\n|---|---:|---:|---:|\n")
	var reading string
	for k, d := range freshnessDiffs {
		a, b := recall[d[0]], recall[d[1]]
		if a == nil || b == nil {
			return "", fmt.Errorf("no row %s or %s", d[0], d[1])
		}
		mean, ulo, uhi := userBootstrap(a, b, users, bootstrapSamples, bootstrapSeed)
		_, elo, ehi := pairedBootstrap(a, b, bootstrapSamples, bootstrapSeed)
		fmt.Fprintf(&sb, "| %s − %s | %+.4f | [%+.4f, %+.4f] | [%+.4f, %+.4f] |\n", d[0], d[1], mean, ulo, uhi, elo, ehi)
		if k == 0 {
			switch {
			case ulo > 0:
				reading = "confirmed"
			case uhi < 0:
				reading = "contradicted"
			default:
				reading = "not confirmed"
			}
		}
	}
	if h1 {
		fmt.Fprintf(&sb, "\nH1 (S3 − F0, the interval over users): %s\n", reading)
	}
	return sb.String(), nil
}

// writeRanks writes ranks, one per line, gzip-compressed, to a new file
// path, and returns the file's SHA-256.
func writeRanks(path string, ranks []int) (string, error) {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o644)
	if err != nil {
		return "", err
	}
	var lines strings.Builder
	for _, r := range ranks {
		fmt.Fprintln(&lines, r)
	}
	h := sha256.New()
	z := gzip.NewWriter(io.MultiWriter(f, h))
	_, err = z.Write([]byte(lines.String()))
	err = errors.Join(err, z.Close(), f.Close())
	return hex.EncodeToString(h.Sum(nil)), err
}

// rankFile is the name of a row's per-event ranks.
func rankFile(row string) string {
	return strings.NewReplacer(" ", "-", "+", "-", ",", "", "/", "-").Replace(strings.ToLower(row)) + ".txt.gz"
}

// trainedItems reports, per event, whether its item has an event in train.
func trainedItems(train, events []dataset.Row) []bool {
	items := map[string]bool{}
	for _, r := range train {
		items[r.Item] = true
	}
	out := make([]bool, len(events))
	for i, e := range events {
		out[i] = items[e.Item]
	}
	return out
}

// TestRealDataFreshness runs test read RECSERVE_READ of the category
// RECSERVE_CATEGORY, Video_Games if unset (docs/registration/m8-freshness.md,
// section 9), after openTestRead's check. Read 1: B1, the bundle at
// RECSERVE_BUNDLE, under TestRealDataQuality's conditions, and the
// reference and healthy rows of B0, at RECSERVE_BUNDLE_B0. Read 2: the
// conditions of section 7 on B1, B1s6 and B1s8 (RECSERVE_BUNDLE_S6, _S8)
// and the bundles rebuilt in the directories RECSERVE_REBUILT_7, _30 and
// _30S8, whose F0 and vector reference must give read 1's ranks, in
// RECSERVE_READ1_RANKS. Musical_Instruments' one read holds both. The
// report goes to RECSERVE_FRESHNESS_OUT and the per-event ranks to the
// directory RECSERVE_RANKS_DIR, outside the repository; neither may exist.
// It refuses Video_Games' read 3, which is TestRealDataQuality's.
func TestRealDataFreshness(t *testing.T) {
	if os.Getenv("RECSERVE_READ") == "" {
		t.Skip("RECSERVE_READ not set")
	}
	n, err := strconv.Atoi(os.Getenv("RECSERVE_READ"))
	if err != nil {
		t.Fatal(err)
	}
	category := cmp.Or(os.Getenv("RECSERVE_CATEGORY"), "Video_Games")
	if category == "Video_Games" && n == 3 {
		t.Fatal(read3)
	}
	read1, read2 := n == 1, category != "Video_Games" || n == 2
	env := func(name string) string {
		v := os.Getenv(name)
		if v == "" {
			t.Fatalf("%s is required", name)
		}
		return v
	}
	out, ranksDir := env("RECSERVE_FRESHNESS_OUT"), env("RECSERVE_RANKS_DIR")
	for _, split := range []string{"train", "valid", "test"} {
		if _, err := os.Stat(filepath.Join(env("RECSERVE_DATA"), category+"."+split+".csv.gz")); err != nil {
			t.Fatal(err)
		}
	}
	// Every check that needs no test event runs before the read is logged.
	initORTIfSet(t)
	b1 := loadBundle(t, env("RECSERVE_BUNDLE"), bundle.Options{})
	plan := [][]string{{"B1", b1.ModelVersion}}
	var b0, s6, s8 *bundle.Bundle
	type rebuildSet struct {
		name   string
		days   int64
		from   *bundle.Bundle
		refs   []rebuiltRef
		latest int64
	}
	var sets []*rebuildSet
	if read1 {
		b0 = loadBundle(t, env("RECSERVE_BUNDLE_B0"), bundle.Options{})
		plan = append(plan, []string{"B0", b0.ModelVersion})
	}
	if read2 {
		s6, s8 = loadBundle(t, env("RECSERVE_BUNDLE_S6"), bundle.Options{}), loadBundle(t, env("RECSERVE_BUNDLE_S8"), bundle.Options{})
		window, err := committedWindow(category)
		if err != nil {
			t.Fatal(err)
		}
		if err := checkStreamBundles(b1, s6, s8, window); err != nil {
			t.Fatal(err)
		}
		plan = append(plan, []string{"B1s6", s6.ModelVersion}, []string{"B1s8", s8.ModelVersion})
		sets = []*rebuildSet{{name: "7", days: 7, from: b1}, {name: "30", days: 30, from: b1}, {name: "30S8", days: 30, from: s8}}
		for _, set := range sets {
			dirs, err := filepath.Glob(filepath.Join(env("RECSERVE_REBUILT_"+set.name), "*", "manifest.json"))
			if err != nil {
				t.Fatal(err)
			}
			row, times := []string{"R" + set.name}, []int64{}
			for _, m := range dirs {
				version, at, err := checkRebuiltDir(filepath.Dir(m), b1, set.from)
				if err != nil {
					t.Fatalf("R%s: %v", set.name, err)
				}
				row, times = append(row, version), append(times, at)
				set.refs = append(set.refs, rebuiltRef{at, openDir(filepath.Dir(m))})
			}
			if set.latest, err = rebuildTimes(times, set.days*day); err != nil {
				t.Fatalf("R%s: %v", set.name, err)
			}
			slices.SortFunc(set.refs, func(a, b rebuiltRef) int { return cmp.Compare(a.at, b.at) })
			plan = append(plan, row)
		}
	}
	if err := openTestRead(registrationPath, amendmentsPath, testReadsLog, category, n, conditionsHash(plan), gitHead, out, ranksDir); err != nil {
		t.Fatal(err)
	}

	rd := loadRealData(t)
	appendTest(t, rd)
	if rd.first < testStart {
		t.Fatalf("a test event at %d, before %d", rd.first, int64(testStart))
	}
	for _, b := range []*bundle.Bundle{b0, s6, s8} {
		if b != nil && b.TrainCutoffTS >= rd.first {
			t.Fatalf("bundle %s: %v: train_cutoff_ts %d, first test event %d", b.ModelVersion, bundle.ErrCutoff, b.TrainCutoffTS, rd.first)
		}
	}
	if err := os.Mkdir(ranksDir, 0o755); err != nil {
		t.Fatal(err)
	}
	users := make([]string, len(rd.test))
	for i, e := range rd.test {
		users[i] = e.User
	}
	trained := trainedItems(rd.train[:len(rd.train)-len(rd.valid)], rd.test) // rd.train: the train split, then valid
	var report strings.Builder
	fmt.Fprintf(&report, "# %s, test read %d\n\n- conditions: %s\n", category, n, conditionsHash(plan))
	for _, r := range plan {
		fmt.Fprintf(&report, "- %s: %s\n", r[0], strings.Join(r[1:], " "))
	}
	var saved []qualityRow
	if read1 {
		rows, noVector := qualityRows(t, rd.bundle, rd.store, rd.test)
		ref0, noVector0, err := referenceRanks(b0, rd.store, rd.test)
		if err != nil {
			t.Fatal(err)
		}
		healthy0, _, err := runCondition(newPipeline(t, b0, rd.store), qualityCondition{name: "B0 healthy"}, rd.test)
		if err != nil {
			t.Fatalf("B0 healthy: not valid: %v", err)
		}
		for i := range rows {
			rows[i].name = "B1 " + rows[i].name
		}
		rows = append(rows, qualityRow{"B0 reference", ref0}, qualityRow{"B0 healthy", healthy0})
		fmt.Fprintf(&report, "\n## Read 1\n\nB0's requests whose history gives no user vector: %d\n\n%s\n%s",
			noVector0, qualityReport(rd.bundle.ModelVersion, len(rd.test), noVector, rows, 1), strataTable(rows, trained))
		saved = append(saved, rows...)
	}
	if read2 {
		all := slices.Concat(rd.train, rd.test)
		last := slices.MaxFunc(rd.test, func(a, b dataset.Row) int { return cmp.Compare(a.TS, b.TS) }).TS
		for _, set := range sets {
			if err := rebuiltThrough(set.latest, set.days*day, last); err != nil {
				t.Fatalf("R%s: %v", set.name, err)
			}
		}
		state, err := streamState(rd.bundle, rd.store, all)
		if err != nil {
			t.Fatal(err)
		}
		f := freshness{b1: rd.bundle, s6: s6, s8: s8, r7: sets[0].refs, r30: sets[1].refs, r30s8: sets[2].refs,
			store: rd.store, state: state, window: s8.Stream.PopularWindowMS}
		for _, b := range []*bundle.Bundle{s6, s8} {
			if err := b.CheckStreamGolden(state); err != nil {
				t.Fatalf("bundle %s: %v", b.ModelVersion, err)
			}
		}
		for _, ref := range f.r30s8 {
			b, closeB, err := ref.open()
			if err != nil {
				t.Fatal(err)
			}
			if err := errors.Join(b.CheckStreamGolden(state), closeB()); err != nil {
				t.Fatalf("bundle %s: %v", b.ModelVersion, err)
			}
		}
		if err := checkWindowCounts(rd.bundle, state, f.window, all, rd.test); err != nil {
			t.Fatal(err)
		}
		rows, err := f.rows(rd.test, t.Logf)
		if err != nil {
			t.Fatal(err)
		}
		if category == "Video_Games" {
			dir := env("RECSERVE_READ1_RANKS")
			for _, c := range [][2]string{{"F0", "B1 healthy"}, {"vector reference", "B1 reference"}} {
				want, err := readRanks(filepath.Join(dir, rankFile(c[1])))
				if err != nil {
					t.Fatal(err)
				}
				if got := rows[slices.IndexFunc(rows, func(r qualityRow) bool { return r.name == c[0] })].ranks; !slices.Equal(got, want) {
					t.Fatalf("%s's ranks differ from read 1's %s", c[0], c[1])
				}
			}
		}
		diffs, err := diffTable(rows, users, category == "Musical_Instruments")
		if err != nil {
			t.Fatal(err)
		}
		fmt.Fprintf(&report, "\n## The conditions of section 7\n\n- window: %d days\n\n%s\n%s", f.window/day, strataTable(rows, trained), diffs)
		saved = append(saved, rows...)
	}
	report.WriteString("\n## Per-event ranks, not committed\n\n| Row | SHA-256 of its file |\n|---|---|\n")
	for _, r := range saved {
		sum, err := writeRanks(filepath.Join(ranksDir, rankFile(r.name)), r.ranks)
		if err != nil {
			t.Fatal(err)
		}
		fmt.Fprintf(&report, "| %s | %s |\n", r.name, sum)
	}
	t.Logf("\n%s", report.String())
	f, err := os.OpenFile(out, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o644)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.WriteString(report.String()); err != nil {
		t.Fatal(err)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}
}

// manifestVersion reads the model_version of the bundle in dir.
func manifestVersion(t *testing.T, dir string) string {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join(dir, "manifest.json"))
	if err != nil {
		t.Fatal(err)
	}
	var m struct {
		ModelVersion string `json:"model_version"`
	}
	if err := json.Unmarshal(raw, &m); err != nil || m.ModelVersion == "" {
		t.Fatalf("%s: no model_version: %v", dir, err)
	}
	return m.ModelVersion
}

// loadBundle loads the bundle in dir, closed when the test ends.
func loadBundle(t *testing.T, dir string, opt bundle.Options) *bundle.Bundle {
	t.Helper()
	b, err := bundle.Load(dir, opt)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := b.Close(); err != nil {
			t.Error(err)
		}
	})
	return b
}

// rebuiltAt returns a copy of b named version, rebuilt at `at` from the
// events before it, with build settings als and the ranker of from, and
// its manifest in a new directory.
func rebuiltAt(t *testing.T, b *bundle.Bundle, version string, at int64, als map[string]float64, from string) *bundle.Bundle {
	t.Helper()
	m := b.Manifest
	m.ModelVersion, m.TrainCutoffTS = version, at-1
	r := bundle.New(m, b.IDs, b.Vectors, b.Cooc, b.Popular)
	r.Dir = t.TempDir()
	raw, err := json.Marshal(map[string]any{"build": map[string]any{"cutoff": at, "als": als, "ranker": map[string]any{"from": from}}})
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(r.Dir, "manifest.json"), raw, 0o644); err != nil {
		t.Fatal(err)
	}
	return r
}

// Bundles rebuilt every 10 days from the test split's start pass, the
// latest of them the last before the latest test event; a missing or
// extra rebuild, other items, ALS settings or encoder, another ranker or
// another stream window do not.
func TestCheckRebuilds(t *testing.T) {
	als := map[string]float64{"factors": 64, "alpha": 1}
	b := bundle.New(bundle.Manifest{ModelVersion: "b1", ItemCount: 1, Dim: 1, MaxSeqLen: 5, UserEncoder: "onnx",
		SHA256: map[string]string{"items.ids": "i"}}, []string{"", "i1"}, matrix(t, 1, []float32{0, 1}), nil, nil)
	b1 := rebuiltAt(t, b, "b1", testStart, als, "")
	s8 := rebuiltAt(t, b, "s8", testStart, als, "")
	s8.Stream = &bundle.Stream{PopularWindowMS: 14 * day}
	set := func(from *bundle.Bundle, days ...int64) []*bundle.Bundle {
		var out []*bundle.Bundle
		for _, d := range days {
			r := rebuiltAt(t, b, fmt.Sprint("r", d), testStart+d*day, als, from.ModelVersion)
			r.Stream = from.Stream
			out = append(out, r)
		}
		return out
	}
	check := func(bundles []*bundle.Bundle, from *bundle.Bundle) (int64, error) {
		var times []int64
		for _, r := range bundles {
			at, err := checkRebuilt(r, b1, from)
			if err != nil {
				return 0, err
			}
			times = append(times, at)
		}
		return rebuildTimes(times, 10*day)
	}
	for _, from := range []*bundle.Bundle{b1, s8} {
		latest, err := check(set(from, 20, 0, 10), from)
		if err != nil || latest != testStart+20*day {
			t.Fatalf("from %s: latest %d, %v", from.ModelVersion, latest, err)
		}
	}
	other := func(edit func(*bundle.Bundle)) []*bundle.Bundle {
		out := set(b1, 0, 10, 20)
		edit(out[1])
		return out
	}
	for name, c := range map[string]struct {
		bundles []*bundle.Bundle
		from    *bundle.Bundle
	}{
		"missing":     {set(b1, 0, 20), b1},
		"twice":       {set(b1, 0, 10, 10, 20), b1},
		"not from 0":  {set(b1, 10, 20), b1},
		"off grid":    {set(b1, 0, 10, 19), b1},
		"none":        {nil, b1},
		"ranker":      {set(s8, 0, 10, 20), b1},
		"from B1, S8": {set(b1, 0, 10, 20), s8},
		"no stream": {func() []*bundle.Bundle {
			out := set(s8, 0, 10, 20)
			out[2].Stream = nil
			return out
		}(), s8},
		"other items":  {other(func(r *bundle.Bundle) { r.SHA256 = map[string]string{"items.ids": "j"} }), b1},
		"mean encoder": {other(func(r *bundle.Bundle) { r.UserEncoder = "mean" }), b1},
		"other ALS": {other(func(r *bundle.Bundle) {
			*r = *rebuiltAt(t, b, "x", testStart+10*day, map[string]float64{"factors": 128, "alpha": 1}, "b1")
		}), b1},
	} {
		if _, err := check(c.bundles, c.from); err == nil {
			t.Errorf("%s: no error", name)
		}
	}
	for _, c := range []struct {
		last int64
		ok   bool
	}{{20, true}, {29, true}, {30, false}, {19, false}} {
		if err := rebuiltThrough(testStart+20*day, 10*day, testStart+c.last*day); (err == nil) != c.ok {
			t.Errorf("latest test event %d days in: %v", c.last, err)
		}
	}
}

// B1s6 and B1s8 pass with B1's items, vectors and encoder, the window
// chosen and six and eight features; another file of B1's, another window
// or the rankers' features swapped do not.
func TestCheckStreamBundles(t *testing.T) {
	sums := map[string]string{"items.ids": "i", "items.npy": "v", "user_encoder.onnx": "e"}
	stream := func(version string, features []string, window int64, edit func(map[string]string)) *bundle.Bundle {
		s := maps.Clone(sums)
		edit(s)
		return bundle.New(bundle.Manifest{ModelVersion: version, Stream: &bundle.Stream{PopularWindowMS: window}, RankerFeatures: features, SHA256: s},
			[]string{""}, matrix(t, 1, []float32{0}), nil, nil)
	}
	b1 := bundle.New(bundle.Manifest{ModelVersion: "b1", SHA256: sums}, []string{""}, matrix(t, 1, []float32{0}), nil, nil)
	six, eight := bundle.RankerFeatures, slices.Concat(bundle.RankerFeatures, bundle.RecencyFeatures)
	same := func(map[string]string) {}
	if err := checkStreamBundles(b1, stream("s6", six, 14*day, same), stream("s8", eight, 14*day, same), 14*day); err != nil {
		t.Fatal(err)
	}
	for name, c := range map[string][2]*bundle.Bundle{
		"other vectors": {stream("s6", six, 14*day, same), stream("s8", eight, 14*day, func(s map[string]string) { s["items.npy"] = "w" })},
		"other encoder": {stream("s6", six, 14*day, func(s map[string]string) { s["user_encoder.onnx"] = "m" }), stream("s8", eight, 14*day, same)},
		"other window":  {stream("s6", six, 14*day, same), stream("s8", eight, 7*day, same)},
		"swapped":       {stream("s8", eight, 14*day, same), stream("s6", six, 14*day, same)},
	} {
		if err := checkStreamBundles(b1, c[0], c[1], 14*day); err == nil {
			t.Errorf("%s: no error", name)
		}
	}
}

// The stream state's window counts pass the check, at the requests' own
// times among events at one time; a state that counts events at the
// request's time, or misses those at the window's start, does not.
func TestCheckWindowCounts(t *testing.T) {
	b := bundle.New(bundle.Manifest{ModelVersion: "t", ItemCount: 3, Dim: 1, MaxSeqLen: 5}, []string{"", "a", "b", "c"}, matrix(t, 1, []float32{0, 1, 1, 1}), nil, nil)
	rows := []dataset.Row{{User: "u", Item: "a", TS: 10}, {User: "v", Item: "a", TS: 10}, {User: "u", Item: "b", TS: 15},
		{User: "w", Item: "a", TS: 20}, {User: "v", Item: "c", TS: 25}, {User: "u", Item: "a", TS: 30}}
	s := fresh.New(3)
	for _, r := range rows {
		id, _ := b.Lookup(r.Item)
		s.Add(fresh.Event{Item: id, TS: r.TS})
	}
	if err := checkWindowCounts(b, s, 10, rows, rows); err != nil {
		t.Fatal(err)
	}
	for name, c := range map[string]windowCounter{
		"at ts":        shifted{s, 0, 1},
		"window start": shifted{s, 1, 0},
	} {
		if err := checkWindowCounts(b, c, 10, rows, rows); err == nil {
			t.Errorf("%s: no error", name)
		}
	}
}

// shifted counts over [from + lo, to + hi): a broken window.
type shifted struct {
	s      *fresh.State
	lo, hi int64
}

func (c shifted) Count(item int32, from, to int64) int { return c.s.Count(item, from+c.lo, to+c.hi) }

// freshnessFixture returns section 7's inputs on random bundles and 600
// random events: the conditions' freshness, whose rebuilt bundles open
// from memory, the events, and the rebuilt bundles of R7, R30 and S3+R30.
func freshnessFixture(t *testing.T) (freshness, []dataset.Row, [3][]*bundle.Bundle) {
	t.Helper()
	r := rand.New(rand.NewPCG(31, 31))
	const n, w = 30, 40
	bundleOf := func(version string, edit func(*bundle.Manifest)) *bundle.Bundle {
		p, err := randomPipeline(r, n, 0)
		if err != nil {
			t.Fatal(err)
		}
		m := p.Bundle.Manifest
		m.ModelVersion = version
		edit(&m)
		return bundle.New(m, p.Bundle.IDs, p.Bundle.Vectors, p.Bundle.Cooc, p.Bundle.Popular)
	}
	b1 := bundleOf("b1", func(*bundle.Manifest) {})
	stream := func(features []string) func(*bundle.Manifest) {
		return func(m *bundle.Manifest) { m.Stream, m.RankerFeatures = &bundle.Stream{PopularWindowMS: w}, features }
	}
	s6 := bundle.New(b1.Manifest, b1.IDs, b1.Vectors, b1.Cooc, b1.Popular)
	stream(bundle.RankerFeatures)(&s6.Manifest)
	s6.ModelVersion = "s6"
	s8 := bundle.New(b1.Manifest, b1.IDs, b1.Vectors, b1.Cooc, b1.Popular)
	stream(slices.Concat(bundle.RankerFeatures, bundle.RecencyFeatures))(&s8.Manifest)
	s8.ModelVersion = "s8"
	rebuilds := func(prefix string, edit func(*bundle.Manifest), ats ...int64) []*bundle.Bundle {
		var out []*bundle.Bundle
		for _, at := range ats {
			out = append(out, rebuiltAt(t, bundleOf(fmt.Sprint(prefix, at), edit), fmt.Sprint(prefix, at), at, nil, ""))
		}
		return out
	}
	var rows []dataset.Row
	for i := range 600 {
		rows = append(rows, dataset.Row{User: fmt.Sprint("u", r.IntN(25)), Item: fmt.Sprint("i", 1+r.IntN(n)), TS: int64(r.IntN(200))})
		rows[i].TS -= rows[i].TS % 3 // events at one time
	}
	store := history.NewMemory()
	evs := make([]history.Event, len(rows))
	for i, e := range rows {
		evs[i] = history.Event{ID: history.EventID(e.User, e.Item, e.TS, dataset.Kind), User: e.User, Item: e.Item, TS: e.TS}
	}
	if err := store.Append(context.Background(), evs); err != nil {
		t.Fatal(err)
	}
	state, err := streamState(b1, store, rows)
	if err != nil {
		t.Fatal(err)
	}
	r7 := rebuilds("r7-", func(*bundle.Manifest) {}, 0, 70, 140)
	r30 := rebuilds("r30-", func(*bundle.Manifest) {}, 0, 100)
	r30s8 := rebuilds("r30s8-", stream(slices.Concat(bundle.RankerFeatures, bundle.RecencyFeatures)), 0, 100)
	f := freshness{b1: b1, s6: s6, s8: s8, store: store, state: state, window: w,
		r7: memoryRefs(t, r7), r30: memoryRefs(t, r30), r30s8: memoryRefs(t, r30s8),
		// B1s6's ranker weighs the sources otherwise than B1's, and B1s8's
		// as B1s6's, with the recency features.
		rank: func(b *bundle.Bundle) Ranker {
			wt := onnxtest.LinearW
			switch {
			case b.Recency():
				wt = []float32{1, 3, 2, 0.5, 0.25, -0.5, 4, -1}
			case b.Stream != nil:
				wt = []float32{1, 3, 2, 0.5, 0.25, -0.5}
			}
			return rankFunc(func(_ context.Context, x []float32) ([]float32, error) {
				out := make([]float32, len(x)/len(wt))
				for i := range out {
					for j, v := range wt {
						out[i] += v * x[i*len(wt)+j]
					}
				}
				return out, nil
			})
		}}
	return f, rows, [3][]*bundle.Bundle{r7, r30, r30s8}
}

// memoryRefs refers to bundles in memory, which close as nothing.
func memoryRefs(t *testing.T, bundles []*bundle.Bundle) []rebuiltRef {
	t.Helper()
	var refs []rebuiltRef
	for _, b := range bundles {
		at, err := rebuildTime(b)
		if err != nil {
			t.Fatal(err)
		}
		refs = append(refs, rebuiltRef{at, func() (*bundle.Bundle, func() error, error) { return b, func() error { return nil }, nil }})
	}
	return refs
}

// Section 7's conditions on random bundles and events: each is valid, and
// each differs from the one it is compared with, which a condition run on
// another's bundles, sources or ranker would not.
func TestFreshnessConditions(t *testing.T) {
	f, rows, _ := freshnessFixture(t)
	got, err := f.rows(rows, t.Logf)
	if err != nil {
		t.Fatal(err)
	}
	ranks := map[string][]int{}
	for _, row := range got {
		ranks[row.name] = row.ranks
	}
	for _, pair := range [][2]string{{"S1", "F0"}, {"S2", "S1"}, {"S3a", "S2"}, {"S3", "S3a"}, {"R7", "F0"}, {"R30", "R7"},
		{"S3+R30", "R30"}, {"S3+R30", "S3"}, {"vector reference", "F0"}, {"popularity reference", "S2"}} {
		if slices.Equal(ranks[pair[0]], ranks[pair[1]]) {
			t.Errorf("%s and %s give the same ranks", pair[0], pair[1])
		}
	}
	if _, err := diffTable(got, func() []string {
		users := make([]string, len(rows))
		for i, e := range rows {
			users[i] = e.User
		}
		return users
	}(), true); err != nil {
		t.Fatal(err)
	}
}

// The rebuilt conditions open one bundle at a time, each once, for the
// events it answers, and give the ranks of the same conditions run on
// pipelines over every bundle at once, which the bundle before would
// change; a bundle opened for another's events fails the run.
func TestRunPeriodic(t *testing.T) {
	f, events, sets := freshnessFixture(t)
	for i, c := range []struct {
		name   string
		refs   []rebuiltRef
		stream bool
	}{{"R7", f.r7, false}, {"R30", f.r30, false}, {"S3+R30", f.r30s8, true}} {
		pr, err := f.periodic(sets[i], c.stream)
		if err != nil {
			t.Fatal(err)
		}
		want, _, err := runConditionOn(pr.pipeline, qualityCondition{name: c.name}, events)
		if err != nil {
			t.Fatal(err)
		}
		before := pr
		before.choose = func(ts int64) int {
			return max(0, latestAtOrBefore(len(pr.bundles), func(k int) int64 { return pr.bundles[k].at }, ts)-1)
		}
		if other, _, err := runConditionOn(before.pipeline, qualityCondition{name: c.name}, events); err != nil || slices.Equal(other, want) {
			t.Fatalf("%s: the bundle before gives the same ranks, %v", c.name, err)
		}

		open, most := 0, 0
		opened := make([]int, len(c.refs))
		refs := slices.Clone(c.refs)
		for k := range refs {
			inner := c.refs[k].open
			refs[k].open = func() (*bundle.Bundle, func() error, error) {
				b, closeB, err := inner()
				opened[k]++
				open++
				most = max(most, open)
				return b, func() error { open--; return closeB() }, err
			}
		}
		got, err := f.runPeriodic(c.name, refs, c.stream, events)
		if err != nil || !slices.Equal(got, want) {
			t.Errorf("%s: %v; the ranks of every bundle at once: %t", c.name, err, slices.Equal(got, want))
		}
		if most != 1 || open != 0 || slices.ContainsFunc(opened, func(n int) bool { return n != 1 }) {
			t.Errorf("%s: %d open at most, %d left open, opened %v; want one at a time, each once", c.name, most, open, opened)
		}

		for _, shift := range []int{-1, 1} {
			wrong := slices.Clone(c.refs)
			for k := range wrong {
				wrong[k].open = c.refs[min(max(k+shift, 0), len(c.refs)-1)].open
			}
			if _, err := f.runPeriodic(c.name, wrong, c.stream, events); err == nil || !strings.Contains(err.Error(), "opened for") {
				t.Errorf("%s: each period's bundle %+d opened: %v", c.name, shift, err)
			}
		}
	}
}

// H1 reads confirmed when S3's recall@10 exceeds F0's for every event,
// contradicted when it falls short, and not confirmed when they are equal,
// whose interval is [0, 0].
func TestDiffTableReadsH1(t *testing.T) {
	const events = 200
	users := make([]string, events)
	for i := range users {
		users[i] = fmt.Sprint("u", i%40)
	}
	ranks := func(rank int) []int {
		out := make([]int, events)
		for i := range out {
			out[i] = rank
		}
		return out
	}
	for _, c := range []struct {
		s3, f0 int
		want   string
	}{{1, 0, "confirmed"}, {0, 1, "contradicted"}, {1, 1, "not confirmed"}} {
		var rows []qualityRow
		for _, name := range []string{"F0", "S1", "S2", "S3a", "R7", "R30", "S3+R30"} {
			rows = append(rows, qualityRow{name, ranks(c.f0)})
		}
		rows = append(rows, qualityRow{"S3", ranks(c.s3)})
		table, err := diffTable(rows, users, true)
		if err != nil {
			t.Fatal(err)
		}
		if !strings.HasSuffix(table, ": "+c.want+"\n") {
			t.Errorf("S3 at rank %d, F0 at %d:\n%s", c.s3, c.f0, table)
		}
	}
}
