package recommend

import (
	"bytes"
	"cmp"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"net/http"
	"os"
	"path"
	"runtime"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/sHyanorn/recserve/internal/bundle"
	"github.com/sHyanorn/recserve/internal/dataset"
	"github.com/sHyanorn/recserve/internal/fresh"
	"github.com/sHyanorn/recserve/internal/history"
	"github.com/sHyanorn/recserve/internal/shard"
)

// The stream state's cost in serving (docs/BENCHMARKS.md, Stream state in
// serving): its benchmarks on the train and valid splits, and the gate
// that compares a gateway in compose with the pipeline in process.

func historyEvents(rows []dataset.Row) []history.Event {
	evs := make([]history.Event, len(rows))
	for i, r := range rows {
		evs[i] = history.Event{ID: history.EventID(r.User, r.Item, r.TS, dataset.Kind), User: r.User, Item: r.Item, TS: r.TS}
	}
	return evs
}

func byTime(rows []dataset.Row) []dataset.Row {
	out := slices.Clone(rows)
	slices.SortStableFunc(out, func(a, b dataset.Row) int { return cmp.Compare(a.TS, b.TS) })
	return out
}

// streamBench is the data of the benchmarks: B1 at RECSERVE_BUNDLE with
// the histories of the train and valid splits (loadRealData), B1s8 at
// RECSERVE_BUNDLE_S8, and the two splits' events, the valid split's in
// time order.
type streamBench struct {
	rd           realData
	s8           *bundle.Bundle
	train, valid []history.Event
}

func loadStreamBench(b *testing.B) streamBench {
	b.Helper()
	rd := loadRealData(b)
	dir := os.Getenv("RECSERVE_BUNDLE_S8")
	if dir == "" {
		b.Skip("RECSERVE_BUNDLE_S8 not set")
	}
	s8, err := bundle.Load(dir, bundle.Options{FirstTestTS: rd.first})
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() {
		if err := s8.Close(); err != nil {
			b.Error(err)
		}
	})
	if rd.bundle.Stream != nil || s8.Stream == nil || s8.SHA256["items.ids"] != rd.bundle.SHA256["items.ids"] {
		b.Fatal("RECSERVE_BUNDLE must name no window, and RECSERVE_BUNDLE_S8 a window, with the same items")
	}
	train := rd.train[:len(rd.train)-len(rd.valid)]
	return streamBench{rd: rd, s8: s8, train: historyEvents(train), valid: historyEvents(byTime(rd.valid))}
}

// feed returns a feed that holds evs and B1s8's state over them.
func (sb streamBench) feed(b *testing.B, evs ...[]history.Event) (*fresh.Feed, *fresh.State) {
	b.Helper()
	f := fresh.NewFeed()
	for _, e := range evs {
		f.Add(e...)
	}
	st, release := f.State(sb.s8.SHA256["items.ids"], sb.s8.ItemCount, sb.s8.Lookup)
	b.Cleanup(release)
	return f, st
}

// BenchmarkRealStreamTake takes the valid split's events one at a time, in
// time order, into a feed that holds the train split's events and B1s8's
// state, as the gateway takes an event (D062): the time per event, and the
// heap each adds, after GC. Its -benchtime must not exceed the valid
// split's 34,510 events.
func BenchmarkRealStreamTake(b *testing.B) {
	sb := loadStreamBench(b)
	feed, _ := sb.feed(b, sb.train)
	var before, after runtime.MemStats
	runtime.GC()
	runtime.ReadMemStats(&before)
	n := 0
	for b.Loop() {
		if n == len(sb.valid) {
			b.Fatalf("more iterations than the %d valid events", len(sb.valid))
		}
		feed.Add(sb.valid[n])
		n++
	}
	runtime.GC()
	runtime.ReadMemStats(&after)
	// The loaded data stays live too: freed between the two readings, it
	// would count against the events.
	runtime.KeepAlive(feed)
	runtime.KeepAlive(sb)
	b.ReportMetric(float64(int64(after.HeapAlloc)-int64(before.HeapAlloc))/float64(n), "heap-B/event")
}

// BenchmarkRealStreamReads reads B1s8's state over the train and valid
// splits at each valid event's time: popular, a popular list that misses
// the one kept, the times being distinct and in order; rows, the
// co-occurrence rows of the request's seeds, its user's 10 latest history
// items as the pipeline takes them; count, the window count and the first
// event of the event's item.
func BenchmarkRealStreamReads(b *testing.B) {
	sb := loadStreamBench(b)
	_, st := sb.feed(b, sb.train, sb.valid)
	w := sb.s8.Stream.PopularWindowMS
	type query struct {
		ts    int64
		item  int32
		seeds []int32
	}
	var qs []query
	var times []int64
	seeds := 0
	for _, e := range sb.valid {
		raw, err := sb.rd.store.Before(context.Background(), e.User, e.TS, 0)
		if err != nil {
			b.Fatal(err)
		}
		hist := internalIDs(sb.s8, raw)
		item, _ := sb.s8.Lookup(e.Item)
		q := query{e.TS, item, hist[max(0, len(hist)-bundle.CoocSeeds):]}
		qs, seeds = append(qs, q), seeds+len(q.seeds)
		if len(times) == 0 || times[len(times)-1] != e.TS {
			times = append(times, e.TS)
		}
	}
	b.Run("popular", func(b *testing.B) {
		i := 0
		for b.Loop() {
			st.Popular(times[i%len(times)], w, bundle.PopularTop)
			i++
		}
	})
	b.Run("rows", func(b *testing.B) {
		i := 0
		for b.Loop() {
			q := qs[i%len(qs)]
			for _, s := range q.seeds {
				st.Row(s, q.ts, bundle.CoocWidth)
			}
			i++
		}
		b.ReportMetric(float64(seeds)/float64(len(qs)), "seeds/op")
	})
	b.Run("count", func(b *testing.B) {
		i := 0
		for b.Loop() {
			q := qs[i%len(qs)]
			st.Count(q.item, q.ts-w, q.ts)
			st.First(q.item, q.ts)
			i++
		}
	})
}

// BenchmarkRealStreamPipeline answers every tenth valid event's request,
// in time order and with k 20 as loadgen asks, through the pipeline in
// process with every stage budget off and two in-process shards: F0 on B1
// without the state, S3 on B1s8 with its state over the train and valid
// splits. A degraded response fails it. A -benchtime of 3451x answers each
// request once per count.
func BenchmarkRealStreamPipeline(b *testing.B) {
	sb := loadStreamBench(b)
	_, st := sb.feed(b, sb.train, sb.valid)
	var reqs []Request
	for i := 0; i < len(sb.valid); i += 10 {
		reqs = append(reqs, Request{User: sb.valid[i].User, K: 20, TS: sb.valid[i].TS})
	}
	for _, c := range []struct {
		name  string
		b     *bundle.Bundle
		state *fresh.State
	}{{"F0", sb.rd.bundle, nil}, {"S3", sb.s8, st}} {
		b.Run(c.name, func(b *testing.B) {
			s, err := shard.Build(c.b.Vectors, shard.DefaultConfig)
			if err != nil {
				b.Fatal(err)
			}
			p := New(c.b, sb.rd.store, s)
			p.Fresh = c.state
			i := 0
			for b.Loop() {
				resp, err := p.Recommend(context.Background(), reqs[i%len(reqs)])
				if err != nil || len(resp.Degraded) > 0 {
					b.Fatalf("request %d: degraded %v, %v", i%len(reqs), resp.Degraded, err)
				}
				i++
			}
		})
	}
}

// gateway is a running gateway, its admin API and its ingest consumer's
// metrics, by address.
type gateway struct{ api, admin, ingest string }

func (g gateway) post(t *testing.T, addr, path string, body any) []byte {
	t.Helper()
	raw, err := json.Marshal(body)
	if err != nil {
		t.Fatal(err)
	}
	resp, err := http.Post("http://"+addr+path, "application/json", bytes.NewReader(raw))
	if err != nil {
		t.Fatal(err)
	}
	out, err := io.ReadAll(resp.Body)
	if err := errors.Join(err, resp.Body.Close()); err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode/100 != 2 {
		t.Fatalf("%s %s: %d %s", path, raw, resp.StatusCode, out)
	}
	return out
}

func (g gateway) recommend(t *testing.T, user string, ts int64) Response {
	t.Helper()
	var r Response
	if err := json.Unmarshal(g.post(t, g.api, "/v1/recommend", map[string]any{"user_id": user, "k": 20, "ts": ts}), &r); err != nil {
		t.Fatal(err)
	}
	return r
}

// metric returns the value of series on addr's /metrics, and false if it
// has none.
func metric(t *testing.T, addr, series string) (float64, bool) {
	t.Helper()
	resp, err := http.Get("http://" + addr + "/metrics")
	if err != nil {
		t.Fatal(err)
	}
	out, err := io.ReadAll(resp.Body)
	if err := errors.Join(err, resp.Body.Close()); err != nil {
		t.Fatal(err)
	}
	for line := range strings.Lines(string(out)) {
		if v, ok := strings.CutPrefix(strings.TrimSpace(line), series+" "); ok {
			var f float64
			if _, err := fmt.Sscan(v, &f); err != nil {
				t.Fatal(err)
			}
			return f, true
		}
	}
	return 0, false
}

// compare sends the requests of rows to the gateway one at a time and
// requires the items, in order, that p gives, and no degradation. It
// returns the largest difference between their scores.
func (g gateway) compare(t *testing.T, p *Pipeline, rows []dataset.Row) float64 {
	t.Helper()
	var worst float64
	for i, r := range rows {
		got := g.recommend(t, r.User, r.TS)
		want, err := p.Recommend(context.Background(), Request{User: r.User, K: 20, TS: r.TS})
		if err != nil {
			t.Fatal(err)
		}
		if len(got.Degraded) > 0 || got.ModelVersion != p.Bundle.ModelVersion {
			t.Fatalf("request %d: degraded %v, version %s", i, got.Degraded, got.ModelVersion)
		}
		ids := func(items []Item) []string {
			out := make([]string, len(items))
			for j, it := range items {
				out[j] = it.ID
			}
			return out
		}
		if !slices.Equal(ids(got.Items), ids(want.Items)) {
			t.Fatalf("request %d (user %s at %d):\n got %v\nwant %v", i, r.User, r.TS, ids(got.Items), ids(want.Items))
		}
		for j := range got.Items {
			worst = max(worst, math.Abs(got.Items[j].Score-want.Items[j].Score))
		}
	}
	return worst
}

// TestRealDataStreamGateway is the gate of docs/BENCHMARKS.md, Stream state
// in serving. It runs when RECSERVE_GATEWAY names a gateway that serves the
// bundle at RECSERVE_BUNDLE, which names a window, with the train and valid
// splits of RECSERVE_DATA preloaded and a new events topic;
// RECSERVE_GATEWAY_ADMIN its admin API, RECSERVE_INGEST its ingest
// consumer's metrics, and RECSERVE_VIA the directory, as the gateway sees
// it, of a bundle without a window to swap to and back. The gateway answers
// the requests of the first 1,000 test events as the pipeline in process
// does over the same bundle, histories and state; again for the next 1,000
// once the first 2,000 are written through /v1/events and applied; and
// again for the next 1,000 after the swap to RECSERVE_VIA and back, which
// builds its state anew. The 100 earliest valid events' requests go first,
// not compared.
func TestRealDataStreamGateway(t *testing.T) {
	g := gateway{os.Getenv("RECSERVE_GATEWAY"), os.Getenv("RECSERVE_GATEWAY_ADMIN"), os.Getenv("RECSERVE_INGEST")}
	via := os.Getenv("RECSERVE_VIA")
	if g.api == "" {
		t.Skip("RECSERVE_GATEWAY not set")
	}
	if g.admin == "" || g.ingest == "" || via == "" {
		t.Fatal("RECSERVE_GATEWAY_ADMIN, RECSERVE_INGEST and RECSERVE_VIA are needed too")
	}
	skipUnlessVideoGames(t)
	rd := loadRealData(t)
	if rd.bundle.Stream == nil {
		t.Fatal("RECSERVE_BUNDLE names no window")
	}
	st, err := streamState(rd.bundle, rd.store, rd.train)
	if err != nil {
		t.Fatal(err)
	}
	s, err := shard.Build(rd.bundle.Vectors, shard.DefaultConfig)
	if err != nil {
		t.Fatal(err)
	}
	p := New(rd.bundle, rd.store, s)
	p.Fresh = st
	test := byTime(rd.test)
	if len(test) < 4000 {
		t.Fatalf("%d test events, want at least 4,000", len(test))
	}
	for _, r := range byTime(rd.valid)[:100] { // not compared
		g.recommend(t, r.User, r.TS)
	}
	t.Logf("preload: largest score difference %g", g.compare(t, p, test[:1000]))

	written := test[:2000]
	for _, r := range written {
		g.post(t, g.api, "/v1/events", map[string]any{"events": []map[string]any{{"user_id": r.User, "item_id": r.Item, "ts": r.TS, "kind": dataset.Kind}}})
	}
	for deadline := time.Now().Add(time.Minute); ; time.Sleep(100 * time.Millisecond) {
		applied, ok1 := metric(t, g.api, `recserve_stream_events_total{result="applied"}`)
		lag, ok2 := metric(t, g.api, "recserve_stream_lag_records")
		ingested, ok3 := metric(t, g.ingest, `recserve_ingest_records_total{result="applied"}`)
		if !ok1 || !ok2 || !ok3 {
			t.Fatal("a metric is missing: the gateway's stream counters or lag, or ingest's records")
		}
		if applied == float64(len(written)) && lag == 0 && ingested >= float64(len(written)) {
			break
		}
		if applied > float64(len(written)) || time.Now().After(deadline) {
			t.Fatalf("applied %v, lag %v, ingested %v; want %d, 0 and %d", applied, lag, ingested, len(written), len(written))
		}
	}
	if err := rd.store.Append(context.Background(), historyEvents(written)); err != nil {
		t.Fatal(err)
	}
	if p.Fresh, err = streamState(rd.bundle, rd.store, append(slices.Clone(rd.train), written...)); err != nil {
		t.Fatal(err)
	}
	t.Logf("written: largest score difference %g", g.compare(t, p, test[2000:3000]))

	for _, dir := range []string{via, path.Join(path.Dir(via), rd.bundle.ModelVersion)} {
		g.post(t, g.admin, "/v1/admin/swap", map[string]string{"bundle": dir})
	}
	t.Logf("rebuilt: largest score difference %g", g.compare(t, p, test[3000:4000]))
}
