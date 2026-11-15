package main

import (
	"compress/gzip"
	"context"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kfake"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/sHyanorn/recserve/internal/api"
	"github.com/sHyanorn/recserve/internal/bundle"
	"github.com/sHyanorn/recserve/internal/bundle/bundletest"
	"github.com/sHyanorn/recserve/internal/events"
	"github.com/sHyanorn/recserve/internal/experiment"
	"github.com/sHyanorn/recserve/internal/fresh"
	"github.com/sHyanorn/recserve/internal/history"
	"github.com/sHyanorn/recserve/internal/recommend"
	"github.com/sHyanorn/recserve/internal/shard"
)

// withWindow gives a test bundle a stream window of 1,000 ms.
func withWindow(_ map[string][]byte, m map[string]any) {
	m["stream"] = map[string]any{"popular_window_ms": 1000}
}

func otherItems(f map[string][]byte, _ map[string]any) {
	f["items.ids"] = []byte(strings.Replace(string(f["items.ids"]), "item12\n", "itemX\n", 1))
}

func ev(user, item string, ts int64) history.Event {
	return events.Event{UserID: user, ItemID: item, TS: ts, Kind: "rating"}.History()
}

// The preparer serves a bundle with a window from the feed's state for its
// items: bundles with the same items share one, a bundle with other items
// has its own, built from every event kept, and one without a window has
// none. Each later event reaches every state held, and a state is no
// longer fed once the pipelines holding it are closed. Without a feed a
// bundle with a window is rejected.
func TestPreparerStreamState(t *testing.T) {
	ctx := context.Background()
	feed := fresh.NewFeed()
	feed.Add(ev("u", "item2", 1), ev("u", "item5", 2))
	prepare := preparer(bundle.Options{}, history.NewMemory(), localShards(t), recommend.Budgets{}, nil, feed)
	pipelines := map[string]*recommend.Pipeline{}
	for name, dir := range map[string]string{
		"a":     bundletest.Write(t, withWindow),
		"a2":    bundletest.Write(t, bundletest.Variant(t, 1), withWindow),
		"other": bundletest.Write(t, withWindow, otherItems),
		"plain": bundletest.Write(t),
	} {
		p, err := prepare(dir)
		if err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		pipelines[name] = p
	}
	a, a2, other, plain := pipelines["a"], pipelines["a2"], pipelines["other"], pipelines["plain"]
	if a.Fresh == nil || a.Fresh != a2.Fresh || other.Fresh == nil || other.Fresh == a.Fresh || plain.Fresh != nil {
		t.Fatal("states not shared by the same items, shared by other items, or given to a bundle without a window")
	}
	if !slices.Equal(a.Fresh.Row(5, 3, 5), []int32{2}) || !slices.Equal(other.Fresh.Row(5, 3, 5), []int32{2}) {
		t.Fatal("a state does not hold the events kept before it was built")
	}
	if resp, err := a.Recommend(ctx, recommend.Request{User: "w", K: 3, TS: 10}); err != nil || len(resp.Items) == 0 {
		t.Fatalf("recommend from the state: %+v %v", resp, err)
	}
	feed.Add(ev("v", "item7", 3))
	if a.Fresh.Count(7, 0, 4) != 1 || other.Fresh.Count(7, 0, 4) != 1 {
		t.Fatal("an event did not reach every state held")
	}
	if err := other.Close(); err != nil {
		t.Fatal(err)
	}
	feed.Add(ev("v", "item8", 4))
	if other.Fresh.Count(8, 0, 5) != 0 || a.Fresh.Count(8, 0, 5) != 1 {
		t.Fatal("a closed pipeline's state is still fed, or a held one is not")
	}
	for _, p := range []*recommend.Pipeline{a, a2, plain} {
		if err := p.Close(); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := preparer(bundle.Options{}, history.NewMemory(), localShards(t), recommend.Budgets{}, nil, nil)(bundletest.Write(t, withWindow)); err == nil || !strings.Contains(err.Error(), "keeps no stream state") {
		t.Fatalf("a bundle with a window without a feed: %v", err)
	}
}

// metric returns the value of the sample named by series, "name{labels}"
// as the text format writes it, on srv's /metrics, and false if there is
// none.
func metric(t *testing.T, srv *api.Server, series string) (float64, bool) {
	t.Helper()
	rec := httptest.NewRecorder()
	srv.Handler().ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	for line := range strings.Lines(rec.Body.String()) {
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

// Without brokers, the events the producer stores reach the feed, an event
// sent twice counted as a duplicate; there is no lag metric.
func TestFollowDirect(t *testing.T) {
	store, feed := history.NewMemory(), fresh.NewFeed()
	srv := api.New(nil, store, slog.New(slog.DiscardHandler))
	producer, stop := follow(feed, nil, events.Direct{Store: store}, srv)
	defer stop()
	e := events.Event{UserID: "u", ItemID: "item1", TS: 5, Kind: "rating"}
	for range 2 {
		if err := producer.Produce(context.Background(), []events.Event{e}); err != nil {
			t.Fatal(err)
		}
	}
	items, err := store.Before(context.Background(), "u", 6, 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(items) != 1 || feed.Len() != 1 {
		t.Fatalf("store holds %v, feed %d events; want one each", items, feed.Len())
	}
	for series, want := range map[string]float64{
		`recserve_stream_events_total{result="applied"}`:   1,
		`recserve_stream_events_total{result="duplicate"}`: 1,
		`recserve_stream_events_total{result="poison"}`:    0,
	} {
		if got, ok := metric(t, srv, series); !ok || got != want {
			t.Errorf("%s = %v (present %v), want %v", series, got, ok, want)
		}
	}
	if _, ok := metric(t, srv, "recserve_stream_lag_records"); ok {
		t.Error("a lag without a topic")
	}
}

// With brokers, the feed takes every event produced to the topic after the
// start, from every partition, and none before; duplicates and records
// that hold no event are counted apart; the lag returns to 0.
func TestFollowTopic(t *testing.T) {
	cluster, err := kfake.NewCluster(kfake.NumBrokers(1), kfake.SeedTopics(6, events.Topic))
	if err != nil {
		t.Fatal(err)
	}
	defer cluster.Close()
	cl, err := kgo.NewClient(kgo.SeedBrokers(cluster.ListenAddrs()...), kgo.DefaultProduceTopic(events.Topic))
	if err != nil {
		t.Fatal(err)
	}
	defer cl.Close()
	produce := func(values ...string) {
		t.Helper()
		for i, v := range values {
			if err := cl.ProduceSync(context.Background(), &kgo.Record{Key: fmt.Appendf(nil, "k%d", i), Value: []byte(v)}).FirstErr(); err != nil {
				t.Fatal(err)
			}
		}
	}
	value := func(user string, ts int64) string {
		return fmt.Sprintf(`{"user_id":%q,"item_id":"item1","ts":%d,"kind":"rating"}`, user, ts)
	}
	produce(value("before", 1))
	tail, err := events.NewTail(context.Background(), cluster.ListenAddrs(), events.Topic, slog.New(slog.DiscardHandler))
	if err != nil {
		t.Fatal(err)
	}
	defer tail.Close()
	feed := fresh.NewFeed()
	srv := api.New(nil, history.NewMemory(), slog.New(slog.DiscardHandler))
	_, stop := follow(feed, tail, nil, srv)
	defer stop()
	var after []string
	for i := range 20 {
		after = append(after, value(fmt.Sprint("u", i), 2))
	}
	produce(append(after, after[0], "{}")...)
	want := map[string]float64{
		`recserve_stream_events_total{result="applied"}`:   20,
		`recserve_stream_events_total{result="duplicate"}`: 1,
		`recserve_stream_events_total{result="poison"}`:    1,
		`recserve_stream_lag_records`:                      0,
	}
	for deadline := time.Now().Add(10 * time.Second); ; time.Sleep(10 * time.Millisecond) {
		done := true
		for series, v := range want {
			if got, ok := metric(t, srv, series); !ok || got != v {
				done = false
			}
		}
		if done {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("the feed did not take the events produced after its start")
		}
	}
	if feed.Len() != 20 {
		t.Errorf("feed holds %d events, want 20: an event produced before the start was taken", feed.Len())
	}
}

// writeSplit writes rows as a split file.
func writeSplit(t *testing.T, rows ...history.Event) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "split.csv.gz")
	f, err := os.Create(path)
	if err != nil {
		t.Fatal(err)
	}
	var b strings.Builder
	b.WriteString("user_id,parent_asin,rating,timestamp\n")
	for _, r := range rows {
		fmt.Fprintf(&b, "%s,%s,5.0,%d\n", r.User, r.Item, r.TS)
	}
	z := gzip.NewWriter(f)
	if _, err := io.WriteString(z, b.String()); err != nil {
		t.Fatal(err)
	}
	if err := z.Close(); err != nil {
		t.Fatal(err)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}
	return path
}

// A gateway started on a bundle with a window serves it from the state:
// the preload is in it, and an event sent to /v1/events reaches it.
func TestRunStreamState(t *testing.T) {
	dir := bundletest.Write(t, withWindow)
	preload := writeSplit(t, history.Event{User: "u", Item: "item2", TS: 10}, history.Event{User: "u", Item: "item5", TS: 20})
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := ln.Addr().String()
	if err := ln.Close(); err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() {
		done <- run(slog.New(slog.NewTextHandler(io.Discard, nil)), addr, "", dir, "", preload, "", "", "", 0, shard.DefaultConfig, "", 0, time.Minute,
			recommend.Budgets{}, 0, time.Minute, 5*time.Second, 0, experiment.None(), false, api.Limits{})
	}()
	for deadline := time.Now().Add(20 * time.Second); ; time.Sleep(20 * time.Millisecond) {
		resp, err := http.Get("http://" + addr + "/readyz")
		if err == nil && resp.Body.Close() == nil && resp.StatusCode == http.StatusOK {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("the gateway is not ready")
		}
	}
	items := func() []string {
		code, out := post(t, "http://"+addr+"/v1/recommend", `{"user_id":"w","k":10,"ts":100}`)
		if code != http.StatusOK {
			t.Fatalf("recommend: %d %v", code, out)
		}
		var ids []string
		for _, it := range out["items"].([]any) {
			ids = append(ids, it.(map[string]any)["id"].(string))
		}
		slices.Sort(ids)
		return ids
	}
	if got := items(); !slices.Equal(got, []string{"item2", "item5"}) {
		t.Fatalf("before the event: %v, want the preload's items", got)
	}
	if code, out := post(t, "http://"+addr+"/v1/events", `{"events":[{"user_id":"v","item_id":"item9","ts":50,"kind":"rating"}]}`); code != http.StatusAccepted {
		t.Fatalf("events: %d %v", code, out)
	}
	if got := items(); !slices.Equal(got, []string{"item2", "item5", "item9"}) {
		t.Fatalf("after the event: %v, want item9 too", got)
	}
	if err := syscall.Kill(os.Getpid(), syscall.SIGTERM); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(20 * time.Second):
		t.Fatal("still running 20 s after SIGTERM")
	}
}
