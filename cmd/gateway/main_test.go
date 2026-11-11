package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"net/http/httptrace"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/sHyanorn/recserve/internal/annshard/annshardtest"
	"github.com/sHyanorn/recserve/internal/api"
	"github.com/sHyanorn/recserve/internal/breaker"
	"github.com/sHyanorn/recserve/internal/bundle"
	"github.com/sHyanorn/recserve/internal/bundle/bundletest"
	"github.com/sHyanorn/recserve/internal/cache"
	"github.com/sHyanorn/recserve/internal/events"
	"github.com/sHyanorn/recserve/internal/experiment"
	"github.com/sHyanorn/recserve/internal/history"
	"github.com/sHyanorn/recserve/internal/infratest"
	"github.com/sHyanorn/recserve/internal/onnx/onnxtest"
	"github.com/sHyanorn/recserve/internal/recommend"
	"github.com/sHyanorn/recserve/internal/shard"
)

func post(t *testing.T, url, body string) (int, map[string]any) {
	t.Helper()
	resp, err := http.Post(url, "application/json", strings.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	raw, err := io.ReadAll(resp.Body)
	if err := errors.Join(err, resp.Body.Close()); err != nil {
		t.Fatal(err)
	}
	var out map[string]any
	if err := json.Unmarshal(raw, &out); err != nil {
		t.Fatal(err)
	}
	return resp.StatusCode, out
}

// localShards returns the gateway's function for shards in this process.
func localShards(t *testing.T) func(*bundle.Bundle) (recommend.Shards, error) {
	t.Helper()
	index, _, _, err := openShards(nil, shard.DefaultConfig, 0, 0, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	return index
}

// With replicas, the preparer has them prepare the bundle's version, and
// the pipeline answers as the one over in-process shards does; a version
// the replicas lack is rejected.
func TestPreparerOnReplicas(t *testing.T) {
	dir := bundletest.Write(t)
	root := t.TempDir() // the replicas' bundle root: one directory per version
	if err := os.Symlink(dir, filepath.Join(root, bundletest.VersionOf(t, dir))); err != nil {
		t.Fatal(err)
	}
	index, _, closeShards, err := openShards(annshardtest.StartAll(t, root, 2, 1), shard.DefaultConfig, 0, 10*time.Second, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer closeShards()
	store := history.NewMemory()
	if err := store.Append(context.Background(), []history.Event{{ID: 1, User: "u", Item: "item2", TS: 1}, {ID: 2, User: "u", Item: "item5", TS: 2}}); err != nil {
		t.Fatal(err)
	}
	remote, err := preparer(bundle.Options{}, store, index, recommend.Budgets{}, nil)(dir)
	if err != nil {
		t.Fatal(err)
	}
	local, err := preparer(bundle.Options{}, store, localShards(t), recommend.Budgets{}, nil)(dir)
	if err != nil {
		t.Fatal(err)
	}
	req := recommend.Request{User: "u", K: 5, TS: 10}
	got, err := remote.Recommend(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
	want, err := local.Recommend(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
	if len(got.Items) == 0 || !slices.Equal(got.Items, want.Items) || len(got.Degraded) > 0 || *got.Coverage != *want.Coverage {
		t.Fatalf("got %+v\nwant %+v", got, want)
	}
	if _, err := preparer(bundle.Options{}, store, index, recommend.Budgets{}, nil)(bundletest.Write(t, bundletest.Variant(t, 1))); err == nil {
		t.Fatal("a version the replicas lack was prepared")
	}
}

// Swaps go through the gateway's own preparer, so a bundle that fails
// bundle.Load's checks, as the first bundle would, is rejected with 422 and
// the active one keeps serving.
func TestSwapThroughPreparer(t *testing.T) {
	opt := bundle.Options{FirstTestTS: 150}
	store := history.NewMemory()
	prepare := preparer(opt, store, localShards(t), recommend.DefaultBudgets, cache.New[recommend.Response](100, time.Hour))
	p, err := prepare(bundletest.Write(t))
	if err != nil {
		t.Fatal(err)
	}
	live := recommend.NewLive(p, slog.New(slog.NewTextHandler(io.Discard, nil)))
	t.Cleanup(func() {
		if err := live.Close(); err != nil {
			t.Error(err)
		}
	})
	srv := api.New(live, store, slog.New(slog.NewTextHandler(io.Discard, nil)))
	srv.Prepare = prepare
	apiSrv, admin := httptest.NewServer(srv.Handler()), httptest.NewServer(srv.AdminHandler())
	t.Cleanup(apiSrv.Close)
	t.Cleanup(admin.Close)

	tampered := bundletest.Write(t, bundletest.Variant(t, 3))
	if err := os.WriteFile(filepath.Join(tampered, "popular.npy"), []byte("x"), 0o644); err != nil {
		t.Fatal(err)
	}
	otherVectors := onnxtest.MeanVectors()
	otherVectors[2] += 0.5 // item 1
	first := bundletest.VersionOf(t, bundletest.Write(t))
	good := bundletest.Write(t, bundletest.Variant(t, 2))
	otherItems := bundletest.Write(t, func(f map[string][]byte, _ map[string]any) {
		f["items.ids"] = []byte(strings.Replace(string(f["items.ids"]), "item12\n", "itemX\n", 1))
	})
	steps := []struct {
		name, dir string
		code      int
		want      string // in the error, or the version now active
	}{
		{"checksum", tampered, http.StatusUnprocessableEntity, "checksum"},
		{"golden", bundletest.Write(t, func(f map[string][]byte, _ map[string]any) {
			f["golden.jsonl"] = []byte(bundletest.GoldenLine(t, otherVectors, 50, []int32{1}))
		}), http.StatusUnprocessableEntity, "user vector"},
		{"cutoff", bundletest.Write(t, bundletest.Variant(t, 5), func(_ map[string][]byte, m map[string]any) {
			m["train_cutoff_ts"] = 150
		}), http.StatusUnprocessableEntity, bundle.ErrCutoff.Error()},
		{"no directory", filepath.Join(t.TempDir(), "none"), http.StatusUnprocessableEntity, "no such file"},
		{"same version", bundletest.Write(t), http.StatusConflict, recommend.ErrActive.Error()},
		{"good", good, http.StatusOK, bundletest.VersionOf(t, good)},
		{"other items", otherItems, http.StatusOK, bundletest.VersionOf(t, otherItems)},
		{"back", bundletest.Write(t), http.StatusOK, first},
	}
	active := first
	for _, s := range steps {
		body, err := json.Marshal(api.SwapRequest{Bundle: s.dir})
		if err != nil {
			t.Fatal(err)
		}
		code, out := post(t, admin.URL+"/v1/admin/swap", string(body))
		if code == http.StatusOK {
			active = out["model_version"].(string)
		}
		got, _ := out["error"].(string)
		if code == http.StatusOK {
			got = active
		}
		if code != s.code || !strings.Contains(got, s.want) {
			t.Fatalf("%s: %d %v, want %d with %q", s.name, code, out, s.code, s.want)
		}
		code, out = post(t, apiSrv.URL+"/v1/recommend", `{"user_id":"u","k":3,"ts":120}`)
		if code != http.StatusOK || out["model_version"] != active {
			t.Fatalf("%s: then recommend: %d %v, want version %s", s.name, code, out, active)
		}
	}
}

func TestLoopback(t *testing.T) {
	host, _, err := net.SplitHostPort(defaultAdminAddr)
	if err != nil || !net.ParseIP(host).IsLoopback() {
		t.Fatalf("default admin address %s is not on the loopback interface", defaultAdminAddr)
	}
	for addr, want := range map[string]bool{"127.0.0.1:0": true, ":0": false} {
		ln, err := net.Listen("tcp", addr)
		if err != nil {
			t.Fatal(err)
		}
		if got := loopback(ln.Addr()); got != want {
			t.Errorf("%s (%s): loopback %v, want %v", addr, ln.Addr(), got, want)
		}
		if err := ln.Close(); err != nil {
			t.Fatal(err)
		}
	}
}

func newLive(t *testing.T) *recommend.Live {
	t.Helper()
	p, err := preparer(bundle.Options{}, history.NewMemory(), localShards(t), recommend.Budgets{}, nil)(bundletest.Write(t))
	if err != nil {
		t.Fatal(err)
	}
	return recommend.NewLive(p, slog.New(slog.NewTextHandler(io.Discard, nil)))
}

// shutdown closes the bundle once the servers have drained, and does not wait
// for a request still running past the drain.
func TestShutdown(t *testing.T) {
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	server := func(live *recommend.Live) *api.Server { return api.New(live, history.NewMemory(), log) }

	live := newLive(t)
	if err := shutdown(log, server(live), nil, live, 0, time.Second); err != nil || live.Version() != "" {
		t.Fatalf("drained: err %v, version %q after shutdown", err, live.Version())
	}

	live = newLive(t)
	entered, unblock := make(chan struct{}), make(chan struct{})
	hs := &http.Server{ReadHeaderTimeout: time.Second, Handler: http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, release, err := live.Acquire()
		if err != nil {
			t.Error(err)
			return
		}
		defer release()
		close(entered)
		<-unblock
		w.WriteHeader(http.StatusOK)
	})}
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	served := make(chan error, 1)
	go func() { served <- hs.Serve(ln) }()
	got := make(chan error, 1)
	go func() {
		resp, err := http.Get("http://" + ln.Addr().String())
		if err == nil {
			err = resp.Body.Close()
		}
		got <- err
	}()
	<-entered
	stopped := make(chan error, 1)
	go func() { stopped <- shutdown(log, server(live), []*http.Server{hs}, live, 0, 20*time.Millisecond) }()
	select {
	case err := <-stopped:
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("shutdown: %v", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("shutdown waited for the request past the drain")
	}
	if live.Version() == "" {
		t.Fatal("shutdown closed a bundle a request still holds")
	}
	close(unblock)
	if err := errors.Join(<-got, live.Close()); err != nil {
		t.Fatal(err)
	}
	if err := <-served; !errors.Is(err, http.ErrServerClosed) {
		t.Fatal(err)
	}
}

// Told to stop, the gateway reports itself not ready at once. It goes on
// answering for the delay, while the load balancers take it out, on new
// connections and on those already open, closing each after its response;
// then it closes its listener, and the requests in flight finish (D045).
func TestShutdownDelay(t *testing.T) {
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	live := newLive(t)
	srv := api.New(live, history.NewMemory(), log)
	srv.SetReady(true)
	entered, unblock := make(chan struct{}), make(chan struct{})
	mux := http.NewServeMux()
	mux.Handle("/", srv.Handler())
	mux.HandleFunc("GET /slow", func(http.ResponseWriter, *http.Request) {
		close(entered)
		<-unblock
	})
	hs := &http.Server{Handler: mux, ReadHeaderTimeout: time.Second}
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	go hs.Serve(ln) //nolint:errcheck // ends with the shutdown below
	// get asks on a connection of tr, a new one if tr is nil, and reports
	// whether the connection was one opened before.
	get := func(tr *http.Transport, path string) (resp *http.Response, reused bool, err error) {
		if tr == nil {
			tr = &http.Transport{}
			defer tr.CloseIdleConnections()
		}
		trace := &httptrace.ClientTrace{GotConn: func(c httptrace.GotConnInfo) { reused = c.Reused }}
		req, err := http.NewRequestWithContext(httptrace.WithClientTrace(context.Background(), trace), http.MethodGet, "http://"+ln.Addr().String()+path, nil)
		if err != nil {
			return nil, false, err
		}
		resp, err = (&http.Client{Transport: tr, Timeout: 30 * time.Second}).Do(req)
		if err != nil {
			return nil, false, err
		}
		return resp, reused, resp.Body.Close()
	}
	kept := &http.Transport{} // holds a connection, idle at the stop
	defer kept.CloseIdleConnections()
	if _, _, err := get(kept, "/livez"); err != nil {
		t.Fatal(err)
	}
	slow := make(chan error, 1)
	go func() {
		resp, _, err := get(nil, "/slow")
		if err == nil && resp.StatusCode != http.StatusOK {
			err = fmt.Errorf("status %d", resp.StatusCode)
		}
		slow <- err
	}()
	select {
	case <-entered:
	case <-time.After(10 * time.Second):
		t.Fatal("the slow request did not arrive")
	}

	const delay = 5 * time.Second
	stopped := make(chan error, 1)
	go func() { stopped <- shutdown(log, srv, []*http.Server{hs}, live, delay, 30*time.Second) }()
	for deadline := time.Now().Add(time.Second); ; time.Sleep(5 * time.Millisecond) {
		if resp, _, err := get(nil, "/readyz"); err == nil && resp.StatusCode == http.StatusServiceUnavailable {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("/readyz is not 503 a second after the stop")
		}
	}
	for _, tr := range []*http.Transport{nil, kept} {
		if resp, reused, err := get(tr, "/livez"); err != nil || resp.StatusCode != http.StatusOK || !resp.Close || reused != (tr != nil) {
			t.Fatalf("during the delay: %v, %v, reused %v; want 200 with Connection: close, reused %v", resp, err, reused, tr != nil)
		}
	}
	for deadline := time.Now().Add(delay + 5*time.Second); ; time.Sleep(10 * time.Millisecond) {
		if _, _, err := get(nil, "/livez"); err != nil {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("still taking requests 5 s after the delay")
		}
	}
	close(unblock)
	if err := <-slow; err != nil {
		t.Fatalf("the request in flight: %v", err)
	}
	select {
	case err := <-stopped:
		if err != nil || live.Version() != "" {
			t.Fatalf("shutdown: %v, version %q", err, live.Version())
		}
	case <-time.After(10 * time.Second):
		t.Fatal("shutdown did not return")
	}
}

// The gateway on annshard replicas, run in this process: it serves, has a
// replica that restarts prepared again with the version served, and stops
// on SIGTERM. Under the race detector this checks the shard client reading
// the version served from its own goroutines while run sets it.
func TestRunOnReplicas(t *testing.T) {
	dir := bundletest.Write(t)
	root := t.TempDir()
	if err := os.Symlink(dir, filepath.Join(root, bundletest.VersionOf(t, dir))); err != nil {
		t.Fatal(err)
	}
	a, stopA := annshardtest.StartOn(t, "127.0.0.1:0", root)
	b := annshardtest.Start(t, root)
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := ln.Addr().String()
	if err := ln.Close(); err != nil {
		t.Fatal(err)
	}
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	done := make(chan error, 1)
	go func() {
		done <- run(log, addr, "", dir, "", "", "", "", "", 0, shard.DefaultConfig, a+";"+b, 0, 10*time.Second,
			recommend.Budgets{}, 0, time.Minute, 5*time.Second, 0, experiment.None(), false, api.Limits{})
	}()
	eventually := func(what string, ok func() bool) {
		t.Helper()
		for deadline := time.Now().Add(20 * time.Second); !ok(); time.Sleep(20 * time.Millisecond) {
			if time.Now().After(deadline) {
				t.Fatal(what)
			}
		}
	}
	eventually("the gateway is not ready", func() bool {
		resp, err := http.Get("http://" + addr + "/readyz")
		if err != nil {
			return false
		}
		return resp.Body.Close() == nil && resp.StatusCode == http.StatusOK
	})
	stopA()
	annshardtest.StartOn(t, a, root)
	eventually("the restarted replica is not ready", func() bool { return annshardtest.Ready(t, a) })
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

// Without an address the histories stay in the process; with one they are
// in Valkey, and a server that does not answer fails the start.
func TestOpenStore(t *testing.T) {
	s, closeStore, err := openStore("")
	if _, ok := s.(*history.Memory); !ok || err != nil {
		t.Fatalf("no address: %T, %v", s, err)
	}
	closeStore()
	if _, _, err := openStore("127.0.0.1:1"); err == nil {
		t.Fatal("a closed port opened a store")
	}
	s, closeStore, err = openStore(infratest.ValkeyAddr(t))
	if _, ok := s.(*history.Valkey); !ok || err != nil {
		t.Fatalf("valkey: %T, %v", s, err)
	}
	defer closeStore()
	if got, err := s.Before(context.Background(), "nobody", 1, 0); err != nil || len(got) != 0 {
		t.Fatalf("Before = %v, %v", got, err)
	}
}

// The result cache is in the process without a Valkey address and on the
// server with one; a size of 0 means no cache, and must be a nil interface
// for the pipeline to see none.
func TestOpenCache(t *testing.T) {
	c, closeCache, err := openCache("", 10, time.Minute)
	if _, ok := c.(*cache.Cache[recommend.Response]); !ok || err != nil {
		t.Fatalf("no address: %T, %v", c, err)
	}
	closeCache()
	for _, addr := range []string{"", "127.0.0.1:1"} {
		c, closeCache, err = openCache(addr, 0, time.Minute)
		if c != nil || err != nil {
			t.Fatalf("size 0 at %q: %T, %v", addr, c, err)
		}
		closeCache()
	}
	if _, _, err := openCache("127.0.0.1:1", 10, time.Minute); err == nil {
		t.Fatal("a closed port opened a cache")
	}
	c, closeCache, err = openCache(infratest.ValkeyAddr(t), 10, time.Minute)
	if _, ok := c.(*cache.Valkey[recommend.Response]); !ok || err != nil {
		t.Fatalf("valkey: %T, %v", c, err)
	}
	closeCache()
}

// Without brokers /v1/events writes the store itself; with them it
// produces to Kafka, and the client is not asked to connect at the start.
func TestOpenProducer(t *testing.T) {
	store := history.NewMemory()
	p, closeProducer, err := openProducer("", store, 0)
	if d, ok := p.(events.Direct); !ok || d.Store != history.Store(store) || err != nil {
		t.Fatalf("no brokers: %T, %v", p, err)
	}
	closeProducer()
	p, closeProducer, err = openProducer("127.0.0.1:1,127.0.0.1:2", store, time.Second)
	if _, ok := p.(*events.Kafka); !ok || err != nil {
		t.Fatalf("brokers: %T, %v", p, err)
	}
	closeProducer()
	if _, _, err := openProducer("127.0.0.1:1", store, 0); err == nil {
		t.Fatal("brokers without a timeout were accepted")
	}
}

type downStore struct{ history.Store }

func (downStore) Before(context.Context, string, int64, int) ([]string, error) {
	return nil, errors.New("down")
}

// The breaker of guard opens after breakerFailures failed history reads,
// logs it, and leaves a gateway without a cache without one.
func TestGuard(t *testing.T) {
	var logged bytes.Buffer
	store, c := guard(downStore{}, nil, slog.New(slog.NewTextHandler(&logged, nil)))
	if c != nil {
		t.Fatalf("no cache became %T", c)
	}
	for i := range breakerFailures + 1 {
		_, err := store.Before(context.Background(), "u", 1, 0)
		if open := errors.Is(err, breaker.ErrOpen); open != (i == breakerFailures) {
			t.Fatalf("read %d: %v", i, err)
		}
	}
	if !strings.Contains(logged.String(), "from=closed to=open") {
		t.Fatalf("log: %q", logged.String())
	}
	_, c = guard(downStore{}, cache.New[recommend.Response](1, time.Minute), slog.New(slog.DiscardHandler))
	if _, ok := c.(breaker.Cache[recommend.Response]); !ok {
		t.Fatalf("the cache is %T", c)
	}
}
