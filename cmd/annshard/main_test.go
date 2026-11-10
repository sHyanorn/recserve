package main

import (
	"bytes"
	"context"
	"io"
	"log/slog"
	"net"
	"strings"
	"sync"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"

	"github.com/sHyanorn/recserve/internal/annshard/annshardpb"
	"github.com/sHyanorn/recserve/internal/annshard/annshardtest"
	"github.com/sHyanorn/recserve/internal/vec"
)

// start runs serve over root with drain, logging to log, until the returned
// function is called, as SIGTERM does, and returns the replica's address
// and the channel serve's result arrives on.
func start(t *testing.T, root string, drain time.Duration, log io.Writer) (string, context.CancelFunc, <-chan error) {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	ctx, stop := context.WithCancel(context.Background())
	t.Cleanup(stop)
	served := make(chan error, 1)
	go func() { served <- serve(ctx, slog.New(slog.NewTextHandler(log, nil)), ln, root, drain) }()
	return ln.Addr().String(), stop, served
}

func dial(t *testing.T, addr string) *grpc.ClientConn {
	t.Helper()
	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { conn.Close() }) //nolint:errcheck // the test is over
	return conn
}

// returned fails the test unless serve's result arrives within d, and
// returns the time it took.
func returned(t *testing.T, served <-chan error, d time.Duration) time.Duration {
	t.Helper()
	start := time.Now()
	select {
	case err := <-served:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(d):
		t.Fatalf("still serving after %v", d)
	}
	return time.Since(start)
}

// The process serves the shard API and the standard health service, ready
// once it holds the version served, and stops when its context ends.
func TestServe(t *testing.T) {
	root := t.TempDir()
	m, err := vec.NewMatrix(2, []float32{0, 0, 1, 0, 0, 1, 2, 2})
	if err != nil {
		t.Fatal(err)
	}
	version := annshardtest.WriteVectors(t, root, m)
	addr, stop, served := start(t, root, 10*time.Second, io.Discard)
	conn := dial(t, addr)
	call, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	health := func(want healthpb.HealthCheckResponse_ServingStatus) {
		t.Helper()
		h, err := healthpb.NewHealthClient(conn).Check(call, &healthpb.HealthCheckRequest{})
		if err != nil || h.GetStatus() != want {
			t.Fatalf("health: %v, %v; want %v", h.GetStatus(), err, want)
		}
	}
	health(healthpb.HealthCheckResponse_NOT_SERVING)
	ix := &annshardpb.Index{Version: version, Shards: 1, Mode: annshardpb.Mode_MODE_EXACT}
	c := annshardpb.NewAnnShardClient(conn)
	if _, err := c.Prepare(call, &annshardpb.PrepareRequest{Index: ix, ServingVersion: version}); err != nil {
		t.Fatal(err)
	}
	health(healthpb.HealthCheckResponse_SERVING)
	got, err := c.Search(call, &annshardpb.SearchRequest{Index: ix, Query: []float32{1, 1}, K: 1})
	if err != nil || len(got.GetIds()) != 1 || got.GetIds()[0] != 3 || got.GetScores()[0] != 4 {
		t.Fatalf("search: %v, %v", got, err)
	}
	stop()
	returned(t, served, 10*time.Second)
}

// Told to stop, a replica reports NOT_SERVING at once and takes no new
// call; the calls in flight go on, and it stops when they have ended, or
// at the end of the drain with one still running (D045). A health watch
// is the call in flight: it lasts until the client ends it.
func TestServeDrains(t *testing.T) {
	root := t.TempDir()
	m, err := vec.NewMatrix(2, []float32{0, 0, 1, 0})
	if err != nil {
		t.Fatal(err)
	}
	version := annshardtest.WriteVectors(t, root, m)
	watch := func(conn *grpc.ClientConn) (healthpb.Health_WatchClient, context.CancelFunc) {
		t.Helper()
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		t.Cleanup(cancel)
		w, err := healthpb.NewHealthClient(conn).Watch(ctx, &healthpb.HealthCheckRequest{})
		if err != nil {
			t.Fatal(err)
		}
		return w, cancel
	}
	next := func(w healthpb.Health_WatchClient, want healthpb.HealthCheckResponse_ServingStatus) {
		t.Helper()
		if h, err := w.Recv(); err != nil || h.GetStatus() != want {
			t.Fatalf("watch: %v, %v; want %v", h.GetStatus(), err, want)
		}
	}

	addr, stop, served := start(t, root, 10*time.Second, io.Discard)
	conn := dial(t, addr)
	ix := &annshardpb.Index{Version: version, Shards: 1, Mode: annshardpb.Mode_MODE_EXACT}
	if _, err := annshardpb.NewAnnShardClient(conn).Prepare(context.Background(), &annshardpb.PrepareRequest{Index: ix, ServingVersion: version}); err != nil {
		t.Fatal(err)
	}
	w, end := watch(conn)
	next(w, healthpb.HealthCheckResponse_SERVING)
	stop()
	next(w, healthpb.HealthCheckResponse_NOT_SERVING)
	for deadline := time.Now().Add(5 * time.Second); ; time.Sleep(10 * time.Millisecond) {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		_, err := healthpb.NewHealthClient(dial(t, addr)).Check(ctx, &healthpb.HealthCheckRequest{})
		cancel()
		if err != nil {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("a new call answered 5 s after the stop")
		}
	}
	select {
	case err := <-served:
		t.Fatalf("stopped with a call in flight: %v", err)
	case <-time.After(500 * time.Millisecond):
	}
	end()
	returned(t, served, 5*time.Second)

	// A call that does not end is cut at the end of the drain, which the
	// log says before the replica stops.
	var log lockedBuffer
	addr, stop, served = start(t, root, time.Second, &log)
	w, _ = watch(dial(t, addr))
	next(w, healthpb.HealthCheckResponse_NOT_SERVING)
	stop()
	if took := returned(t, served, 10*time.Second); took < 900*time.Millisecond {
		t.Fatalf("stopped after %v, before the drain of 1 s", took)
	}
	if !strings.Contains(log.String(), "after the drain") {
		t.Fatalf("the log does not say that calls were cut:\n%s", log.String())
	}
}

// lockedBuffer is a log that the replica writes while the test reads it.
type lockedBuffer struct {
	mu sync.Mutex
	b  bytes.Buffer
}

func (l *lockedBuffer) Write(p []byte) (int, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.b.Write(p)
}

func (l *lockedBuffer) String() string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.b.String()
}

func TestRunRejects(t *testing.T) {
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	for _, bundles := range []string{"", t.TempDir() + "/missing"} {
		if err := run(log, "127.0.0.1:0", bundles, time.Second); err == nil {
			t.Errorf("-bundles %q accepted", bundles)
		}
	}
}
