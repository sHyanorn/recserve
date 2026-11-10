package main

import (
	"context"
	"io"
	"log/slog"
	"net"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"

	"github.com/sHyanorn/recserve/internal/annshard/annshardpb"
	"github.com/sHyanorn/recserve/internal/annshard/annshardtest"
	"github.com/sHyanorn/recserve/internal/vec"
)

// The process serves the shard API and the standard health service, ready
// once it holds the version served, and stops when its context ends.
func TestServe(t *testing.T) {
	root := t.TempDir()
	m, err := vec.NewMatrix(2, []float32{0, 0, 1, 0, 0, 1, 2, 2})
	if err != nil {
		t.Fatal(err)
	}
	version := annshardtest.WriteVectors(t, root, m)
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	ctx, stop := context.WithCancel(context.Background())
	served := make(chan error, 1)
	go func() { served <- serve(ctx, slog.New(slog.NewTextHandler(io.Discard, nil)), ln, root) }()

	conn, err := grpc.NewClient(ln.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close() //nolint:errcheck // the test is over
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
	select {
	case err := <-served:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("still serving after its context ended")
	}
}

func TestRunRejects(t *testing.T) {
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	for _, bundles := range []string{"", t.TempDir() + "/missing"} {
		if err := run(log, "127.0.0.1:0", bundles); err == nil {
			t.Errorf("-bundles %q accepted", bundles)
		}
	}
}
