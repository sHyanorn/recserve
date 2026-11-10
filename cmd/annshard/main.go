// Command annshard serves shards of bundles' item vectors over gRPC
// (docs/DECISIONS.md D040). It has no shard settings: the gateway's
// requests name the bundle version, the shard and how it is built.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net"
	"os"
	"os/signal"
	"syscall"

	"google.golang.org/grpc"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"

	"github.com/sHyanorn/recserve/internal/annshard"
	"github.com/sHyanorn/recserve/internal/annshard/annshardpb"
)

func main() {
	addr := flag.String("addr", ":9090", "gRPC listen address, without TLS or authentication: it must be reachable from the gateways only")
	bundles := flag.String("bundles", "", "directory that holds one bundle directory per version, named by the version")
	flag.Parse()
	log := slog.New(slog.NewJSONHandler(os.Stderr, nil))
	if err := run(log, *addr, *bundles); err != nil {
		log.Error("annshard stopped", "err", err)
		os.Exit(1)
	}
}

func run(log *slog.Logger, addr, bundles string) error {
	if bundles == "" {
		return errors.New("-bundles is required")
	}
	if st, err := os.Stat(bundles); err != nil {
		return err
	} else if !st.IsDir() {
		return fmt.Errorf("%s is not a directory", bundles)
	}
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return err
	}
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	log.Info("serving", "addr", ln.Addr().String(), "bundles", bundles)
	return serve(ctx, log, ln, bundles)
}

// serve answers on ln from the bundle directories under bundles until ctx
// ends, then lets the calls in flight finish.
func serve(ctx context.Context, log *slog.Logger, ln net.Listener, bundles string) error {
	gs := grpc.NewServer(annshard.ServerOptions()...)
	annshardpb.RegisterAnnShardServer(gs, annshard.NewServer(bundles, log))
	healthpb.RegisterHealthServer(gs, health.NewServer())
	go func() {
		<-ctx.Done()
		gs.GracefulStop()
	}()
	return gs.Serve(ln)
}
