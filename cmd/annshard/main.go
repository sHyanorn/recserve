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
	"time"

	"google.golang.org/grpc"

	"github.com/sHyanorn/recserve/internal/annshard"
)

func main() {
	addr := flag.String("addr", ":9090", "gRPC listen address, without TLS or authentication: it must be reachable from the gateways only")
	bundles := flag.String("bundles", "", "directory that holds one bundle directory per version, named by the version")
	drain := flag.Duration("drain", 10*time.Second, "time allowed for the calls in flight at shutdown")
	flag.Parse()
	log := slog.New(slog.NewJSONHandler(os.Stderr, nil))
	if err := run(log, *addr, *bundles, *drain); err != nil {
		log.Error("annshard stopped", "err", err)
		os.Exit(1)
	}
}

func run(log *slog.Logger, addr, bundles string, drain time.Duration) error {
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
	return serve(ctx, log, ln, bundles, drain)
}

// serve answers on ln from the bundle directories under bundles until ctx
// ends. Then it reports the replica not ready, takes no new call, and
// returns once the calls in flight have finished or drain has passed,
// which ends those still running (D045).
func serve(ctx context.Context, log *slog.Logger, ln net.Listener, bundles string, drain time.Duration) error {
	gs := grpc.NewServer(annshard.ServerOptions()...)
	s := annshard.NewServer(bundles, log)
	s.Register(gs)
	go func() {
		<-ctx.Done()
		s.Shutdown()
		cut := time.AfterFunc(drain, func() {
			log.Warn("calls still running after the drain are ended", "drain", drain)
			gs.Stop()
		})
		gs.GracefulStop()
		cut.Stop()
	}()
	return gs.Serve(ln)
}
