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
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"google.golang.org/grpc"

	"github.com/sHyanorn/recserve/internal/annshard"
	"github.com/sHyanorn/recserve/internal/profiling"
	"github.com/sHyanorn/recserve/internal/tracing"
)

func main() {
	addr := flag.String("addr", ":9090", "gRPC listen address, without TLS or authentication: it must be reachable from the gateways only")
	bundles := flag.String("bundles", "", "directory that holds one bundle directory per version, named by the version")
	drain := flag.Duration("drain", 10*time.Second, "time allowed for the calls in flight at shutdown")
	debugAddr := flag.String("debug-addr", "", "HTTP listen address of the runtime's profiles (/debug/pprof/), without authentication; empty for none")
	flag.Parse()
	log := slog.New(slog.NewJSONHandler(os.Stderr, nil))
	if err := run(log, *addr, *debugAddr, *bundles, *drain); err != nil {
		log.Error("annshard stopped", "err", err)
		os.Exit(1)
	}
}

func run(log *slog.Logger, addr, debugAddr, bundles string, drain time.Duration) error {
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
	if debugAddr != "" {
		dl, err := net.Listen("tcp", debugAddr)
		if err != nil {
			return err
		}
		serveProfiles(log, dl)
	}
	stopTracing, err := tracing.Start("annshard", log)
	if err != nil {
		return err
	}
	defer stopTracing()
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
	opts := annshard.ServerOptions()
	if tracing.Enabled() {
		opts = append(opts, tracing.ServerOption())
	}
	gs := grpc.NewServer(opts...)
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

// serveProfiles serves the runtime's profiles on ln for as long as the
// process runs.
func serveProfiles(log *slog.Logger, ln net.Listener) {
	mux := http.NewServeMux()
	profiling.Handle(mux)
	srv := &http.Server{Handler: mux, ReadHeaderTimeout: 5 * time.Second}
	go func() {
		if err := srv.Serve(ln); !errors.Is(err, net.ErrClosed) {
			log.Error("profiles stopped", "err", err)
		}
	}()
}
