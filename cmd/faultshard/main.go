// Command faultshard is annshard with one fault, for the fault runs of
// docs/BENCHMARKS.md (F6) only: no manifest and no default image build
// runs it. After SIGUSR1 every search waits until its caller cancels it,
// while gRPC's transport goes on answering keepalive pings, so the gateway
// keeps the replica as ready (D041 Limits point 2); SIGUSR2 ends the fault.
package main

import (
	"context"
	"errors"
	"flag"
	"log/slog"
	"net"
	"os"
	"os/signal"
	"sync/atomic"
	"syscall"

	"google.golang.org/grpc"
	"google.golang.org/grpc/status"

	"github.com/sHyanorn/recserve/internal/annshard"
	"github.com/sHyanorn/recserve/internal/annshard/annshardpb"
)

func main() {
	addr := flag.String("addr", ":9090", "gRPC listen address")
	bundles := flag.String("bundles", "", "directory that holds one bundle directory per version, named by the version")
	flag.Parse()
	log := slog.New(slog.NewJSONHandler(os.Stderr, nil))
	if err := run(log, *addr, *bundles); err != nil {
		log.Error("faultshard stopped", "err", err)
		os.Exit(1)
	}
}

func run(log *slog.Logger, addr, bundles string) error {
	if bundles == "" {
		return errors.New("-bundles is required")
	}
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return err
	}
	var h hang
	switches := make(chan os.Signal, 1)
	signal.Notify(switches, syscall.SIGUSR1, syscall.SIGUSR2)
	go h.follow(switches)
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	gs := grpc.NewServer(append(annshard.ServerOptions(), grpc.UnaryInterceptor(h.intercept))...)
	annshard.NewServer(bundles, log).Register(gs)
	go func() {
		<-ctx.Done()
		gs.Stop()
	}()
	log.Info("serving", "addr", ln.Addr().String(), "bundles", bundles)
	return gs.Serve(ln)
}

// hang holds searches while on.
type hang struct{ on atomic.Bool }

// follow turns the fault on at SIGUSR1 and off at SIGUSR2.
func (h *hang) follow(signals <-chan os.Signal) {
	for s := range signals {
		h.on.Store(s == syscall.SIGUSR1)
	}
}

func (h *hang) intercept(ctx context.Context, req any, _ *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
	if _, ok := req.(*annshardpb.SearchRequest); ok && h.on.Load() {
		<-ctx.Done()
		return nil, status.FromContextError(ctx.Err()).Err()
	}
	return handler(ctx, req)
}
