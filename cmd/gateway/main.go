// Command gateway serves recommendations from one bundle (docs/SPEC.md).
package main

import (
	"context"
	"errors"
	"flag"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/sHyanorn/recserve/internal/api"
	"github.com/sHyanorn/recserve/internal/bundle"
	"github.com/sHyanorn/recserve/internal/dataset"
	"github.com/sHyanorn/recserve/internal/history"
	"github.com/sHyanorn/recserve/internal/recommend"
)

func main() {
	addr := flag.String("addr", ":8080", "listen address")
	dir := flag.String("bundle", "", "bundle directory")
	testSplit := flag.String("test-split", "", "test split file; its first event bounds the bundle's train cutoff")
	preload := flag.String("preload", "", "comma-separated split files whose events are loaded into the history store at start")
	drain := flag.Duration("drain", 10*time.Second, "time allowed for in-flight requests at shutdown")
	flag.Parse()
	log := slog.New(slog.NewJSONHandler(os.Stderr, nil))
	if err := run(log, *addr, *dir, *testSplit, *preload, *drain); err != nil {
		log.Error("gateway stopped", "err", err)
		os.Exit(1)
	}
}

func run(log *slog.Logger, addr, dir, testSplit, preload string, drain time.Duration) error {
	if dir == "" {
		return errors.New("-bundle is required")
	}
	var opt bundle.Options
	if testSplit != "" {
		rows, err := dataset.Read(testSplit)
		if err != nil {
			return err
		}
		if len(rows) == 0 {
			return errors.New("-test-split is empty")
		}
		opt.FirstTestTS = dataset.FirstTS(rows)
	} else {
		log.Warn("no -test-split: the bundle's train cutoff is not checked")
	}
	b, err := bundle.Load(dir, opt)
	if err != nil {
		return err
	}
	store := history.NewMemory()
	if preload != "" {
		for _, path := range strings.Split(preload, ",") {
			n, err := preloadHistory(store, b, path)
			if err != nil {
				return err
			}
			log.Info("history preloaded", "file", path, "events", n)
		}
	}
	srv := api.New(&recommend.Pipeline{Bundle: b, History: store}, store, log)
	hs := &http.Server{Addr: addr, Handler: srv.Handler(), ReadHeaderTimeout: 5 * time.Second}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return err
	}
	errc := make(chan error, 1)
	go func() { errc <- hs.Serve(ln) }()
	srv.SetReady(true)
	log.Info("serving", "addr", addr, "model_version", b.ModelVersion, "items", b.ItemCount)
	select {
	case err := <-errc:
		return err
	case <-ctx.Done():
	}
	srv.SetReady(false)
	sctx, cancel := context.WithTimeout(context.Background(), drain)
	defer cancel()
	return hs.Shutdown(sctx)
}

func preloadHistory(store history.Store, b *bundle.Bundle, path string) (int, error) {
	rows, err := dataset.Read(path)
	if err != nil {
		return 0, err
	}
	evs := make([]history.Event, 0, len(rows))
	for _, r := range rows {
		if id, ok := b.Lookup(r.Item); ok {
			evs = append(evs, history.Event{ID: history.EventID(r.User, r.Item, r.TS, dataset.Kind), User: r.User, Item: id, TS: r.TS})
		}
	}
	return len(evs), store.Append(context.Background(), evs)
}
