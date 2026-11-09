// Command gateway serves recommendations from one bundle (docs/SPEC.md).
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
	"strings"
	"syscall"
	"time"

	"github.com/sHyanorn/recserve/internal/api"
	"github.com/sHyanorn/recserve/internal/bundle"
	"github.com/sHyanorn/recserve/internal/dataset"
	"github.com/sHyanorn/recserve/internal/history"
	"github.com/sHyanorn/recserve/internal/onnx"
	"github.com/sHyanorn/recserve/internal/recommend"
	"github.com/sHyanorn/recserve/internal/shard"
)

func main() {
	addr := flag.String("addr", ":8080", "listen address")
	dir := flag.String("bundle", "", "bundle directory")
	testSplit := flag.String("test-split", "", "test split file; its first event bounds the bundle's train cutoff")
	preload := flag.String("preload", "", "comma-separated split files whose events are loaded into the history store at start")
	drain := flag.Duration("drain", 10*time.Second, "time allowed for in-flight requests at shutdown")
	ortLib := flag.String("ort-lib", "", "ONNX Runtime shared library (libonnxruntime.so.1.31.x), needed by a bundle with an ONNX user encoder or a ranker")
	shards := shard.DefaultConfig
	flag.IntVar(&shards.Shards, "shards", shards.Shards, "in-process shards of the item vectors")
	flag.StringVar((*string)(&shards.Mode), "shard-mode", string(shards.Mode), "vector search in each shard: exact or hnsw")
	flag.IntVar(&shards.Ef, "hnsw-ef", shards.Ef, "HNSW search list size, with -shard-mode hnsw")
	budgets := recommend.DefaultBudgets
	flag.DurationVar(&budgets.History, "budget-history", budgets.History, "history lookup budget; 0 for none")
	flag.DurationVar(&budgets.Encoder, "budget-encoder", budgets.Encoder, "user encoder budget; 0 for none")
	flag.DurationVar(&budgets.Retrieval, "budget-retrieval", budgets.Retrieval, "retrieval budget, all sources; 0 for none")
	flag.DurationVar(&budgets.Shard, "budget-shard", budgets.Shard, "budget of each shard search; 0 for none")
	flag.DurationVar(&budgets.Ranker, "budget-ranker", budgets.Ranker, "ranker budget; 0 for none")
	flag.Parse()
	log := slog.New(slog.NewJSONHandler(os.Stderr, nil))
	if err := run(log, *addr, *dir, *testSplit, *preload, *ortLib, shards, budgets, *drain); err != nil {
		log.Error("gateway stopped", "err", err)
		os.Exit(1)
	}
}

func run(log *slog.Logger, addr, dir, testSplit, preload, ortLib string, shards shard.Config, budgets recommend.Budgets, drain time.Duration) (err error) {
	if dir == "" {
		return errors.New("-bundle is required")
	}
	if ortLib != "" {
		if err := onnx.Init(ortLib); err != nil {
			return err
		}
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
	if errors.Is(err, onnx.ErrNotInitialized) {
		return fmt.Errorf("%w (start with -ort-lib)", err)
	}
	if err != nil {
		return err
	}
	defer func() { err = errors.Join(err, b.Close()) }()
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
	p, err := recommend.New(b, store, shards)
	if err != nil {
		return err
	}
	p.Budgets = budgets
	srv := api.New(p, store, log)
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
	log.Info("serving", "addr", addr, "model_version", b.ModelVersion, "items", b.ItemCount,
		"shards", shards.Shards, "shard_mode", shards.Mode, "zero_vectors", p.Shards.Zeroes())
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
