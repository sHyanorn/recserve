// Command gateway serves recommendations from one bundle at a time and
// swaps bundles through its admin API (docs/SPEC.md).
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
	"github.com/sHyanorn/recserve/internal/cache"
	"github.com/sHyanorn/recserve/internal/dataset"
	"github.com/sHyanorn/recserve/internal/events"
	"github.com/sHyanorn/recserve/internal/history"
	"github.com/sHyanorn/recserve/internal/onnx"
	"github.com/sHyanorn/recserve/internal/recommend"
	"github.com/sHyanorn/recserve/internal/shard"
)

// defaultAdminAddr keeps the admin API, which has no authentication, on this host.
const defaultAdminAddr = "127.0.0.1:8081"

func main() {
	addr := flag.String("addr", ":8080", "listen address")
	adminAddr := flag.String("admin-addr", defaultAdminAddr, "admin API listen address (POST /v1/admin/swap), without authentication; empty for none")
	dir := flag.String("bundle", "", "bundle directory")
	testSplit := flag.String("test-split", "", "test split file; its first event bounds the bundle's train cutoff")
	preload := flag.String("preload", "", "comma-separated split files whose events are loaded into the history store at start")
	valkeyAddr := flag.String("valkey-addr", "", "Valkey address for the user histories; empty keeps them in this process")
	kafkaBrokers := flag.String("kafka-brokers", "", "comma-separated Kafka brokers: /v1/events produces to the events topic; empty makes it write the histories itself")
	eventsTimeout := flag.Duration("events-timeout", api.DefaultEventsTimeout, "time /v1/events waits for its events to be accepted before it answers 503; at least 1s with -kafka-brokers, 0 for none without")
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
	cacheSize := flag.Int("cache-size", 13000, "result cache entries; 0 for no cache")
	cacheTTL := flag.Duration("cache-ttl", 5*time.Minute, "result cache time to live, changed by up to 10% either way")
	flag.Parse()
	log := slog.New(slog.NewJSONHandler(os.Stderr, nil))
	if err := run(log, *addr, *adminAddr, *dir, *testSplit, *preload, *ortLib, *valkeyAddr, *kafkaBrokers, *eventsTimeout, shards, budgets, *cacheSize, *cacheTTL, *drain); err != nil {
		log.Error("gateway stopped", "err", err)
		os.Exit(1)
	}
}

func run(log *slog.Logger, addr, adminAddr, dir, testSplit, preload, ortLib, valkeyAddr, kafkaBrokers string, eventsTimeout time.Duration, shards shard.Config, budgets recommend.Budgets, cacheSize int, cacheTTL time.Duration, drain time.Duration) (err error) {
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
	var c *cache.Cache[recommend.Response]
	if cacheSize > 0 {
		c = cache.New[recommend.Response](cacheSize, cacheTTL)
	}
	store, closeStore, err := openStore(valkeyAddr)
	if err != nil {
		return err
	}
	defer closeStore()
	prepare := preparer(opt, store, shards, budgets, c)
	p, err := prepare(dir)
	if err != nil {
		return err
	}
	live := recommend.NewLive(p, log)
	closeLive := true // until serving starts; then shutdown closes it
	defer func() {
		if closeLive {
			err = errors.Join(err, live.Close())
		}
	}()
	if preload != "" {
		for _, path := range strings.Split(preload, ",") {
			n, err := preloadHistory(store, path)
			if err != nil {
				return err
			}
			log.Info("history preloaded", "file", path, "events", n)
		}
	}
	producer, closeProducer, err := openProducer(kafkaBrokers, store, eventsTimeout)
	if err != nil {
		return err
	}
	defer closeProducer() // after the servers have shut down
	srv := api.New(live, store, log)
	srv.Prepare, srv.Events, srv.EventsTimeout = prepare, producer, eventsTimeout
	servers := []*http.Server{{Addr: addr, Handler: srv.Handler(), ReadHeaderTimeout: 5 * time.Second}}
	if adminAddr != "" {
		servers = append(servers, &http.Server{Addr: adminAddr, Handler: srv.AdminHandler(), ReadHeaderTimeout: 5 * time.Second})
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	errc := make(chan error, len(servers))
	for i, hs := range servers {
		ln, err := net.Listen("tcp", hs.Addr)
		if err != nil {
			return err
		}
		if i == 1 && !loopback(ln.Addr()) { // the admin listener
			log.Warn("the admin API listens beyond this host and has no authentication", "addr", ln.Addr().String())
		}
		go func() { errc <- hs.Serve(ln) }()
	}
	srv.SetReady(true)
	log.Info("serving", "addr", addr, "admin_addr", adminAddr, "model_version", p.Bundle.ModelVersion, "items", p.Bundle.ItemCount,
		"shards", shards.Shards, "shard_mode", shards.Mode, "zero_vectors", p.Shards.Zeroes())
	var serveErr error
	select {
	case serveErr = <-errc:
	case <-ctx.Done():
	}
	srv.SetReady(false)
	closeLive = false
	return errors.Join(serveErr, shutdown(log, servers, live, drain))
}

// shutdown shuts the servers down, giving requests in flight drain to finish,
// then closes live. Requests still running at the deadline may hold the
// active bundle, and closing it would wait for them without limit, so it
// then logs a warning and returns without closing it.
func shutdown(log *slog.Logger, servers []*http.Server, live *recommend.Live, drain time.Duration) error {
	ctx, cancel := context.WithTimeout(context.Background(), drain)
	defer cancel()
	var errs []error
	for _, hs := range servers {
		errs = append(errs, hs.Shutdown(ctx))
	}
	err := errors.Join(errs...)
	if errors.Is(err, context.DeadlineExceeded) {
		log.Warn("requests still running after the drain; exiting without closing the bundle", "drain", drain)
		return err
	}
	return errors.Join(err, live.Close())
}

// openStore returns the history store on the Valkey server at addr, or an
// in-process one if addr is empty, and the function that closes it.
func openStore(addr string) (history.Store, func(), error) {
	if addr == "" {
		return history.NewMemory(), func() {}, nil
	}
	v, err := history.NewValkey(addr)
	if err != nil {
		return nil, nil, err
	}
	return v, v.Close, nil
}

// openProducer returns what takes the events of /v1/events, and the
// function that closes it: a producer to the events topic on the brokers,
// or, without brokers, one that writes them to store itself.
func openProducer(brokers string, store history.Store, timeout time.Duration) (events.Producer, func(), error) {
	if brokers == "" {
		return events.Direct{Store: store}, func() {}, nil
	}
	k, err := events.NewKafka(strings.Split(brokers, ","), events.Topic, timeout)
	if err != nil {
		return nil, nil, err
	}
	return k, k.Close, nil
}

// preparer returns the function that loads, verifies and indexes the
// bundle in a directory, for the first bundle and for every swap: the
// checksums, golden cases and train cutoff are checked against opt, and
// the vectors split into shards by cfg. The pipelines share store and
// cache c, and have budgets.
func preparer(opt bundle.Options, store history.Store, cfg shard.Config, budgets recommend.Budgets, c *cache.Cache[recommend.Response]) func(dir string) (*recommend.Pipeline, error) {
	return func(dir string) (*recommend.Pipeline, error) {
		b, err := bundle.Load(dir, opt)
		if errors.Is(err, onnx.ErrNotInitialized) {
			return nil, fmt.Errorf("%w (start with -ort-lib)", err)
		}
		if err != nil {
			return nil, err
		}
		p, err := recommend.New(b, store, cfg)
		if err != nil {
			return nil, errors.Join(err, b.Close())
		}
		p.Budgets, p.Cache = budgets, c
		return p, nil
	}
}

// loopback reports whether a listener's address is reachable from this
// host only.
func loopback(a net.Addr) bool {
	t, ok := a.(*net.TCPAddr)
	return ok && t.IP.IsLoopback()
}

func preloadHistory(store history.Store, path string) (int, error) {
	rows, err := dataset.Read(path)
	if err != nil {
		return 0, err
	}
	evs := make([]history.Event, len(rows))
	for i, r := range rows {
		evs[i] = history.Event{ID: history.EventID(r.User, r.Item, r.TS, dataset.Kind), User: r.User, Item: r.Item, TS: r.TS}
	}
	return len(evs), store.Append(context.Background(), evs)
}
