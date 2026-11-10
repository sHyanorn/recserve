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

	"github.com/prometheus/client_golang/prometheus"

	"github.com/sHyanorn/recserve/internal/annshard"
	"github.com/sHyanorn/recserve/internal/api"
	"github.com/sHyanorn/recserve/internal/breaker"
	"github.com/sHyanorn/recserve/internal/bundle"
	"github.com/sHyanorn/recserve/internal/cache"
	"github.com/sHyanorn/recserve/internal/dataset"
	"github.com/sHyanorn/recserve/internal/events"
	"github.com/sHyanorn/recserve/internal/history"
	"github.com/sHyanorn/recserve/internal/onnx"
	"github.com/sHyanorn/recserve/internal/recommend"
	"github.com/sHyanorn/recserve/internal/shard"
	"github.com/sHyanorn/recserve/internal/valkeyclient"
)

// defaultAdminAddr keeps the admin API, which has no authentication, on this host.
const defaultAdminAddr = "127.0.0.1:8081"

func main() {
	addr := flag.String("addr", ":8080", "listen address")
	adminAddr := flag.String("admin-addr", defaultAdminAddr, "admin API listen address (POST /v1/admin/swap), without authentication; empty for none")
	dir := flag.String("bundle", "", "bundle directory")
	testSplit := flag.String("test-split", "", "test split file; its first event bounds the bundle's train cutoff")
	preload := flag.String("preload", "", "comma-separated split files whose events are loaded into the history store at start")
	valkeyAddr := flag.String("valkey-addr", "", "Valkey address for the user histories and the result cache; empty keeps them in this process")
	kafkaBrokers := flag.String("kafka-brokers", "", "comma-separated Kafka brokers: /v1/events produces to the events topic; empty makes it write the histories itself")
	eventsTimeout := flag.Duration("events-timeout", api.DefaultEventsTimeout, "time /v1/events waits for its events to be accepted before it answers 503; at least 1s with -kafka-brokers, 0 for none without")
	drain := flag.Duration("drain", 10*time.Second, "time allowed for in-flight requests at shutdown")
	ortLib := flag.String("ort-lib", "", "ONNX Runtime shared library (libonnxruntime.so.1.31.x), needed by a bundle with an ONNX user encoder or a ranker")
	shards := shard.DefaultConfig
	flag.IntVar(&shards.Shards, "shards", shards.Shards, "in-process shards of the item vectors; with -shard-addrs the number of shards is the number of address groups")
	shardAddrs := flag.String("shard-addrs", "", "annshard replicas that hold the shards, as \"a:9090,b:9090;c:9090,d:9090\": shards separated by ';', a shard's replicas by ','; empty keeps the shards in this process")
	hedgeDelay := flag.Duration("hedge-delay", 5*time.Millisecond, "with -shard-addrs: time a shard search waits for one replica before it also asks another; 0 never does")
	prepareTimeout := flag.Duration("prepare-timeout", 2*time.Minute, "time the replicas of -shard-addrs get to load and index a bundle version, at start, in a swap and when one is found to lack it")
	flag.StringVar((*string)(&shards.Mode), "shard-mode", string(shards.Mode), "vector search in each shard: exact or hnsw")
	flag.IntVar(&shards.Ef, "hnsw-ef", shards.Ef, "HNSW search list size, with -shard-mode hnsw")
	budgets := recommend.DefaultBudgets
	flag.DurationVar(&budgets.History, "budget-history", budgets.History, "history lookup budget; 0 for none")
	flag.DurationVar(&budgets.Encoder, "budget-encoder", budgets.Encoder, "user encoder budget; 0 for none")
	flag.DurationVar(&budgets.Retrieval, "budget-retrieval", budgets.Retrieval, "retrieval budget, all sources; 0 for none")
	flag.DurationVar(&budgets.Shard, "budget-shard", budgets.Shard, "budget of each shard search; 0 for none")
	flag.DurationVar(&budgets.Ranker, "budget-ranker", budgets.Ranker, "ranker budget; 0 for none")
	cacheSize := flag.Int("cache-size", 13000, "result cache entries; 0 for no cache. With -valkey-addr the cache is on Valkey, bounded by the server's memory, and this only turns it on or off")
	cacheTTL := flag.Duration("cache-ttl", 5*time.Minute, "result cache time to live, changed by up to 10% either way")
	flag.Parse()
	log := slog.New(slog.NewJSONHandler(os.Stderr, nil))
	if err := run(log, *addr, *adminAddr, *dir, *testSplit, *preload, *ortLib, *valkeyAddr, *kafkaBrokers, *eventsTimeout, shards, *shardAddrs, *hedgeDelay, *prepareTimeout, budgets, *cacheSize, *cacheTTL, *drain); err != nil {
		log.Error("gateway stopped", "err", err)
		os.Exit(1)
	}
}

func run(log *slog.Logger, addr, adminAddr, dir, testSplit, preload, ortLib, valkeyAddr, kafkaBrokers string, eventsTimeout time.Duration, shards shard.Config, shardAddrs string, hedgeDelay, prepareTimeout time.Duration, budgets recommend.Budgets, cacheSize int, cacheTTL time.Duration, drain time.Duration) (err error) {
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
	store, closeStore, err := openStore(valkeyAddr)
	if err != nil {
		return err
	}
	defer closeStore()
	c, closeCache, err := openCache(valkeyAddr, cacheSize, cacheTTL)
	if err != nil {
		return err
	}
	defer closeCache()
	if valkeyAddr != "" {
		store, c = guard(store, c, log)
	}
	var replicas [][]string
	if shardAddrs != "" {
		if replicas, err = annshard.ParseAddrs(shardAddrs); err != nil {
			return fmt.Errorf("-shard-addrs: %w", err)
		}
		shards.Shards = len(replicas)
	}
	var live *recommend.Live
	serving := func() string {
		if live == nil { // the first bundle is being prepared
			return ""
		}
		return live.Version()
	}
	index, shardMetrics, closeShards, err := openShards(replicas, shards, hedgeDelay, prepareTimeout, serving, log)
	if err != nil {
		return err
	}
	defer closeShards() // after live is closed: no request is searching
	prepare := preparer(opt, store, index, budgets, c)
	p, err := prepare(dir)
	if err != nil {
		return err
	}
	live = recommend.NewLive(p, log)
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
	srv.Register(shardMetrics...)
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
		"shards", shards.Shards, "shard_mode", shards.Mode, "shard_addrs", shardAddrs, "zero_vectors", zeroVectors(p.Bundle))
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
	c, err := valkeyclient.New(addr)
	if err != nil {
		return nil, nil, err
	}
	return history.NewValkey(c), c.Close, nil
}

// openCache returns the result cache and the function that closes it: on
// the Valkey server at addr, where every instance shares it, or in this
// process without an address; none if size is 0.
func openCache(addr string, size int, ttl time.Duration) (recommend.Cache, func(), error) {
	switch {
	case size <= 0:
		return nil, func() {}, nil
	case addr == "":
		return cache.New[recommend.Response](size, ttl), func() {}, nil
	}
	c, err := valkeyclient.New(addr) // a client of its own, as the history store has (D036)
	if err != nil {
		return nil, nil, err
	}
	v, err := cache.NewValkey[recommend.Response](c, ttl)
	if err != nil {
		c.Close()
		return nil, nil, err
	}
	return v, c.Close, nil
}

// The circuit breaker on Valkey opens after breakerFailures failed calls in
// a row and lets a call try the server again after breakerOpen (D037).
const (
	breakerFailures = 5
	breakerOpen     = time.Second
)

// guard returns store with its reads, and c with its reads and writes,
// behind one circuit breaker that logs its changes of state.
func guard(store history.Store, c recommend.Cache, log *slog.Logger) (history.Store, recommend.Cache) {
	b := breaker.New(breakerFailures, breakerOpen, func(from, to string) {
		log.Warn("valkey circuit breaker", "from", from, "to", to)
	})
	if c != nil {
		c = breaker.Cache[recommend.Response]{Cache: c, Breaker: b}
	}
	return breaker.Store{Store: store, Breaker: b}, c
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

// openShards returns the function that puts a bundle's vectors into shards
// by cfg, the metrics of the shard tier and the function that releases what
// it holds: the shards are built in this process or, given replicas,
// prepared on the annshard replicas of every shard, each call within
// timeout, and searched with hedgeDelay. serving returns the version the
// gateway serves, which the replicas are told to keep.
func openShards(replicas [][]string, cfg shard.Config, hedgeDelay, timeout time.Duration, serving func() string, log *slog.Logger) (func(*bundle.Bundle) (recommend.Shards, error), []prometheus.Collector, func(), error) {
	if replicas == nil {
		return func(b *bundle.Bundle) (recommend.Shards, error) {
			s, err := shard.Build(b.Vectors, cfg)
			if err != nil {
				return nil, err
			}
			return s, nil
		}, nil, func() {}, nil
	}
	client, err := annshard.Dial(replicas, annshard.Options{HedgeDelay: hedgeDelay, PrepareTimeout: timeout, Serving: serving, Log: log})
	if err != nil {
		return nil, nil, nil, err
	}
	return func(b *bundle.Bundle) (recommend.Shards, error) {
		ctx, cancel := context.WithTimeout(context.Background(), timeout)
		defer cancel()
		s, err := client.Prepare(ctx, b.ModelVersion, b.ItemCount, cfg)
		if err != nil {
			return nil, err
		}
		return s, nil
	}, client.Collectors(), func() { client.Close() }, nil //nolint:errcheck // the process is stopping
}

// preparer returns the function that loads, verifies and indexes the
// bundle in a directory, for the first bundle and for every swap: the
// checksums, golden cases and train cutoff are checked against opt, and
// index puts the vectors into shards. The pipelines share store and cache
// c, and have budgets.
func preparer(opt bundle.Options, store history.Store, index func(*bundle.Bundle) (recommend.Shards, error), budgets recommend.Budgets, c recommend.Cache) func(dir string) (*recommend.Pipeline, error) {
	return func(dir string) (*recommend.Pipeline, error) {
		b, err := bundle.Load(dir, opt)
		if errors.Is(err, onnx.ErrNotInitialized) {
			return nil, fmt.Errorf("%w (start with -ort-lib)", err)
		}
		if err != nil {
			return nil, err
		}
		s, err := index(b)
		if err != nil {
			return nil, errors.Join(err, b.Close())
		}
		p := recommend.New(b, store, s)
		p.Budgets, p.Cache = budgets, c
		return p, nil
	}
}

// zeroVectors returns the number of b's items whose vector is zero, which
// the shards leave out.
func zeroVectors(b *bundle.Bundle) int {
	n := 0
	for _, inv := range b.InvNorms[1:] {
		if inv == 0 {
			n++
		}
	}
	return n
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
