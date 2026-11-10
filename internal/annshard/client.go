package annshard

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"google.golang.org/grpc"
	"google.golang.org/grpc/backoff"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/connectivity"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"

	"github.com/sHyanorn/recserve/internal/annshard/annshardpb"
	"github.com/sHyanorn/recserve/internal/shard"
	"github.com/sHyanorn/recserve/internal/vec"
)

// ParseAddrs parses the replica addresses of every shard: shards separated
// by ';', a shard's replicas by ','.
func ParseAddrs(s string) ([][]string, error) {
	var out [][]string
	for i, group := range strings.Split(s, ";") {
		replicas := strings.Split(group, ",")
		for _, a := range replicas {
			if a == "" {
				return nil, fmt.Errorf("shard %d of %q has an empty address", i, s)
			}
		}
		out = append(out, replicas)
	}
	return out, nil
}

// Options sets up a Client.
type Options struct {
	// HedgeDelay is how long a search waits for the replica it asked first
	// before it also asks another; 0 never does.
	HedgeDelay time.Duration
}

// The hedge budget of a shard, in tenths of a hedge: every search earns
// hedgeEarn, a hedge costs hedgeCost, and at most hedgeMax are kept. Over
// n searches a shard so sends at most hedgeMax/hedgeCost + n/10 hedges.
const (
	hedgeEarn = 1
	hedgeCost = 10
	hedgeMax  = 10 * hedgeCost
)

// The connection settings: gRPC's defaults but for the wait between
// attempts to reach a replica that is down, which would grow to two
// minutes, and for the time an attempt may take, 20 s.
const (
	maxReconnectWait = 5 * time.Second
	connectTimeout   = 5 * time.Second
)

// Client reaches the replicas of every shard. Its connections are opened
// and kept by gRPC, without TLS (D040). A search asks one replica, another
// at once if that one fails, and a second one if the first is slow (D041).
type Client struct {
	hedgeDelay time.Duration
	shards     []*group
	hedges     *prometheus.CounterVec
	failovers  *prometheus.CounterVec
}

// group is the replicas of one shard.
type group struct {
	replicas []*replica
	next     atomic.Uint32 // counts searches, to take the ready replicas in turn

	mu     sync.Mutex
	tokens int // hedge budget

	hedgeWon, hedgeLost, hedgeDenied, failover prometheus.Counter
}

type replica struct {
	addr string
	conn *grpc.ClientConn
	rpc  annshardpb.AnnShardClient
}

// Dial returns a client of the replicas at addrs[shard][replica]. It does
// not wait for a connection: the first call to a replica opens it.
func Dial(addrs [][]string, opt Options) (*Client, error) {
	c := &Client{
		hedgeDelay: opt.HedgeDelay,
		hedges: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "recserve_shard_hedges_total",
			Help: "Shard searches that were slow enough for a second replica to be asked, by shard and result: won (the second replica answered first), lost, or denied (not asked: the shard's hedge budget was spent).",
		}, []string{"shard", "result"}),
		failovers: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "recserve_shard_failovers_total",
			Help: "Shard searches sent to another replica because one failed, by shard.",
		}, []string{"shard"}),
	}
	wait := backoff.DefaultConfig
	wait.MaxDelay = maxReconnectWait
	for i, replicas := range addrs {
		if len(replicas) == 0 {
			return nil, errors.Join(fmt.Errorf("shard %d has no replica", i), c.Close())
		}
		shard := strconv.Itoa(i)
		g := &group{
			tokens:   hedgeMax,
			hedgeWon: c.hedges.WithLabelValues(shard, "won"), hedgeLost: c.hedges.WithLabelValues(shard, "lost"),
			hedgeDenied: c.hedges.WithLabelValues(shard, "denied"), failover: c.failovers.WithLabelValues(shard),
		}
		c.shards = append(c.shards, g)
		for _, addr := range replicas {
			conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()),
				grpc.WithConnectParams(grpc.ConnectParams{Backoff: wait, MinConnectTimeout: connectTimeout}))
			if err != nil {
				return nil, errors.Join(fmt.Errorf("shard %d, replica %s: %w", i, addr, err), c.Close())
			}
			g.replicas = append(g.replicas, &replica{addr: addr, conn: conn, rpc: annshardpb.NewAnnShardClient(conn)})
		}
	}
	return c, nil
}

// Collectors returns the client's metrics.
func (c *Client) Collectors() []prometheus.Collector {
	return []prometheus.Collector{c.hedges, c.failovers}
}

// Close closes the connections; calls in flight fail.
func (c *Client) Close() error {
	var errs []error
	for _, g := range c.shards {
		for _, r := range g.replicas {
			errs = append(errs, r.conn.Close())
		}
	}
	return errors.Join(errs...)
}

// Shards is one bundle version on the client's replicas, split and built
// by one configuration: every search names that index, and a replica
// answers from it or fails. It implements recommend.Shards.
type Shards struct {
	c       *Client
	cfg     shard.Config
	indexes []*annshardpb.Index // by shard
}

// Prepare has every replica load version and build its shard by cfg, whose
// Shards must be the client's number of shards, and returns the version's
// shards once all have.
func (c *Client) Prepare(ctx context.Context, version string, cfg shard.Config) (*Shards, error) {
	if cfg.Shards != len(c.shards) {
		return nil, fmt.Errorf("%d shards configured, replicas of %d", cfg.Shards, len(c.shards))
	}
	mode, ok := map[shard.Mode]annshardpb.Mode{shard.Exact: annshardpb.Mode_MODE_EXACT, shard.HNSW: annshardpb.Mode_MODE_HNSW}[cfg.Mode]
	if !ok {
		return nil, fmt.Errorf("shard mode %q", cfg.Mode)
	}
	s := &Shards{c: c, cfg: cfg, indexes: make([]*annshardpb.Index, cfg.Shards)}
	var wg sync.WaitGroup
	var mu sync.Mutex
	var errs []error
	for i, g := range c.shards {
		ix := &annshardpb.Index{Version: version, Shard: int32(i), Shards: int32(cfg.Shards), Mode: mode}
		if cfg.Mode == shard.HNSW {
			ix.HnswM, ix.HnswEfConstruction, ix.HnswSeed = int32(cfg.HNSW.M), int32(cfg.HNSW.EfConstruction), cfg.HNSW.Seed
		}
		s.indexes[i] = ix
		for _, r := range g.replicas {
			wg.Go(func() {
				if _, err := r.rpc.Prepare(ctx, &annshardpb.PrepareRequest{Index: ix}); err != nil {
					mu.Lock()
					errs = append(errs, fmt.Errorf("shard %d, replica %s: %w", i, r.addr, err))
					mu.Unlock()
				}
			})
		}
	}
	wg.Wait()
	if len(errs) > 0 {
		return nil, fmt.Errorf("prepare %s: %w", version, errors.Join(errs...))
	}
	return s, nil
}

// Config returns the settings the shards are built and searched with.
func (s *Shards) Config() shard.Config { return s.cfg }

// order returns the shard's replicas in the order a search asks them: those
// with a ready connection first, a different one leading each time, then
// the others as listed. A connection gRPC has let go idle is told to
// connect again, since no search would be sent to wake it.
func (g *group) order() []*replica {
	ready := make([]*replica, 0, len(g.replicas))
	var rest []*replica
	for _, r := range g.replicas {
		switch r.conn.GetState() {
		case connectivity.Ready:
			ready = append(ready, r)
		case connectivity.Idle:
			r.conn.Connect()
			rest = append(rest, r)
		default:
			rest = append(rest, r)
		}
	}
	if n := uint32(len(ready)); n > 1 {
		lead := int(g.next.Add(1) % n)
		ready = append(ready[lead:len(ready):len(ready)], ready[:lead]...)
	}
	return append(ready, rest...)
}

// earn adds a search's share of a hedge to the budget.
func (g *group) earn() {
	g.mu.Lock()
	g.tokens = min(g.tokens+hedgeEarn, hedgeMax)
	g.mu.Unlock()
}

// spend takes a hedge out of the budget, if it holds one.
func (g *group) spend() bool {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.tokens < hedgeCost {
		return false
	}
	g.tokens -= hedgeCost
	return true
}

type answer struct {
	hits  []vec.Hit
	err   error
	hedge bool
}

// Search returns shard i's k items with the highest inner product with q,
// best first, ties to the lower ID, without the IDs in seen; only those of
// shard i are sent. It asks one replica. If that one fails, it asks the
// next at once, each replica at most once. If the first has not answered
// after the hedge delay, it asks one more replica, one that is connected,
// as long as the shard's hedge budget allows. The first answer wins and the
// calls still out are cancelled; Search returns once all of them have. A
// call that the deadline or a cancellation ended is not tried elsewhere,
// and if ctx ends first the error wraps ctx's, as an in-process search's
// does.
func (s *Shards) Search(ctx context.Context, i int, q []float32, k int, seen map[int32]bool) ([]vec.Hit, error) {
	req := &annshardpb.SearchRequest{Index: s.indexes[i], Query: q, K: int32(k), Ef: int32(s.cfg.Ef)}
	for id := range seen {
		if shard.Of(id, s.cfg.Shards) == i {
			req.Exclude = append(req.Exclude, id)
		}
	}
	g := s.c.shards[i]
	order := g.order()
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	answers := make(chan answer, len(order))
	asked := 0
	ask := func(hedge bool) {
		r := order[asked]
		asked++
		go func() {
			hits, err := r.search(ctx, req)
			answers <- answer{hits, err, hedge}
		}()
	}
	g.earn()
	ask(false)
	var hedgeDue <-chan time.Time
	if s.c.hedgeDelay > 0 && len(order) > 1 {
		t := time.NewTimer(s.c.hedgeDelay)
		defer t.Stop()
		hedgeDue = t.C
	}
	hedged := false
	var errs []error
	for returned := 0; returned < asked; {
		select {
		case a := <-answers:
			returned++
			if a.err == nil {
				cancel()
				for ; returned < asked; returned++ {
					<-answers
				}
				switch {
				case a.hedge:
					g.hedgeWon.Inc()
				case hedged:
					g.hedgeLost.Inc()
				}
				return a.hits, nil
			}
			errs = append(errs, a.err)
			ended := errors.Is(a.err, context.DeadlineExceeded) || errors.Is(a.err, context.Canceled)
			if !ended && asked < len(order) && ctx.Err() == nil {
				g.failover.Inc()
				ask(false)
			}
		case <-hedgeDue:
			hedgeDue = nil
			switch {
			case asked == len(order) || order[asked].conn.GetState() != connectivity.Ready:
			case !g.spend():
				g.hedgeDenied.Inc()
			default:
				hedged = true
				ask(true)
			}
		}
	}
	if hedged {
		g.hedgeLost.Inc()
	}
	return nil, errors.Join(errs...)
}

// search runs one search on the replica.
func (r *replica) search(ctx context.Context, req *annshardpb.SearchRequest) ([]vec.Hit, error) {
	resp, err := r.rpc.Search(ctx, req)
	if err != nil {
		return nil, fmt.Errorf("replica %s: %w", r.addr, contextError(err))
	}
	if len(resp.GetScores()) != len(resp.GetIds()) {
		return nil, fmt.Errorf("replica %s: %d IDs with %d scores", r.addr, len(resp.GetIds()), len(resp.GetScores()))
	}
	hits := make([]vec.Hit, len(resp.GetIds()))
	for j, id := range resp.GetIds() {
		hits[j] = vec.Hit{ID: id, Score: resp.GetScores()[j]}
	}
	return hits, nil
}

// contextError makes a call's end by deadline or cancellation, which gRPC
// reports as a status, wrap the context error it stands for, so that
// callers tell a timeout from a failure as they do for work in process.
func contextError(err error) error {
	switch status.Code(err) {
	case codes.DeadlineExceeded:
		return fmt.Errorf("%w: %w", context.DeadlineExceeded, err)
	case codes.Canceled:
		return fmt.Errorf("%w: %w", context.Canceled, err)
	}
	return err
}
