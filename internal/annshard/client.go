package annshard

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
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

// Client reaches the replicas of every shard. Its connections are opened
// and kept by gRPC, without TLS (D040).
type Client struct {
	shards [][]*replica
}

type replica struct {
	addr string
	conn *grpc.ClientConn
	rpc  annshardpb.AnnShardClient
}

// Dial returns a client of the replicas at addrs[shard][replica]. It does
// not wait for a connection: the first call to a replica opens it.
func Dial(addrs [][]string) (*Client, error) {
	c := &Client{shards: make([][]*replica, len(addrs))}
	for i, group := range addrs {
		if len(group) == 0 {
			return nil, errors.Join(fmt.Errorf("shard %d has no replica", i), c.Close())
		}
		for _, addr := range group {
			conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
			if err != nil {
				return nil, errors.Join(fmt.Errorf("shard %d, replica %s: %w", i, addr, err), c.Close())
			}
			c.shards[i] = append(c.shards[i], &replica{addr: addr, conn: conn, rpc: annshardpb.NewAnnShardClient(conn)})
		}
	}
	return c, nil
}

// Close closes the connections; calls in flight fail.
func (c *Client) Close() error {
	var errs []error
	for _, group := range c.shards {
		for _, r := range group {
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
	for i, group := range c.shards {
		ix := &annshardpb.Index{Version: version, Shard: int32(i), Shards: int32(cfg.Shards), Mode: mode}
		if cfg.Mode == shard.HNSW {
			ix.HnswM, ix.HnswEfConstruction, ix.HnswSeed = int32(cfg.HNSW.M), int32(cfg.HNSW.EfConstruction), cfg.HNSW.Seed
		}
		s.indexes[i] = ix
		for _, r := range group {
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

// Search asks a replica of shard i for its k items with the highest inner
// product with q, best first, ties to the lower ID, without the IDs in
// seen; only those of shard i are sent. If ctx ends first, the error wraps
// ctx's, as an in-process search's does.
func (s *Shards) Search(ctx context.Context, i int, q []float32, k int, seen map[int32]bool) ([]vec.Hit, error) {
	req := &annshardpb.SearchRequest{Index: s.indexes[i], Query: q, K: int32(k), Ef: int32(s.cfg.Ef)}
	for id := range seen {
		if shard.Of(id, s.cfg.Shards) == i {
			req.Exclude = append(req.Exclude, id)
		}
	}
	r := s.c.shards[i][0]
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
