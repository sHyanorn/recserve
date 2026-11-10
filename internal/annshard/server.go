// Package annshard is the shard tier over gRPC (docs/DECISIONS.md D040):
// Server answers searches from the indexes a replica has prepared, and
// Client searches the replicas for the gateway.
package annshard

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"log/slog"
	"path/filepath"
	"sync"
	"time"

	"golang.org/x/sync/singleflight"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/sHyanorn/recserve/internal/annshard/annshardpb"
	"github.com/sHyanorn/recserve/internal/bundle"
	"github.com/sHyanorn/recserve/internal/hnsw"
	"github.com/sHyanorn/recserve/internal/shard"
)

// index is an annshardpb.Index as a map key: one shard of one bundle
// version, split and built by cfg, whose Ef is not part of it.
type index struct {
	version string
	shard   int
	cfg     shard.Config
}

// parseIndex checks x and returns it as a key. The HNSW parameters of an
// exact index are dropped, so they cannot make two keys of one index.
func parseIndex(x *annshardpb.Index) (index, error) {
	ix := index{version: x.GetVersion(), shard: int(x.GetShard()), cfg: shard.Config{Shards: int(x.GetShards())}}
	// The version becomes a directory name under the bundle root.
	if v := ix.version; v == "" || v == "." || v == ".." || v != filepath.Base(v) {
		return index{}, fmt.Errorf("version %q is not a directory name", v)
	}
	if ix.cfg.Shards < 1 || ix.shard < 0 || ix.shard >= ix.cfg.Shards {
		return index{}, fmt.Errorf("shard %d of %d", ix.shard, ix.cfg.Shards)
	}
	switch x.GetMode() {
	case annshardpb.Mode_MODE_EXACT:
		ix.cfg.Mode = shard.Exact
	case annshardpb.Mode_MODE_HNSW:
		ix.cfg.Mode = shard.HNSW
		ix.cfg.HNSW = hnsw.Config{M: int(x.GetHnswM()), EfConstruction: int(x.GetHnswEfConstruction()), Seed: x.GetHnswSeed()}
	default:
		return index{}, fmt.Errorf("mode %v", x.GetMode())
	}
	return ix, nil
}

// Server is one annshard replica. It holds the indexes it was asked to
// prepare and answers a search only from the index the request names.
type Server struct {
	annshardpb.UnimplementedAnnShardServer
	root string // holds one bundle directory per version
	log  *slog.Logger

	loading singleflight.Group // one build per index at a time
	mu      sync.RWMutex
	parts   map[index]*shard.Part
}

// NewServer returns a replica that loads bundle versions from the
// directories under root.
func NewServer(root string, log *slog.Logger) *Server {
	return &Server{root: root, log: log, parts: map[index]*shard.Part{}}
}

func (s *Server) part(ix index) *shard.Part {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.parts[ix]
}

// Prepare loads the vectors of the index's bundle version, verifies them
// against the bundle's manifest and builds the index. Requests for one
// index share one build, which goes on if they give up.
func (s *Server) Prepare(ctx context.Context, req *annshardpb.PrepareRequest) (*annshardpb.PrepareResponse, error) {
	ix, err := parseIndex(req.GetIndex())
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	done := s.loading.DoChan(fmt.Sprintf("%+v", ix), func() (any, error) { return nil, s.prepare(ix) })
	select {
	case r := <-done:
		if r.Err != nil {
			return nil, r.Err
		}
		return &annshardpb.PrepareResponse{}, nil
	case <-ctx.Done():
		return nil, status.FromContextError(ctx.Err()).Err()
	}
}

// prepare builds ix unless the replica holds it; its error is a gRPC status.
func (s *Server) prepare(ix index) error {
	if s.part(ix) != nil {
		return nil
	}
	start := time.Now()
	dir := filepath.Join(s.root, ix.version)
	m, rows, err := bundle.Vectors(dir)
	switch {
	case errors.Is(err, fs.ErrNotExist):
		return status.Error(codes.NotFound, err.Error())
	case err != nil:
		return status.Error(codes.FailedPrecondition, err.Error())
	case m.ModelVersion != ix.version:
		return status.Errorf(codes.FailedPrecondition, "bundle %s holds version %q", dir, m.ModelVersion)
	}
	p, err := shard.BuildPart(rows, ix.cfg, ix.shard)
	if err != nil {
		return status.Error(codes.InvalidArgument, err.Error())
	}
	s.mu.Lock()
	s.parts[ix] = p
	s.mu.Unlock()
	s.log.Info("index prepared", "version", ix.version, "shard", ix.shard, "shards", ix.cfg.Shards, "mode", ix.cfg.Mode,
		"items", p.Len(), "took", time.Since(start))
	return nil
}

// Search answers from the index the request names, within the caller's
// deadline: an exact search stops soon after it (shard.Part.Search).
func (s *Server) Search(ctx context.Context, req *annshardpb.SearchRequest) (*annshardpb.SearchResponse, error) {
	ix, err := parseIndex(req.GetIndex())
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	p := s.part(ix)
	if p == nil {
		return nil, status.Errorf(codes.FailedPrecondition, "index not prepared: version %s, shard %d of %d", ix.version, ix.shard, ix.cfg.Shards)
	}
	if len(req.GetQuery()) != p.Dim() {
		return nil, status.Errorf(codes.InvalidArgument, "query has %d values, the index %d", len(req.GetQuery()), p.Dim())
	}
	if req.GetK() < 1 {
		return nil, status.Errorf(codes.InvalidArgument, "k = %d", req.GetK())
	}
	seen := make(map[int32]bool, len(req.GetExclude()))
	for _, id := range req.GetExclude() {
		seen[id] = true
	}
	// A part has no more than Len items to return, whatever k asks for.
	hits, err := p.Search(ctx, req.GetQuery(), min(int(req.GetK()), p.Len()), int(req.GetEf()), seen)
	if err != nil {
		return nil, status.FromContextError(err).Err()
	}
	resp := &annshardpb.SearchResponse{Ids: make([]int32, len(hits)), Scores: make([]float64, len(hits))}
	for i, h := range hits {
		resp.Ids[i], resp.Scores[i] = h.ID, h.Score
	}
	return resp, nil
}
