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
	"sync/atomic"
	"time"

	"golang.org/x/sync/singleflight"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/keepalive"
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

// KeepVersions is the number of bundle versions a replica holds indexes of:
// the active one and, during a swap, the one before or after it.
const KeepVersions = 2

// Server is one annshard replica. It holds the indexes it was asked to
// prepare, of KeepVersions bundle versions: the serving version, which the
// Prepare with the latest time of those that name one said its caller
// serves (D057), and otherwise those that requests named last. It answers a
// search only from the index the request names, and is ready while it
// holds the serving version (D044).
type Server struct {
	annshardpb.UnimplementedAnnShardServer
	root   string // holds one bundle directory per version
	log    *slog.Logger
	health *health.Server

	loading  singleflight.Group // one build per index at a time
	mu       sync.RWMutex
	versions map[string]*version
	serving  string
	since    int64        // the time of serving, in Unix nanoseconds; 0 for none
	clock    atomic.Int64 // counts the requests that named a held version
}

// version is the indexes held of one bundle version.
type version struct {
	parts map[index]*shard.Part // guarded by Server.mu
	named atomic.Int64          // the clock at the last request that named the version
}

// ServerOptions returns the options a replica's gRPC server needs for the
// gateway's connections (DialOptions): it accepts their keepalive pings,
// also with no call in flight. By default grpc-go takes pings less than
// 5 minutes apart for abuse and closes the connection.
func ServerOptions() []grpc.ServerOption {
	return []grpc.ServerOption{grpc.KeepaliveEnforcementPolicy(keepalive.EnforcementPolicy{MinTime: keepaliveTime / 2, PermitWithoutStream: true})}
}

// NewServer returns a replica that loads bundle versions from the
// directories under root.
func NewServer(root string, log *slog.Logger) *Server {
	h := health.NewServer()
	h.SetServingStatus("", healthpb.HealthCheckResponse_NOT_SERVING)
	return &Server{root: root, log: log, health: h, versions: map[string]*version{}}
}

// Register serves the replica on gs, with the standard health service,
// whose status is SERVING while the replica is ready.
func (s *Server) Register(gs *grpc.Server) {
	annshardpb.RegisterAnnShardServer(gs, s)
	healthpb.RegisterHealthServer(gs, s.health)
}

// Shutdown reports the replica not ready from then on, as it stops.
func (s *Server) Shutdown() { s.health.Shutdown() }

// setHealth reports whether the replica holds the serving version; the
// caller holds s.mu, so that the reports keep the order of the changes.
func (s *Server) setHealth() {
	st := healthpb.HealthCheckResponse_NOT_SERVING
	if s.versions[s.serving] != nil {
		st = healthpb.HealthCheckResponse_SERVING
	}
	s.health.SetServingStatus("", st)
}

// part returns the index ix, nil if the replica does not hold it, and
// notes that a request named its version.
func (s *Server) part(ix index) *shard.Part {
	s.mu.RLock()
	defer s.mu.RUnlock()
	v := s.versions[ix.version]
	if v == nil {
		return nil
	}
	v.named.Store(s.clock.Add(1))
	return v.parts[ix]
}

// hold adds the index p under ix. If the replica then holds more than
// KeepVersions versions, it drops every index of the version named longest
// ago, but never of the serving version (D043); a search already running on
// a dropped index finishes on it.
func (s *Server) hold(ix index, p *shard.Part) {
	s.mu.Lock()
	defer s.mu.Unlock()
	v := s.versions[ix.version]
	if v == nil {
		v = &version{parts: map[index]*shard.Part{}}
		s.versions[ix.version] = v
	}
	v.parts[ix] = p
	v.named.Store(s.clock.Add(1))
	for len(s.versions) > KeepVersions {
		oldest := ""
		for name, o := range s.versions {
			if name == s.serving {
				continue
			}
			if oldest == "" || o.named.Load() < s.versions[oldest].named.Load() {
				oldest = name
			}
		}
		delete(s.versions, oldest)
		s.log.Info("version dropped", "version", oldest)
	}
	s.setHealth()
}

// Prepare loads the vectors of the index's bundle version, verifies them
// against the bundle's manifest and builds the index. The serving version
// it names, if any, becomes the replica's at once, unless its time is
// before that of the replica's (D057). Requests for one index share one
// build, which goes on if they give up.
func (s *Server) Prepare(ctx context.Context, req *annshardpb.PrepareRequest) (*annshardpb.PrepareResponse, error) {
	ix, err := parseIndex(req.GetIndex())
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	if v := req.GetServingVersion(); v != "" {
		s.serve(v, req.GetServingSinceUnixNano())
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

// serve makes v, served since the Unix time in nanoseconds since, the
// serving version, unless since is before the time of the serving version:
// a Prepare that arrives after one sent later does not undo it. A since of
// 0, from a caller that sends no time, makes v the serving version whatever
// the time, and leaves the time for later ones to be compared with.
func (s *Server) serve(v string, since int64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if since != 0 {
		if since < s.since {
			return
		}
		s.since = since
	}
	s.serving = v
	s.setHealth()
}

// prepare builds ix unless the replica holds it; its error is a gRPC
// status.
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
	s.hold(ix, p)
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
