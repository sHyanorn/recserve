// Package recommend runs the request path: history lookup, retrieval from
// several sources in parallel, scoring, ranking with the bundle's ranker,
// and re-ranking with the seen-item filter, deduplication and MMR.
package recommend

import (
	"cmp"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	"golang.org/x/sync/singleflight"

	"github.com/sHyanorn/recserve/internal/breaker"
	"github.com/sHyanorn/recserve/internal/bundle"
	"github.com/sHyanorn/recserve/internal/cache"
	"github.com/sHyanorn/recserve/internal/history"
	"github.com/sHyanorn/recserve/internal/shard"
	"github.com/sHyanorn/recserve/internal/vec"
)

// Defaults for the request path.
const (
	MaxK          = 100
	VectorTop     = 200 // candidates from vector retrieval
	CoocSeeds     = 10  // most recent history items used as co-occurrence seeds
	PopularTop    = 100 // candidates from the popularity list
	MMRLambda     = 0.7 // weight of relevance against diversity
	MaxRecent     = 500 // items in a request's recent list
	SourceVector  = "vector"
	SourceCooc    = "cooc"
	SourcePopular = "popular"
)

// Degradation reasons reported in a response (docs/SPEC.md §5).
const (
	HistoryUnavailable = "history_unavailable"
	EncoderTimeout     = "encoder_timeout"
	EncoderError       = "encoder_error"
	VectorTimeout      = "vector_timeout"
	VectorError        = "vector_error"
	ShardsPartial      = "shards_partial"
	CoocTimeout        = "cooc_timeout"
	CoocError          = "cooc_error"
	PopularTimeout     = "popular_timeout"
	PopularError       = "popular_error"
	RankerTimeout      = "ranker_timeout"
	RankerError        = "ranker_error"
)

// Reasons lists the degradation reasons in the order a response reports them.
var Reasons = []string{
	HistoryUnavailable, EncoderTimeout, EncoderError, VectorTimeout, VectorError, ShardsPartial,
	CoocTimeout, CoocError, PopularTimeout, PopularError, RankerTimeout, RankerError,
}

// Stages of a request, as the test hook names them.
const (
	stageHistory = "history"
	stageCache   = "cache"
	stageFlight  = "singleflight"
	stageEncoder = "encoder"
	stageVector  = SourceVector // one call per shard
	stageCooc    = SourceCooc
	stagePopular = SourcePopular
	stageRanker  = "ranker"
)

// Budgets bounds the stages of a request (docs/SPEC.md §5). A stage that
// runs out of time is skipped or falls back, and the response says so; zero
// leaves a stage unbounded. Each shard search is bounded by Shard and by
// what is left of Retrieval.
type Budgets struct {
	History   time.Duration
	Encoder   time.Duration
	Retrieval time.Duration
	Shard     time.Duration
	Ranker    time.Duration
}

// DefaultBudgets are the budgets of docs/SPEC.md §5.
var DefaultBudgets = Budgets{
	History:   3 * time.Millisecond,
	Encoder:   5 * time.Millisecond,
	Retrieval: 15 * time.Millisecond,
	Shard:     15 * time.Millisecond,
	Ranker:    15 * time.Millisecond,
}

// The sources' positions in the results of a request.
const (
	srcVector = iota
	srcCooc
	srcPopular
)

// Request is one recommendation request.
type Request struct {
	User    string
	K       int
	TS      int64    // only events strictly before TS are used
	Exclude []string // raw item IDs never to return
	// Recent is the client's list of the user's latest items, oldest
	// first, used as the history when the stored one cannot be read.
	Recent []string
}

// Item is one recommended item.
type Item struct {
	ID    string  `json:"id"`
	Score float64 `json:"score"`
}

// Response is the result of a request.
type Response struct {
	Items        []Item    `json:"items"`
	ModelVersion string    `json:"model_version"`
	Degraded     []string  `json:"degraded"` // what was skipped or fell back, in the order of Reasons; empty if nothing
	Coverage     *Coverage `json:"coverage"` // nil when vector retrieval did not run
	// Dropped counts the candidates left out because the ranker takes at
	// most bundle.RankerRows; they are the ones with the lowest retrieval
	// scores.
	Dropped int `json:"-"`
	// Failures holds the errors behind Degraded, except timeouts.
	Failures []Failure `json:"-"`
	// Cached is true for a response from the cache, nil if the
	// pipeline has none or the history could not be read.
	Cached *bool `json:"-"`
	// CacheErr is the first error of reading or writing the cache. The
	// request went on as a miss and is not degraded by it.
	CacheErr error `json:"-"`
}

// Cache holds responses under the key of what they depend on. An error of
// either method costs the request nothing but the cache: it is answered as
// a miss.
type Cache interface {
	Get(ctx context.Context, k cache.Key) (Response, bool, error)
	Put(ctx context.Context, k cache.Key, r Response) error
}

// Coverage counts the shards searched for vector retrieval and those that
// answered in time.
type Coverage struct {
	Shards   int `json:"shards"`
	Answered int `json:"answered"`
}

// Failure is an error that degraded a response.
type Failure struct {
	Reason string
	Err    error
}

// degradation collects what a request skipped or fell back on.
type degradation struct {
	reasons  map[string]bool
	failures []Failure
}

// add records reason; err is kept unless it is a timeout or the refusal of
// an open circuit breaker, which reports its state itself.
func (d *degradation) add(reason string, err error) {
	if d.reasons == nil {
		d.reasons = map[string]bool{}
	}
	d.reasons[reason] = true
	if err != nil && !errors.Is(err, context.DeadlineExceeded) && !errors.Is(err, breaker.ErrOpen) {
		d.failures = append(d.failures, Failure{reason, err})
	}
}

// list returns the reasons in the order of Reasons.
func (d *degradation) list() []string {
	out := []string{}
	for _, r := range Reasons {
		if d.reasons[r] {
			out = append(out, r)
		}
	}
	return out
}

// timeoutOr returns timeout if err is a deadline's end, otherwise failed.
func timeoutOr(err error, timeout, failed string) string {
	if errors.Is(err, context.DeadlineExceeded) {
		return timeout
	}
	return failed
}

// within returns ctx bounded by d, or ctx unchanged if d is zero.
func within(ctx context.Context, d time.Duration) (context.Context, context.CancelFunc) {
	if d <= 0 {
		return ctx, func() {}
	}
	return context.WithTimeout(ctx, d)
}

// ended returns the request's own end, if it has ended: a request cancelled
// or past its caller's deadline fails instead of degrading.
func ended(ctx context.Context, stage string) error {
	if err := ctx.Err(); err != nil {
		return fmt.Errorf("%s: %w", stage, err)
	}
	return nil
}

// Ranker scores rows of candidate features, bundle.RankerFeatures in order,
// one score per row; higher is better.
type Ranker interface {
	Rank(ctx context.Context, features []float32) ([]float32, error)
}

// Pipeline answers requests from one bundle and a history store.
type Pipeline struct {
	Bundle  *bundle.Bundle
	Shards  *shard.Set // the bundle's item vectors, for vector retrieval
	History history.Store
	Budgets Budgets
	// Cache, if not nil, holds responses that were not degraded. It may be
	// shared by pipelines over different bundles, in this process or, on
	// Valkey, in others: keys include the bundle version. Responses from it
	// may share their slices; callers must not modify them. A read runs
	// within what the history read left of Budgets.History, a write within
	// Budgets.History again.
	Cache Cache

	// flight lets the requests that miss the cache on one key at the same
	// time compute the response once (once). New sets it.
	flight *singleflight.Group

	rankWith          Ranker // test only: replaces the bundle's ranker
	skipSeenFilter    bool   // test only: the broken variant of property 2
	keyWithoutVersion bool   // test only: the broken variant of property 5
	shardShare        bool   // test only: the broken variant of property 6, ceil(VectorTop/n) items per shard
	// scoreBundle, test only, is the broken variant of property 4: it
	// returns the bundle whose vectors score the candidates, read again
	// instead of the one the request started on.
	scoreBundle   func() *bundle.Bundle
	cacheDegraded bool // test only: the broken variant of property 10
	noFlight      bool // test only: the broken variant of property 11
	// join, test only, runs when a request has joined the requests on its
	// cache key, before it computes or waits.
	join func()
	// stall, test only, runs at the start of a stage under the stage's
	// context; shard is the shard searched, -1 outside vector retrieval.
	// An error fails the stage.
	stall func(ctx context.Context, stage string, shard int) error
}

func (p *Pipeline) joined() {
	if p.join != nil {
		p.join()
	}
}

func (p *Pipeline) stalled(ctx context.Context, stage string, shard int) error {
	if p.stall == nil {
		return nil
	}
	return p.stall(ctx, stage, shard)
}

// New returns a pipeline over b and h, with b's vectors split into shards
// by cfg.
func New(b *bundle.Bundle, h history.Store, cfg shard.Config) (*Pipeline, error) {
	s, err := shard.Build(b.Vectors, cfg)
	if err != nil {
		return nil, err
	}
	return &Pipeline{Bundle: b, Shards: s, History: h, flight: new(singleflight.Group)}, nil
}

func (p *Pipeline) ranker() Ranker {
	if p.rankWith != nil {
		return p.rankWith
	}
	if p.Bundle.Ranker != nil {
		return p.Bundle.Ranker
	}
	return nil
}

type candidate struct {
	id    int32
	score float64 // source score, used when there is no user vector
}

// Recommend answers a request. Each stage runs within its budget; one that
// fails or runs out of time is skipped or falls back, and the response
// reports why. If ctx ends, the request fails with an error that wraps
// ctx.Err(). A request that fails after its cache lookup returns a response
// that holds Cached and CacheErr only, so the lookup can still be counted.
func (p *Pipeline) Recommend(ctx context.Context, req Request) (Response, error) {
	if req.K <= 0 || req.K > MaxK {
		return Response{}, fmt.Errorf("k must be in 1..%d", MaxK)
	}
	if len(req.Recent) > MaxRecent {
		return Response{}, fmt.Errorf("recent holds more than %d items", MaxRecent)
	}
	var d degradation
	// The history read and the cache read share one budget.
	hctx, cancel := within(ctx, p.Budgets.History)
	defer cancel()
	hist, err := p.history(ctx, hctx, req, &d)
	if err != nil {
		return Response{}, err
	}
	if p.Cache == nil || len(d.reasons) > 0 && !p.cacheDegraded {
		return p.compute(ctx, req, hist, &d)
	}
	key := p.cacheKey(req, hist)
	resp, ok, cacheErr := p.Cache.Get(hctx, key)
	cancel()
	if cacheErr != nil {
		if err := ended(ctx, stageCache); err != nil {
			return Response{}, err
		}
	} else if ok {
		resp.Cached = &ok
		return resp, nil
	}
	resp, led, err := p.once(ctx, key, func() (Response, error) {
		resp, err := p.compute(ctx, req, hist, &d)
		// A cache that could not be read is not written either.
		if err != nil || (cacheErr != nil || len(resp.Degraded) > 0) && !p.cacheDegraded {
			return resp, err
		}
		cctx, cancel := within(ctx, p.Budgets.History)
		defer cancel()
		// A request that ended while writing is no cache failure.
		stored := resp
		stored.Dropped = 0 // a hit ranks nothing
		if err := p.Cache.Put(cctx, key, stored); ctx.Err() == nil {
			resp.CacheErr = err
		}
		return resp, nil
	})
	if err != nil {
		return Response{Cached: new(bool), CacheErr: cacheErr}, err
	}
	if !led {
		// Another request computed the response: its ranking and its
		// errors are counted and logged with that request.
		resp.Dropped, resp.Failures, resp.CacheErr = 0, nil, nil
	}
	resp.Cached = new(bool)
	if cacheErr != nil {
		resp.CacheErr = cacheErr
	}
	return resp, nil
}

// The states of a request in once.
const (
	flightWaiting int32 = iota
	flightStarted       // its compute runs: the request leads
	flightLeft          // it returned before its compute could start
)

// once returns compute's response for key, computed once for the requests
// that ask for it at the same time: the first of them leads and runs
// compute, the others wait for its response, each as long as its own ctx
// allows, and led is false for them. A leader that fails, which it does
// only when its own ctx ends, takes no other request with it: those that
// waited start over, and one of them leads.
func (p *Pipeline) once(ctx context.Context, key cache.Key, compute func() (Response, error)) (resp Response, led bool, err error) {
	if p.noFlight {
		p.joined()
		resp, err := compute()
		return resp, true, err
	}
	for {
		var state atomic.Int32
		ch := p.flight.DoChan(string(key[:]), func() (any, error) {
			if !state.CompareAndSwap(flightWaiting, flightStarted) {
				return nil, ctx.Err()
			}
			return compute()
		})
		p.joined()
		var r singleflight.Result
		select {
		case r = <-ch:
		case <-ctx.Done():
			if state.CompareAndSwap(flightWaiting, flightLeft) {
				return Response{}, false, ended(ctx, stageFlight)
			}
			r = <-ch // a leader waits for what it started (D029)
		}
		led := state.Load() == flightStarted
		if r.Err == nil {
			return r.Val.(Response), led, nil
		}
		if led {
			return Response{}, true, r.Err
		}
		if err := ended(ctx, stageFlight); err != nil {
			return Response{}, false, err
		}
	}
}

// compute answers a request from its history, without the cache. The
// response holds neither Cached nor CacheErr.
func (p *Pipeline) compute(ctx context.Context, req Request, hist []int32, d *degradation) (Response, error) {
	b := p.Bundle
	seen := make(map[int32]bool, len(hist)+len(req.Exclude))
	for _, id := range hist {
		seen[id] = true
	}
	for _, raw := range req.Exclude {
		if id, ok := b.Lookup(raw); ok {
			seen[id] = true
		}
	}
	uv, err := p.userVector(ctx, hist, d)
	if err != nil {
		return Response{}, err
	}
	results, cov, err := p.retrieve(ctx, hist, uv, seen, d)
	if err != nil {
		return Response{}, err
	}
	resp := Response{ModelVersion: b.ModelVersion, Coverage: cov}
	scored := p.retrievalScores(results, uv, seen)
	if rk := p.ranker(); rk != nil && len(scored) > 0 {
		kctx, cancel := within(ctx, p.Budgets.Ranker)
		err := p.stalled(kctx, stageRanker, -1)
		var ranked []vec.Hit
		var dropped int
		if err == nil {
			ranked, dropped, err = p.rank(kctx, rk, scored, results, hist, uv)
		}
		cancel()
		if err != nil {
			if err := ended(ctx, stageRanker); err != nil {
				return Response{}, err
			}
			d.add(timeoutOr(err, RankerTimeout, RankerError), err)
		} else {
			scored, resp.Dropped = ranked, dropped
		}
	}
	picked := mmr(b.Vectors, b.InvNorms, scored, req.K, MMRLambda)
	resp.Items = make([]Item, len(picked))
	for i, h := range picked {
		resp.Items[i] = Item{ID: b.IDs[h.ID], Score: h.Score}
	}
	resp.Degraded, resp.Failures = d.list(), d.failures
	return resp, nil
}

// cacheKey returns the digest of what a response depends on, given the
// history: the bundle version, the shard settings, the user, k, the
// exclusions as a set and the history items in order. The request time only selects the history, and
// the recent items are used only when the history cannot be read, which
// is never cached.
func (p *Pipeline) cacheKey(req Request, hist []int32) cache.Key {
	str := func(b []byte, s string) []byte { return append(binary.AppendUvarint(b, uint64(len(s))), s...) }
	var b []byte
	if !p.keyWithoutVersion {
		b = str(b, p.Bundle.ModelVersion)
	}
	// Gateways that share a cache may search with other settings.
	b = str(b, fmt.Sprintf("%+v", p.Shards.Config()))
	b = binary.AppendUvarint(str(b, req.User), uint64(req.K))
	exclude := slices.Compact(slices.Sorted(slices.Values(req.Exclude)))
	b = binary.AppendUvarint(b, uint64(len(exclude)))
	for _, raw := range exclude {
		b = str(b, raw)
	}
	b = binary.AppendUvarint(b, uint64(len(hist)))
	for _, id := range hist {
		b = binary.LittleEndian.AppendUint32(b, uint32(id))
	}
	return sha256.Sum256(b)
}

// history returns the user's stored history before req.TS or, if it cannot
// be read within hctx, the request's context bounded by the history budget,
// req.Recent, as the bundle's internal IDs: the store holds raw item IDs,
// and those the bundle does not know are left out.
func (p *Pipeline) history(ctx, hctx context.Context, req Request, d *degradation) ([]int32, error) {
	err := p.stalled(hctx, stageHistory, -1)
	var raw []string
	if err == nil {
		raw, err = p.History.Before(hctx, req.User, req.TS, 0)
	}
	if err != nil {
		if err := ended(ctx, stageHistory); err != nil {
			return nil, err
		}
		d.add(HistoryUnavailable, err)
		raw = req.Recent
	}
	return internalIDs(p.Bundle, raw), nil
}

// internalIDs maps raw item IDs to b's internal IDs, in order, leaving out
// those b does not know.
func internalIDs(b *bundle.Bundle, raw []string) []int32 {
	out := make([]int32, 0, len(raw))
	for _, r := range raw {
		if id, ok := b.Lookup(r); ok {
			out = append(out, id)
		}
	}
	return out
}

// userVector returns the user vector of hist, or nil if there is none or
// the encoder fails or runs out of time.
func (p *Pipeline) userVector(ctx context.Context, hist []int32, d *degradation) ([]float32, error) {
	ectx, cancel := within(ctx, p.Budgets.Encoder)
	defer cancel()
	err := p.stalled(ectx, stageEncoder, -1)
	var uv []float32
	if err == nil {
		uv, err = p.Bundle.UserVector(ectx, hist)
	}
	if err == nil {
		return uv, nil
	}
	if err := ended(ctx, stageEncoder); err != nil {
		return nil, err
	}
	d.add(timeoutOr(err, EncoderTimeout, EncoderError), err)
	return nil, nil
}

// retrieve runs the retrieval sources in parallel within the retrieval
// budget and returns their hits, in the order of srcVector, srcCooc and
// srcPopular, nil for a source that failed, and the coverage of vector
// retrieval, nil without a user vector.
func (p *Pipeline) retrieve(ctx context.Context, hist []int32, uv []float32, seen map[int32]bool, d *degradation) ([][]vec.Hit, *Coverage, error) {
	rctx, cancel := within(ctx, p.Budgets.Retrieval)
	defer cancel()
	results := make([][]vec.Hit, 3)
	errs := make([]error, 3)
	var shardHits [][]vec.Hit
	var shardErrs []error
	var wg sync.WaitGroup
	if uv != nil {
		wg.Go(func() { shardHits, shardErrs = p.searchShards(rctx, uv, seen) })
	}
	wg.Go(func() { results[srcCooc], errs[srcCooc] = p.cooc(rctx, hist) })
	wg.Go(func() { results[srcPopular], errs[srcPopular] = p.popular(rctx) })
	wg.Wait()
	if err := ended(ctx, "retrieval"); err != nil {
		return nil, nil, err
	}
	for _, s := range []struct {
		src              int
		timeout, failure string
	}{{srcCooc, CoocTimeout, CoocError}, {srcPopular, PopularTimeout, PopularError}} {
		if err := errs[s.src]; err != nil {
			results[s.src] = nil
			d.add(timeoutOr(err, s.timeout, s.failure), err)
		}
	}
	if uv == nil {
		return results, nil, nil
	}
	cov := &Coverage{Shards: len(shardErrs)}
	var answered [][]vec.Hit
	var failed []error
	allTimeouts := true
	for i, err := range shardErrs {
		if err != nil {
			failed = append(failed, fmt.Errorf("shard %d: %w", i, err))
			allTimeouts = allTimeouts && errors.Is(err, context.DeadlineExceeded)
			continue
		}
		answered = append(answered, shardHits[i])
	}
	cov.Answered = len(answered)
	reason := ShardsPartial
	if cov.Answered == 0 {
		reason = VectorError
		if allTimeouts {
			reason = VectorTimeout
		}
	}
	for _, err := range failed {
		d.add(reason, err)
	}
	if cov.Answered > 0 {
		results[srcVector] = shard.Merge(answered, VectorTop)
	}
	return results, cov, nil
}

// searchShards searches every shard in parallel, each within the shard
// budget, for the VectorTop unseen items closest to uv.
func (p *Pipeline) searchShards(ctx context.Context, uv []float32, seen map[int32]bool) ([][]vec.Hit, []error) {
	n := p.Shards.Shards()
	hits, errs := make([][]vec.Hit, n), make([]error, n)
	skip := func(id int32) bool { return seen[id] }
	top := VectorTop
	if p.shardShare {
		top = (VectorTop + n - 1) / n
	}
	var wg sync.WaitGroup
	for i := range n {
		wg.Go(func() {
			sctx, cancel := within(ctx, p.Budgets.Shard)
			defer cancel()
			if errs[i] = p.stalled(sctx, stageVector, i); errs[i] == nil {
				hits[i], errs[i] = p.Shards.Search(sctx, i, uv, top, skip)
			}
		})
	}
	wg.Wait()
	return hits, errs
}

// retrievalScores merges the sources' results and drops seen items. A
// candidate's score is its dot product with the user vector, or, without
// one, its best source score.
func (p *Pipeline) retrievalScores(results [][]vec.Hit, uv []float32, seen map[int32]bool) []vec.Hit {
	b := p.Bundle
	if p.scoreBundle != nil {
		b = p.scoreBundle()
	}
	cands := merge(results)
	scored := make([]vec.Hit, 0, len(cands))
	for _, c := range cands {
		s := c.score
		if uv != nil {
			s = vec.Dot(uv, b.Vectors.Row(c.id))
		}
		scored = append(scored, vec.Hit{ID: c.id, Score: s})
	}
	if !p.skipSeenFilter {
		scored = slices.DeleteFunc(scored, func(h vec.Hit) bool { return seen[h.ID] })
	}
	return scored
}

// rank scores the candidates with rk: the bundle.RankerRows best by
// retrieval score, whose scores become rk's; the rest are dropped, and
// their number returned.
func (p *Pipeline) rank(ctx context.Context, rk Ranker, scored []vec.Hit, results [][]vec.Hit, hist []int32, uv []float32) ([]vec.Hit, int, error) {
	cands := slices.Clone(scored)
	slices.SortFunc(cands, func(a, b vec.Hit) int { // vec.Better first
		return cmp.Or(cmp.Compare(b.Score, a.Score), cmp.Compare(a.ID, b.ID))
	})
	dropped := max(0, len(cands)-bundle.RankerRows)
	cands = cands[:len(cands)-dropped]
	scores, err := rk.Rank(ctx, p.features(cands, results, hist, uv))
	if err != nil {
		return nil, 0, err
	}
	if len(scores) != len(cands) {
		return nil, 0, fmt.Errorf("ranker returned %d scores for %d candidates", len(scores), len(cands))
	}
	for i, s := range scores {
		cands[i].Score = float64(s)
	}
	return cands, dropped, nil
}

// features returns the ranker's input for cands, one row per candidate,
// bundle.RankerFeatures in order (docs/SPEC.md §8). Each value is computed
// in float64 and rounded to float32.
func (p *Pipeline) features(cands []vec.Hit, results [][]vec.Hit, hist []int32, uv []float32) []float32 {
	b := p.Bundle
	cooc, popular := best(results[srcCooc]), best(results[srcPopular])
	var last []float32
	var lastNorm float64
	if len(hist) > 0 {
		last = b.Vectors.Row(hist[len(hist)-1])
		lastNorm = math.Sqrt(vec.Dot(last, last))
	}
	histLen := math.Log1p(float64(len(hist)))
	nf := len(bundle.RankerFeatures)
	out := make([]float32, len(cands)*nf)
	for i, c := range cands {
		row := b.Vectors.Row(c.ID)
		norm := math.Sqrt(vec.Dot(row, row))
		var vs, cos float64
		if uv != nil {
			vs = vec.Dot(uv, row)
		}
		if norm > 0 && lastNorm > 0 {
			cos = vec.Dot(row, last) / (norm * lastNorm)
		}
		f := out[i*nf : (i+1)*nf]
		f[0], f[1], f[2] = float32(vs), float32(cooc[c.ID]), float32(popular[c.ID])
		f[3], f[4], f[5] = float32(norm), float32(cos), float32(histLen)
	}
	return out
}

// best returns each ID's highest score among hits.
func best(hits []vec.Hit) map[int32]float64 {
	out := make(map[int32]float64, len(hits))
	for _, h := range hits {
		if s, ok := out[h.ID]; !ok || h.Score > s {
			out[h.ID] = h.Score
		}
	}
	return out
}

// cooc returns the co-occurrence candidates: the neighbor at position r of
// the seed s positions from the latest history item scores
// 1 / ((s + 1)(r + 1)), as docs/SPEC.md §8 writes it.
func (p *Pipeline) cooc(ctx context.Context, hist []int32) ([]vec.Hit, error) {
	if err := p.stalled(ctx, stageCooc, -1); err != nil {
		return nil, err
	}
	var out []vec.Hit
	for s := 0; s < CoocSeeds && s < len(hist); s++ {
		for r, id := range p.Bundle.CoocRow(hist[len(hist)-1-s]) {
			out = append(out, vec.Hit{ID: id, Score: 1 / float64((s+1)*(r+1))})
		}
	}
	return out, nil
}

// popular returns the PopularTop most popular items, scored by rank.
func (p *Pipeline) popular(ctx context.Context) ([]vec.Hit, error) {
	if err := p.stalled(ctx, stagePopular, -1); err != nil {
		return nil, err
	}
	pop := p.Bundle.Popular[:min(PopularTop, len(p.Bundle.Popular))]
	out := make([]vec.Hit, len(pop))
	for r, id := range pop {
		out[r] = vec.Hit{ID: id, Score: 1 / float64(r+1)}
	}
	return out, nil
}

// merge returns the union of the sources' hits, each ID once with its best
// source score, ordered by ID.
func merge(results [][]vec.Hit) []candidate {
	top := best(slices.Concat(results...))
	out := make([]candidate, 0, len(top))
	for id, s := range top {
		out = append(out, candidate{id, s})
	}
	slices.SortFunc(out, func(a, b candidate) int { return cmp.Compare(a.id, b.id) })
	return out
}

// cosine is the cosine similarity of rows a and b of m, given the inverse
// row norms; 0 if either row is zero.
func cosine(m vec.Matrix, inv []float32, a, b int32) float64 {
	if inv[a] == 0 || inv[b] == 0 {
		return 0
	}
	return float64(vec.Dot32(m.Row(a), m.Row(b)) * inv[a] * inv[b])
}

// mmr picks k hits by maximal marginal relevance: relevance is the score
// scaled to [0, 1] over the candidates, redundancy the highest cosine
// similarity to an already picked item (0 before any pick); inv holds the
// inverse row norms of m. Ties go to the lower ID.
//
// Redundancy only grows as items are picked, so a value computed in an
// earlier round bounds the current one from above. The candidates sit in a
// heap ordered by that bound; the top is brought up to date against the
// items picked since its last update, and is picked once it is up to date
// and still on top. This picks what recomputing every value each round
// would, with far fewer similarity computations.
func mmr(m vec.Matrix, inv []float32, hits []vec.Hit, k int, lambda float64) []vec.Hit {
	if len(hits) == 0 || k <= 0 {
		return nil
	}
	lo, hi := math.Inf(1), math.Inf(-1)
	for _, h := range hits {
		lo, hi = min(lo, h.Score), max(hi, h.Score)
	}
	rel := func(s float64) float64 {
		if hi == lo {
			return 1
		}
		return (s - lo) / (hi - lo)
	}
	value := func(e *mmrEntry) float64 { return lambda*rel(e.hit.Score) - (1-lambda)*e.redundancy }
	h := make(mmrHeap, len(hits))
	for i, hit := range hits {
		h[i] = mmrEntry{hit: hit}
		h[i].value = value(&h[i])
	}
	h.init()
	out := make([]vec.Hit, 0, min(k, len(hits)))
	for len(out) < k && len(h) > 0 {
		e := &h[0]
		if e.upTo == len(out) {
			out = append(out, e.hit)
			h = h.popTop()
			continue
		}
		for _, p := range out[e.upTo:] {
			e.redundancy = max(e.redundancy, cosine(m, inv, e.hit.ID, p.ID))
		}
		e.upTo = len(out)
		e.value = value(e)
		h.down(0)
	}
	return out
}

type mmrEntry struct {
	hit        vec.Hit
	redundancy float64 // highest similarity to the first upTo picked items
	upTo       int
	value      float64 // MMR value as of upTo picks: an upper bound on the current one
}

// mmrHeap is a max-heap by value, then by lower ID.
type mmrHeap []mmrEntry

func (h mmrHeap) less(i, j int) bool {
	return h[i].value > h[j].value || (h[i].value == h[j].value && h[i].hit.ID < h[j].hit.ID)
}

func (h mmrHeap) init() {
	for i := len(h)/2 - 1; i >= 0; i-- {
		h.down(i)
	}
}

func (h mmrHeap) down(i int) {
	for {
		c := 2*i + 1
		if c >= len(h) {
			return
		}
		if r := c + 1; r < len(h) && h.less(r, c) {
			c = r
		}
		if !h.less(c, i) {
			return
		}
		h[i], h[c] = h[c], h[i]
		i = c
	}
}

func (h mmrHeap) popTop() mmrHeap {
	n := len(h) - 1
	h[0] = h[n]
	h = h[:n]
	h.down(0)
	return h
}
