package recommend

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"slices"
	"sync"
	"sync/atomic"
	"time"
)

// Errors of a swap that leave the active version in place.
var (
	// ErrSwapInProgress is returned while another swap runs.
	ErrSwapInProgress = errors.New("a swap is in progress")
	// ErrActive is returned for a bundle whose version is already active.
	ErrActive = errors.New("bundle version is already active")
	// ErrIDsDiffer is returned for a bundle whose item IDs differ from the
	// active one's: the history store holds internal IDs (D031).
	ErrIDsDiffer = errors.New("bundle item IDs differ from the active bundle's")
	// ErrLiveClosed is returned after Close.
	ErrLiveClosed = errors.New("no active bundle: closed")
)

// DrainWarnEvery is how often a swap that waits for requests on the
// previous version logs that it is still waiting.
const DrainWarnEvery = 5 * time.Second

// Live serves requests from the active pipeline and replaces it in two
// phases (docs/SPEC.md §4, D031): a new pipeline is prepared and verified
// while the active one serves, then activated by one atomic store. Each
// request holds the pipeline it started on until it returns, and a
// pipeline's bundle is closed only once no request holds it.
type Live struct {
	log       *slog.Logger
	warnEvery time.Duration // DrainWarnEvery, shorter in tests
	cur       atomic.Pointer[version]
	swapMu    sync.Mutex

	releaseEarly bool // test only: the broken variant that does not hold a version during a request
	// draining, test only, is called when a swap starts waiting for the
	// requests that hold the previous version, with their number.
	draining func(held int64)
}

// version is one pipeline and the references to it: one per request that
// holds it, plus one while it is active.
type version struct {
	p    *Pipeline
	refs atomic.Int64
	free chan struct{} // closed when refs reaches zero
}

func newVersion(p *Pipeline) *version {
	v := &version{p: p, free: make(chan struct{})}
	v.refs.Store(1)
	return v
}

func (v *version) release() {
	if v.refs.Add(-1) == 0 {
		close(v.free)
	}
}

// NewLive returns a Live serving p.
func NewLive(p *Pipeline, log *slog.Logger) *Live {
	l := &Live{log: log, warnEvery: DrainWarnEvery}
	l.cur.Store(newVersion(p))
	return l
}

// Acquire returns the active pipeline and the function that releases it.
// The pipeline and its bundle stay usable until it is released, also if
// another is activated meanwhile.
func (l *Live) Acquire() (*Pipeline, func(), error) {
	for {
		v := l.cur.Load()
		if v == nil {
			return nil, nil, ErrLiveClosed
		}
		// A version whose count is zero has been retired and may be closed;
		// the pointer already names its successor, so load again.
		n := v.refs.Load()
		if n > 0 && v.refs.CompareAndSwap(n, n+1) {
			if l.releaseEarly {
				v.release()
				return v.p, func() {}, nil
			}
			return v.p, v.release, nil
		}
	}
}

// Recommend answers req from the pipeline active when it starts.
func (l *Live) Recommend(ctx context.Context, req Request) (Response, error) {
	p, release, err := l.Acquire()
	if err != nil {
		return Response{}, err
	}
	defer release()
	return p.Recommend(ctx, req)
}

// Version returns the active bundle's version, or "" after Close.
func (l *Live) Version() string {
	if v := l.cur.Load(); v != nil {
		return v.p.Bundle.ModelVersion
	}
	return ""
}

// SwapResult describes a completed swap.
type SwapResult struct {
	Version  string        // the version now active
	Previous string        // the version it replaced
	Prepare  time.Duration // loading, verifying and indexing the new bundle
	Drain    time.Duration // from activation until the previous bundle was closed
}

// Swap replaces the active pipeline with the one prepare returns. prepare
// loads and verifies a bundle and builds its index while the active
// pipeline serves; if it fails, or its bundle's version is already active,
// or its item IDs differ from the active bundle's, the new bundle is closed
// and the active pipeline stays. Otherwise the new pipeline is activated,
// and Swap waits for the requests that hold the previous one, then closes
// its bundle. The wait has no limit, since a bundle in use must not be
// closed; it is logged every DrainWarnEvery. One swap runs at a time;
// another returns ErrSwapInProgress.
func (l *Live) Swap(prepare func() (*Pipeline, error)) (SwapResult, error) {
	if !l.swapMu.TryLock() {
		return SwapResult{}, ErrSwapInProgress
	}
	defer l.swapMu.Unlock()
	old := l.cur.Load()
	if old == nil {
		return SwapResult{}, ErrLiveClosed
	}
	start := time.Now()
	p, err := prepare()
	if err != nil {
		return SwapResult{}, fmt.Errorf("prepare: %w", err)
	}
	ob, nb := old.p.Bundle, p.Bundle
	switch {
	case nb.ModelVersion == ob.ModelVersion:
		err = fmt.Errorf("%w: %s", ErrActive, nb.ModelVersion)
	case !slices.Equal(nb.IDs, ob.IDs):
		err = fmt.Errorf("%w: %s has %d items, %s has %d", ErrIDsDiffer, nb.ModelVersion, nb.ItemCount, ob.ModelVersion, ob.ItemCount)
	}
	if err != nil {
		return SwapResult{}, errors.Join(err, nb.Close())
	}
	res := SwapResult{Version: nb.ModelVersion, Previous: ob.ModelVersion, Prepare: time.Since(start)}
	activated := time.Now()
	l.cur.Store(newVersion(p))
	if err := l.retire(old); err != nil {
		l.log.Error("closing the previous bundle failed", "version", ob.ModelVersion, "err", err)
	}
	res.Drain = time.Since(activated)
	return res, nil
}

// retire drops the active reference to v, which is no longer active, waits
// until no request holds it and closes its bundle.
func (l *Live) retire(v *version) error {
	start := time.Now()
	v.release()
	if held := v.refs.Load(); held > 0 && l.draining != nil {
		l.draining(held)
	}
	tick := time.NewTicker(l.warnEvery)
	defer tick.Stop()
	for {
		select {
		case <-v.free:
			return v.p.Bundle.Close()
		case <-tick.C:
			l.log.Warn("waiting for requests on the previous bundle before closing it",
				"version", v.p.Bundle.ModelVersion, "requests", v.refs.Load(), "waited", time.Since(start))
		}
	}
}

// Close waits for a running swap and for the requests on the active
// pipeline, then closes its bundle. Later requests fail with ErrLiveClosed.
func (l *Live) Close() error {
	l.swapMu.Lock()
	defer l.swapMu.Unlock()
	v := l.cur.Swap(nil)
	if v == nil {
		return nil
	}
	return l.retire(v)
}
