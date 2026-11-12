package recommend

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math/rand/v2"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/sHyanorn/recserve/internal/bundle"
	"github.com/sHyanorn/recserve/internal/cache"
	"github.com/sHyanorn/recserve/internal/history"
	"github.com/sHyanorn/recserve/internal/onnx"
	"github.com/sHyanorn/recserve/internal/onnx/onnxtest"
	"github.com/sHyanorn/recserve/internal/shard"
)

const (
	swapItems = 40
	swapUsers = 10
)

var discard = slog.New(slog.NewTextHandler(io.Discard, nil))

// swapOffset shifts a version's raw item IDs: a holds i1 to i40 and b i6 to
// i45, so the two share most items under different internal IDs and each
// has five the other lacks.
func swapOffset(version string) int { return 5 * int(version[0]-'a') }

// swapRaw is the number of raw item IDs over versions a and b.
const swapRaw = swapItems + 5

// swapBundleOf returns a new bundle of the given version and number of
// items: its own item IDs (swapOffset), and vectors (none zero),
// co-occurrence rows and a popularity list drawn from the version's own
// seed, so two versions score every candidate differently. ranker, if not
// nil, is loaded as the bundle's ranker.
func swapBundleOf(t testing.TB, version string, ranker []byte, items int) *bundle.Bundle {
	t.Helper()
	r := rand.New(rand.NewPCG(uint64(len(version)), uint64(version[0])))
	const dim = 4
	data := make([]float32, (items+1)*dim)
	for i := dim; i < len(data); i++ {
		data[i] = float32(r.IntN(9)-4) + 0.5
	}
	ids := make([]string, items+1)
	cooc := make([]int32, (items+1)*bundle.CoocWidth)
	for i := 1; i <= items; i++ {
		ids[i] = fmt.Sprint("i", i+swapOffset(version))
		for j := range r.IntN(6) {
			cooc[i*bundle.CoocWidth+j] = int32(1 + r.IntN(items))
		}
	}
	popular := make([]int32, 10)
	for i := range popular {
		popular[i] = int32(1 + r.IntN(items))
	}
	man := bundle.Manifest{ModelVersion: version, ItemCount: items, Dim: dim, MaxSeqLen: 5}
	b := bundle.New(man, ids, matrix(t, dim, data), cooc, popular)
	if ranker != nil {
		rk, err := onnx.NewRanker(ranker, len(bundle.RankerFeatures), bundle.RankerRows)
		if err != nil {
			t.Fatal(err)
		}
		b.Ranker = rk
	}
	return b
}

func swapStore(t testing.TB) history.Store {
	r := rand.New(rand.NewPCG(4, 4))
	var events []history.Event
	for u := range swapUsers {
		for e := range r.IntN(15) {
			events = append(events, history.Event{ID: uint64(u<<20 | e), User: fmt.Sprint("u", u), Item: fmt.Sprint("i", 1+r.IntN(swapRaw)), TS: int64(r.IntN(100))})
		}
	}
	return storeWith(t, events...)
}

// swapTest serves two bundle versions, a and b, through a Live and keeps
// a reference pipeline per version that is never swapped or cached.
type swapTest struct {
	t      testing.TB
	ranker []byte
	items  int                                  // per bundle
	shards func(*bundle.Bundle) (Shards, error) // where a prepared bundle's vectors go
	store  history.Store
	cache  *cache.Cache[Response]
	refs   map[string]*Pipeline
	live   *Live
	broken string // "", "rescore" or "release early"

	mu      sync.Mutex
	bundles []*bundle.Bundle // every bundle the Live served or was offered

	// The hook stops the first request that reaches vector retrieval until
	// a swap has activated another version and either waits for that
	// request or has finished.
	holdFirst atomic.Bool // set by checkProperty4
	hookOnce  sync.Once
	hooked    chan struct{} // closed when the hook runs
	draining  chan struct{} // closed when a swap waits for requests
	swapped   chan struct{} // closed when a swap completes
	hookStuck atomic.Bool   // the hook gave up waiting
}

func newSwapTest(t testing.TB, ranker []byte, broken string) *swapTest {
	return newSwapTestOn(t, ranker, broken, swapItems, func(b *bundle.Bundle) (Shards, error) {
		return localShards(b.Vectors, shard.DefaultConfig)
	})
}

// newSwapTestOn is newSwapTest with bundles of items items whose vectors
// shards puts into shards. The references search in-process shards.
func newSwapTestOn(t testing.TB, ranker []byte, broken string, items int, shards func(*bundle.Bundle) (Shards, error)) *swapTest {
	st := &swapTest{t: t, ranker: ranker, items: items, shards: shards, store: swapStore(t), cache: cache.New[Response](1000, time.Hour),
		refs: map[string]*Pipeline{}, broken: broken,
		hooked: make(chan struct{}), draining: make(chan struct{}), swapped: make(chan struct{})}
	for _, v := range []string{"a", "b"} {
		b := swapBundleOf(t, v, ranker, items)
		t.Cleanup(func() {
			if err := b.Close(); err != nil {
				t.Error(err)
			}
		})
		st.refs[v] = newPipeline(t, b, st.store)
	}
	p, err := st.prepare("a")()
	if err != nil {
		t.Fatal(err)
	}
	st.live = NewLive(p, discard)
	st.live.releaseEarly = broken == "release early"
	var once sync.Once
	st.live.draining = func(int64) { once.Do(func() { close(st.draining) }) }
	return st
}

// prepare returns a function that builds a new pipeline over a new bundle
// of version v.
func (st *swapTest) prepare(v string) func() (*Pipeline, error) {
	return func() (*Pipeline, error) {
		b := swapBundleOf(st.t, v, st.ranker, st.items)
		st.mu.Lock()
		st.bundles = append(st.bundles, b)
		st.mu.Unlock()
		s, err := st.shards(b)
		if err != nil {
			return nil, err
		}
		p := New(b, st.store, s)
		p.Cache, p.stall = st.cache, st.hook
		if st.broken == "rescore" {
			p.scoreBundle = func() *bundle.Bundle {
				q, release, err := st.live.Acquire()
				if err != nil {
					panic(err)
				}
				defer release()
				return q.Bundle
			}
		}
		return p, nil
	}
}

func (st *swapTest) hook(_ context.Context, stage string, sh int) error {
	if !st.holdFirst.Load() || stage != stageVector || sh != 0 {
		return nil
	}
	st.hookOnce.Do(func() {
		close(st.hooked)
		select {
		case <-st.draining:
		case <-st.swapped:
		case <-time.After(10 * time.Second):
			st.hookStuck.Store(true)
		}
	})
	return nil
}

// check returns an error unless resp equals what the reference pipeline
// of the version it names returns for req, item for item, score for score.
func (st *swapTest) check(req Request, resp Response) error {
	ref, ok := st.refs[resp.ModelVersion]
	if !ok {
		return fmt.Errorf("%+v: unknown version %q", req, resp.ModelVersion)
	}
	want, err := ref.Recommend(context.Background(), req)
	if err != nil {
		return err
	}
	if len(resp.Degraded) > 0 || !slices.Equal(resp.Items, want.Items) {
		return fmt.Errorf("%+v on version %s: got %v %v, its vectors give %v", req, resp.ModelVersion, resp.Items, resp.Degraded, want.Items)
	}
	return nil
}

// checkProperty4 sends requests from workers goroutines while it swaps
// between versions a and b swaps times; the first request to reach vector
// retrieval is held there until a swap has activated the other version.
// It returns the first response that does not recompute from the version
// it names, and the number of responses per version.
func checkProperty4(st *swapTest, workers, swaps int) (map[string]int, error) {
	var (
		stop     atomic.Bool
		mu       sync.Mutex
		firstErr error
		served   = map[string]int{}
		wg       sync.WaitGroup
	)
	st.holdFirst.Store(true)
	fail := func(err error) {
		mu.Lock()
		defer mu.Unlock()
		if firstErr == nil {
			firstErr = err
		}
	}
	for w := range workers {
		wg.Go(func() {
			r := rand.New(rand.NewPCG(uint64(w), 44))
			for !stop.Load() {
				req := Request{User: fmt.Sprint("u", r.IntN(swapUsers)), K: 1 + r.IntN(10), TS: int64(r.IntN(110))}
				if r.IntN(3) == 0 {
					req.Exclude = []string{fmt.Sprint("i", 1+r.IntN(swapRaw))}
				}
				resp, err := st.live.Recommend(context.Background(), req)
				if err == nil {
					err = st.check(req, resp)
				}
				if err != nil {
					fail(err)
					return
				}
				mu.Lock()
				served[resp.ModelVersion]++
				mu.Unlock()
			}
		})
	}
	select {
	case <-st.hooked:
	case <-time.After(10 * time.Second):
		fail(errors.New("no request reached vector retrieval"))
	}
	for i := range swaps {
		if _, err := st.live.Swap(st.prepare([]string{"b", "a"}[i%2])); err != nil {
			fail(err)
			break
		}
		if i == 0 {
			close(st.swapped)
		}
	}
	stop.Store(true)
	wg.Wait()
	if st.hookStuck.Load() {
		fail(errors.New("the held request waited 10 s for a swap"))
	}
	return served, firstErr
}

// Property 4: during concurrent requests and repeated swaps between two
// bundles with different items, every response recomputes exactly from the
// vectors of the version it names. The broken
// variant scores candidates with the bundle active when scoring starts; the
// held request makes sure one request scores after a swap.
func TestProperty4SwapUnderLoad(t *testing.T) {
	st := newSwapTest(t, nil, "")
	served, err := checkProperty4(st, 8, 200)
	if err != nil {
		t.Fatal(err)
	}
	if served["a"] == 0 || served["b"] == 0 {
		t.Fatalf("responses per version: %v", served)
	}
	t.Logf("responses per version: %v", served)
	if err := st.live.Close(); err != nil {
		t.Fatal(err)
	}

	broken := newSwapTest(t, nil, "rescore")
	if _, err := checkProperty4(broken, 8, 200); err == nil {
		t.Fatal("broken variant (rescoring with the active bundle) passed")
	} else {
		t.Logf("broken variant caught: %v", err)
	}
}

// checkProperty2AcrossSwap serves version a, then b, then a again, and
// returns the first response that holds an item of the user's stored
// history before the request. With broken set, version b's pipeline skips
// the seen filter.
func checkProperty2AcrossSwap(t *testing.T, broken bool) error {
	st := newSwapTest(t, nil, "")
	t.Cleanup(func() {
		if err := st.live.Close(); err != nil {
			t.Error(err)
		}
	})
	ctx := context.Background()
	r := rand.New(rand.NewPCG(9, 9))
	for i, v := range []string{"a", "b", "a"} {
		if i > 0 {
			if _, err := st.live.Swap(func() (*Pipeline, error) {
				p, err := st.prepare(v)()
				if err == nil {
					p.skipSeenFilter = broken && v == "b"
				}
				return p, err
			}); err != nil {
				return err
			}
		}
		for range 300 {
			req := Request{User: fmt.Sprint("u", r.IntN(swapUsers)), K: swapItems, TS: int64(r.IntN(110))}
			resp, err := st.live.Recommend(ctx, req)
			if err != nil {
				return err
			}
			seen, err := st.store.Before(ctx, req.User, req.TS, 0)
			if err != nil {
				return err
			}
			if resp.ModelVersion != v {
				return fmt.Errorf("served by %s, want %s", resp.ModelVersion, v)
			}
			for _, it := range resp.Items {
				if slices.Contains(seen, it.ID) {
					return fmt.Errorf("swap %d, version %s: %s at ts %d got seen item %s", i, v, req.User, req.TS, it.ID)
				}
			}
		}
	}
	return nil
}

// Property 2 holds across swaps between bundles with different items: the
// store holds raw item IDs, and each version maps them with its own IDs.
func TestProperty2AcrossSwap(t *testing.T) {
	if err := checkProperty2AcrossSwap(t, false); err != nil {
		t.Fatal(err)
	}
	if err := checkProperty2AcrossSwap(t, true); err == nil {
		t.Fatal("broken variant (no seen filter after the swap) passed")
	} else {
		t.Logf("broken variant caught: %v", err)
	}
}

// A bundle is closed only once no request holds it: with ONNX rankers, a
// request held during a swap still ranks on its version, every replaced
// bundle is closed by the end, and the active one only by Close. The broken
// variant releases a version as soon as a request takes it, so the held
// request's ranker is closed under it and the response degrades.
func TestSwapClosesOnlyUnusedBundles(t *testing.T) {
	if err := onnx.Init(onnxtest.Lib(t)); err != nil {
		t.Fatal(err)
	}
	model := onnxtest.Model(t, "linear_ranker.onnx")
	st := newSwapTest(t, model, "")
	if _, err := checkProperty4(st, 4, 20); err != nil {
		t.Fatal(err)
	}
	closed := func(b *bundle.Bundle) bool {
		_, err := b.Ranker.Rank(context.Background(), make([]float32, bundle.RankerRows*len(bundle.RankerFeatures)))
		return errors.Is(err, onnx.ErrClosed)
	}
	active := st.bundles[len(st.bundles)-1]
	for i, b := range st.bundles[:len(st.bundles)-1] {
		if !closed(b) {
			t.Fatalf("bundle %d (%s) was replaced but not closed", i, b.ModelVersion)
		}
	}
	if closed(active) {
		t.Fatal("the active bundle is closed")
	}
	if err := st.live.Close(); err != nil {
		t.Fatal(err)
	}
	if !closed(active) {
		t.Fatal("Close left the active bundle open")
	}
	if _, err := st.live.Recommend(context.Background(), Request{User: "u1", K: 3}); !errors.Is(err, ErrLiveClosed) {
		t.Fatalf("after Close: %v", err)
	}

	broken := newSwapTest(t, model, "release early")
	_, err := checkProperty4(broken, 4, 20)
	if err == nil {
		t.Fatal("broken variant (releasing early) passed")
	}
	t.Logf("broken variant caught: %v", err)
	if err := broken.live.Close(); err != nil {
		t.Fatal(err)
	}
}

// A swap that fails leaves the active version serving, and closes the
// bundle it was offered.
func TestSwapRejects(t *testing.T) {
	if err := onnx.Init(onnxtest.Lib(t)); err != nil {
		t.Fatal(err)
	}
	st := newSwapTest(t, onnxtest.Model(t, "linear_ranker.onnx"), "")
	t.Cleanup(func() {
		if err := st.live.Close(); err != nil {
			t.Error(err)
		}
	})
	errLoad := errors.New("checksum mismatch")
	cases := map[string]struct {
		prepare func() (*Pipeline, error)
		want    error
	}{
		"prepare fails":  {func() (*Pipeline, error) { return nil, errLoad }, errLoad},
		"same version":   {st.prepare("a"), ErrActive},
		"swap in flight": {nil, ErrSwapInProgress},
	}
	req := Request{User: "u1", K: 5, TS: 100}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			before := len(st.bundles)
			var err error
			if tc.prepare == nil {
				entered, release := make(chan struct{}), make(chan struct{})
				done := make(chan error, 1)
				go func() {
					_, err := st.live.Swap(func() (*Pipeline, error) {
						close(entered)
						<-release
						return nil, errLoad
					})
					done <- err
				}()
				<-entered
				_, err = st.live.Swap(st.prepare("b"))
				close(release)
				if err := <-done; !errors.Is(err, errLoad) {
					t.Fatalf("first swap: %v", err)
				}
			} else {
				_, err = st.live.Swap(tc.prepare)
			}
			if !errors.Is(err, tc.want) {
				t.Fatalf("err = %v, want %v", err, tc.want)
			}
			if v := st.live.Version(); v != "a" {
				t.Fatalf("active version %q after a failed swap", v)
			}
			resp, err := st.live.Recommend(context.Background(), req)
			if err == nil {
				err = st.check(req, resp)
			}
			if err != nil {
				t.Fatal(err)
			}
			for _, b := range st.bundles[before:] {
				if _, err := b.Ranker.Rank(context.Background(), make([]float32, bundle.RankerRows*len(bundle.RankerFeatures))); !errors.Is(err, onnx.ErrClosed) {
					t.Fatalf("the rejected bundle is open: %v", err)
				}
			}
		})
	}
	res, err := st.live.Swap(st.prepare("b"))
	if err != nil || res.Version != "b" || res.Previous != "a" {
		t.Fatalf("swap after failures: %+v %v", res, err)
	}
}

// Serving gives the active version and the time it was activated, after
// its prepare ended, so each swap gives a later time, a swap back to a
// version included; a rejected swap leaves both as they were (D057).
func TestServingTime(t *testing.T) {
	st := newSwapTest(t, nil, "")
	check := func(want string, after ...time.Time) time.Time {
		t.Helper()
		v, since := st.live.Serving()
		for _, a := range after {
			if v != want || !since.After(a) {
				t.Fatalf("Serving() = %s, %v; want %s after %v", v, since, want, a)
			}
		}
		return since
	}
	swap := func(v string) time.Time {
		t.Helper()
		var prepared time.Time
		prepare := st.prepare(v)
		if _, err := st.live.Swap(func() (*Pipeline, error) {
			p, err := prepare()
			prepared = time.Now()
			return p, err
		}); err != nil {
			t.Fatal(err)
		}
		return prepared
	}
	since := check("a", time.Time{})
	since = check("b", since, swap("b"))
	if _, err := st.live.Swap(st.prepare("b")); !errors.Is(err, ErrActive) {
		t.Fatalf("err = %v, want %v", err, ErrActive)
	}
	if v, s := st.live.Serving(); v != "b" || !s.Equal(since) {
		t.Fatalf("after a rejected swap, Serving() = %s, %v; want b, %v", v, s, since)
	}
	check("a", since, swap("a"))
}

// A swap waits for a request that holds the previous version as long as
// it takes, logging while it waits, and closes the bundle once it returns.
func TestSwapWaitsForRequests(t *testing.T) {
	st := newSwapTest(t, nil, "")
	var logs syncBuffer
	st.live.log = slog.New(slog.NewTextHandler(&logs, nil))
	st.live.warnEvery = 5 * time.Millisecond
	p, release, err := st.live.Acquire()
	if err != nil {
		t.Fatal(err)
	}
	type result struct {
		res SwapResult
		err error
	}
	done := make(chan result, 1)
	go func() {
		res, err := st.live.Swap(st.prepare("b"))
		done <- result{res, err}
	}()
	deadline := time.Now().Add(10 * time.Second)
	for !strings.Contains(logs.String(), "requests=1") {
		if time.Now().After(deadline) {
			t.Fatalf("no warning while the swap waits; log: %s", logs.String())
		}
		time.Sleep(time.Millisecond)
	}
	select {
	case r := <-done:
		t.Fatalf("swap returned while a request holds the previous version: %+v %v", r.res, r.err)
	default:
	}
	if v := st.live.Version(); v != "b" {
		t.Fatalf("active version %q while the swap drains", v)
	}
	// The held pipeline still serves.
	req := Request{User: "u2", K: 4, TS: 100}
	resp, err := p.Recommend(context.Background(), req)
	if err == nil {
		err = st.check(req, resp)
	}
	if err != nil || resp.ModelVersion != "a" {
		t.Fatalf("held pipeline: %v %v", resp.ModelVersion, err)
	}
	release()
	r := <-done
	if r.err != nil || r.res.Version != "b" || r.res.Previous != "a" || r.res.Drain <= 0 {
		t.Fatalf("swap: %+v %v", r.res, r.err)
	}
	if err := st.live.Close(); err != nil {
		t.Fatal(err)
	}
}

// syncBuffer is a bytes.Buffer safe for concurrent use.
type syncBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *syncBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *syncBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}
