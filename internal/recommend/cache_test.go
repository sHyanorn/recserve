package recommend

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"math/rand/v2"
	"slices"
	"testing"
	"time"

	"github.com/sHyanorn/recserve/internal/bundle"
	"github.com/sHyanorn/recserve/internal/cache"
	"github.com/sHyanorn/recserve/internal/experiment"
	"github.com/sHyanorn/recserve/internal/history"
	"github.com/sHyanorn/recserve/internal/infratest"
	"github.com/sHyanorn/recserve/internal/shard"
	"github.com/sHyanorn/recserve/internal/vec"
)

// checkProperty5 sends requests from a small pool to two pipelines that
// share a cache and a store but serve bundles of different versions and
// vectors, and returns the first response that differs from the one the
// same pipeline computes without the cache. Each case gets its cache from
// newCache. broken leaves the version out of the cache key.
func checkProperty5(seed uint64, cases int, newCache func() Cache, broken bool) error {
	r := rand.New(rand.NewPCG(seed, 5))
	ctx := context.Background()
	var hits int
	for c := range cases {
		p1, err := randomPipeline(r, 2+r.IntN(40), 3)
		if err != nil {
			return err
		}
		b1 := p1.Bundle
		data := make([]float32, len(b1.Vectors.Data))
		for i := b1.Dim; i < len(data); i++ {
			data[i] = float32(r.IntN(7) - 3)
		}
		m, err := vec.NewMatrix(b1.Dim, data)
		if err != nil {
			return err
		}
		man := b1.Manifest
		man.ModelVersion = "t2"
		p2, err := newLocal(bundle.New(man, b1.IDs, m, b1.Cooc, b1.Popular), p1.History, shard.DefaultConfig)
		if err != nil {
			return err
		}
		shared := newCache()
		for _, p := range []*Pipeline{p1, p2} {
			p.Cache, p.keyWithoutVersion = shared, broken
		}
		for range 40 {
			p := []*Pipeline{p1, p2}[r.IntN(2)]
			req := Request{User: fmt.Sprint("u", r.IntN(2)), K: 5 + 5*r.IntN(2), TS: int64(50 * r.IntN(3)),
				Exclude: []string{fmt.Sprint("i", 1+r.IntN(2))}}
			got, err := p.Recommend(ctx, req)
			if err != nil {
				return err
			}
			q := *p
			q.Cache = nil
			want, err := q.Recommend(ctx, req)
			if err != nil {
				return err
			}
			if got.ModelVersion != want.ModelVersion || !slices.Equal(got.Items, want.Items) {
				return fmt.Errorf("case %d, %+v: %s %v, without the cache %s %v", c, req, got.ModelVersion, got.Items, want.ModelVersion, want.Items)
			}
			if *got.Cached {
				hits++
			}
		}
	}
	if hits == 0 {
		return fmt.Errorf("no request hit the cache")
	}
	return nil
}

func memoryCache() Cache { return cache.New[Response](1000, time.Hour) }

func TestProperty5CacheKeepsVersionsApart(t *testing.T) {
	if err := checkProperty5(1, 100, memoryCache, false); err != nil {
		t.Fatal(err)
	}
}

func TestProperty5CatchesBrokenVariant(t *testing.T) {
	if err := checkProperty5(1, 100, memoryCache, true); err == nil {
		t.Fatal("the check passed a cache key without the version")
	}
}

// salted gives a test keys of its own in a cache on a shared server, where
// another test or an earlier run may have stored the same request.
type salted struct {
	Cache
	salt uint64
}

func (s salted) key(k cache.Key) cache.Key {
	return sha256.Sum256(binary.LittleEndian.AppendUint64(k[:], s.salt))
}

func (s salted) Get(ctx context.Context, k cache.Key) (Response, bool, error) {
	return s.Cache.Get(ctx, s.key(k))
}

func (s salted) Put(ctx context.Context, k cache.Key, r Response) error {
	return s.Cache.Put(ctx, s.key(k), r)
}

// valkeyCache returns a client of its own to the result cache on the test
// server.
func valkeyCache(t *testing.T) Cache {
	t.Helper()
	addr := infratest.ValkeyAddr(t)
	v, err := cache.NewValkey[Response](infratest.ValkeyClient(t, addr), time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	return v
}

// Property 5 with the cache on Valkey, where the two pipelines could as
// well be two gateways, and its broken variant.
func TestProperty5OnValkey(t *testing.T) {
	v := valkeyCache(t)
	fresh := func() Cache { return salted{v, rand.Uint64()} }
	if err := checkProperty5(1, 100, fresh, false); err != nil {
		t.Fatal(err)
	}
	if err := checkProperty5(1, 100, fresh, true); err == nil {
		t.Fatal("the check passed a cache key without the version")
	}
}

// Two gateways share the cache on Valkey: what one computed, the other
// answers from the cache, equal to its own computation item by item and
// score by score, and equal as the JSON a client gets.
func TestInstancesShareValkeyCache(t *testing.T) {
	r := rand.New(rand.NewPCG(7, 7))
	const n, dim = 60, 4
	data := make([]float32, (n+1)*dim)
	for i := dim; i < len(data); i++ {
		data[i] = float32(r.NormFloat64())
	}
	ids := make([]string, n+1)
	popular := make([]int32, n)
	for i := 1; i <= n; i++ {
		ids[i], popular[i-1] = fmt.Sprint("i", i), int32(i)
	}
	store := storeWith(t,
		history.Event{ID: 1, User: "u", Item: "i3", TS: 5},
		history.Event{ID: 2, User: "u", Item: "i8", TS: 6},
		history.Event{ID: 3, User: "u", Item: "i21", TS: 7})
	salt := rand.Uint64()
	instance := func() *Pipeline {
		b := bundle.New(bundle.Manifest{ModelVersion: "t", ItemCount: n, Dim: dim, MaxSeqLen: 5}, ids, matrix(t, dim, data), nil, popular)
		p := newPipeline(t, b, store)
		p.Cache = salted{valkeyCache(t), salt}
		return p
	}
	a, b := instance(), instance()
	ctx := context.Background()
	req := Request{User: "u", K: 20, TS: 10, Exclude: []string{"i5"}}
	miss, err := a.Recommend(ctx, req)
	if err != nil {
		t.Fatal(err)
	}
	hit, err := b.Recommend(ctx, req)
	if err != nil {
		t.Fatal(err)
	}
	if *miss.Cached || !*hit.Cached || miss.CacheErr != nil || hit.CacheErr != nil {
		t.Fatalf("first instance cached %v (%v), second %v (%v)", *miss.Cached, miss.CacheErr, *hit.Cached, hit.CacheErr)
	}
	q := *b
	q.Cache = nil
	own, err := q.Recommend(ctx, req)
	if err != nil {
		t.Fatal(err)
	}
	if len(hit.Items) != 20 || !slices.Equal(hit.Items, miss.Items) || !slices.Equal(hit.Items, own.Items) {
		t.Fatalf("hit %v\nmiss %v\nown %v", hit.Items, miss.Items, own.Items)
	}
	whole := 0
	for _, it := range hit.Items {
		if it.Score == math.Trunc(it.Score) {
			whole++
		}
	}
	if whole == len(hit.Items) {
		t.Fatal("every score is a whole number: the comparison shows nothing about fractions")
	}
	mj, err := json.Marshal(miss)
	if err != nil {
		t.Fatal(err)
	}
	hj, err := json.Marshal(hit)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(mj, hj) {
		t.Fatalf("as JSON the miss is %s\nand the hit %s", mj, hj)
	}
}

// failing is a cache that cannot be written, nor read unless readable.
type failing struct {
	readable   bool
	gets, puts int
}

func (f *failing) Get(context.Context, cache.Key) (Response, bool, error) {
	f.gets++
	if f.readable {
		return Response{}, false, nil
	}
	return Response{}, false, errInjected
}

func (f *failing) Put(context.Context, cache.Key, Response) error {
	f.puts++
	return errInjected
}

// A cache that fails costs the request nothing: it is answered as a miss,
// in full and not degraded, and reports the error.
func TestCacheErrorIsAMiss(t *testing.T) {
	p, err := randomPipeline(rand.New(rand.NewPCG(16, 16)), 30, 1)
	if err != nil {
		t.Fatal(err)
	}
	req := Request{User: "u0", K: 5, TS: 50}
	want, err := p.Recommend(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
	// With the read failing, and with only the write failing, as on a
	// server whose memory is full.
	for _, readable := range []bool{false, true} {
		f := &failing{readable: readable}
		p.Cache = f
		got, err := p.Recommend(context.Background(), req)
		if err != nil {
			t.Fatal(err)
		}
		if len(got.Degraded) != 0 || got.Cached == nil || *got.Cached || !errors.Is(got.CacheErr, errInjected) || !slices.Equal(got.Items, want.Items) {
			t.Fatalf("readable %v: degraded %v, cached %v, cache error %v, items %v, want %v", readable, got.Degraded, got.Cached, got.CacheErr, got.Items, want.Items)
		}
		// A cache that could not be read is not written either.
		if wantPuts := map[bool]int{false: 0, true: 1}[readable]; f.gets != 1 || f.puts != wantPuts {
			t.Fatalf("readable %v: %d reads and %d writes, want 1 and %d", readable, f.gets, f.puts, wantPuts)
		}
	}

	// The request's own end during the read is the request's failure, and
	// no lookup is reported.
	p.Cache = &failing{}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if resp, err := p.Recommend(ctx, req); !errors.Is(err, context.Canceled) || resp.Cached != nil {
		t.Fatalf("a cancelled request returned cached %v, %v", resp.Cached, err)
	}
}

// The arm is in the cache key: gateways that share a cache may run other
// experiments, so a user's response is held once per arm, and each names
// the arm and the ordering it was computed with.
func TestArmJoinsCacheKey(t *testing.T) {
	p, err := randomPipeline(rand.New(rand.NewPCG(17, 17)), 30, 1)
	if err != nil {
		t.Fatal(err)
	}
	p.rankWith, p.Cache = positionRanker(1, 0), memoryCache()
	arms := []struct {
		arm     experiment.Arm
		ranking string
	}{
		{experiment.Arm{Name: "control", Mode: experiment.Retrieval}, RankedByRetrieval},
		{experiment.Arm{Name: "treatment", Mode: experiment.Ranker}, RankedByRanker},
	}
	items := map[string][]Item{}
	for round := range 2 {
		for _, a := range arms {
			resp, err := p.Recommend(context.Background(), Request{User: "u0", K: 5, TS: 50, Arm: a.arm})
			if err != nil {
				t.Fatal(err)
			}
			if resp.Arm != a.arm.Name || resp.Ranking != a.ranking || *resp.Cached != (round == 1) {
				t.Fatalf("round %d, arm %s: arm %q, ranking %q, cached %v", round, a.arm.Name, resp.Arm, resp.Ranking, *resp.Cached)
			}
			if round == 0 {
				items[a.arm.Name] = resp.Items
			} else if !slices.Equal(resp.Items, items[a.arm.Name]) {
				t.Fatalf("arm %s: the cache served %v, computed %v", a.arm.Name, resp.Items, items[a.arm.Name])
			}
		}
	}
	if slices.Equal(items["control"], items["treatment"]) {
		t.Fatal("the ranker left the order as it was; the arms are not told apart")
	}
}

// An event that arrives late, with a time before the user's latest event,
// changes the history but not its latest event; the next response must
// reflect it.
func TestCacheLateEvent(t *testing.T) {
	m := matrix(t, 2, []float32{0, 0, 1, 0, 0, 1, 1, 1})
	b := bundle.New(bundle.Manifest{ModelVersion: "t", ItemCount: 3, Dim: 2, MaxSeqLen: 5}, []string{"", "a", "b", "c"}, m, nil, []int32{1, 2, 3})
	store := storeWith(t, history.Event{ID: 1, User: "u", Item: "b", TS: 5})
	p := newPipeline(t, b, store)
	p.Cache = cache.New[Response](10, time.Hour)
	req := Request{User: "u", K: 3, TS: 10}
	ids := func() ([]string, bool) {
		resp, err := p.Recommend(context.Background(), req)
		if err != nil {
			t.Fatal(err)
		}
		var out []string
		for _, it := range resp.Items {
			out = append(out, it.ID)
		}
		return out, *resp.Cached
	}
	if got, cached := ids(); !slices.Contains(got, "a") || cached {
		t.Fatalf("before the late event: %v, cached %v", got, cached)
	}
	if _, cached := ids(); !cached {
		t.Fatal("the repeated request missed the cache")
	}
	if err := store.Append(context.Background(), []history.Event{{ID: 2, User: "u", Item: "a", TS: 3}}); err != nil {
		t.Fatal(err)
	}
	if got, cached := ids(); slices.Contains(got, "a") || cached {
		t.Fatalf("after the late event: %v, cached %v", got, cached)
	}
}

// A degraded response is not stored, and a request whose history cannot be
// read does not use the cache at all.
func TestCacheSkipsDegraded(t *testing.T) {
	p, err := randomPipeline(rand.New(rand.NewPCG(16, 16)), 30, 1)
	if err != nil {
		t.Fatal(err)
	}
	p.Bundle.Popular = []int32{1, 2, 3}
	c := cache.New[Response](10, time.Hour)
	p.Cache = c
	var fail string
	p.stall = func(_ context.Context, stage string, _ int) error {
		if stage == fail {
			return errInjected
		}
		return nil
	}
	req := Request{User: "u0", K: 5, TS: 50}
	for i, c := range []struct {
		fail     string
		degraded bool
		cached   *bool
	}{
		{stageHistory, true, nil},
		{stageCooc, true, new(bool)},
		{"", false, new(bool)}, // neither degraded response was stored
		{stageHistory, true, nil},
		{"", false, func() *bool { b := true; return &b }()},
	} {
		fail = c.fail
		resp, err := p.Recommend(context.Background(), req)
		if err != nil {
			t.Fatal(err)
		}
		if (len(resp.Degraded) > 0) != c.degraded || (resp.Cached == nil) != (c.cached == nil) || (c.cached != nil && *resp.Cached != *c.cached) {
			t.Fatalf("request %d failing %q: degraded %v, cached %v", i, c.fail, resp.Degraded, resp.Cached)
		}
	}
	if c.Len() != 1 {
		t.Fatalf("%d entries, want 1", c.Len())
	}
}
