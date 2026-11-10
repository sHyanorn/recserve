package annshard_test

import (
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/connectivity"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"

	"github.com/sHyanorn/recserve/internal/annshard"
	"github.com/sHyanorn/recserve/internal/annshard/annshardpb"
	"github.com/sHyanorn/recserve/internal/annshard/annshardtest"
	"github.com/sHyanorn/recserve/internal/hnsw"
	"github.com/sHyanorn/recserve/internal/infratest"
	"github.com/sHyanorn/recserve/internal/shard"
	"github.com/sHyanorn/recserve/internal/shard/shardtest"
	"github.com/sHyanorn/recserve/internal/vec"
)

// remote returns a build function for shardtest that puts each catalog
// on replicas.
func remote(t *testing.T) func(m vec.Matrix, cfg shard.Config) (shardtest.Searcher, error) {
	build := annshardtest.Remote(t, 7)
	return func(m vec.Matrix, cfg shard.Config) (shardtest.Searcher, error) { return build(m, cfg) }
}

// Property 6 over gRPC: the replicas' merged top k equals single-node exact
// top k item by item.
func TestProperty6OverGRPC(t *testing.T) {
	if err := shardtest.CheckProperty6(1, 200, shardtest.All, remote(t)); err != nil {
		t.Fatal(err)
	}
}

func TestProperty6OverGRPCCatchesBrokenVariant(t *testing.T) {
	if err := shardtest.CheckProperty6(1, 200, shardtest.Share, remote(t)); err == nil {
		t.Fatal("the check passed shards that return ceil(k/n) items each")
	}
}

// In HNSW mode the replicas build the graphs the gateway would, and a
// search through the client equals the in-process one hit by hit, with the
// set's search list size.
func TestHNSWEqualsInProcess(t *testing.T) {
	r := rand.New(rand.NewPCG(2, 2))
	const n, dim = 600, 3
	m := shardtest.RandomMatrix(r, n, dim)
	cfg := shard.Config{Shards: 3, Mode: shard.HNSW, HNSW: hnsw.Config{M: 8, EfConstruction: 50, Seed: 7}, Ef: 40}
	local, err := shard.Build(m, cfg)
	if err != nil {
		t.Fatal(err)
	}
	s, err := remote(t)(m, cfg)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	for c := range 60 {
		q := []float32{float32(r.IntN(5) - 2), float32(r.IntN(5) - 2), float32(r.IntN(5) - 2)}
		k := 1 + r.IntN(100)
		seen := map[int32]bool{}
		for range r.IntN(40) {
			seen[int32(1+r.IntN(n))] = true
		}
		for i := range cfg.Shards {
			want, err := local.Search(ctx, i, q, k, seen)
			if err != nil {
				t.Fatal(err)
			}
			got, err := s.Search(ctx, i, q, k, seen)
			if err != nil {
				t.Fatal(err)
			}
			if !slices.Equal(got, want) {
				t.Fatalf("case %d, shard %d, k %d:\n got %v\nwant %v", c, i, k, got, want)
			}
		}
	}
}

// A search that its context ends returns the context's error, as an
// in-process search does; a replica that is gone is another error.
func TestSearchErrors(t *testing.T) {
	r := rand.New(rand.NewPCG(3, 3))
	root := t.TempDir()
	annshardtest.WriteVectors(t, root, "v1", shardtest.RandomMatrix(r, 50, 2))
	proxy := infratest.NewProxy(t, annshardtest.Start(t, root))
	c := annshardtest.Dial(t, [][]string{{proxy.Addr()}}, annshard.Options{})
	s, err := c.Prepare(context.Background(), "v1", 50, shard.Config{Shards: 1, Mode: shard.Exact})
	if err != nil {
		t.Fatal(err)
	}
	search := func(ctx context.Context) error {
		_, err := s.Search(ctx, 0, []float32{1, 1}, 5, nil)
		return err
	}
	if err := search(context.Background()); err != nil {
		t.Fatal(err)
	}

	proxy.Blackhole()
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	start := time.Now()
	if err := search(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("silent replica: err = %v, want context.DeadlineExceeded", err)
	}
	if d := time.Since(start); d > 5*time.Second {
		t.Fatalf("a search with 50 ms returned after %v", d)
	}
	ctx, cancel = context.WithCancel(context.Background())
	time.AfterFunc(20*time.Millisecond, cancel)
	if err := search(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled search: err = %v, want context.Canceled", err)
	}

	proxy.Refuse()
	ctx, cancel = context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := search(ctx); err == nil || errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) {
		t.Fatalf("replica gone: err = %v, want an error that is not the context's", err)
	}
}

// A replica's failure is not hidden by another replica's timeout: with one
// replica that fails every search and one that is silent, a search that
// asks the failing one first, and then waits for the silent one in vain,
// returns the failure and is no timeout (D029). One that asks the silent
// one first is a timeout: it asks no other.
func TestFailureIsNotHiddenByTimeout(t *testing.T) {
	r := rand.New(rand.NewPCG(15, 15))
	root := t.TempDir()
	annshardtest.WriteVectors(t, root, "v1", shardtest.RandomMatrix(r, 50, 2))
	failing := annshardtest.Start(t, root, grpc.UnaryInterceptor(func(ctx context.Context, req any, _ *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		if _, ok := req.(*annshardpb.SearchRequest); ok {
			return nil, status.Error(codes.Internal, "injected")
		}
		return handler(ctx, req)
	}))
	silent := infratest.NewProxy(t, annshardtest.Start(t, root))
	c := annshardtest.Dial(t, [][]string{{failing, silent.Addr()}}, annshard.Options{})
	s, err := c.Prepare(context.Background(), "v1", 50, shard.Config{Shards: 1, Mode: shard.Exact})
	if err != nil {
		t.Fatal(err)
	}
	silent.Blackhole()
	// Both connections are ready, so the two searches lead with one replica each.
	var failures, timeouts int
	for range 2 {
		ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
		_, err := s.Search(ctx, 0, []float32{1, 1}, 5, nil)
		cancel()
		switch {
		case err == nil:
			t.Fatal("a search succeeded with no replica that answers")
		case errors.Is(err, context.DeadlineExceeded):
			timeouts++
		case status.Code(err) == codes.Internal:
			failures++
		default:
			t.Fatal(err)
		}
	}
	if failures != 1 || timeouts != 1 {
		t.Fatalf("%d failures and %d timeouts, want one of each", failures, timeouts)
	}
}

// A replica given other vectors under a version's name may return an item
// the gateway's bundle does not hold. Its answer is a failure that names
// the replica and the version: the shard's other replica answers, and with
// no other the search fails.
func TestReplicaWithAnotherCatalogFails(t *testing.T) {
	r := rand.New(rand.NewPCG(16, 16))
	m := shardtest.RandomMatrix(r, 50, 2)
	root, larger := t.TempDir(), t.TempDir()
	annshardtest.WriteVectors(t, root, "v1", m)
	annshardtest.WriteVectors(t, larger, "v1", shardtest.RandomMatrix(r, 60, 2))
	stray := annshardtest.Start(t, larger)
	cfg := shard.Config{Shards: 1, Mode: shard.Exact}
	local, err := shard.Build(m, cfg)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	q := []float32{1, 1}

	alone, err := annshardtest.Dial(t, [][]string{{stray}}, annshard.Options{}).Prepare(ctx, "v1", 50, cfg)
	if err != nil {
		t.Fatal(err)
	}
	_, err = alone.Search(ctx, 0, q, 60, nil)
	if err == nil || !strings.Contains(err.Error(), stray) || !strings.Contains(err.Error(), "v1") {
		t.Fatalf("err = %v, want one that names the replica %s and the version", err, stray)
	}

	both, err := annshardtest.Dial(t, [][]string{{stray, annshardtest.Start(t, root)}}, annshard.Options{}).Prepare(ctx, "v1", 50, cfg)
	if err != nil {
		t.Fatal(err)
	}
	want, err := local.Search(ctx, 0, q, 60, nil)
	if err != nil {
		t.Fatal(err)
	}
	for range 4 { // each replica leads twice
		got, err := both.Search(ctx, 0, q, 60, nil)
		if err != nil {
			t.Fatal(err)
		}
		if !slices.Equal(got, want) {
			t.Fatalf("got %v\nwant %v", got, want)
		}
	}
}

func TestPrepareErrors(t *testing.T) {
	r := rand.New(rand.NewPCG(4, 4))
	m := shardtest.RandomMatrix(r, 50, 2)
	rootA, rootB := t.TempDir(), t.TempDir()
	annshardtest.WriteVectors(t, rootA, "v1", m)
	annshardtest.WriteVectors(t, rootA, "v2", m)
	annshardtest.WriteVectors(t, rootB, "v1", m)
	c := annshardtest.Dial(t, [][]string{{annshardtest.Start(t, rootA)}, {annshardtest.Start(t, rootB)}}, annshard.Options{})
	ctx := context.Background()
	cfg := shard.Config{Shards: 2, Mode: shard.Exact}
	if _, err := c.Prepare(ctx, "v1", 50, cfg); err != nil {
		t.Fatal(err)
	}
	// Shard 1's replica lacks v2.
	if _, err := c.Prepare(ctx, "v2", 50, cfg); err == nil {
		t.Fatal("a version one shard lacks was prepared")
	}
	for _, bad := range []shard.Config{{Shards: 3, Mode: shard.Exact}, {Shards: 2, Mode: "ivf"}} {
		if _, err := c.Prepare(ctx, "v1", 50, bad); err == nil {
			t.Errorf("%+v accepted", bad)
		}
	}
}

func TestParseAddrs(t *testing.T) {
	got, err := annshard.ParseAddrs("a:1,b:1;c:1")
	if want := [][]string{{"a:1", "b:1"}, {"c:1"}}; err != nil || !slices.EqualFunc(got, want, slices.Equal) {
		t.Fatalf("got %v, %v", got, err)
	}
	for _, bad := range []string{"", "a:1;", "a:1,,b:1", ";a:1"} {
		if _, err := annshard.ParseAddrs(bad); err == nil {
			t.Errorf("%q accepted", bad)
		}
	}
}

// slowOnce is a server interceptor shared by the replicas of a shard that
// makes every search slow, as on busy replicas: the first call of a search
// to arrive takes service, and a later call of the same search, a hedge,
// never answers by itself: it waits for its caller to cancel it. Searches
// are told apart by their k.
type slowOnce struct {
	service time.Duration

	mu        sync.Mutex
	arrived   map[int32]bool
	hedges    int // calls that arrived second
	cancelled int // of those, the ones whose caller cancelled them
}

func (s *slowOnce) intercept(ctx context.Context, req any, _ *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
	search, ok := req.(*annshardpb.SearchRequest)
	if !ok {
		return handler(ctx, req)
	}
	s.mu.Lock()
	first := !s.arrived[search.GetK()]
	s.arrived[search.GetK()] = true
	if !first {
		s.hedges++
	}
	s.mu.Unlock()
	if first {
		time.Sleep(s.service)
		return handler(ctx, req)
	}
	select {
	case <-ctx.Done():
		if errors.Is(ctx.Err(), context.Canceled) {
			s.mu.Lock()
			s.cancelled++
			s.mu.Unlock()
		}
	case <-time.After(30 * time.Second):
	}
	return nil, status.FromContextError(ctx.Err()).Err()
}

// checkProperty13 runs n searches, ten at a time, against a shard of two
// replicas that both take longer than the hedge delay, through search,
// which is given the two replicas' addresses. Every search must succeed,
// the hedges must stay within 10 + n/10, some must be sent, and every
// hedge, which loses by construction, must see its caller cancel it.
func checkProperty13(t *testing.T, n int, client func(addrs []string) func(ctx context.Context, k int) error) error {
	r := rand.New(rand.NewPCG(13, 13))
	root := t.TempDir()
	annshardtest.WriteVectors(t, root, "v1", shardtest.RandomMatrix(r, n+50, 2))
	slow := &slowOnce{service: 60 * time.Millisecond, arrived: map[int32]bool{}}
	opt := grpc.UnaryInterceptor(slow.intercept)
	search := client([]string{annshardtest.Start(t, root, opt), annshardtest.Start(t, root, opt)})
	errs := make([]error, n)
	var wg sync.WaitGroup
	sem := make(chan struct{}, 10)
	for j := range n {
		sem <- struct{}{}
		wg.Go(func() {
			errs[j] = search(context.Background(), 1+j)
			<-sem
		})
	}
	wg.Wait()
	if err := errors.Join(errs...); err != nil {
		return err
	}
	// A caller has its answer before the replica of the losing call learns
	// of the cancellation, so that count may still be rising.
	for deadline := time.Now().Add(10 * time.Second); ; time.Sleep(5 * time.Millisecond) {
		slow.mu.Lock()
		hedges, cancelled := slow.hedges, slow.cancelled
		slow.mu.Unlock()
		switch {
		case hedges == 0 || hedges > 10+n/10:
			return fmt.Errorf("%d hedges for %d slow searches, want 1 to %d", hedges, n, 10+n/10)
		case cancelled == hedges:
			return nil
		case time.Now().After(deadline):
			return fmt.Errorf("%d of %d hedges saw their caller cancel them", cancelled, hedges)
		}
	}
}

const property13HedgeDelay = 10 * time.Millisecond

// Property 13: with every replica slower than the hedge delay, n searches
// send at most 10 + n/10 hedges, and the call that loses is cancelled.
func TestProperty13HedgesStayWithinBudget(t *testing.T) {
	err := checkProperty13(t, 150, func(addrs []string) func(ctx context.Context, k int) error {
		c := annshardtest.Dial(t, [][]string{addrs}, annshard.Options{HedgeDelay: property13HedgeDelay})
		s, err := c.Prepare(context.Background(), "v1", 200, shard.Config{Shards: 1, Mode: shard.Exact})
		if err != nil {
			t.Fatal(err)
		}
		return func(ctx context.Context, k int) error {
			_, err := s.Search(ctx, 0, []float32{1, 1}, k, nil)
			return err
		}
	})
	if err != nil {
		t.Fatal(err)
	}
}

// The broken variant hedges as the client does, without a budget: every
// slow search is sent twice.
func TestProperty13CatchesBrokenVariant(t *testing.T) {
	err := checkProperty13(t, 150, func(addrs []string) func(ctx context.Context, k int) error {
		ix := exactIndex("v1", 0, 1)
		var replicas []annshardpb.AnnShardClient
		for _, addr := range addrs {
			conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { conn.Close() }) //nolint:errcheck // the test is over
			c := annshardpb.NewAnnShardClient(conn)
			if _, err := c.Prepare(context.Background(), &annshardpb.PrepareRequest{Index: ix}); err != nil {
				t.Fatal(err)
			}
			replicas = append(replicas, c)
		}
		return func(ctx context.Context, k int) error {
			ctx, cancel := context.WithCancel(ctx)
			defer cancel()
			answers := make(chan error, 2)
			ask := func(c annshardpb.AnnShardClient) {
				_, err := c.Search(ctx, &annshardpb.SearchRequest{Index: ix, Query: []float32{1, 1}, K: int32(k)})
				answers <- err
			}
			go ask(replicas[k%2])
			select {
			case err := <-answers:
				return err
			case <-time.After(property13HedgeDelay):
			}
			go ask(replicas[(k+1)%2])
			err := <-answers
			cancel()
			if loser := <-answers; loser == nil {
				return nil
			}
			return err
		}
	})
	if err == nil {
		t.Fatal("the check passed a client that hedges every slow search")
	}
	t.Log(err)
}

// A replica that goes away while it works on searches costs none of them:
// each is sent to the other replica and answered as on a healthy shard.
func TestReplicaDiesDuringSearches(t *testing.T) {
	r := rand.New(rand.NewPCG(5, 5))
	m := shardtest.RandomMatrix(r, 200, 2)
	root := t.TempDir()
	annshardtest.WriteVectors(t, root, "v1", m)
	cfg := shard.Config{Shards: 1, Mode: shard.Exact}
	// The replica that dies holds every search it gets until it is gone.
	const searches = 8
	held := make(chan struct{}, 2*searches)
	hold := grpc.UnaryInterceptor(func(ctx context.Context, req any, _ *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		if _, ok := req.(*annshardpb.SearchRequest); ok {
			held <- struct{}{}
			<-ctx.Done()
		}
		return handler(ctx, req)
	})
	dying := infratest.NewProxy(t, annshardtest.Start(t, root, hold))
	c := annshardtest.Dial(t, [][]string{{dying.Addr(), annshardtest.Start(t, root)}}, annshard.Options{})
	s, err := c.Prepare(context.Background(), "v1", 200, cfg)
	if err != nil {
		t.Fatal(err)
	}
	local, err := shard.Build(m, cfg)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	got, errs := make([][]vec.Hit, searches), make([]error, searches)
	var wg sync.WaitGroup
	for j := range searches {
		wg.Go(func() { got[j], errs[j] = s.Search(ctx, 0, []float32{1, float32(j)}, 20, nil) })
	}
	// The replicas are asked in turn, so some of the searches are on the
	// dying one; once it holds one, it goes.
	select {
	case <-held:
	case <-ctx.Done():
		t.Fatal("no search reached the replica that dies")
	}
	dying.Refuse()
	wg.Wait()
	for j := range searches {
		if errs[j] != nil {
			t.Fatalf("search %d: %v", j, errs[j])
		}
		want, err := local.Search(ctx, 0, []float32{1, float32(j)}, 20, nil)
		if err != nil {
			t.Fatal(err)
		}
		if !slices.Equal(got[j], want) {
			t.Fatalf("search %d:\n got %v\nwant %v", j, got[j], want)
		}
	}
}

// holds reports whether the replica at addr answers a search of shard 0 of
// one, for version, in exact mode.
func holds(t *testing.T, addr, version string) bool {
	t.Helper()
	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close() //nolint:errcheck // a probe
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	_, err = annshardpb.NewAnnShardClient(conn).Search(ctx, &annshardpb.SearchRequest{Index: exactIndex(version, 0, 1), Query: []float32{1, 1}, K: 1})
	return err == nil
}

// searchUntil searches through s, requiring every answer to equal the
// in-process one, until done returns true; it fails after 30 s.
func searchUntil(t *testing.T, s *annshard.Shards, m vec.Matrix, done func() bool) {
	t.Helper()
	local, err := shard.Build(m, s.Config())
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	for deadline := time.Now().Add(30 * time.Second); ; time.Sleep(5 * time.Millisecond) {
		q := []float32{1, float32(time.Now().UnixNano() % 7)}
		got, err := s.Search(ctx, 0, q, 10, nil)
		if err != nil {
			t.Fatal(err)
		}
		want, err := local.Search(ctx, 0, q, 10, nil)
		if err != nil {
			t.Fatal(err)
		}
		if !slices.Equal(got, want) {
			t.Fatalf("got %v\nwant %v", got, want)
		}
		if done() {
			return
		}
		if time.Now().After(deadline) {
			t.Fatal("still waiting after 30 s")
		}
	}
}

// A version is prepared once every shard has a replica that holds it. A
// replica that could not prepare it costs no search, and holds it soon
// after it can: the first search that finds it lacking has it prepare.
func TestReplicaCatchesUp(t *testing.T) {
	r := rand.New(rand.NewPCG(6, 6))
	m := shardtest.RandomMatrix(r, 100, 2)
	full, late := t.TempDir(), t.TempDir()
	annshardtest.WriteVectors(t, full, "v1", m)
	behind := annshardtest.Start(t, late)
	c := annshardtest.Dial(t, [][]string{{behind, annshardtest.Start(t, full)}}, annshard.Options{})
	s, err := c.Prepare(context.Background(), "v1", 100, shard.Config{Shards: 1, Mode: shard.Exact})
	if err != nil {
		t.Fatal(err)
	}
	if holds(t, behind, "v1") {
		t.Fatal("the replica without the bundle holds its index")
	}
	annshardtest.WriteVectors(t, late, "v1", m)
	searchUntil(t, s, m, func() bool { return holds(t, behind, "v1") })
}

// The client tells a replica which version the gateway serves, when it
// prepares a version and when it has a replica catch up, and the replica
// keeps that version although another was named later (D043).
func TestReplicasKeepTheServingVersion(t *testing.T) {
	r := rand.New(rand.NewPCG(17, 17))
	m := shardtest.RandomMatrix(r, 100, 2)
	rootA, rootB := t.TempDir(), t.TempDir()
	for _, v := range []string{"v1", "v3", "v4"} {
		annshardtest.WriteVectors(t, rootA, v, m)
	}
	for _, v := range []string{"v1", "v2"} {
		annshardtest.WriteVectors(t, rootB, v, m)
	}
	a := annshardtest.Start(t, rootA)
	c := annshardtest.Dial(t, [][]string{{a, annshardtest.Start(t, rootB)}}, annshard.Options{Serving: func() string { return "v1" }})
	ctx := context.Background()
	cfg := shard.Config{Shards: 1, Mode: shard.Exact}
	var s2 *annshard.Shards
	for _, v := range []string{"v1", "v3", "v2"} { // a holds v1 and v3, v3 named last
		s, err := c.Prepare(ctx, v, 100, cfg)
		if err != nil {
			t.Fatal(err)
		}
		s2 = s
	}
	// Catching up on v2, a has three versions and drops v3, not v1.
	annshardtest.WriteVectors(t, rootA, "v2", m)
	searchUntil(t, s2, m, func() bool { return holds(t, a, "v2") })
	if holds(t, a, "v3") || !holds(t, a, "v1") {
		t.Fatal("catching up, the replica dropped the version served")
	}
	// Preparing v4, it drops v2, which the probe below names last, not v1.
	if !holds(t, a, "v2") {
		t.Fatal("the replica lost v2")
	}
	if _, err := c.Prepare(ctx, "v4", 100, cfg); err != nil {
		t.Fatal(err)
	}
	if holds(t, a, "v2") || !holds(t, a, "v1") {
		t.Fatal("preparing a version, the replica dropped the version served")
	}
}

// A replica that restarts has no index. The searches go on, answered by
// the other replica, and the restarted one is prepared again by the first
// search that reaches it.
func TestReplicaRestartsEmpty(t *testing.T) {
	r := rand.New(rand.NewPCG(7, 7))
	m := shardtest.RandomMatrix(r, 100, 2)
	root := t.TempDir()
	annshardtest.WriteVectors(t, root, "v1", m)
	addr, stop := annshardtest.StartOn(t, "127.0.0.1:0", root)
	c := annshardtest.Dial(t, [][]string{{addr, annshardtest.Start(t, root)}}, annshard.Options{})
	s, err := c.Prepare(context.Background(), "v1", 100, shard.Config{Shards: 1, Mode: shard.Exact})
	if err != nil {
		t.Fatal(err)
	}
	if !holds(t, addr, "v1") {
		t.Fatal("the replica does not hold the index it prepared")
	}
	stop()
	down := true
	searchUntil(t, s, m, func() bool {
		if down { // a few searches with the replica gone, then it is back, empty
			annshardtest.StartOn(t, addr, root)
			down = false
			return false
		}
		return holds(t, addr, "v1")
	})
}

// No shard may be left without a replica that holds the version.
func TestPrepareNeedsAReplicaPerShard(t *testing.T) {
	r := rand.New(rand.NewPCG(8, 8))
	m := shardtest.RandomMatrix(r, 100, 2)
	full, empty := t.TempDir(), t.TempDir()
	annshardtest.WriteVectors(t, full, "v1", m)
	has, lacks := func() string { return annshardtest.Start(t, full) }, func() string { return annshardtest.Start(t, empty) }
	cfg := shard.Config{Shards: 2, Mode: shard.Exact}
	ctx := context.Background()
	if _, err := annshardtest.Dial(t, [][]string{{has(), lacks()}, {lacks(), has()}}, annshard.Options{}).Prepare(ctx, "v1", 100, cfg); err != nil {
		t.Fatal(err)
	}
	if _, err := annshardtest.Dial(t, [][]string{{has(), has()}, {lacks(), lacks()}}, annshard.Options{}).Prepare(ctx, "v1", 100, cfg); err == nil {
		t.Fatal("prepared with a shard none of whose replicas holds the version")
	}
}

// checkSilentReplica connects to a replica with the dial options opts,
// makes the replica go silent with the connection up, and returns an error
// unless the connection leaves the ready state within 18 s: the keepalive's
// 10 s without a read and 3 s for the ping's answer, and 5 s to spare.
func checkSilentReplica(t *testing.T, opts []grpc.DialOption) error {
	proxy := infratest.NewProxy(t, annshardtest.Start(t, t.TempDir()))
	conn, err := grpc.NewClient(proxy.Addr(), opts...)
	if err != nil {
		return err
	}
	defer conn.Close() //nolint:errcheck // the test is over
	ctx, cancel := context.WithTimeout(context.Background(), 18*time.Second)
	defer cancel()
	// Any answer, here a refusal, shows the connection is up.
	if _, err := annshardpb.NewAnnShardClient(conn).Search(ctx, &annshardpb.SearchRequest{}); status.Code(err) != codes.InvalidArgument {
		return fmt.Errorf("before the replica went silent: %w", err)
	}
	proxy.Blackhole()
	if conn.WaitForStateChange(ctx, connectivity.Ready) {
		return nil
	}
	return errors.New("the connection to a silent replica is still ready after 18 s")
}

// A replica that goes silent while its connection stays up is no longer a
// ready replica once the keepalive has pinged it in vain.
func TestSilentReplicaLeavesReady(t *testing.T) {
	t.Parallel()
	if err := checkSilentReplica(t, annshard.DialOptions()); err != nil {
		t.Fatal(err)
	}
}

// The broken variant connects without the keepalive.
func TestSilentReplicaCatchesBrokenVariant(t *testing.T) {
	t.Parallel()
	if err := checkSilentReplica(t, []grpc.DialOption{grpc.WithTransportCredentials(insecure.NewCredentials())}); err == nil {
		t.Fatal("the check passed a connection without keepalive")
	}
}

// hedgeCalls returns the number of searches of c that reached the hedge
// delay, hedged or denied a hedge.
func hedgeCalls(t *testing.T, c *annshard.Client) float64 {
	t.Helper()
	reg := prometheus.NewRegistry()
	reg.MustRegister(c.Collectors()...)
	families, err := reg.Gather()
	if err != nil {
		t.Fatal(err)
	}
	var n float64
	for _, f := range families {
		if f.GetName() == "recserve_shard_hedges_total" {
			for _, m := range f.GetMetric() {
				n += m.GetCounter().GetValue()
			}
		}
	}
	return n
}

// Once the keepalive has taken a silent replica out of the ready ones, its
// shard is served by the other replica alone: 40 searches, more than the
// hedge budget could cover, all succeed, and none waits for a hedge.
func TestSilentReplicaStopsCostingHedges(t *testing.T) {
	t.Parallel()
	r := rand.New(rand.NewPCG(9, 9))
	m := shardtest.RandomMatrix(r, 100, 2)
	root := t.TempDir()
	annshardtest.WriteVectors(t, root, "v1", m)
	silent := infratest.NewProxy(t, annshardtest.Start(t, root))
	c := annshardtest.Dial(t, [][]string{{silent.Addr(), annshardtest.Start(t, root)}}, annshard.Options{HedgeDelay: 20 * time.Millisecond})
	cfg := shard.Config{Shards: 1, Mode: shard.Exact}
	s, err := c.Prepare(context.Background(), "v1", 100, cfg)
	if err != nil {
		t.Fatal(err)
	}
	local, err := shard.Build(m, cfg)
	if err != nil {
		t.Fatal(err)
	}
	silent.Blackhole()
	time.Sleep(16 * time.Second) // the keepalive's 10 s and 3 s, and 3 s to spare
	before := hedgeCalls(t, c)
	for j := range 40 {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		q := []float32{1, float32(j)}
		got, err := s.Search(ctx, 0, q, 10, nil)
		cancel()
		if err != nil {
			t.Fatalf("search %d: %v", j, err)
		}
		want, err := local.Search(context.Background(), 0, q, 10, nil)
		if err != nil {
			t.Fatal(err)
		}
		if !slices.Equal(got, want) {
			t.Fatalf("search %d:\n got %v\nwant %v", j, got, want)
		}
	}
	if after := hedgeCalls(t, c); after != before {
		t.Fatalf("%v of 40 searches reached the hedge delay", after-before)
	}
}
