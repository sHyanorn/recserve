package annshard_test

import (
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"slices"
	"sync"
	"testing"
	"time"

	"google.golang.org/grpc"
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
	s, err := c.Prepare(context.Background(), "v1", shard.Config{Shards: 1, Mode: shard.Exact})
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
	if _, err := c.Prepare(ctx, "v1", cfg); err != nil {
		t.Fatal(err)
	}
	// Shard 1's replica lacks v2.
	if _, err := c.Prepare(ctx, "v2", cfg); err == nil {
		t.Fatal("a version one shard lacks was prepared")
	}
	for _, bad := range []shard.Config{{Shards: 3, Mode: shard.Exact}, {Shards: 2, Mode: "ivf"}} {
		if _, err := c.Prepare(ctx, "v1", bad); err == nil {
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
		s, err := c.Prepare(context.Background(), "v1", shard.Config{Shards: 1, Mode: shard.Exact})
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
	s, err := c.Prepare(context.Background(), "v1", cfg)
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
