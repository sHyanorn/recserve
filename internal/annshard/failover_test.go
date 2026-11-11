package annshard_test

import (
	"context"
	"errors"
	"math/rand/v2"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/sHyanorn/recserve/internal/annshard"
	"github.com/sHyanorn/recserve/internal/annshard/annshardpb"
	"github.com/sHyanorn/recserve/internal/annshard/annshardtest"
	"github.com/sHyanorn/recserve/internal/infratest"
	"github.com/sHyanorn/recserve/internal/shard"
	"github.com/sHyanorn/recserve/internal/shard/shardtest"
	"github.com/sHyanorn/recserve/internal/vec"
)

// failSearches is a server interceptor that fails every search.
func failSearches(ctx context.Context, req any, _ *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
	if _, ok := req.(*annshardpb.SearchRequest); ok {
		return nil, status.Error(codes.Internal, "injected")
	}
	return handler(ctx, req)
}

// A search sent to the next replica because one failed is counted under
// the failed replica's address, as -shard-addrs gives it (D041).
func TestFailoverCountsTheFailedReplica(t *testing.T) {
	r := rand.New(rand.NewPCG(19, 19))
	root := t.TempDir()
	version := annshardtest.WriteVectors(t, root, shardtest.RandomMatrix(r, 50, 2))
	failing, healthy := annshardtest.Start(t, root, grpc.UnaryInterceptor(failSearches)), annshardtest.Start(t, root)
	c := annshardtest.Dial(t, [][]string{{failing, healthy}}, annshard.Options{Config: shard.Config{Shards: 1, Mode: shard.Exact}})
	s, err := c.Prepare(context.Background(), version, 50)
	if err != nil {
		t.Fatal(err)
	}
	for range 10 { // half of them ask the failing replica first
		if _, err := s.Search(context.Background(), 0, []float32{1, 1}, 5, nil); err != nil {
			t.Fatal(err)
		}
	}
	got := counts(t, c, "recserve_shard_failovers_total", "replica")
	if got[failing] == 0 || got[healthy] != 0 {
		t.Fatalf("failovers by replica %v, want them all under the failing one, %s", got, failing)
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
	version := annshardtest.WriteVectors(t, root, shardtest.RandomMatrix(r, 50, 2))
	var asked atomic.Int32 // searches that reached the failing replica
	failing := annshardtest.Start(t, root, grpc.UnaryInterceptor(func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		if _, ok := req.(*annshardpb.SearchRequest); ok {
			asked.Add(1)
		}
		return failSearches(ctx, req, info, handler)
	}))
	silent := infratest.NewProxy(t, annshardtest.Start(t, root))
	c := annshardtest.Dial(t, [][]string{{failing, silent.Addr()}}, annshard.Options{Config: shard.Config{Shards: 1, Mode: shard.Exact}})
	s, err := c.Prepare(context.Background(), version, 50)
	if err != nil {
		t.Fatal(err)
	}
	silent.Blackhole()
	// The ready replicas lead in turn, and gRPC may report a connection
	// ready a moment after Prepare's call on it returned, so the searches
	// go on until each replica has led one. A search that reached the
	// failing replica led with it.
	var failures, timeouts int
	for range 4 {
		before := asked.Load()
		ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
		_, err := s.Search(ctx, 0, []float32{1, 1}, 5, nil)
		cancel()
		ledFailing := asked.Load() > before
		switch {
		case err == nil:
			t.Fatal("a search succeeded with no replica that answers")
		case ledFailing && status.Code(err) == codes.Internal:
			failures++
		case !ledFailing && errors.Is(err, context.DeadlineExceeded):
			timeouts++
		default:
			t.Fatalf("search that asked the failing replica first %t: %v", ledFailing, err)
		}
		if failures > 0 && timeouts > 0 {
			return
		}
	}
	t.Fatalf("%d failures and %d timeouts, want one of each", failures, timeouts)
}

// A replica that returns an item the gateway's bundle does not hold, as a
// faulty one may, fails: its answer is an error that names the replica and
// the version, the shard's other replica answers, and with no other the
// search fails. The gateway has no vector for such an item.
func TestReplicaWithAnItemOutsideFails(t *testing.T) {
	r := rand.New(rand.NewPCG(16, 16))
	m := shardtest.RandomMatrix(r, 50, 2)
	root := t.TempDir()
	version := annshardtest.WriteVectors(t, root, m)
	stray := annshardtest.Start(t, root, grpc.UnaryInterceptor(func(ctx context.Context, req any, _ *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		resp, err := handler(ctx, req)
		if s, ok := resp.(*annshardpb.SearchResponse); ok && err == nil {
			s.Ids[0] = 51
		}
		return resp, err
	}))
	cfg := shard.Config{Shards: 1, Mode: shard.Exact}
	local, err := shard.Build(m, cfg)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	q := []float32{1, 1}

	alone, err := annshardtest.Dial(t, [][]string{{stray}}, annshard.Options{Config: cfg}).Prepare(ctx, version, 50)
	if err != nil {
		t.Fatal(err)
	}
	_, err = alone.Search(ctx, 0, q, 60, nil)
	if err == nil || !strings.Contains(err.Error(), stray) || !strings.Contains(err.Error(), version) {
		t.Fatalf("err = %v, want one that names the replica %s and the version", err, stray)
	}

	both, err := annshardtest.Dial(t, [][]string{{stray, annshardtest.Start(t, root)}}, annshard.Options{Config: cfg}).Prepare(ctx, version, 50)
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

// A replica that goes away while it works on searches costs none of them:
// each is sent to the other replica and answered as on a healthy shard.
func TestReplicaDiesDuringSearches(t *testing.T) {
	r := rand.New(rand.NewPCG(5, 5))
	m := shardtest.RandomMatrix(r, 200, 2)
	root := t.TempDir()
	version := annshardtest.WriteVectors(t, root, m)
	cfg := shard.Config{Shards: 1, Mode: shard.Exact}
	// The replica that dies, once holding, holds every search it gets until
	// it is gone.
	const searches = 8
	held := make(chan struct{}, 2*searches)
	var holding atomic.Bool
	var answered atomic.Int32
	hold := grpc.UnaryInterceptor(func(ctx context.Context, req any, _ *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		if _, ok := req.(*annshardpb.SearchRequest); ok {
			if holding.Load() {
				held <- struct{}{}
				<-ctx.Done()
			} else {
				answered.Add(1)
			}
		}
		return handler(ctx, req)
	})
	dying := infratest.NewProxy(t, annshardtest.Start(t, root, hold))
	c := annshardtest.Dial(t, [][]string{{dying.Addr(), annshardtest.Start(t, root)}}, annshard.Options{Config: cfg})
	s, err := c.Prepare(context.Background(), version, 200)
	if err != nil {
		t.Fatal(err)
	}
	local, err := shard.Build(m, cfg)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	// Only a replica whose connection gRPC reports ready leads a search,
	// and it may report one ready a moment after Prepare's call on it
	// returned: search until the dying one has led one.
	for answered.Load() == 0 {
		if _, err := s.Search(ctx, 0, []float32{1, 1}, 20, nil); err != nil {
			t.Fatal(err)
		}
	}
	holding.Store(true)
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

// recserve_shard_search_seconds observes every call to a replica, the
// failed ones and those sent after a failure included, from its send until
// it returns.
func TestSearchLatencyObservesEveryCall(t *testing.T) {
	const slow = 3 * time.Millisecond
	slowSearches := func(ctx context.Context, req any, _ *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		if _, ok := req.(*annshardpb.SearchRequest); ok {
			time.Sleep(slow)
		}
		return handler(ctx, req)
	}
	r := rand.New(rand.NewPCG(23, 23))
	root := t.TempDir()
	version := annshardtest.WriteVectors(t, root, shardtest.RandomMatrix(r, 50, 2))
	failing, healthy := annshardtest.Start(t, root, grpc.UnaryInterceptor(failSearches)), annshardtest.Start(t, root, grpc.UnaryInterceptor(slowSearches))
	c := annshardtest.Dial(t, [][]string{{failing, healthy}}, annshard.Options{Config: shard.Config{Shards: 1, Mode: shard.Exact}})
	s, err := c.Prepare(context.Background(), version, 50)
	if err != nil {
		t.Fatal(err)
	}
	const searches = 10
	for range searches {
		if _, err := s.Search(context.Background(), 0, []float32{1, 1}, 5, nil); err != nil {
			t.Fatal(err)
		}
	}
	failovers := counts(t, c, "recserve_shard_failovers_total", "shard")["0"]
	reg := prometheus.NewRegistry()
	reg.MustRegister(c.Collectors()...)
	families, err := reg.Gather()
	if err != nil {
		t.Fatal(err)
	}
	var calls uint64
	var sum float64
	for _, f := range families {
		for _, m := range f.GetMetric() {
			if f.GetName() == "recserve_shard_search_seconds" && m.GetLabel()[0].GetValue() == "0" {
				calls, sum = m.GetHistogram().GetSampleCount(), m.GetHistogram().GetSampleSum()
			}
		}
	}
	if failovers == 0 || calls != searches+uint64(failovers) {
		t.Fatalf("%d calls observed, want %d searches and %.0f failovers", calls, searches, failovers)
	}
	if sum < searches*slow.Seconds() {
		t.Fatalf("observed %.4f s in all, below %d answers of %v", sum, searches, slow)
	}
}
