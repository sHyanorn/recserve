package annshard_test

import (
	"context"
	"errors"
	"math/rand/v2"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

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

// A replica's failure is not hidden by another replica's timeout: with one
// replica that fails every search and one that is silent, a search that
// asks the failing one first, and then waits for the silent one in vain,
// returns the failure and is no timeout (D029). One that asks the silent
// one first is a timeout: it asks no other.
func TestFailureIsNotHiddenByTimeout(t *testing.T) {
	r := rand.New(rand.NewPCG(15, 15))
	root := t.TempDir()
	version := annshardtest.WriteVectors(t, root, shardtest.RandomMatrix(r, 50, 2))
	failing := annshardtest.Start(t, root, grpc.UnaryInterceptor(func(ctx context.Context, req any, _ *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		if _, ok := req.(*annshardpb.SearchRequest); ok {
			return nil, status.Error(codes.Internal, "injected")
		}
		return handler(ctx, req)
	}))
	silent := infratest.NewProxy(t, annshardtest.Start(t, root))
	c := annshardtest.Dial(t, [][]string{{failing, silent.Addr()}}, annshard.Options{Config: shard.Config{Shards: 1, Mode: shard.Exact}})
	s, err := c.Prepare(context.Background(), version, 50)
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
