package annshard_test

import (
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"sync"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"

	"github.com/sHyanorn/recserve/internal/annshard"
	"github.com/sHyanorn/recserve/internal/annshard/annshardpb"
	"github.com/sHyanorn/recserve/internal/annshard/annshardtest"
	"github.com/sHyanorn/recserve/internal/shard"
	"github.com/sHyanorn/recserve/internal/shard/shardtest"
)

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
// which is given the two replicas' addresses and the version they hold.
// Every search must succeed, the hedges must stay within 10 + n/10, some
// must be sent, and every hedge, which loses by construction, must see its
// caller cancel it.
func checkProperty13(t *testing.T, n int, client func(addrs []string, version string) func(ctx context.Context, k int) error) error {
	r := rand.New(rand.NewPCG(13, 13))
	root := t.TempDir()
	version := annshardtest.WriteVectors(t, root, shardtest.RandomMatrix(r, n+50, 2))
	slow := &slowOnce{service: 60 * time.Millisecond, arrived: map[int32]bool{}}
	opt := grpc.UnaryInterceptor(slow.intercept)
	search := client([]string{annshardtest.Start(t, root, opt), annshardtest.Start(t, root, opt)}, version)
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
	err := checkProperty13(t, 150, func(addrs []string, version string) func(ctx context.Context, k int) error {
		c := annshardtest.Dial(t, [][]string{addrs}, annshard.Options{Config: shard.Config{Shards: 1, Mode: shard.Exact}, HedgeDelay: property13HedgeDelay})
		s, err := c.Prepare(context.Background(), version, 200)
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
	err := checkProperty13(t, 150, func(addrs []string, version string) func(ctx context.Context, k int) error {
		ix := exactIndex(version, 0, 1)
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
