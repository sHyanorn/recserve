package annshard_test

import (
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"slices"
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
	"github.com/sHyanorn/recserve/internal/infratest"
	"github.com/sHyanorn/recserve/internal/shard"
	"github.com/sHyanorn/recserve/internal/shard/shardtest"
)

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
