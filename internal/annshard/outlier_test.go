package annshard_test

import (
	"context"
	"fmt"
	"maps"
	"math/rand/v2"
	"sync/atomic"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/status"

	"github.com/sHyanorn/recserve/internal/annshard"
	"github.com/sHyanorn/recserve/internal/annshard/annshardpb"
	"github.com/sHyanorn/recserve/internal/annshard/annshardtest"
	"github.com/sHyanorn/recserve/internal/shard"
	"github.com/sHyanorn/recserve/internal/shard/shardtest"
)

// How a scripted replica handles a search.
const (
	answer int32 = iota
	hang         // until its caller cancels it or the deadline ends it
	fail
	crawl // answers crawlFor after it arrived, unless cancelled or ended first
)

// crawlFor is how long a crawling replica takes to answer a search: past
// the hedge delay of TestNoHedgeToTakenOutReplica, and well within
// outlierDeadline.
const crawlFor = 15 * time.Millisecond

// scripted returns a server interceptor that handles every search as mode,
// which the test may change at any time, says.
func scripted(mode *atomic.Int32) grpc.ServerOption {
	return grpc.UnaryInterceptor(func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		switch mode.Load() {
		case hang:
			return hangSearches(ctx, req, info, handler)
		case fail:
			return failSearches(ctx, req, info, handler)
		case crawl:
			return crawlSearches(ctx, req, info, handler)
		}
		return handler(ctx, req)
	})
}

// crawlSearches is a server interceptor that answers every search crawlFor
// after it arrived, unless its caller cancels it or the deadline ends it
// first.
func crawlSearches(ctx context.Context, req any, _ *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
	if _, ok := req.(*annshardpb.SearchRequest); ok {
		select {
		case <-time.After(crawlFor):
		case <-ctx.Done():
			return nil, status.FromContextError(ctx.Err()).Err()
		}
	}
	return handler(ctx, req)
}

// outlierDeadline is the deadline of the searches of the outlier tests: far
// above a search's time on loopback, and short, for many to end at it.
const outlierDeadline = 50 * time.Millisecond

// outlierShard is a shard of two replicas, a and b, that handle searches as
// their modes say, and a client of it.
type outlierShard struct {
	t            *testing.T
	c            *annshard.Client
	s            *annshard.Shards
	a, b         string       // the replicas' addresses
	aMode, bMode atomic.Int32 // answer at first
	stopB        func()       // stops b before the test ends, cutting its connections
}

// newOutlierShard starts the replicas of an outlierShard and prepares them
// through a client that hedges after hedgeDelay, never if 0.
func newOutlierShard(t *testing.T, hedgeDelay time.Duration) *outlierShard {
	t.Helper()
	r := rand.New(rand.NewPCG(41, 41))
	root := t.TempDir()
	version := annshardtest.WriteVectors(t, root, shardtest.RandomMatrix(r, 50, 2))
	o := &outlierShard{t: t}
	o.a = annshardtest.Start(t, root, scripted(&o.aMode))
	o.b, o.stopB = annshardtest.StartOn(t, "127.0.0.1:0", root, scripted(&o.bMode))
	o.c = annshardtest.Dial(t, [][]string{{o.a, o.b}}, annshard.Options{Config: shard.Config{Shards: 1, Mode: shard.Exact}, HedgeDelay: hedgeDelay})
	var err error
	if o.s, err = o.c.Prepare(context.Background(), version, 50); err != nil {
		t.Fatal(err)
	}
	return o
}

// search runs one search of the shard within timeout.
func (o *outlierShard) search(timeout time.Duration) error {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	_, err := o.s.Search(ctx, 0, []float32{1, 1}, 5, nil)
	return err
}

// calls returns the calls to the replicas by "address result".
func (o *outlierShard) calls() map[string]float64 {
	o.t.Helper()
	return counts(o.t, o.c, "recserve_shard_calls_total", "replica", "result")
}

// callsTo returns the calls to the replica at addr in calls, as calls
// gives them, whatever their result.
func callsTo(calls map[string]float64, addr string) float64 {
	var n float64
	for _, result := range []string{"answered", "failed", "deadline", "cancelled"} {
		n += calls[addr+" "+result]
	}
	return n
}

// ejections returns the ejections by replica address.
func (o *outlierShard) ejections() map[string]float64 {
	o.t.Helper()
	return counts(o.t, o.c, "recserve_shard_ejections_total", "replica")
}

// until searches within outlierDeadline until n more calls to the replica
// at addr have ended as result, and fails the test if that takes longer
// than within. The searches' own results are not checked.
func (o *outlierShard) until(addr, result string, n float64, within time.Duration) {
	o.t.Helper()
	key := addr + " " + result
	want := o.calls()[key] + n
	for end := time.Now().Add(within); o.calls()[key] < want; {
		if time.Now().After(end) {
			o.t.Fatalf("not %v more calls to %s %s within %v: %v", n, addr, result, within, o.calls())
		}
		o.search(outlierDeadline) //nolint:errcheck // the calls' counts are the test
	}
}

// Every replica has its series of recserve_shard_ejections_total from the
// start, at 0, as it has those of its calls: a scrape shows that the client
// takes replicas out also before it has taken one out.
func TestEjectionsCountedFromDial(t *testing.T) {
	addrs := annshardtest.StartAll(t, t.TempDir(), 2, 2)
	c := annshardtest.Dial(t, addrs, annshard.Options{Config: shard.Config{Shards: 2, Mode: shard.Exact}})
	want := map[string]float64{}
	for i, replicas := range addrs {
		for _, addr := range replicas {
			want[fmt.Sprintf("%d %s", i, addr)] = 0
		}
	}
	if got := counts(t, c, "recserve_shard_ejections_total", "shard", "replica"); !maps.Equal(got, want) {
		t.Fatalf("ejections %v, want %v", got, want)
	}
}

// A replica whose searches hang is taken out of its shard's selection once
// five of its calls in a row have ended at the deadline, and the ejection
// is counted: the 50 searches that follow, well within the second before it
// is asked again, are all answered by the other replica, none left out.
func TestHungReplicaIsTakenOut(t *testing.T) {
	o := newOutlierShard(t, 0)
	o.aMode.Store(hang)
	o.until(o.a, "deadline", 5, 2*time.Second)
	if got := o.ejections(); got[o.a] != 1 || got[o.b] != 0 {
		t.Fatalf("ejections %v after five calls to %s ended at the deadline, want one of it", got, o.a)
	}
	before := o.calls()
	for j := range 50 {
		if err := o.search(outlierDeadline); err != nil {
			t.Fatalf("search %d after the ejection: %v", j, err)
		}
	}
	after := o.calls()
	for _, result := range []string{"answered", "failed", "deadline", "cancelled"} {
		if n := after[o.a+" "+result] - before[o.a+" "+result]; n != 0 {
			t.Fatalf("%v calls to the replica taken out ended %s", n, result)
		}
	}
	if n := after[o.b+" answered"] - before[o.b+" answered"]; n != 50 {
		t.Fatalf("%v of 50 searches answered by %s, want all", n, o.b)
	}
}

// A replica taken out is asked first by one search, a probe, a second after
// it was taken out: a probe that ends at the deadline leaves it out another
// second, and one that is answered brings it back, with about half of the
// shard's calls again.
func TestTakenOutReplicaComesBack(t *testing.T) {
	o := newOutlierShard(t, 0)
	o.aMode.Store(hang)
	o.until(o.a, "deadline", 5, 2*time.Second)
	ejected := time.Now()
	o.until(o.a, "deadline", 1, 3*time.Second)
	if d := time.Since(ejected); d < 900*time.Millisecond {
		t.Fatalf("%s asked again %v after it was taken out, want a second", o.a, d)
	}
	o.aMode.Store(answer)
	probed := time.Now()
	o.until(o.a, "answered", 1, 3*time.Second)
	// The next probe is due a second after the last one was sent, so about
	// 950 ms after that one ended at the deadline.
	if d := time.Since(probed); d < 800*time.Millisecond {
		t.Fatalf("%s asked again %v after a probe that ended at the deadline, want a second", o.a, d)
	}
	before := o.calls()
	for j := range 40 {
		if err := o.search(outlierDeadline); err != nil {
			t.Fatalf("search %d after the replica came back: %v", j, err)
		}
	}
	if n := o.calls()[o.a+" answered"] - before[o.a+" answered"]; n < 16 || n > 24 {
		t.Fatalf("%v of 40 searches answered by %s once back, want about half", n, o.a)
	}
}

// While every replica of a shard hangs, none is taken out, and the shard's
// searches keep going to each in turn: a slow shard is the hedge budget's
// case. b answers once after a's run began and only then hangs, so what
// keeps a in is b's calls that ended at the deadline since, not an answer
// from before the run. Once one of them answers again, the other is taken
// out.
func TestNoReplicaTakenOutWhileAllHang(t *testing.T) {
	o := newOutlierShard(t, 0)
	o.aMode.Store(hang)
	o.until(o.a, "deadline", 1, 2*time.Second)
	o.until(o.b, "answered", 1, 2*time.Second)
	o.bMode.Store(hang)
	for range 20 {
		o.search(outlierDeadline) //nolint:errcheck // the calls' counts are the test
	}
	if got := o.calls(); got[o.a+" deadline"] < 8 || got[o.b+" deadline"] < 8 {
		t.Fatalf("calls %v, want each replica asked first by about half of 20 searches", got)
	}
	if got := o.ejections(); len(got) != 2 || got[o.a] != 0 || got[o.b] != 0 {
		t.Fatalf("ejections %v with every replica hung, want none", got)
	}
	o.bMode.Store(answer)
	o.until(o.b, "answered", 1, 2*time.Second)
	o.until(o.a, "deadline", 1, 2*time.Second)
	if got := o.ejections(); got[o.a] != 1 || got[o.b] != 0 {
		t.Fatalf("ejections %v once %s answered again, want one of %s", got, o.b, o.a)
	}
}

// A replica is taken out only when the other has answered since the first
// of its run of calls ended at the deadline: one that answered before the
// run and since only failed has not stood in for it. Once the other answers,
// the next call that ends at the deadline takes it out.
func TestCoverMustAnswerDuringTheRun(t *testing.T) {
	o := newOutlierShard(t, 0)
	o.until(o.b, "answered", 2, 2*time.Second)
	o.aMode.Store(hang)
	o.bMode.Store(fail)
	o.until(o.a, "deadline", 8, 2*time.Second)
	if got := o.ejections(); got[o.a] != 0 {
		t.Fatalf("ejections %v with %s failing every search since %s's run began, want none", got, o.b, o.a)
	}
	o.bMode.Store(answer)
	o.until(o.b, "answered", 1, 2*time.Second)
	o.until(o.a, "deadline", 1, 2*time.Second)
	if got := o.ejections(); got[o.a] != 1 || got[o.b] != 0 {
		t.Fatalf("ejections %v once %s answered again, want one of %s", got, o.b, o.a)
	}
}

// A failure, which the search takes to the next replica at once, counts
// neither way: a replica that fails every search stays in the selection,
// and failures between calls that end at the deadline neither clear their
// count nor add to it.
func TestFailuresCountNeitherWay(t *testing.T) {
	o := newOutlierShard(t, 0)
	o.aMode.Store(hang)
	o.until(o.a, "deadline", 4, 2*time.Second)
	o.aMode.Store(fail)
	before := o.calls()
	for j := range 20 {
		if err := o.search(outlierDeadline); err != nil {
			t.Fatalf("search %d: %v", j, err)
		}
	}
	if n := o.calls()[o.a+" failed"] - before[o.a+" failed"]; n < 8 {
		t.Fatalf("%v of 20 searches asked %s first, want about half", n, o.a)
	}
	if got := o.ejections(); got[o.a] != 0 {
		t.Fatalf("ejections %v after four calls ended at the deadline and failures, want none", got)
	}
	o.aMode.Store(hang)
	o.until(o.a, "deadline", 1, 2*time.Second)
	if got := o.ejections(); got[o.a] != 1 {
		t.Fatalf("ejections %v after the fifth call in a row ended at the deadline, failures between, want one of %s", got, o.a)
	}
}

// A call cancelled because a hedge answered first counts neither way: a
// replica that always loses the race to its hedge stays in the selection,
// and lost races between calls that end at the deadline neither clear their
// count nor add to it. The hedge delay is above outlierDeadline, so the
// searches within that send no hedge.
func TestLostHedgesCountNeitherWay(t *testing.T) {
	o := newOutlierShard(t, 2*outlierDeadline)
	o.aMode.Store(hang)
	o.until(o.a, "deadline", 4, 2*time.Second)
	before := o.calls()
	for j := range 16 { // about 8 hedges, within the budget's reserve of 10
		if err := o.search(5 * time.Second); err != nil {
			t.Fatalf("search %d: %v", j, err)
		}
	}
	if n := o.calls()[o.a+" cancelled"] - before[o.a+" cancelled"]; n < 6 {
		t.Fatalf("%v of 16 searches asked %s first and had it lose to their hedge, want about half", n, o.a)
	}
	if got := o.ejections(); got[o.a] != 0 {
		t.Fatalf("ejections %v after four calls ended at the deadline and lost hedges, want none", got)
	}
	o.until(o.a, "deadline", 1, 2*time.Second)
	if got := o.ejections(); got[o.a] != 1 {
		t.Fatalf("ejections %v after the fifth call in a row ended at the deadline, lost hedges between, want one of %s", got, o.a)
	}
}

// An answer clears a replica's count: one whose calls end at the deadline
// four times in a row and then answer, over and over, stays in the
// selection, until five in a row take it out.
func TestAnswerClearsTheCount(t *testing.T) {
	o := newOutlierShard(t, 0)
	for range 3 {
		o.aMode.Store(hang)
		o.until(o.a, "deadline", 4, 2*time.Second)
		o.aMode.Store(answer)
		o.until(o.a, "answered", 1, 2*time.Second)
	}
	if got := o.ejections(); got[o.a] != 0 {
		t.Fatalf("ejections %v with an answer after every four calls that ended at the deadline, want none", got)
	}
	o.aMode.Store(hang)
	o.until(o.a, "deadline", 5, 2*time.Second)
	if got := o.ejections(); got[o.a] != 1 {
		t.Fatalf("ejections %v after five calls in a row ended at the deadline, want one of %s", got, o.a)
	}
}

// No hedge asks a replica taken out: once a hanging replica is out, the
// searches of its shard wait for the other replica, which takes longer than
// the hedge delay, and send the one taken out nothing but its probes, a
// second apart. The budget is not what holds the hedges back: the searches
// that took the replica out spent a few of its reserve of ten.
func TestNoHedgeToTakenOutReplica(t *testing.T) {
	o := newOutlierShard(t, 5*time.Millisecond)
	// With b failing at once, every search waits for a until the deadline:
	// one that asks b first goes on to a, and one that asks a first sends
	// its hedge to b, which fails.
	o.aMode.Store(hang)
	o.bMode.Store(fail)
	o.until(o.a, "deadline", 4, 2*time.Second)
	// b answers once during a's run, so it stands in for a; its failures
	// after that count neither way.
	o.bMode.Store(answer)
	o.until(o.b, "answered", 1, 2*time.Second)
	o.bMode.Store(fail)
	fifth := time.Now() // a is taken out after this, so probed no sooner than a second after it
	o.until(o.a, "deadline", 1, 2*time.Second)
	if got := o.ejections(); got[o.a] != 1 || got[o.b] != 0 {
		t.Fatalf("ejections %v after five calls to %s ended at the deadline, want one of it", got, o.a)
	}
	o.bMode.Store(crawl)
	before := o.calls()
	for j := range 20 {
		if err := o.search(outlierDeadline); err != nil {
			t.Fatalf("search %d after the ejection: %v", j, err)
		}
	}
	after := o.calls()
	took := time.Since(fifth)
	probes := float64(took / time.Second)
	if n := callsTo(after, o.a) - callsTo(before, o.a); n > probes {
		t.Fatalf("%v calls to %s, taken out, in %v from 20 searches slower than the hedge delay, want at most %v, its probes: %v", n, o.a, took, probes, after)
	}
	if n := after[o.b+" answered"] - before[o.b+" answered"]; n != 20 {
		t.Fatalf("%v of 20 searches answered by %s, want all", n, o.b)
	}
}

// A replica is not taken out while no other replica of its shard has a
// ready connection: a replica whose searches hang has its run begin, the
// other answers once and then its server stops, and the first has ten calls
// in a row end at the deadline and stays in, although the other is in the
// selection, its count is clear and it answered during the run.
func TestNoEjectionWhileOtherReplicaDown(t *testing.T) {
	o := newOutlierShard(t, 0)
	o.aMode.Store(hang)
	o.until(o.a, "deadline", 1, 2*time.Second)
	o.until(o.b, "answered", 1, 2*time.Second)
	if got := o.calls(); got[o.a+" deadline"] >= 5 {
		t.Fatalf("calls %v: %s's run reached five before %s stopped", got, o.a, o.b)
	}
	o.stopB()
	// Wait until the client's connection to b has left the ready state, for
	// good, as no server is left to take it: while both connections are
	// ready, of two searches in a row one asks b first, and b's failure then
	// counts a failover under b. a fails meanwhile, which neither adds to
	// its run nor clears it, and so the search fails over to b when it asks
	// a first; a call that b fails counts neither way either.
	o.aMode.Store(fail)
	failovers := func() float64 {
		return counts(t, o.c, "recserve_shard_failovers_total", "replica")[o.b]
	}
	for end, without := time.Now().Add(5*time.Second), 0; without < 2; {
		if time.Now().After(end) {
			t.Fatalf("searches still ask %s first 5 s after it stopped: %v", o.b, o.calls())
		}
		before := failovers()
		o.search(outlierDeadline) //nolint:errcheck // both replicas fail it
		if failovers() == before {
			without++
		} else {
			without = 0
		}
	}
	if got := o.calls(); got[o.b+" deadline"] != 0 {
		t.Fatalf("calls %v: a call to the stopped %s ended at the deadline, want none, so that only its connection keeps %s in", got, o.b, o.a)
	}
	o.aMode.Store(hang)
	o.until(o.a, "deadline", 10, 3*time.Second)
	if got := o.ejections(); got[o.a] != 0 || got[o.b] != 0 {
		t.Fatalf("ejections %v with %s's connection down, want none", got, o.b)
	}
}
