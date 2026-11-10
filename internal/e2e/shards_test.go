package e2e

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"

	"github.com/sHyanorn/recserve/internal/annshard/annshardpb"
	"github.com/sHyanorn/recserve/internal/bundle/bundletest"
)

// shardResponse is what the shard tests read of a recommendation.
type shardResponse struct {
	Items        []struct{ ID string } `json:"items"`
	ModelVersion string                `json:"model_version"`
	Degraded     []string              `json:"degraded"`
	Coverage     *struct{ Shards, Answered int }
}

// probe calls the replica at addr on a connection of its own, within 2 s.
func probe(t *testing.T, addr string, call func(context.Context, *grpc.ClientConn) error) error {
	t.Helper()
	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close() //nolint:errcheck // a probe
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	return call(ctx, conn)
}

// health returns the status the replica at addr gives in its health
// service, UNKNOWN if it does not answer. It is SERVING while the replica
// holds the version the gateway serves (D044).
func health(t *testing.T, addr string) (st healthpb.HealthCheckResponse_ServingStatus) {
	t.Helper()
	if err := probe(t, addr, func(ctx context.Context, conn *grpc.ClientConn) error {
		h, err := healthpb.NewHealthClient(conn).Check(ctx, &healthpb.HealthCheckRequest{})
		st = h.GetStatus()
		return err
	}); err != nil {
		return healthpb.HealthCheckResponse_UNKNOWN
	}
	return st
}

// holdsIndex reports whether the replica at addr answers a search of the
// shard of 2 of the version: whether it has prepared that index.
func holdsIndex(t *testing.T, addr, version string, shard int) bool {
	t.Helper()
	return probe(t, addr, func(ctx context.Context, conn *grpc.ClientConn) error {
		_, err := annshardpb.NewAnnShardClient(conn).Search(ctx, &annshardpb.SearchRequest{
			Index: &annshardpb.Index{Version: version, Shard: int32(shard), Shards: 2, Mode: annshardpb.Mode_MODE_EXACT}, Query: []float32{1, 1}, K: 1,
		})
		return err
	}) == nil
}

// link puts the bundle in dir under root, in the directory named by its
// version, as a replica finds it, and returns the version.
func link(t *testing.T, root, dir string) string {
	t.Helper()
	v := bundletest.VersionOf(t, dir)
	if err := os.Symlink(dir, filepath.Join(root, v)); err != nil {
		t.Fatal(err)
	}
	return v
}

// tier is the gateway binary on four annshard processes over one bundle
// root, two shards of two replicas, with no result cache, so that every
// request searches the shards, and generous budgets: the tests are about
// replicas that fail or stop, not slow ones. All the tests' bundle
// versions have the same items and vectors, so every response is the same.
type tier struct {
	t       *testing.T
	root    string
	addrs   []string // replica i serves shard i/2
	procs   []*exec.Cmd
	gw      string // the gateway's base URL
	admin   string // the admin API's address
	healthy shardResponse
}

// newTier starts the replicas over root and the gateway on version first.
func newTier(t *testing.T, root, first string) *tier {
	t.Helper()
	x := &tier{t: t, root: root, addrs: []string{freeAddr(t), freeAddr(t), freeAddr(t), freeAddr(t)}}
	x.procs = make([]*exec.Cmd, len(x.addrs))
	for i := range x.addrs {
		x.replica(i)
	}
	addr, admin := freeAddr(t), freeAddr(t)
	start(t, "gateway", "-addr", addr, "-admin-addr", admin, "-bundle", filepath.Join(root, first), "-cache-size", "0",
		"-shard-addrs", x.addrs[0]+","+x.addrs[1]+";"+x.addrs[2]+","+x.addrs[3], "-budget-retrieval", "10s", "-budget-shard", "10s")
	x.gw, x.admin = "http://"+addr, admin
	eventually(t, 20*time.Second, "the gateway is not ready", func() bool { return up(x.gw + "/readyz") })
	// A user with a history has a user vector, so its requests search the shards.
	if status, _ := post(t, x.gw+"/v1/events", map[string]any{"events": []map[string]any{
		{"user_id": "u", "item_id": "item2", "ts": 1, "kind": "view"}, {"user_id": "u", "item_id": "item5", "ts": 2, "kind": "view"},
	}}); status != http.StatusAccepted {
		t.Fatalf("events: status %d", status)
	}
	x.healthy = x.ask()
	if len(x.healthy.Items) == 0 || x.healthy.Coverage == nil {
		t.Fatalf("healthy response %+v", x.healthy)
	}
	return x
}

// replica starts replica i and waits until it answers.
func (x *tier) replica(i int) {
	x.t.Helper()
	x.procs[i] = start(x.t, "annshard", "-addr", x.addrs[i], "-bundles", x.root)
	eventually(x.t, 20*time.Second, "the replica does not answer", func() bool {
		return health(x.t, x.addrs[i]) != healthpb.HealthCheckResponse_UNKNOWN
	})
}

func (x *tier) ask() shardResponse {
	x.t.Helper()
	resp, err := http.Post(x.gw+"/v1/recommend", "application/json", strings.NewReader(`{"user_id":"u","k":5}`))
	if err != nil {
		x.t.Fatal(err)
	}
	defer resp.Body.Close() //nolint:errcheck // read to its end below
	var out shardResponse
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil || resp.StatusCode != http.StatusOK {
		x.t.Fatalf("recommend: status %d, %v", resp.StatusCode, err)
	}
	return out
}

// complete fails unless n responses, asked one after another, are the
// healthy one from the given version, with both shards answered.
func (x *tier) complete(n int, version string) {
	x.t.Helper()
	for range n {
		x.check(x.ask(), version)
	}
}

// check fails unless got is the healthy response from version, with both
// shards answered.
func (x *tier) check(got shardResponse, version string) {
	x.t.Helper()
	if got.ModelVersion != version || len(got.Degraded) > 0 || got.Coverage == nil || *got.Coverage != struct{ Shards, Answered int }{2, 2} ||
		!slices.Equal(got.Items, x.healthy.Items) {
		x.t.Fatalf("got %+v, want the healthy items %v from version %s", got, x.healthy.Items, version)
	}
}

// swap asks the gateway to swap to the bundle in dir and returns the status.
func (x *tier) swap(dir string) int {
	x.t.Helper()
	resp, err := http.Post("http://"+x.admin+"/v1/admin/swap", "application/json", strings.NewReader(`{"bundle":"`+dir+`"}`))
	if err != nil {
		x.t.Fatal(err)
	}
	defer resp.Body.Close() //nolint:errcheck // only the status is used
	return resp.StatusCode
}

// The gateway binary on four annshard processes, two shards of two
// replicas: a replica killed with SIGKILL under requests costs no response;
// a swap through the admin API goes through with that replica down; the
// replica, restarted empty, is prepared again with no request and then
// serves its shard alone; and a swap to a version the replicas lack is
// rejected.
func TestShardReplicasKilledAndSwapped(t *testing.T) {
	need(t)
	root := t.TempDir()
	v1, v2 := link(t, root, bundletest.Write(t)), link(t, root, bundletest.Write(t, bundletest.Variant(t, 2)))
	x := newTier(t, root, v1)
	x.complete(20, v1)

	// A replica of shard 0 is killed while requests run.
	killed := make(chan struct{})
	go func() {
		time.Sleep(20 * time.Millisecond)
		kill(x.procs[0])
		close(killed)
	}()
	for running := true; running; {
		select {
		case <-killed:
			running = false
		default:
		}
		x.complete(10, v1)
	}
	x.complete(50, v1)

	// A swap goes through on the replicas that are left.
	if status := x.swap(filepath.Join(root, v2)); status != http.StatusOK {
		t.Fatalf("swap with one replica down: status %d", status)
	}
	x.complete(20, v2)

	// The killed replica comes back empty; once the gateway's connection to
	// it is up, the gateway has it prepare the active version, with no
	// request to find it lacking. Then it serves shard 0 alone.
	x.replica(0)
	eventually(t, 20*time.Second, "the restarted replica was not prepared again", func() bool {
		return health(t, x.addrs[0]) == healthpb.HealthCheckResponse_SERVING
	})
	if !holdsIndex(t, x.addrs[0], v2, 0) {
		t.Fatal("the restarted replica is ready without the active version")
	}
	kill(x.procs[1])
	x.complete(20, v2)

	// A version that no replica has is rejected, and v2 goes on serving.
	if status := x.swap(bundletest.Write(t, bundletest.Variant(t, 3))); status != http.StatusUnprocessableEntity {
		t.Fatalf("swap to a version the replicas lack: status %d", status)
	}
	x.complete(20, v2)

	resp, err := http.Get(x.gw + "/metrics")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close() //nolint:errcheck // read to its end below
	metrics, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"recserve_shard_hedges_total", "recserve_shard_failovers_total"} {
		if !strings.Contains(string(metrics), name) {
			t.Errorf("/metrics lacks %s", name)
		}
	}
}

// Swaps in a row over three bundles through the gateway binary, each
// prepare followed by a search of the version before it at every replica,
// as a request still running on that version makes: with no request in
// between to name the version served, the replicas keep it because the
// gateway names it in its Prepare (D043), and no shard is left without it.
func TestSwapsInARow(t *testing.T) {
	need(t)
	root := t.TempDir()
	a := link(t, root, bundletest.Write(t))
	b := link(t, root, bundletest.Write(t, bundletest.Variant(t, 2)))
	c := link(t, root, bundletest.Write(t, bundletest.Variant(t, 3)))
	x := newTier(t, root, a)
	swap := func(version string) {
		t.Helper()
		if status := x.swap(filepath.Join(root, version)); status != http.StatusOK {
			t.Fatalf("swap to %s: status %d", version, status)
		}
	}
	// late searches version at every replica, which must hold it.
	late := func(version string) {
		t.Helper()
		for i, addr := range x.addrs {
			if !holdsIndex(t, addr, version, i/2) {
				t.Fatalf("replica %s lacks %s", addr, version)
			}
		}
	}
	swap(b)
	late(a)
	swap(c) // a is dropped although named after b: b is served
	late(b)
	x.complete(20, c)
	late(b)
	swap(a) // b is dropped although named after c: c is served
	late(c)
	x.complete(20, a)
}
