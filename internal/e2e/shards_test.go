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

// holdsIndex reports whether the replica at addr answers a search of shard
// 0 of 2 of the version: whether it has prepared that index.
func holdsIndex(t *testing.T, addr, version string) bool {
	t.Helper()
	return probe(t, addr, func(ctx context.Context, conn *grpc.ClientConn) error {
		_, err := annshardpb.NewAnnShardClient(conn).Search(ctx, &annshardpb.SearchRequest{
			Index: &annshardpb.Index{Version: version, Shards: 2, Mode: annshardpb.Mode_MODE_EXACT}, Query: []float32{1, 1}, K: 1,
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

// The gateway binary on four annshard processes, two shards of two
// replicas: a replica killed with SIGKILL under requests costs no response;
// a swap through the admin API goes through with that replica down; the
// replica, restarted empty, is prepared again with no request and then
// serves its shard alone; and a swap to a version the replicas lack is
// rejected.
func TestShardReplicasKilledAndSwapped(t *testing.T) {
	need(t)
	// The replicas' bundle root holds one directory per version; the two
	// versions have the same items and vectors.
	root := t.TempDir()
	v1, v2 := link(t, root, bundletest.Write(t)), link(t, root, bundletest.Write(t, bundletest.Variant(t, 2)))
	replica := func(addr string) *exec.Cmd {
		t.Helper()
		cmd := start(t, "annshard", "-addr", addr, "-bundles", root)
		eventually(t, 20*time.Second, "the replica does not answer", func() bool {
			return health(t, addr) != healthpb.HealthCheckResponse_UNKNOWN
		})
		return cmd
	}
	addrs := []string{freeAddr(t), freeAddr(t), freeAddr(t), freeAddr(t)} // shard 0: two replicas, shard 1: two
	procs := make([]*exec.Cmd, len(addrs))
	for i, addr := range addrs {
		procs[i] = replica(addr)
	}
	addr, admin := freeAddr(t), freeAddr(t)
	// No result cache, so that every request searches the shards, and
	// generous budgets: the test is about replicas that fail, not slow ones.
	start(t, "gateway", "-addr", addr, "-admin-addr", admin, "-bundle", filepath.Join(root, v1), "-cache-size", "0",
		"-shard-addrs", addrs[0]+","+addrs[1]+";"+addrs[2]+","+addrs[3], "-budget-retrieval", "10s", "-budget-shard", "10s")
	gw := "http://" + addr
	eventually(t, 20*time.Second, "the gateway is not ready", func() bool { return up(gw + "/readyz") })

	// A user with a history has a user vector, so its requests search the shards.
	if status, _ := post(t, gw+"/v1/events", map[string]any{"events": []map[string]any{
		{"user_id": "u", "item_id": "item2", "ts": 1, "kind": "view"}, {"user_id": "u", "item_id": "item5", "ts": 2, "kind": "view"},
	}}); status != http.StatusAccepted {
		t.Fatalf("events: status %d", status)
	}
	ask := func() shardResponse {
		t.Helper()
		resp, err := http.Post(gw+"/v1/recommend", "application/json", strings.NewReader(`{"user_id":"u","k":5}`))
		if err != nil {
			t.Fatal(err)
		}
		defer resp.Body.Close() //nolint:errcheck // read to its end below
		var out shardResponse
		if err := json.NewDecoder(resp.Body).Decode(&out); err != nil || resp.StatusCode != http.StatusOK {
			t.Fatalf("recommend: status %d, %v", resp.StatusCode, err)
		}
		return out
	}
	healthy := ask()
	if len(healthy.Items) == 0 || healthy.Coverage == nil {
		t.Fatalf("healthy response %+v", healthy)
	}
	// complete fails unless n responses, asked one after another, are the
	// healthy one from the given version, with both shards answered.
	complete := func(n int, version string) {
		t.Helper()
		for range n {
			got := ask()
			if got.ModelVersion != version || len(got.Degraded) > 0 || got.Coverage == nil || *got.Coverage != struct{ Shards, Answered int }{2, 2} ||
				!slices.Equal(got.Items, healthy.Items) {
				t.Fatalf("got %+v, want the healthy items %v from version %s", got, healthy.Items, version)
			}
		}
	}
	complete(20, v1)

	// A replica of shard 0 is killed while requests run.
	killed := make(chan struct{})
	go func() {
		time.Sleep(20 * time.Millisecond)
		kill(procs[0])
		close(killed)
	}()
	for running := true; running; {
		select {
		case <-killed:
			running = false
		default:
		}
		complete(10, v1)
	}
	complete(50, v1)

	// A swap goes through on the replicas that are left.
	swap := func(dir string) int {
		t.Helper()
		resp, err := http.Post("http://"+admin+"/v1/admin/swap", "application/json", strings.NewReader(`{"bundle":"`+dir+`"}`))
		if err != nil {
			t.Fatal(err)
		}
		defer resp.Body.Close() //nolint:errcheck // only the status is used
		return resp.StatusCode
	}
	if status := swap(filepath.Join(root, v2)); status != http.StatusOK {
		t.Fatalf("swap with one replica down: status %d", status)
	}
	complete(20, v2)

	// The killed replica comes back empty; once the gateway's connection to
	// it is up, the gateway has it prepare the active version, with no
	// request to find it lacking. Then it serves shard 0 alone.
	procs[0] = replica(addrs[0])
	eventually(t, 20*time.Second, "the restarted replica was not prepared again", func() bool {
		return health(t, addrs[0]) == healthpb.HealthCheckResponse_SERVING
	})
	if !holdsIndex(t, addrs[0], v2) {
		t.Fatal("the restarted replica is ready without the active version")
	}
	kill(procs[1])
	complete(20, v2)

	// A version that no replica has is rejected, and v2 goes on serving.
	if status := swap(bundletest.Write(t, bundletest.Variant(t, 3))); status != http.StatusUnprocessableEntity {
		t.Fatalf("swap to a version the replicas lack: status %d", status)
	}
	complete(20, v2)

	resp, err := http.Get(gw + "/metrics")
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
