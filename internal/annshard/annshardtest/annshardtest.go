// Package annshardtest runs annshard replicas inside tests and writes the
// bundle directories they load.
package annshardtest

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"io"
	"log/slog"
	"net"
	"os"
	"path/filepath"
	"slices"
	"sync"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"

	"github.com/sHyanorn/recserve/internal/annshard"
	"github.com/sHyanorn/recserve/internal/annshard/annshardpb"
	"github.com/sHyanorn/recserve/internal/bundle"
	"github.com/sHyanorn/recserve/internal/npy"
	"github.com/sHyanorn/recserve/internal/shard"
	"github.com/sHyanorn/recserve/internal/vec"
)

// WriteVectors writes what a replica reads of a bundle, its manifest and
// items.npy, under root, in the directory named by the version these files
// give, and returns the version. m holds the item vectors from row 1 up,
// after a zero padding row.
func WriteVectors(t testing.TB, root string, m vec.Matrix) string {
	t.Helper()
	items := npy.Encode([]int{m.Rows(), m.Dim}, m.Data, nil)
	sum := sha256.Sum256(items)
	sums := map[string]string{"items.npy": hex.EncodeToString(sum[:])}
	version := bundle.VersionOf(sums)
	manifest, err := json.Marshal(map[string]any{
		"format_version": 1, "model_version": version, "item_count": m.Rows() - 1, "dim": m.Dim,
		"metric": "dot", "pad_id": 0, "user_encoder": "mean", "max_seq_len": 1, "sha256": sums,
	})
	if err != nil {
		t.Fatal(err)
	}
	dir := filepath.Join(root, version)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	for name, data := range map[string][]byte{"manifest.json": manifest, "items.npy": items} {
		if err := os.WriteFile(filepath.Join(dir, name), data, 0o644); err != nil {
			t.Fatal(err)
		}
	}
	return version
}

// Start serves a replica over the bundle directories under root on a
// loopback port, until the test ends, and returns its address. opts are
// added to the gRPC server's.
func Start(t testing.TB, root string, opts ...grpc.ServerOption) string {
	t.Helper()
	addr, _ := StartOn(t, "127.0.0.1:0", root, opts...)
	return addr
}

// StartOn is Start on the given address, and also returns the function
// that stops the replica, cutting its connections, before the test ends.
// A replica started on a stopped one's address is that replica restarted:
// it holds no index.
func StartOn(t testing.TB, addr, root string, opts ...grpc.ServerOption) (string, func()) {
	t.Helper()
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	gs := grpc.NewServer(append(annshard.ServerOptions(), opts...)...)
	annshard.NewServer(root, slog.New(slog.NewTextHandler(io.Discard, nil))).Register(gs)
	go gs.Serve(ln) //nolint:errcheck // ends with the Stop below
	t.Cleanup(gs.Stop)
	return ln.Addr().String(), gs.Stop
}

// StartAll serves replicas replicas for each of shards shards, all over
// root, and returns their addresses by shard.
func StartAll(t testing.TB, root string, shards, replicas int) [][]string {
	t.Helper()
	addrs := make([][]string, shards)
	for i := range addrs {
		for range replicas {
			addrs[i] = append(addrs[i], Start(t, root))
		}
	}
	return addrs
}

// Ready reports whether the replica at addr answers the standard health
// check with SERVING.
func Ready(t testing.TB, addr string) bool {
	t.Helper()
	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close() //nolint:errcheck // a probe
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	h, err := healthpb.NewHealthClient(conn).Check(ctx, &healthpb.HealthCheckRequest{})
	return err == nil && h.GetStatus() == healthpb.HealthCheckResponse_SERVING
}

// Dial returns a client of the replicas at addrs, closed when the test ends.
func Dial(t testing.TB, addrs [][]string, opt annshard.Options) *annshard.Client {
	t.Helper()
	p := &prepares{done: map[[2]string]bool{}}
	for _, replicas := range addrs {
		p.addrs = append(p.addrs, replicas...)
	}
	opt.Dial = append(slices.Clip(opt.Dial), grpc.WithChainUnaryInterceptor(p.note))
	c, err := annshard.Dial(addrs, opt)
	if err != nil {
		t.Fatal(err)
	}
	clients.Store(c, p)
	t.Cleanup(func() {
		clients.Delete(c)
		if err := c.Close(); err != nil {
			t.Error(err)
		}
	})
	return c
}

// clients holds the prepares of each client Dial returned.
var clients sync.Map

// prepares notes, by replica address and version, the Prepare calls of a
// client that have succeeded.
type prepares struct {
	addrs []string
	mu    sync.Mutex
	done  map[[2]string]bool
}

func (p *prepares) note(ctx context.Context, method string, req, reply any, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
	err := invoker(ctx, method, req, reply, cc, opts...)
	if r, ok := req.(*annshardpb.PrepareRequest); ok && err == nil {
		p.mu.Lock()
		p.done[[2]string{cc.Target(), r.GetIndex().GetVersion()}] = true
		p.mu.Unlock()
	}
	return err
}

// PrepareAll is c.Prepare, for a client from Dial, followed by a wait until
// every replica's Prepare of version has succeeded, so that each holds it
// and has its connection up. Prepare itself returns once each shard has a
// replica that holds the version (D057); a test that needs every replica
// prepared calls PrepareAll, and only where every replica can prepare it.
func PrepareAll(t testing.TB, c *annshard.Client, version string, items int) (*annshard.Shards, error) {
	t.Helper()
	s, err := c.Prepare(context.Background(), version, items)
	if err != nil {
		return nil, err
	}
	v, _ := clients.Load(c)
	p := v.(*prepares)
	for deadline := time.Now().Add(10 * time.Second); ; time.Sleep(time.Millisecond) {
		p.mu.Lock()
		missing := ""
		for _, a := range p.addrs {
			if !p.done[[2]string{a, version}] {
				missing = a
			}
		}
		p.mu.Unlock()
		if missing == "" {
			return s, nil
		}
		if time.Now().After(deadline) {
			t.Fatalf("replica %s has not prepared %s after 10 s", missing, version)
		}
	}
}

// Remote returns a function that puts a catalog on annshard replicas: it
// writes m as a new bundle version and prepares it, split by cfg into at
// most maxShards shards, on replicas, one per shard, that serve every
// catalog of the test.
func Remote(t testing.TB, maxShards int) func(m vec.Matrix, cfg shard.Config) (*annshard.Shards, error) {
	t.Helper()
	root := t.TempDir()
	addrs := StartAll(t, root, maxShards, 1)
	clients := map[shard.Config]*annshard.Client{}
	return func(m vec.Matrix, cfg shard.Config) (*annshard.Shards, error) {
		version := WriteVectors(t, root, m)
		if clients[cfg] == nil {
			clients[cfg] = Dial(t, addrs[:cfg.Shards], annshard.Options{Config: cfg})
		}
		return clients[cfg].Prepare(context.Background(), version, m.Rows()-1)
	}
}
