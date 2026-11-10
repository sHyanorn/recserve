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
	"testing"

	"google.golang.org/grpc"

	"github.com/sHyanorn/recserve/internal/annshard"
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

// Dial returns a client of the replicas at addrs, closed when the test ends.
func Dial(t testing.TB, addrs [][]string, opt annshard.Options) *annshard.Client {
	t.Helper()
	c, err := annshard.Dial(addrs, opt)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := c.Close(); err != nil {
			t.Error(err)
		}
	})
	return c
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
