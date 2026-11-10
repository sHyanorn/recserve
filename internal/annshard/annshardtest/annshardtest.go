// Package annshardtest runs annshard replicas inside tests and writes the
// bundle directories they load.
package annshardtest

import (
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
	"github.com/sHyanorn/recserve/internal/annshard/annshardpb"
	"github.com/sHyanorn/recserve/internal/npy"
	"github.com/sHyanorn/recserve/internal/vec"
)

// WriteVectors writes what a replica reads of a bundle, its manifest and
// items.npy, to the directory version under root. m holds the item vectors
// from row 1 up, after a zero padding row.
func WriteVectors(t testing.TB, root, version string, m vec.Matrix) {
	t.Helper()
	items := npy.Encode([]int{m.Rows(), m.Dim}, m.Data, nil)
	sum := sha256.Sum256(items)
	manifest, err := json.Marshal(map[string]any{
		"format_version": 1, "model_version": version, "item_count": m.Rows() - 1, "dim": m.Dim,
		"metric": "dot", "pad_id": 0, "user_encoder": "mean", "max_seq_len": 1,
		"sha256": map[string]string{"items.npy": hex.EncodeToString(sum[:])},
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
}

// Start serves a replica over the bundle directories under root on a
// loopback port, until the test ends, and returns its address. opts are
// added to the gRPC server's.
func Start(t testing.TB, root string, opts ...grpc.ServerOption) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	gs := grpc.NewServer(opts...)
	annshardpb.RegisterAnnShardServer(gs, annshard.NewServer(root, slog.New(slog.NewTextHandler(io.Discard, nil))))
	go gs.Serve(ln) //nolint:errcheck // ends with the Stop below
	t.Cleanup(gs.Stop)
	return ln.Addr().String()
}
