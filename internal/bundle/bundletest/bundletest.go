// Package bundletest writes small bundles for tests: 12 items with the
// 2-dimensional vectors of the test encoder mean.onnx, valid unless an edit
// breaks them.
package bundletest

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"maps"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"github.com/sHyanorn/recserve/internal/npy"
	"github.com/sHyanorn/recserve/internal/onnx/onnxtest"
	"github.com/sHyanorn/recserve/internal/vec"
)

// Items is the number of items in a written bundle.
const Items = onnxtest.MeanItems

// CoocWidth is the width of a co-occurrence row, as in package bundle.
const CoocWidth = 50

// Edit changes a bundle's files and manifest before its checksums are written.
type Edit func(files map[string][]byte, manifest map[string]any)

// Write writes a bundle with user_encoder mean and train_cutoff_ts 100 to a
// new temporary directory and returns it. The edits apply in order. Its
// model_version is the one its files give, unless an edit sets one.
func Write(t testing.TB, edits ...Edit) string {
	t.Helper()
	var ids strings.Builder
	for i := 1; i <= Items; i++ {
		fmt.Fprintf(&ids, "item%d\n", i)
	}
	vectors := onnxtest.MeanVectors()
	files := map[string][]byte{
		"items.ids":    []byte(ids.String()),
		"items.npy":    npy.Encode([]int{Items + 1, 2}, vectors, nil),
		"popular.npy":  npy.Encode([]int{2}, nil, []int64{3, 1}),
		"golden.jsonl": []byte(GoldenLine(t, vectors, 50, []int32{1}) + GoldenLine(t, vectors, 50, []int32{2, 5})),
	}
	cooc := make([]int64, (Items+1)*CoocWidth)
	cooc[1*CoocWidth], cooc[1*CoocWidth+1] = 3, 2
	files["cooc_ids.npy"] = npy.Encode([]int{Items + 1, CoocWidth}, nil, cooc)
	m := map[string]any{
		"format_version": 1, "dataset": "test", "train_cutoff_ts": 100,
		"item_count": Items, "dim": 2, "metric": "dot", "normalized": false, "max_seq_len": 50,
		"pad_id": 0, "user_encoder": "mean",
	}
	for _, edit := range edits {
		edit(files, m)
	}
	dir := t.TempDir()
	sums := map[string]string{}
	for name, data := range files {
		sum := sha256.Sum256(data)
		sums[name] = hex.EncodeToString(sum[:])
		if err := os.WriteFile(filepath.Join(dir, name), data, 0o644); err != nil {
			t.Fatal(err)
		}
	}
	m["sha256"] = sums
	if _, set := m["model_version"]; !set {
		m["model_version"] = versionOf(sums)
	}
	if err := os.WriteFile(filepath.Join(dir, "manifest.json"), Marshal(t, m), 0o644); err != nil {
		t.Fatal(err)
	}
	return dir
}

// versionOf is bundle.VersionOf, which this package cannot call: the
// bundle package's own tests import it.
func versionOf(sums map[string]string) string {
	h := sha256.New()
	for _, name := range slices.Sorted(maps.Keys(sums)) {
		h.Write([]byte(name + sums[name]))
	}
	return hex.EncodeToString(h.Sum(nil))[:12]
}

// Version sets the bundle's model_version, which the loader rejects unless
// the bundle's files give it.
func Version(v string) Edit {
	return func(_ map[string][]byte, m map[string]any) { m["model_version"] = v }
}

// Variant adds a golden case for the history of item n, 1 to Items, to the
// bundle Write makes: a bundle of its own version that serves the same
// responses.
func Variant(t testing.TB, n int32) Edit {
	return func(f map[string][]byte, _ map[string]any) {
		f["golden.jsonl"] = append(f["golden.jsonl"], GoldenLine(t, onnxtest.MeanVectors(), 50, []int32{n})...)
	}
}

// VersionOf returns the model_version of the bundle in dir.
func VersionOf(t testing.TB, dir string) string {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join(dir, "manifest.json"))
	if err != nil {
		t.Fatal(err)
	}
	var m struct {
		ModelVersion string `json:"model_version"`
	}
	if err := json.Unmarshal(raw, &m); err != nil {
		t.Fatal(err)
	}
	return m.ModelVersion
}

// GoldenLine is a correct golden case for history over the given
// 2-dimensional item vectors: the user vector is the mean of the last
// maxSeqLen items, computed here independently of any encoder.
func GoldenLine(t testing.TB, vectors []float32, maxSeqLen int, history []int32) string {
	t.Helper()
	m, err := vec.NewMatrix(2, vectors)
	if err != nil {
		t.Fatal(err)
	}
	last := history[max(0, len(history)-maxSeqLen):]
	uv := m.Mean(last)
	if !slices.ContainsFunc(uv, func(x float32) bool { return x != 0 }) {
		uv = nil
	}
	seen := map[int32]bool{}
	for _, id := range history {
		seen[id] = true
	}
	var top []int32
	for _, h := range m.TopK(uv, 10, func(id int32) bool { return seen[id] }) {
		top = append(top, h.ID)
	}
	return string(Marshal(t, map[string]any{"history": history, "user_emb": uv, "top10": top})) + "\n"
}

// Marshal returns v as JSON.
func Marshal(t testing.TB, v any) []byte {
	t.Helper()
	b, err := json.Marshal(v)
	if err != nil {
		t.Fatal(err)
	}
	return b
}
