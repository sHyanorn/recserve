package bundle

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/sHyanorn/recserve/internal/npy"
)

// writeBundle writes a 4-item, 2-dimensional bundle and returns its directory.
// edit, if non-nil, may change the files before checksums are written.
func writeBundle(t *testing.T, edit func(files map[string][]byte, m map[string]any)) string {
	t.Helper()
	vecs := []float32{0, 0, 1, 0, 0, 1, 1, 1, -1, 0}
	files := map[string][]byte{
		"items.ids":    []byte("a\nb\nc\nd\n"),
		"items.npy":    npy.Encode([]int{5, 2}, vecs, nil),
		"popular.npy":  npy.Encode([]int{2}, nil, []int64{3, 1}),
		"golden.jsonl": []byte(`{"history":[1],"user_emb":[1,0],"top10":[3,2]}` + "\n"),
	}
	cooc := make([]int64, 5*CoocWidth)
	cooc[1*CoocWidth], cooc[1*CoocWidth+1] = 3, 2
	files["cooc_ids.npy"] = npy.Encode([]int{5, CoocWidth}, nil, cooc)
	m := map[string]any{
		"format_version": 1, "model_version": "test", "dataset": "test", "train_cutoff_ts": 100,
		"item_count": 4, "dim": 2, "metric": "dot", "normalized": false, "max_seq_len": 50,
		"pad_id": 0, "user_encoder": "mean",
	}
	if edit != nil {
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
	raw, _ := json.Marshal(m)
	if err := os.WriteFile(filepath.Join(dir, "manifest.json"), raw, 0o644); err != nil {
		t.Fatal(err)
	}
	return dir
}

func TestLoad(t *testing.T) {
	b, err := Load(writeBundle(t, nil), Options{FirstTestTS: 101})
	if err != nil {
		t.Fatal(err)
	}
	if id, ok := b.Lookup("c"); !ok || id != 3 {
		t.Fatalf("Lookup(c) = %d, %v", id, ok)
	}
	if got := b.CoocRow(1); len(got) != 2 || got[0] != 3 || got[1] != 2 {
		t.Fatalf("CoocRow(1) = %v", got)
	}
	if len(b.CoocRow(2)) != 0 || len(b.Popular) != 2 {
		t.Fatalf("CoocRow(2) = %v, Popular = %v", b.CoocRow(2), b.Popular)
	}
}

func TestLoadRejects(t *testing.T) {
	cases := map[string]struct {
		edit func(map[string][]byte, map[string]any)
		opt  Options
		want string
	}{
		"cutoff equal": {nil, Options{FirstTestTS: 100}, "test period"},
		"cutoff after": {nil, Options{FirstTestTS: 50}, "test period"},
		"golden top": {func(f map[string][]byte, _ map[string]any) {
			f["golden.jsonl"] = []byte(`{"history":[1],"user_emb":[1,0],"top10":[2,3]}`)
		}, Options{}, "top-10"},
		"golden vector": {func(f map[string][]byte, _ map[string]any) {
			f["golden.jsonl"] = []byte(`{"history":[1],"user_emb":[1,0.01],"top10":[3,2]}`)
		}, Options{}, "user vector"},
		"no golden": {func(f map[string][]byte, _ map[string]any) { f["golden.jsonl"] = nil }, Options{}, "no cases"},
		"padding": {func(f map[string][]byte, _ map[string]any) {
			f["items.npy"] = npy.Encode([]int{5, 2}, []float32{1, 0, 1, 0, 0, 1, 1, 1, -1, 0}, nil)
		}, Options{}, "padding"},
		"shape":          {func(_ map[string][]byte, m map[string]any) { m["item_count"] = 3 }, Options{}, "lines"},
		"duplicate id":   {func(f map[string][]byte, _ map[string]any) { f["items.ids"] = []byte("a\nb\na\nd\n") }, Options{}, "repeats"},
		"cooc range":     {func(f map[string][]byte, _ map[string]any) { f["popular.npy"] = npy.Encode([]int{1}, nil, []int64{9}) }, Options{}, "out of range"},
		"encoder":        {func(_ map[string][]byte, m map[string]any) { m["user_encoder"] = "onnx" }, Options{}, "unsupported"},
		"missing vector": {func(f map[string][]byte, _ map[string]any) { delete(f, "items.npy") }, Options{}, "missing"},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			_, err := Load(writeBundle(t, tc.edit), tc.opt)
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("err = %v, want %q", err, tc.want)
			}
		})
	}
	if _, err := Load(writeBundle(t, nil), Options{FirstTestTS: 100}); !errors.Is(err, ErrCutoff) {
		t.Fatalf("cutoff error %v is not ErrCutoff", err)
	}
}

func TestLoadRejectsTamperedFile(t *testing.T) {
	dir := writeBundle(t, nil)
	if err := os.WriteFile(filepath.Join(dir, "items.ids"), []byte("a\nb\nc\nx\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := Load(dir, Options{}); err == nil || !strings.Contains(err.Error(), "checksum") {
		t.Fatalf("err = %v", err)
	}
}

// TestRealBundle loads the bundle built by tools/bundle when RECSERVE_BUNDLE names its directory.
func TestRealBundle(t *testing.T) {
	dir := os.Getenv("RECSERVE_BUNDLE")
	if dir == "" {
		t.Skip("RECSERVE_BUNDLE not set")
	}
	b, err := Load(dir, Options{})
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("%s: %d items, dim %d", b.ModelVersion, b.ItemCount, b.Dim)
}
