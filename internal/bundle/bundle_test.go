package bundle

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/sHyanorn/recserve/internal/npy"
	"github.com/sHyanorn/recserve/internal/vec"
)

// testItems is the item count of the test bundle; item i's vector is (cos i, sin i) / i.
const testItems = 12

func testVectors() []float32 {
	v := make([]float32, 2*(testItems+1))
	for i := 1; i <= testItems; i++ {
		v[2*i], v[2*i+1] = float32(math.Cos(float64(i))/float64(i)), float32(math.Sin(float64(i))/float64(i))
	}
	return v
}

// goldenLine is a correct golden case for the given history.
func goldenLine(t testing.TB, history []int32) string {
	t.Helper()
	m, err := vec.NewMatrix(2, testVectors())
	if err != nil {
		t.Fatal(err)
	}
	b := New(Manifest{Dim: 2, MaxSeqLen: 50}, []string{""}, m, nil, nil)
	uv := b.UserVector(history)
	seen := map[int32]bool{}
	for _, id := range history {
		seen[id] = true
	}
	var top []int32
	for _, h := range m.TopK(uv, 10, func(id int32) bool { return seen[id] }) {
		top = append(top, h.ID)
	}
	return string(marshal(t, map[string]any{"history": history, "user_emb": uv, "top10": top})) + "\n"
}

func marshal(t testing.TB, v any) []byte {
	t.Helper()
	b, err := json.Marshal(v)
	if err != nil {
		t.Fatal(err)
	}
	return b
}

// writeBundle writes a 12-item, 2-dimensional bundle and returns its directory.
// edit, if non-nil, may change the files and manifest before checksums are written.
func writeBundle(t *testing.T, edit func(files map[string][]byte, m map[string]any)) string {
	t.Helper()
	var ids strings.Builder
	for i := 1; i <= testItems; i++ {
		fmt.Fprintf(&ids, "item%d\n", i)
	}
	files := map[string][]byte{
		"items.ids":    []byte(ids.String()),
		"items.npy":    npy.Encode([]int{testItems + 1, 2}, testVectors(), nil),
		"popular.npy":  npy.Encode([]int{2}, nil, []int64{3, 1}),
		"golden.jsonl": []byte(goldenLine(t, []int32{1}) + goldenLine(t, []int32{2, 5})),
	}
	cooc := make([]int64, (testItems+1)*CoocWidth)
	cooc[1*CoocWidth], cooc[1*CoocWidth+1] = 3, 2
	files["cooc_ids.npy"] = npy.Encode([]int{testItems + 1, CoocWidth}, nil, cooc)
	m := map[string]any{
		"format_version": 1, "model_version": "test", "dataset": "test", "train_cutoff_ts": 100,
		"item_count": testItems, "dim": 2, "metric": "dot", "normalized": false, "max_seq_len": 50,
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
	if err := os.WriteFile(filepath.Join(dir, "manifest.json"), marshal(t, m), 0o644); err != nil {
		t.Fatal(err)
	}
	return dir
}

func TestLoad(t *testing.T) {
	b, err := Load(writeBundle(t, nil), Options{FirstTestTS: 101})
	if err != nil {
		t.Fatal(err)
	}
	if id, ok := b.Lookup("item3"); !ok || id != 3 {
		t.Fatalf("Lookup(item3) = %d, %v", id, ok)
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
			f["golden.jsonl"] = editGolden(t, func(c map[string]any) {
				top := c["top10"].([]any)
				top[0], top[1] = top[1], top[0]
			})
		}, Options{}, "top-10"},
		"golden vector": {func(f map[string][]byte, _ map[string]any) {
			f["golden.jsonl"] = editGolden(t, func(c map[string]any) { c["user_emb"].([]any)[0] = 0.5 })
		}, Options{}, "user vector"},
		"golden short": {func(f map[string][]byte, _ map[string]any) {
			f["golden.jsonl"] = editGolden(t, func(c map[string]any) { c["top10"] = c["top10"].([]any)[:1] })
		}, Options{}, "10 top10"},
		"golden field": {func(f map[string][]byte, _ map[string]any) {
			f["golden.jsonl"] = editGolden(t, func(c map[string]any) { c["top_10"] = c["top10"] })
		}, Options{}, "unknown field"},
		"golden empty": {func(f map[string][]byte, _ map[string]any) { f["golden.jsonl"] = []byte("{}\n") }, Options{}, "needs a history"},
		"no golden":    {func(f map[string][]byte, _ map[string]any) { f["golden.jsonl"] = nil }, Options{}, "no cases"},
		"padding": {func(f map[string][]byte, _ map[string]any) {
			v := testVectors()
			v[0] = 1
			f["items.npy"] = npy.Encode([]int{testItems + 1, 2}, v, nil)
		}, Options{}, "padding"},
		"shape": {func(_ map[string][]byte, m map[string]any) { m["item_count"] = 3 }, Options{}, "lines"},
		"duplicate id": {func(f map[string][]byte, _ map[string]any) {
			f["items.ids"] = []byte(strings.Replace(string(f["items.ids"]), "item2\n", "item1\n", 1))
		}, Options{}, "repeats"},
		"popular range":  {func(f map[string][]byte, _ map[string]any) { f["popular.npy"] = npy.Encode([]int{1}, nil, []int64{99}) }, Options{}, "out of range"},
		"popular zero":   {func(f map[string][]byte, _ map[string]any) { f["popular.npy"] = npy.Encode([]int{1}, nil, []int64{0}) }, Options{}, "padding ID"},
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

// editGolden returns the golden file of history [1], changed by edit.
func editGolden(t testing.TB, edit func(map[string]any)) []byte {
	t.Helper()
	var c map[string]any
	if err := json.Unmarshal([]byte(goldenLine(t, []int32{1})), &c); err != nil {
		t.Fatal(err)
	}
	edit(c)
	return append(marshal(t, c), '\n')
}

func TestLoadRejectsTamperedFile(t *testing.T) {
	dir := writeBundle(t, nil)
	if err := os.WriteFile(filepath.Join(dir, "items.ids"), []byte("tampered\n"), 0o644); err != nil {
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
