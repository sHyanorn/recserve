package bundle

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"math"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/sHyanorn/recserve/internal/bundle/bundletest"
	"github.com/sHyanorn/recserve/internal/npy"
	"github.com/sHyanorn/recserve/internal/onnx"
	"github.com/sHyanorn/recserve/internal/onnx/onnxtest"
)

// The test bundle has the items of the test encoder mean.onnx: item i's
// vector is (cos i, sin i) / i.
const testItems = bundletest.Items

func testVectors() []float32 { return onnxtest.MeanVectors() }

var (
	goldenLine  = bundletest.GoldenLine
	marshal     = bundletest.Marshal
	writeBundle = func(t *testing.T, edits ...editFunc) string { t.Helper(); return bundletest.Write(t, edits...) }
)

type editFunc = bundletest.Edit

// withEncoder makes the bundle use the named test model as its user encoder.
func withEncoder(t testing.TB, model string) editFunc {
	data := onnxtest.Model(t, model)
	return func(f map[string][]byte, m map[string]any) {
		f["user_encoder.onnx"] = data
		m["user_encoder"] = "onnx"
	}
}

// load loads dir and closes the bundle when the test ends.
func load(t *testing.T, dir string, opt Options) *Bundle {
	t.Helper()
	b, err := Load(dir, opt)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := b.Close(); err != nil {
			t.Error(err)
		}
	})
	return b
}

func initORT(t *testing.T) {
	t.Helper()
	if err := onnx.Init(onnxtest.Lib(t)); err != nil {
		t.Fatal(err)
	}
}

func TestLoad(t *testing.T) {
	b := load(t, writeBundle(t), Options{FirstTestTS: 101})
	if b.Encoder != nil {
		t.Fatal("a mean bundle has an encoder")
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

// TestLoadONNX loads a bundle whose user encoder is mean.onnx; its golden
// cases are means computed in Go, so loading checks the encoder against
// them. After Close the encoder refuses to run.
func TestLoadONNX(t *testing.T) {
	initORT(t)
	b, err := Load(writeBundle(t, withEncoder(t, "mean.onnx")), Options{})
	if err != nil {
		t.Fatal(err)
	}
	if b.Encoder == nil {
		t.Fatal("no encoder")
	}
	ctx := context.Background()
	for _, h := range [][]int32{{4}, {1, 2, 3}, {12, 12, 7}} {
		got, err := b.UserVector(ctx, h)
		if err != nil {
			t.Fatal(err)
		}
		want := b.Vectors.Mean(h)
		for j := range want {
			if math.Abs(float64(got[j]-want[j])) > 1e-5 {
				t.Fatalf("history %v: encoder %v, mean %v", h, got, want)
			}
		}
	}
	if err := b.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := b.UserVector(ctx, []int32{1}); !errors.Is(err, onnx.ErrClosed) {
		t.Fatalf("after Close: %v", err)
	}
}

// With max_seq_len 3 the golden case of history 1, 1, 1, 2, 2 holds the
// mean of 1, 2, 2, so it passes only if the history is cut before the
// encoder runs.
func TestLoadONNXTruncates(t *testing.T) {
	initORT(t)
	load(t, writeBundle(t, withEncoder(t, "mean.onnx"), func(f map[string][]byte, m map[string]any) {
		m["max_seq_len"] = 3
		f["golden.jsonl"] = []byte(goldenLine(t, testVectors(), 3, []int32{1, 1, 1, 2, 2}))
	}), Options{})
}

func TestLoadRejects(t *testing.T) {
	cases := map[string]struct {
		edit editFunc
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
		"popular range":   {func(f map[string][]byte, _ map[string]any) { f["popular.npy"] = npy.Encode([]int{1}, nil, []int64{99}) }, Options{}, "out of range"},
		"popular zero":    {func(f map[string][]byte, _ map[string]any) { f["popular.npy"] = npy.Encode([]int{1}, nil, []int64{0}) }, Options{}, "padding ID"},
		"unknown encoder": {func(_ map[string][]byte, m map[string]any) { m["user_encoder"] = "rnn" }, Options{}, "unsupported"},
		"onnx, no model":  {func(_ map[string][]byte, m map[string]any) { m["user_encoder"] = "onnx" }, Options{}, "user_encoder.onnx missing"},
		"mean with model": {func(f map[string][]byte, _ map[string]any) { f["user_encoder.onnx"] = []byte("x") }, Options{}, "user_encoder.onnx listed"},
		"missing vector":  {func(f map[string][]byte, _ map[string]any) { delete(f, "items.npy") }, Options{}, "missing"},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			var edits []editFunc
			if tc.edit != nil {
				edits = append(edits, tc.edit)
			}
			checkRejected(t, writeBundle(t, edits...), tc.opt, tc.want)
		})
	}
	if _, err := Load(writeBundle(t), Options{FirstTestTS: 100}); !errors.Is(err, ErrCutoff) {
		t.Fatalf("cutoff error %v is not ErrCutoff", err)
	}
}

// TestLoadRejectsONNX covers bundles whose encoder does not reproduce the
// golden cases.
func TestLoadRejectsONNX(t *testing.T) {
	initORT(t)
	// Items 1 and 2 swapped in items.npy, with golden cases that match it;
	// the encoder's own table still has them in the original order.
	swapped := testVectors()
	swapped[2], swapped[3], swapped[4], swapped[5] = swapped[4], swapped[5], swapped[2], swapped[3]
	cases := map[string]struct {
		edit editFunc
		want string
	}{
		"golden vector": {func(f map[string][]byte, _ map[string]any) {
			f["golden.jsonl"] = editGolden(t, func(c map[string]any) { c["user_emb"].([]any)[1] = 0.5 })
		}, "user vector differs"},
		"items reordered": {func(f map[string][]byte, _ map[string]any) {
			f["items.npy"] = npy.Encode([]int{testItems + 1, 2}, swapped, nil)
			f["golden.jsonl"] = []byte(goldenLine(t, swapped, 50, []int32{1}))
		}, "user vector differs"},
		"other model": {withEncoder(t, "slow.onnx"), "Invalid input name: item_seq"},
		"zero output": {withEncoder(t, "zero.onnx"), "output is zero"},
		// Item 13 is past the encoder's table, and no golden case uses it.
		"unknown item": {func(f map[string][]byte, m map[string]any) {
			v := append(testVectors(), 0.5, 0.5)
			f["items.ids"] = append(f["items.ids"], "item13\n"...)
			f["items.npy"] = npy.Encode([]int{testItems + 2, 2}, v, nil)
			f["cooc_ids.npy"] = npy.Encode([]int{testItems + 2, CoocWidth}, nil, make([]int64, (testItems+2)*CoocWidth))
			f["golden.jsonl"] = []byte(goldenLine(t, v, 50, []int32{1}))
			m["item_count"] = testItems + 1
		}, "items 1 and 13"},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			checkRejected(t, writeBundle(t, withEncoder(t, "mean.onnx"), tc.edit), Options{}, tc.want)
		})
	}
}

// withRanker adds the named test model as the bundle's ranker, with the
// service's ranker_features.
func withRanker(t testing.TB, model string) editFunc {
	data := onnxtest.Model(t, model)
	return func(f map[string][]byte, m map[string]any) {
		f["ranker.onnx"] = data
		m["ranker_features"] = RankerFeatures
	}
}

func TestLoadRanker(t *testing.T) {
	initORT(t)
	b, err := Load(writeBundle(t, withRanker(t, "linear_ranker.onnx")), Options{})
	if err != nil {
		t.Fatal(err)
	}
	if b.Ranker == nil || b.Ranker.Rows != RankerRows || b.Ranker.Features != len(RankerFeatures) {
		t.Fatalf("ranker %+v", b.Ranker)
	}
	x := []float32{1, 1, 1, 1, 1, 1}
	if _, err := b.Ranker.Rank(context.Background(), x); err != nil {
		t.Fatal(err)
	}
	if err := b.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := b.Ranker.Rank(context.Background(), x); !errors.Is(err, onnx.ErrClosed) {
		t.Fatalf("after Close: %v", err)
	}
}

func TestLoadRejectsRanker(t *testing.T) {
	initORT(t)
	features := func(v any) editFunc {
		return func(_ map[string][]byte, m map[string]any) { m["ranker_features"] = v }
	}
	reordered := append([]string{RankerFeatures[1], RankerFeatures[0]}, RankerFeatures[2:]...)
	cases := map[string]struct {
		edits []editFunc
		want  string
	}{
		"features missing":   {[]editFunc{withRanker(t, "linear_ranker.onnx"), features(nil)}, "ranker_features"},
		"features reordered": {[]editFunc{withRanker(t, "linear_ranker.onnx"), features(reordered)}, "ranker_features"},
		"feature extra":      {[]editFunc{withRanker(t, "linear_ranker.onnx"), features(append(RankerFeatures[:6:6], "x"))}, "ranker_features"},
		"features alone":     {[]editFunc{features(RankerFeatures)}, "ranker_features without ranker.onnx"},
		"rows mixed":         {[]editFunc{withRanker(t, "cross_row_ranker.onnx")}, "depends on row 0"},
		"column output":      {[]editFunc{withRanker(t, "column_ranker.onnx")}, "Invalid rank for output: score"},
		"other model":        {[]editFunc{withRanker(t, "mean.onnx")}, "Invalid input name: features"},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			checkRejected(t, writeBundle(t, tc.edits...), Options{}, tc.want)
		})
	}
}

func checkRejected(t *testing.T, dir string, opt Options, want string) {
	t.Helper()
	b, err := Load(dir, opt)
	if err == nil {
		t.Errorf("accepted; want an error with %q", want)
		if err := b.Close(); err != nil {
			t.Error(err)
		}
		return
	}
	if !strings.Contains(err.Error(), want) {
		t.Fatalf("err = %v, want %q", err, want)
	}
	t.Logf("rejected: %v", err)
}

// editGolden returns the golden file of history [1], changed by edit.
func editGolden(t testing.TB, edit func(map[string]any)) []byte {
	t.Helper()
	var c map[string]any
	if err := json.Unmarshal([]byte(goldenLine(t, testVectors(), 50, []int32{1})), &c); err != nil {
		t.Fatal(err)
	}
	edit(c)
	return append(marshal(t, c), '\n')
}

func TestLoadRejectsTamperedFile(t *testing.T) {
	dir := writeBundle(t)
	if err := os.WriteFile(filepath.Join(dir, "items.ids"), []byte("tampered\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := Load(dir, Options{}); err == nil || !strings.Contains(err.Error(), "checksum") {
		t.Fatalf("err = %v", err)
	}
}

// TestRealBundle loads the bundle built by tools/bundle when RECSERVE_BUNDLE
// names its directory; a bundle with an ONNX user encoder also needs
// RECSERVE_ORT_LIB. The bundle must have been built on this machine: the
// user vectors computed here must match its golden ones within 1e-5, the
// bound for the same machine, tighter than the 1e-4 Load accepts.
func TestRealBundle(t *testing.T) {
	dir := os.Getenv("RECSERVE_BUNDLE")
	if dir == "" {
		t.Skip("RECSERVE_BUNDLE not set")
	}
	if lib := os.Getenv("RECSERVE_ORT_LIB"); lib != "" {
		if err := onnx.Init(lib); err != nil {
			t.Fatal(err)
		}
	}
	b := load(t, dir, Options{})
	golden, err := os.ReadFile(filepath.Join(dir, "golden.jsonl"))
	if err != nil {
		t.Fatal(err)
	}
	var worst float64
	sc := bufio.NewScanner(bytes.NewReader(golden))
	sc.Buffer(nil, 1<<20)
	for sc.Scan() {
		var c goldenCase
		if err := json.Unmarshal(sc.Bytes(), &c); err != nil {
			t.Fatal(err)
		}
		uv, err := b.UserVector(context.Background(), c.History)
		if err != nil {
			t.Fatal(err)
		}
		for j, v := range uv {
			worst = max(worst, math.Abs(float64(v-c.UserEmb[j])))
		}
	}
	if err := sc.Err(); err != nil {
		t.Fatal(err)
	}
	t.Logf("%s: %d items, dim %d, user encoder %s, largest difference from the golden user vectors %g",
		b.ModelVersion, b.ItemCount, b.Dim, b.UserEncoder, worst)
	if worst > 1e-5 {
		t.Fatalf("user vectors differ from the golden ones by %g", worst)
	}
}
