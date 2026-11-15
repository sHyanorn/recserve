// Package bundle loads and verifies a model bundle (docs/SPEC.md §8).
package bundle

import (
	"bufio"
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"maps"
	"math"
	"os"
	"path/filepath"
	"slices"
	"strings"

	"github.com/sHyanorn/recserve/internal/npy"
	"github.com/sHyanorn/recserve/internal/onnx"
	"github.com/sHyanorn/recserve/internal/vec"
)

// FormatVersion is the manifest format this package reads.
const FormatVersion = 1

// GoldenTolerance is the largest accepted difference in a golden user vector.
const GoldenTolerance = 1e-4

// ErrCutoff reports a bundle trained on events at or after the start of the test period.
var ErrCutoff = errors.New("bundle trained on events from the test period")

// Manifest is manifest.json.
type Manifest struct {
	FormatVersion int    `json:"format_version"`
	ModelVersion  string `json:"model_version"`
	Dataset       string `json:"dataset"`
	TrainCutoffTS int64  `json:"train_cutoff_ts"`
	ItemCount     int    `json:"item_count"`
	Dim           int    `json:"dim"`
	Metric        string `json:"metric"`
	Normalized    bool   `json:"normalized"`
	MaxSeqLen     int    `json:"max_seq_len"`
	PadID         int    `json:"pad_id"`
	UserEncoder   string `json:"user_encoder"`
	// Stream, if set, names the stream state's window: the popularity and
	// co-occurrence sources come from the stream state, not from
	// popular.npy and cooc_ids.npy (D060).
	Stream *Stream `json:"stream,omitempty"`
	// RankerFeatures names the ranker's input columns; it must equal
	// RankerFeatures when the bundle has ranker.onnx, or with Stream, those
	// followed by RecencyFeatures, and be absent without ranker.onnx.
	RankerFeatures []string          `json:"ranker_features,omitempty"`
	SHA256         map[string]string `json:"sha256"`
}

// Stream is the stream state a bundle's sources and ranker read (D060).
type Stream struct {
	PopularWindowMS int64 `json:"popular_window_ms"`
}

// Recency reports whether the bundle's ranker takes RecencyFeatures.
func (m *Manifest) Recency() bool { return len(m.RankerFeatures) > len(RankerFeatures) }

// featureCount is the number of the ranker's input columns.
func (m *Manifest) featureCount() int {
	if m.Recency() {
		return len(RankerFeatures) + len(RecencyFeatures)
	}
	return len(RankerFeatures)
}

// CoocWidth is the number of co-occurring items stored per item.
const CoocWidth = 50

// RankerFeatures are the ranker's input columns the service computes, in
// order (docs/SPEC.md §8).
var RankerFeatures = []string{"vector_score", "cooc_score", "popular_score", "item_norm", "last_item_cosine", "log_history_len"}

// RecencyFeatures may follow RankerFeatures in the ranker's input of a
// bundle with a stream window (D060): ln(1 + the item's events in the
// window before the request time), and ln(1 + the days from the item's
// first event to the request time), 0 for an item with none before it.
var RecencyFeatures = []string{"window_count", "item_age"}

// optionalFiles are the files a bundle may have; each is read only if the
// manifest lists it.
var optionalFiles = []string{"user_encoder.onnx", "ranker.onnx", "cooc_ids.npy", "cooc_counts.npy", "popular.npy"}

// RankerRows is the number of rows every ranker call passes: at most this
// many candidates are ranked, and the rest of the rows are zero.
const RankerRows = 512

// Bundle is a loaded, verified bundle.
type Bundle struct {
	Manifest
	Dir      string
	IDs      []string // IDs[i] is the raw ID of internal ID i; IDs[0] is ""
	Vectors  vec.Matrix
	InvNorms []float32     // 1 / the norm of each row of Vectors, 0 for a zero row (vec.Matrix.InvNorms)
	Cooc     []int32       // row i holds item i's co-occurring IDs, CoocWidth per row, 0-padded
	Popular  []int32       // best first
	Encoder  *onnx.Encoder // user_encoder.onnx; nil when the user vector is the mean
	Ranker   *onnx.Ranker  // ranker.onnx; nil without one

	index map[string]int32
}

// Options control Load.
type Options struct {
	// FirstTestTS, if non-zero, is the time of the first test event; a bundle
	// whose train_cutoff_ts is not before it is rejected with ErrCutoff, and
	// one whose train_cutoff_ts is under 1/100 of it as a time in another
	// unit.
	FirstTestTS int64
}

// Load reads dir, verifies every checksum, shape and golden case, and
// applies the train cutoff check. A bundle with user_encoder onnx or a
// ranker needs ONNX Runtime (onnx.Init) and must be closed when no longer
// used.
func Load(dir string, opt Options) (*Bundle, error) {
	b := &Bundle{Dir: dir}
	if err := b.load(opt); err != nil {
		return nil, fmt.Errorf("bundle %s: %w", dir, errors.Join(err, b.Close()))
	}
	return b, nil
}

// VersionOf returns the version of a bundle whose files have the given
// SHA-256 checksums, as tools/bundle/build.py derives it: the first 12 hex
// digits of the SHA-256 of every file name followed by its checksum, in
// name order. A version so names the content of every file.
func VersionOf(sums map[string]string) string {
	h := sha256.New()
	for _, name := range slices.Sorted(maps.Keys(sums)) {
		h.Write([]byte(name + sums[name]))
	}
	return hex.EncodeToString(h.Sum(nil))[:12]
}

// ReadManifest reads dir's manifest.json and checks the fields every
// reader depends on.
func ReadManifest(dir string) (Manifest, error) {
	var m Manifest
	raw, err := os.ReadFile(filepath.Join(dir, "manifest.json"))
	if err != nil {
		return m, err
	}
	if err := json.Unmarshal(raw, &m); err != nil {
		return m, fmt.Errorf("manifest: %w", err)
	}
	switch {
	case m.FormatVersion != FormatVersion:
		return m, fmt.Errorf("format_version %d, want %d", m.FormatVersion, FormatVersion)
	case m.Metric != "dot" || m.PadID != 0 || (m.UserEncoder != "mean" && m.UserEncoder != "onnx"):
		return m, fmt.Errorf("unsupported metric %q, pad_id %d or user_encoder %q", m.Metric, m.PadID, m.UserEncoder)
	case m.ItemCount <= 0 || m.Dim <= 0 || m.MaxSeqLen <= 0 || m.ModelVersion == "":
		return m, errors.New("manifest: item_count, dim, max_seq_len and model_version are required")
	case m.Stream != nil && m.Stream.PopularWindowMS <= 0:
		return m, fmt.Errorf("stream popular_window_ms %d, want a window above 0", m.Stream.PopularWindowMS)
	case m.ModelVersion != VersionOf(m.SHA256):
		// A replica and the gateway given different files under one name
		// would otherwise serve them as one version (D046).
		return m, fmt.Errorf("model_version %q, but the checksums give %q", m.ModelVersion, VersionOf(m.SHA256))
	}
	return m, nil
}

// readListed reads a file of the bundle in dir and verifies it against the
// checksum the manifest lists for it.
func (m *Manifest) readListed(dir, name string) ([]byte, error) {
	want, ok := m.SHA256[name]
	if !ok {
		return nil, fmt.Errorf("%s missing from the manifest", name)
	}
	if name != filepath.Base(name) || name == "manifest.json" {
		return nil, fmt.Errorf("manifest lists %q", name)
	}
	data, err := os.ReadFile(filepath.Join(dir, name))
	if err != nil {
		return nil, err
	}
	sum := sha256.Sum256(data)
	if hex.EncodeToString(sum[:]) != want {
		return nil, fmt.Errorf("%s: checksum mismatch", name)
	}
	return data, nil
}

// vectors parses items.npy: finite float32 values, one row of dim per
// internal ID, the padding row zero.
func (m *Manifest) vectors(data []byte) (vec.Matrix, error) {
	a, err := npy.Parse(data)
	if err != nil {
		return vec.Matrix{}, fmt.Errorf("items.npy: %w", err)
	}
	if a.F32 == nil || !slices.Equal(a.Shape, []int{m.ItemCount + 1, m.Dim}) {
		return vec.Matrix{}, fmt.Errorf("items.npy: shape %v, want [%d %d] float32", a.Shape, m.ItemCount+1, m.Dim)
	}
	for _, v := range a.F32 {
		if math.IsNaN(float64(v)) || math.IsInf(float64(v), 0) {
			return vec.Matrix{}, errors.New("items.npy: non-finite value")
		}
	}
	rows, err := vec.NewMatrix(m.Dim, a.F32)
	if err != nil {
		return vec.Matrix{}, err
	}
	for _, v := range rows.Row(0) {
		if v != 0 {
			return vec.Matrix{}, errors.New("items.npy: padding row is not zero")
		}
	}
	return rows, nil
}

// Vectors reads only the manifest and the item vectors of the bundle in
// dir, verifying items.npy as Load does: what a shard replica needs. The
// rest of the bundle, its golden cases and its train cutoff are for Load
// to check, and ONNX Runtime is not needed.
func Vectors(dir string) (Manifest, vec.Matrix, error) {
	fail := func(err error) (Manifest, vec.Matrix, error) {
		return Manifest{}, vec.Matrix{}, fmt.Errorf("bundle %s: %w", dir, err)
	}
	m, err := ReadManifest(dir)
	if err != nil {
		return fail(err)
	}
	data, err := m.readListed(dir, "items.npy")
	if err != nil {
		return fail(err)
	}
	rows, err := m.vectors(data)
	if err != nil {
		return fail(err)
	}
	return m, rows, nil
}

func (b *Bundle) load(opt Options) error {
	var err error
	if b.Manifest, err = ReadManifest(b.Dir); err != nil {
		return err
	}
	m := &b.Manifest
	if err := checkCutoff(m.TrainCutoffTS, opt.FirstTestTS); err != nil {
		return err
	}

	files := map[string][]byte{}
	for name := range m.SHA256 {
		if files[name], err = m.readListed(b.Dir, name); err != nil {
			return err
		}
	}
	// An optional file the manifest does not list would be ignored, so the
	// bundle would serve without it.
	for _, name := range optionalFiles {
		if _, listed := files[name]; listed {
			continue
		}
		if _, err := os.Stat(filepath.Join(b.Dir, name)); err == nil {
			return fmt.Errorf("%s is in the directory but not in the manifest", name)
		} else if !errors.Is(err, fs.ErrNotExist) {
			return err
		}
	}
	need := func(name string) ([]byte, error) {
		data, ok := files[name]
		if !ok {
			return nil, fmt.Errorf("%s missing from the manifest", name)
		}
		return data, nil
	}

	ids, err := need("items.ids")
	if err != nil {
		return err
	}
	b.IDs = append([]string{""}, strings.Split(strings.TrimSuffix(string(ids), "\n"), "\n")...)
	if len(b.IDs) != m.ItemCount+1 {
		return fmt.Errorf("items.ids has %d lines, item_count is %d", len(b.IDs)-1, m.ItemCount)
	}
	b.index = make(map[string]int32, m.ItemCount)
	for i, id := range b.IDs[1:] {
		if id == "" {
			return fmt.Errorf("items.ids line %d is empty", i+1)
		}
		if _, dup := b.index[id]; dup {
			return fmt.Errorf("items.ids repeats %q", id)
		}
		b.index[id] = int32(i + 1)
	}

	data, err := need("items.npy")
	if err != nil {
		return err
	}
	if b.Vectors, err = m.vectors(data); err != nil {
		return err
	}
	b.InvNorms = b.Vectors.InvNorms()

	if _, ok := files["cooc_ids.npy"]; ok {
		if b.Cooc, err = idArray(files["cooc_ids.npy"], m.ItemCount, []int{m.ItemCount + 1, CoocWidth}); err != nil {
			return fmt.Errorf("cooc_ids.npy: %w", err)
		}
	}
	if data, ok := files["popular.npy"]; ok {
		if b.Popular, err = idArray(data, m.ItemCount, nil); err != nil {
			return fmt.Errorf("popular.npy: %w", err)
		}
		if slices.Contains(b.Popular, 0) {
			return errors.New("popular.npy: padding ID 0")
		}
	}

	if _, listed := files["user_encoder.onnx"]; listed && m.UserEncoder == "mean" {
		return errors.New("user_encoder.onnx listed for user_encoder mean")
	}
	if m.UserEncoder == "onnx" {
		model, err := need("user_encoder.onnx")
		if err != nil {
			return err
		}
		if b.Encoder, err = onnx.NewEncoder(model, m.MaxSeqLen, m.Dim); err != nil {
			return fmt.Errorf("user_encoder.onnx: %w", err)
		}
		// The golden cases use few IDs; the highest one shows that the
		// encoder knows every item. A call takes at most max_seq_len items.
		for seq := range slices.Chunk([]int32{1, int32(m.ItemCount)}, m.MaxSeqLen) {
			if _, err := b.Encoder.Encode(context.Background(), seq); err != nil {
				return fmt.Errorf("user_encoder.onnx: items %v: %w", seq, err)
			}
		}
	}

	if err := b.loadRanker(files["ranker.onnx"]); err != nil {
		return err
	}

	golden, err := need("golden.jsonl")
	if err != nil {
		return err
	}
	return b.checkGolden(golden)
}

// unitRatio bounds how much earlier than the first test event a train
// cutoff may be before it is taken for a time in another unit: seconds are a
// thousandth of milliseconds, while a cutoff and a test event in one unit
// differ by a small fraction of either.
const unitRatio = 100

// checkCutoff requires a train cutoff in milliseconds since the Unix epoch,
// as the dataset's times are, and, given the first test event's time, one
// before it. A manifest without train_cutoff_ts reads as 0, and a cutoff in
// seconds reads as a time long past: either would pass the comparison alone.
func checkCutoff(cutoff, firstTest int64) error {
	switch {
	case cutoff <= 0:
		return fmt.Errorf("train_cutoff_ts %d: a time in milliseconds is required", cutoff)
	case firstTest == 0:
		return nil
	case cutoff >= firstTest:
		return fmt.Errorf("%w: train_cutoff_ts %d, first test event %d", ErrCutoff, cutoff, firstTest)
	case cutoff*unitRatio < firstTest: // cutoff < firstTest here, so no overflow
		return fmt.Errorf("train_cutoff_ts %d is under 1/%d of the first test event's time %d: not in milliseconds", cutoff, unitRatio, firstTest)
	}
	return nil
}

// checkRows are the rows the ranker check changes, one set at a time from
// all-zero rows: the first row, then rows past it.
var checkRows = [][]int{{0}, {1, RankerRows / 2, RankerRows - 1}}

// loadRanker loads ranker.onnx, if listed, and checks that it scores
// RankerRows rows, finitely, each row independently of the others: the
// service pads with zero rows, so a ranker that mixes rows would score a
// candidate differently with different padding. The check scores all-zero
// rows, then the same with each set of checkRows given fixed values; the
// other rows' scores must not change, bit for bit.
func (b *Bundle) loadRanker(model []byte) error {
	if model == nil {
		if len(b.RankerFeatures) != 0 {
			return errors.New("ranker_features without ranker.onnx")
		}
		return nil
	}
	recency := slices.Concat(RankerFeatures, RecencyFeatures)
	six, eight := slices.Equal(b.RankerFeatures, RankerFeatures), b.Stream != nil && slices.Equal(b.RankerFeatures, recency)
	if !six && !eight {
		return fmt.Errorf("ranker_features %q, the service computes %q, or %q with a stream window", b.RankerFeatures, RankerFeatures, recency)
	}
	nf := b.featureCount()
	var err error
	if b.Ranker, err = onnx.NewRanker(model, nf, RankerRows); err != nil {
		return fmt.Errorf("ranker.onnx: %w", err)
	}
	zero, err := b.Ranker.Rank(context.Background(), make([]float32, RankerRows*nf))
	if err != nil {
		return fmt.Errorf("ranker.onnx: %w", err)
	}
	for _, rows := range checkRows {
		x := make([]float32, RankerRows*nf)
		for k, r := range rows {
			for j := range nf {
				x[r*nf+j] = float32(j+1+k) / 4
			}
		}
		got, err := b.Ranker.Rank(context.Background(), x)
		if err != nil {
			return fmt.Errorf("ranker.onnx: %w", err)
		}
		for i := range RankerRows {
			if !slices.Contains(rows, i) && math.Float32bits(zero[i]) != math.Float32bits(got[i]) {
				return fmt.Errorf("ranker.onnx: the score of row %d changes with rows %v", i, rows)
			}
		}
	}
	return nil
}

// idArray parses an int64 array of internal IDs in [0, itemCount]; shape, if
// non-nil, is the required shape, otherwise the array must be 1-D.
func idArray(data []byte, itemCount int, shape []int) ([]int32, error) {
	a, err := npy.Parse(data)
	if err != nil {
		return nil, err
	}
	if a.I64 == nil || (shape != nil && !slices.Equal(a.Shape, shape)) || (shape == nil && len(a.Shape) != 1) {
		return nil, fmt.Errorf("shape %v, want int64 %v", a.Shape, shape)
	}
	out := make([]int32, len(a.I64))
	for i, v := range a.I64 {
		if v < 0 || v > int64(itemCount) {
			return nil, fmt.Errorf("ID %d out of range", v)
		}
		out[i] = int32(v)
	}
	return out, nil
}

type goldenCase struct {
	History []int32 `json:"history"`
	// HistoryRaw, if present, is History in raw item IDs: items.ids must map
	// each to the internal ID in its place, which the other fields cannot
	// check, since they hold internal IDs only.
	HistoryRaw []string  `json:"history_raw,omitempty"`
	UserEmb    []float32 `json:"user_emb"`
	Top10      []int32   `json:"top10"`
	// With a ranker: the case's candidates and their features, one row
	// each, as the bundle's trainer computed them.
	Candidates []int32     `json:"candidates,omitempty"`
	Features   [][]float32 `json:"features,omitempty"`
	// With a ranker and a stream window: the request time and the stream
	// inputs of the features as the trainer's stream state gave them at
	// that time (D060): the co-occurrence row of each seed, the latest
	// history item first, the popular list, and each candidate's window
	// count and first event time, null for none.
	TS           *int64    `json:"ts,omitempty"`
	CoocRows     [][]int32 `json:"cooc_rows,omitempty"`
	Popular      []int32   `json:"popular,omitempty"`
	WindowCounts []int     `json:"window_counts,omitempty"`
	FirstTS      []*int64  `json:"first_ts,omitempty"`
}

// FeatureTolerance bounds the difference between a golden case's features
// and the service's: rounding to float32 only.
const FeatureTolerance = 1e-6

func (b *Bundle) checkGolden(data []byte) error {
	sc := bufio.NewScanner(bytes.NewReader(data))
	sc.Buffer(nil, 1<<20)
	n := 0
	for sc.Scan() {
		var c goldenCase
		d := json.NewDecoder(bytes.NewReader(sc.Bytes()))
		d.DisallowUnknownFields()
		if err := d.Decode(&c); err != nil {
			return fmt.Errorf("golden %d: %w", n, err)
		}
		if len(c.History) == 0 || len(c.UserEmb) != b.Dim || len(c.Top10) != 10 {
			return fmt.Errorf("golden %d: needs a history, %d user_emb values and 10 top10 IDs", n, b.Dim)
		}
		for _, id := range c.History {
			if id <= 0 || int(id) > b.ItemCount {
				return fmt.Errorf("golden %d: history ID %d out of range", n, id)
			}
		}
		if err := b.checkRaw(c); err != nil {
			return fmt.Errorf("golden %d: %w", n, err)
		}
		uv, err := b.UserVector(context.Background(), c.History)
		if err != nil {
			return fmt.Errorf("golden %d: %w", n, err)
		}
		if len(uv) != len(c.UserEmb) {
			return fmt.Errorf("golden %d: user vector has %d values, want %d", n, len(uv), len(c.UserEmb))
		}
		for j, v := range uv {
			if d := math.Abs(float64(v - c.UserEmb[j])); d > GoldenTolerance {
				return fmt.Errorf("golden %d: user vector differs by %g", n, d)
			}
		}
		seen := make(map[int32]bool, len(c.History))
		for _, id := range c.History {
			seen[id] = true
		}
		hits := b.Vectors.TopK(uv, len(c.Top10), func(id int32) bool { return seen[id] })
		got := make([]int32, len(hits))
		for i, h := range hits {
			got[i] = h.ID
		}
		if !slices.Equal(got, c.Top10) {
			return fmt.Errorf("golden %d: top-10 %v, want %v", n, got, c.Top10)
		}
		if err := b.checkFeatures(c); err != nil {
			return fmt.Errorf("golden %d: %w", n, err)
		}
		n++
	}
	if err := sc.Err(); err != nil {
		return fmt.Errorf("golden: %w", err)
	}
	if n == 0 {
		return errors.New("golden.jsonl has no cases")
	}
	return nil
}

// checkRaw requires a golden case's raw history, if it has one, to name its
// history's items through items.ids.
func (b *Bundle) checkRaw(c goldenCase) error {
	if c.HistoryRaw == nil {
		return nil
	}
	if len(c.HistoryRaw) != len(c.History) {
		return fmt.Errorf("history_raw has %d items, history %d", len(c.HistoryRaw), len(c.History))
	}
	for i, raw := range c.HistoryRaw {
		id, ok := b.Lookup(raw)
		if !ok {
			return fmt.Errorf("history_raw %q is not in items.ids", raw)
		}
		if id != c.History[i] {
			return fmt.Errorf("history_raw %q is internal ID %d in items.ids, history has %d", raw, id, c.History[i])
		}
	}
	return nil
}

// checkFeatures requires a bundle with a ranker to compute the features its
// trainer gave for a golden case's candidates, within FeatureTolerance,
// from the case's history and user vector, and with a stream window from
// its stream inputs: a ranker trained on features that the service
// computes otherwise would rank by others.
func (b *Bundle) checkFeatures(c goldenCase) error {
	streamInputs := c.TS != nil || c.CoocRows != nil || c.Popular != nil || c.WindowCounts != nil || c.FirstTS != nil
	if b.Ranker == nil {
		if c.Candidates != nil || c.Features != nil || streamInputs {
			return errors.New("candidates without a ranker")
		}
		return nil
	}
	if len(c.Candidates) == 0 || len(c.Features) != len(c.Candidates) {
		return errors.New("a bundle with a ranker needs candidates and their features")
	}
	for _, id := range c.Candidates {
		if id <= 0 || int(id) > b.ItemCount {
			return fmt.Errorf("candidate ID %d out of range", id)
		}
	}
	cooc, popular := BestScores(b.CoocHits(c.History)), BestScores(b.PopularHits())
	var rec *Recency
	switch {
	case b.Stream == nil && streamInputs:
		return errors.New("stream inputs without a stream window")
	case b.Stream != nil:
		var err error
		if cooc, popular, rec, err = b.streamInputs(c); err != nil {
			return err
		}
	}
	got := b.Features(c.Candidates, c.History, c.UserEmb, cooc, popular, rec)
	nf := b.featureCount()
	names := slices.Concat(RankerFeatures, RecencyFeatures)
	for i, row := range c.Features {
		if len(row) != nf {
			return fmt.Errorf("candidate %d has %d features, want %d", c.Candidates[i], len(row), nf)
		}
		for j, want := range row {
			if d := math.Abs(float64(got[i*nf+j]) - float64(want)); !(d <= FeatureTolerance) {
				return fmt.Errorf("feature %s of candidate %d differs by %g", names[j], c.Candidates[i], d)
			}
		}
	}
	return nil
}

// streamInputs returns the sources' scores, and with RecencyFeatures what
// they read, of a golden case of a bundle with a stream window, from the
// stream inputs the case carries.
func (b *Bundle) streamInputs(c goldenCase) (cooc, popular map[int32]float64, rec *Recency, err error) {
	seeds := min(CoocSeeds, len(c.History))
	if c.TS == nil || len(c.CoocRows) != seeds || c.Popular == nil {
		return nil, nil, nil, fmt.Errorf("a bundle with a stream window needs ts, %d cooc_rows and popular", seeds)
	}
	rows := make(map[int32][]int32, seeds)
	for s, row := range c.CoocRows {
		rows[c.History[len(c.History)-1-s]] = row
	}
	cooc = BestScores(CoocHitsOf(c.History, func(id int32) []int32 { return rows[id] }))
	popular = BestScores(PopularHitsOf(c.Popular))
	if len(c.WindowCounts) != len(c.Candidates) || len(c.FirstTS) != len(c.Candidates) {
		return nil, nil, nil, errors.New("a bundle with a stream window needs window_counts and first_ts for each candidate")
	}
	if !b.Recency() {
		return cooc, popular, nil, nil
	}
	counts, firsts := make(map[int32]int, len(c.Candidates)), make(map[int32]*int64, len(c.Candidates))
	for i, id := range c.Candidates {
		counts[id], firsts[id] = c.WindowCounts[i], c.FirstTS[i]
	}
	rec = &Recency{TS: *c.TS, Count: func(id int32) int { return counts[id] }, First: func(id int32) (int64, bool) {
		if t := firsts[id]; t != nil {
			return *t, true
		}
		return 0, false
	}}
	return cooc, popular, rec, nil
}

// StreamState is what a stream state gives at a time ts (fresh.State, D059).
type StreamState interface {
	Row(item int32, ts int64, n int) []int32
	Popular(ts, w int64, n int) []int32
	Count(item int32, from, to int64) int
	First(item int32, ts int64) (int64, bool)
}

// CheckStreamGolden requires the stream inputs of every golden case of a
// bundle with a stream window and a ranker to equal those s gives at the
// case's time (D060 point 3): s, given the events the trainer's stream
// state was given, must keep them as the trainer's did.
func (b *Bundle) CheckStreamGolden(s StreamState) error {
	if b.Stream == nil || b.Ranker == nil {
		return errors.New("golden stream inputs need a stream window and a ranker")
	}
	data, err := b.readListed(b.Dir, "golden.jsonl")
	if err != nil {
		return err
	}
	w := b.Stream.PopularWindowMS
	sc := bufio.NewScanner(bytes.NewReader(data))
	sc.Buffer(nil, 1<<20)
	for n := 0; sc.Scan(); n++ {
		var c goldenCase
		if err := json.Unmarshal(sc.Bytes(), &c); err != nil {
			return fmt.Errorf("golden %d: %w", n, err)
		}
		ts := *c.TS // Load requires it
		for k, row := range c.CoocRows {
			seed := c.History[len(c.History)-1-k]
			if got := s.Row(seed, ts, CoocWidth); !slices.Equal(got, row) {
				return fmt.Errorf("golden %d: item %d's co-occurrence row %v, the trainer's %v", n, seed, got, row)
			}
		}
		if got := s.Popular(ts, w, PopularTop); !slices.Equal(got, c.Popular) {
			return fmt.Errorf("golden %d: popular list %v, the trainer's %v", n, got, c.Popular)
		}
		for i, id := range c.Candidates {
			count := s.Count(id, ts-w, ts)
			first, ok := s.First(id, ts)
			want := c.FirstTS[i]
			if count != c.WindowCounts[i] || ok != (want != nil) || ok && first != *want {
				got, trainer := "none", "none"
				if ok {
					got = fmt.Sprint(first)
				}
				if want != nil {
					trainer = fmt.Sprint(*want)
				}
				return fmt.Errorf("golden %d: item %d's window count %d and first event %s, the trainer's %d and %s",
					n, id, count, got, c.WindowCounts[i], trainer)
			}
		}
	}
	return sc.Err()
}

// New assembles a bundle from parts already in memory, without files or
// checksums; ids[0] must be "" (padding). For tests and tools.
func New(m Manifest, ids []string, vectors vec.Matrix, cooc, popular []int32) *Bundle {
	b := &Bundle{Manifest: m, IDs: ids, Vectors: vectors, InvNorms: vectors.InvNorms(), Cooc: cooc, Popular: popular, index: make(map[string]int32, len(ids))}
	for i, id := range ids[1:] {
		b.index[id] = int32(i + 1)
	}
	return b
}

// UserVector returns the user vector of a history, oldest item first: the
// encoder's output for the last MaxSeqLen items, or their mean if the bundle
// has no encoder. It is nil when the history is empty or the vector is zero
// (items without training data have zero vectors), so the caller falls back
// to sources that need no user vector. An encoder error is returned, also
// a zero output for items that have vectors; if ctx ends during the encoder
// run, the error wraps ctx.Err().
func (b *Bundle) UserVector(ctx context.Context, history []int32) ([]float32, error) {
	if len(history) > b.MaxSeqLen {
		history = history[len(history)-b.MaxSeqLen:]
	}
	zero := func(v []float32) bool { return !slices.ContainsFunc(v, func(x float32) bool { return x != 0 }) }
	var uv []float32
	if b.Encoder != nil {
		var err error
		if uv, err = b.Encoder.Encode(ctx, history); err != nil {
			return nil, err
		}
		if zero(uv) && slices.ContainsFunc(history, func(id int32) bool { return b.InvNorms[id] != 0 }) {
			return nil, errors.New("user encoder output is zero for items that have vectors")
		}
	} else {
		uv = b.Vectors.Mean(history)
	}
	if zero(uv) {
		return nil, nil
	}
	return uv, nil
}

// Close releases the encoder and the ranker, if any; the bundle must not
// be used after.
func (b *Bundle) Close() error {
	var errs []error
	if b.Encoder != nil {
		errs = append(errs, b.Encoder.Close())
	}
	if b.Ranker != nil {
		errs = append(errs, b.Ranker.Close())
	}
	return errors.Join(errs...)
}

// Lookup returns the internal ID of a raw item ID.
func (b *Bundle) Lookup(raw string) (int32, bool) {
	id, ok := b.index[raw]
	return id, ok
}

// CoocRow returns item id's co-occurring items, best first, without padding.
func (b *Bundle) CoocRow(id int32) []int32 {
	if b.Cooc == nil {
		return nil
	}
	row := b.Cooc[int(id)*CoocWidth : (int(id)+1)*CoocWidth]
	if n := slices.Index(row, 0); n >= 0 {
		return row[:n]
	}
	return row
}
