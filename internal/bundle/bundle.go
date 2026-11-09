// Package bundle loads and verifies a model bundle (docs/SPEC.md §8).
package bundle

import (
	"bufio"
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"slices"
	"strings"

	"github.com/sHyanorn/recserve/internal/npy"
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
	FormatVersion int               `json:"format_version"`
	ModelVersion  string            `json:"model_version"`
	Dataset       string            `json:"dataset"`
	TrainCutoffTS int64             `json:"train_cutoff_ts"`
	ItemCount     int               `json:"item_count"`
	Dim           int               `json:"dim"`
	Metric        string            `json:"metric"`
	Normalized    bool              `json:"normalized"`
	MaxSeqLen     int               `json:"max_seq_len"`
	PadID         int               `json:"pad_id"`
	UserEncoder   string            `json:"user_encoder"`
	SHA256        map[string]string `json:"sha256"`
}

// CoocWidth is the number of co-occurring items stored per item.
const CoocWidth = 50

// Bundle is a loaded, verified bundle.
type Bundle struct {
	Manifest
	Dir     string
	IDs     []string // IDs[i] is the raw ID of internal ID i; IDs[0] is ""
	Vectors vec.Matrix
	Cooc    []int32 // row i holds item i's co-occurring IDs, CoocWidth per row, 0-padded
	Popular []int32 // best first

	index map[string]int32
}

// Options control Load.
type Options struct {
	// FirstTestTS, if non-zero, is the time of the first test event; a bundle
	// whose train_cutoff_ts is not before it is rejected with ErrCutoff.
	FirstTestTS int64
}

// Load reads dir, verifies every checksum, shape and golden case, and
// applies the train cutoff check.
func Load(dir string, opt Options) (*Bundle, error) {
	b, err := load(dir, opt)
	if err != nil {
		return nil, fmt.Errorf("bundle %s: %w", dir, err)
	}
	return b, nil
}

func load(dir string, opt Options) (*Bundle, error) {
	raw, err := os.ReadFile(filepath.Join(dir, "manifest.json"))
	if err != nil {
		return nil, err
	}
	b := &Bundle{Dir: dir}
	if err := json.Unmarshal(raw, &b.Manifest); err != nil {
		return nil, fmt.Errorf("manifest: %w", err)
	}
	m := &b.Manifest
	switch {
	case m.FormatVersion != FormatVersion:
		return nil, fmt.Errorf("format_version %d, want %d", m.FormatVersion, FormatVersion)
	case m.Metric != "dot" || m.PadID != 0 || m.UserEncoder != "mean":
		return nil, fmt.Errorf("unsupported metric %q, pad_id %d or user_encoder %q", m.Metric, m.PadID, m.UserEncoder)
	case m.ItemCount <= 0 || m.Dim <= 0 || m.MaxSeqLen <= 0 || m.ModelVersion == "":
		return nil, errors.New("manifest: item_count, dim, max_seq_len and model_version are required")
	}
	if opt.FirstTestTS != 0 && m.TrainCutoffTS >= opt.FirstTestTS {
		return nil, fmt.Errorf("%w: train_cutoff_ts %d, first test event %d", ErrCutoff, m.TrainCutoffTS, opt.FirstTestTS)
	}

	files := map[string][]byte{}
	for name, want := range m.SHA256 {
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
		files[name] = data
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
		return nil, err
	}
	b.IDs = append([]string{""}, strings.Split(strings.TrimSuffix(string(ids), "\n"), "\n")...)
	if len(b.IDs) != m.ItemCount+1 {
		return nil, fmt.Errorf("items.ids has %d lines, item_count is %d", len(b.IDs)-1, m.ItemCount)
	}
	b.index = make(map[string]int32, m.ItemCount)
	for i, id := range b.IDs[1:] {
		if id == "" {
			return nil, fmt.Errorf("items.ids line %d is empty", i+1)
		}
		if _, dup := b.index[id]; dup {
			return nil, fmt.Errorf("items.ids repeats %q", id)
		}
		b.index[id] = int32(i + 1)
	}

	data, err := need("items.npy")
	if err != nil {
		return nil, err
	}
	a, err := npy.Parse(data)
	if err != nil {
		return nil, fmt.Errorf("items.npy: %w", err)
	}
	if a.F32 == nil || !slices.Equal(a.Shape, []int{m.ItemCount + 1, m.Dim}) {
		return nil, fmt.Errorf("items.npy: shape %v, want [%d %d] float32", a.Shape, m.ItemCount+1, m.Dim)
	}
	for _, v := range a.F32 {
		if math.IsNaN(float64(v)) || math.IsInf(float64(v), 0) {
			return nil, errors.New("items.npy: non-finite value")
		}
	}
	if b.Vectors, err = vec.NewMatrix(m.Dim, a.F32); err != nil {
		return nil, err
	}
	for _, v := range b.Vectors.Row(0) {
		if v != 0 {
			return nil, errors.New("items.npy: padding row is not zero")
		}
	}

	if _, ok := files["cooc_ids.npy"]; ok {
		if b.Cooc, err = idArray(files["cooc_ids.npy"], m.ItemCount, []int{m.ItemCount + 1, CoocWidth}); err != nil {
			return nil, fmt.Errorf("cooc_ids.npy: %w", err)
		}
	}
	if data, ok := files["popular.npy"]; ok {
		if b.Popular, err = idArray(data, m.ItemCount, nil); err != nil {
			return nil, fmt.Errorf("popular.npy: %w", err)
		}
	}

	golden, err := need("golden.jsonl")
	if err != nil {
		return nil, err
	}
	if err := b.checkGolden(golden); err != nil {
		return nil, err
	}
	return b, nil
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
	History []int32   `json:"history"`
	UserEmb []float32 `json:"user_emb"`
	Top10   []int32   `json:"top10"`
}

func (b *Bundle) checkGolden(data []byte) error {
	sc := bufio.NewScanner(bytes.NewReader(data))
	sc.Buffer(nil, 1<<20)
	n := 0
	for sc.Scan() {
		var c goldenCase
		if err := json.Unmarshal(sc.Bytes(), &c); err != nil {
			return fmt.Errorf("golden %d: %w", n, err)
		}
		for _, id := range c.History {
			if id <= 0 || int(id) > b.ItemCount {
				return fmt.Errorf("golden %d: history ID %d out of range", n, id)
			}
		}
		uv := b.UserVector(c.History)
		if len(uv) != len(c.UserEmb) {
			return fmt.Errorf("golden %d: user vector has %d values, want %d", n, len(c.UserEmb), len(uv))
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

// New assembles a bundle from parts already in memory, without files or
// checksums; ids[0] must be "" (padding). For tests and tools.
func New(m Manifest, ids []string, vectors vec.Matrix, cooc, popular []int32) *Bundle {
	b := &Bundle{Manifest: m, IDs: ids, Vectors: vectors, Cooc: cooc, Popular: popular, index: make(map[string]int32, len(ids))}
	for i, id := range ids[1:] {
		b.index[id] = int32(i + 1)
	}
	return b
}

// UserVector is the mean of the vectors of the last MaxSeqLen history items; nil for an empty history.
func (b *Bundle) UserVector(history []int32) []float32 {
	if len(history) > b.MaxSeqLen {
		history = history[len(history)-b.MaxSeqLen:]
	}
	return b.Vectors.Mean(history)
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
