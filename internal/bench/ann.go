package bench

import (
	"encoding/binary"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/sHyanorn/recserve/internal/vec"
)

const (
	// TruthK is the number of true neighbors per query in a ground-truth file.
	TruthK = 100
	// RecallTol absorbs float32 rounding when a returned item's similarity
	// is compared with that of the true k-th neighbor.
	RecallTol = 1e-5
)

// ANNSet is a dataset as experiments/m0/ann/prep.py writes it: NAME.dim,
// NAME_base.f32 and NAME_query.f32 (little-endian float32 rows), and
// NAME_gt.i32 (per query the IDs of the exact top 100 by inner product,
// best first).
type ANNSet struct {
	Dim   int
	Base  []float32
	Query []float32
	Truth []int32
}

// LoadANNSet reads set name from dir and checks that the files agree.
func LoadANNSet(dir, name string) (*ANNSet, error) {
	path := func(suffix string) string { return filepath.Join(dir, name+suffix) }
	b, err := os.ReadFile(path(".dim"))
	if err != nil {
		return nil, err
	}
	dim, err := strconv.Atoi(strings.TrimSpace(string(b)))
	if err != nil || dim <= 0 {
		return nil, fmt.Errorf("%s: dimension %q", path(".dim"), b)
	}
	s := &ANNSet{Dim: dim}
	if s.Base, err = readF32(path("_base.f32"), dim); err != nil {
		return nil, err
	}
	if s.Query, err = readF32(path("_query.f32"), dim); err != nil {
		return nil, err
	}
	if s.Truth, err = readI32(path("_gt.i32")); err != nil {
		return nil, err
	}
	if len(s.Truth) != s.NQuery()*TruthK {
		return nil, fmt.Errorf("%s: %d IDs for %d queries", path("_gt.i32"), len(s.Truth), s.NQuery())
	}
	for _, id := range s.Truth {
		if id < 0 || int(id) >= s.NBase() {
			return nil, fmt.Errorf("%s: ID %d outside %d rows", path("_gt.i32"), id, s.NBase())
		}
	}
	return s, nil
}

// NBase returns the number of base rows.
func (s *ANNSet) NBase() int { return len(s.Base) / s.Dim }

// NQuery returns the number of queries.
func (s *ANNSet) NQuery() int { return len(s.Query) / s.Dim }

// Row returns base row i.
func (s *ANNSet) Row(i int32) []float32 { return s.Base[int(i)*s.Dim : (int(i)+1)*s.Dim] }

// QueryVec returns query q.
func (s *ANNSet) QueryVec(q int) []float32 { return s.Query[q*s.Dim : (q+1)*s.Dim] }

// Recall is the ann-benchmarks definition for query q: the share of the
// first k of got whose inner product with the query is at least that of
// the true k-th neighbor, less RecallTol, so ties do not lower it. Repeated
// IDs count once; negative IDs (faiss pads missing results with -1) and IDs
// outside the base count as misses. k must be at most TruthK.
func (s *ANNSet) Recall(q int, got []int32, k int) float64 {
	qv := s.QueryVec(q)
	thr := vec.Dot(qv, s.Row(s.Truth[q*TruthK+k-1])) - RecallTol
	seen := make(map[int32]bool, k)
	n := 0
	for _, id := range got[:min(k, len(got))] {
		if id < 0 || int(id) >= s.NBase() || seen[id] {
			continue
		}
		seen[id] = true
		if vec.Dot(qv, s.Row(id)) >= thr {
			n++
		}
	}
	return float64(n) / float64(k)
}

// CheckTruth verifies query q's true neighbors with an exact scan: they are
// distinct and best first, and no other row is more similar than the last
// of them by more than RecallTol. A ground truth computed for other data
// fails it.
func (s *ANNSet) CheckTruth(q int) error {
	qv := s.QueryVec(q)
	in := make(map[int32]bool, TruthK)
	last := math.Inf(1)
	for r, id := range s.Truth[q*TruthK : (q+1)*TruthK] {
		if in[id] {
			return fmt.Errorf("query %d: neighbor %d repeated", q, id)
		}
		in[id] = true
		sim := vec.Dot(qv, s.Row(id))
		if sim > last+RecallTol {
			return fmt.Errorf("query %d: neighbor %d at rank %d is more similar than rank %d", q, id, r+1, r)
		}
		last = sim
	}
	for id := range int32(s.NBase()) {
		if !in[id] && vec.Dot(qv, s.Row(id)) > last+RecallTol {
			return fmt.Errorf("query %d: row %d is more similar than the true neighbor at rank %d", q, id, TruthK)
		}
	}
	return nil
}

func readF32(path string, dim int) ([]float32, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	if len(b)%(4*dim) != 0 {
		return nil, fmt.Errorf("%s: %d bytes do not form rows of %d float32", path, len(b), dim)
	}
	v := make([]float32, len(b)/4)
	for i := range v {
		v[i] = math.Float32frombits(binary.LittleEndian.Uint32(b[4*i:]))
	}
	return v, nil
}

func readI32(path string) ([]int32, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	if len(b)%4 != 0 {
		return nil, fmt.Errorf("%s: %d bytes do not form int32 values", path, len(b))
	}
	v := make([]int32, len(b)/4)
	for i := range v {
		v[i] = int32(binary.LittleEndian.Uint32(b[4*i:]))
	}
	return v, nil
}
