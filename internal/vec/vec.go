// Package vec holds item vectors and exact dot-product search over them.
package vec

import (
	"container/heap"
	"fmt"
	"math"
)

// Matrix is a row-major float32 matrix; row i is internal item ID i.
type Matrix struct {
	Dim  int
	Data []float32
}

// NewMatrix checks that data holds whole rows of dim values.
func NewMatrix(dim int, data []float32) (Matrix, error) {
	if dim <= 0 || len(data)%dim != 0 {
		return Matrix{}, fmt.Errorf("%d values do not form rows of %d", len(data), dim)
	}
	return Matrix{Dim: dim, Data: data}, nil
}

// Rows returns the number of rows.
func (m Matrix) Rows() int { return len(m.Data) / m.Dim }

// Row returns row i.
func (m Matrix) Row(i int32) []float32 { return m.Data[int(i)*m.Dim : (int(i)+1)*m.Dim] }

// Dot32 is the inner product in float32, over four independent partial
// sums: about twice as fast as Dot, with float32 rounding. len(b) must be
// at least len(a).
func Dot32(a, b []float32) float32 {
	b = b[:len(a)]
	var s0, s1, s2, s3 float32
	i := 0
	for ; i+4 <= len(a); i += 4 {
		x, y := a[i:i+4:i+4], b[i:i+4:i+4]
		s0 += x[0] * y[0]
		s1 += x[1] * y[1]
		s2 += x[2] * y[2]
		s3 += x[3] * y[3]
	}
	for ; i < len(a); i++ {
		s0 += a[i] * b[i]
	}
	return (s0 + s1) + (s2 + s3)
}

// InvNorms returns 1 / the Euclidean norm of every row, 0 for a zero row.
func (m Matrix) InvNorms() []float32 {
	out := make([]float32, m.Rows())
	for i := range out {
		row := m.Row(int32(i))
		if n := math.Sqrt(Dot(row, row)); n > 0 {
			out[i] = float32(1 / n)
		}
	}
	return out
}

// Mean returns the mean of the given rows, accumulated in float64; nil if ids is empty.
func (m Matrix) Mean(ids []int32) []float32 {
	if len(ids) == 0 {
		return nil
	}
	acc := make([]float64, m.Dim)
	for _, id := range ids {
		for j, v := range m.Row(id) {
			acc[j] += float64(v)
		}
	}
	out := make([]float32, m.Dim)
	for j, v := range acc {
		out[j] = float32(v / float64(len(ids)))
	}
	return out
}

// Hit is a scored internal ID.
type Hit struct {
	ID    int32
	Score float64
}

// Better orders hits by score, then by ID ascending.
func Better(a, b Hit) bool {
	return a.Score > b.Score || (a.Score == b.Score && a.ID < b.ID)
}

type worstFirst []Hit

func (h worstFirst) Len() int           { return len(h) }
func (h worstFirst) Less(i, j int) bool { return Better(h[j], h[i]) }
func (h worstFirst) Swap(i, j int)      { h[i], h[j] = h[j], h[i] }
func (h *worstFirst) Push(x any)        { *h = append(*h, x.(Hit)) }
func (h *worstFirst) Pop() any {
	o := *h
	x := o[len(o)-1]
	*h = o[:len(o)-1]
	return x
}

// TopK returns the k rows with the highest dot product with q, best first,
// over IDs 1 and up; rows for which skip returns true are left out.
func (m Matrix) TopK(q []float32, k int, skip func(int32) bool) []Hit {
	t := NewTop(k)
	for id := int32(1); int(id) < m.Rows(); id++ {
		if skip == nil || !skip(id) {
			t.Push(Hit{id, Dot(q, m.Row(id))})
		}
	}
	return t.Hits()
}

// Top keeps the best k hits pushed into it, by Better.
type Top struct {
	k int
	h worstFirst
}

// NewTop returns a Top that keeps k hits.
func NewTop(k int) *Top { return &Top{k: k, h: make(worstFirst, 0, max(k, 0)+1)} }

// Push offers c.
func (t *Top) Push(c Hit) {
	switch {
	case t.k <= 0:
	case len(t.h) < t.k:
		heap.Push(&t.h, c)
	case Better(c, t.h[0]):
		t.h[0] = c
		heap.Fix(&t.h, 0)
	}
}

// Hits returns the kept hits, best first, and empties t.
func (t *Top) Hits() []Hit {
	out := make([]Hit, len(t.h))
	for i := len(t.h) - 1; i >= 0; i-- {
		out[i] = heap.Pop(&t.h).(Hit)
	}
	return out
}
