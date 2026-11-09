// Package vec holds item vectors and exact dot-product search over them.
package vec

import (
	"container/heap"
	"fmt"
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

// Dot is the inner product, accumulated in float64.
func Dot(a, b []float32) float64 {
	var s float64
	for i, v := range a {
		s += float64(v) * float64(b[i])
	}
	return s
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
	if k <= 0 {
		return nil
	}
	h := make(worstFirst, 0, k+1)
	for id := int32(1); int(id) < m.Rows(); id++ {
		if skip != nil && skip(id) {
			continue
		}
		c := Hit{id, Dot(q, m.Row(id))}
		if len(h) < k {
			heap.Push(&h, c)
		} else if Better(c, h[0]) {
			h[0] = c
			heap.Fix(&h, 0)
		}
	}
	out := make([]Hit, len(h))
	for i := len(h) - 1; i >= 0; i-- {
		out[i] = heap.Pop(&h).(Hit)
	}
	return out
}
