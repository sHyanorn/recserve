package vec

import (
	"math"
	"math/rand/v2"
	"slices"
	"testing"
)

func TestTopKMatchesSort(t *testing.T) {
	r := rand.New(rand.NewPCG(1, 2))
	for trial := range 200 {
		rows, dim := 1+r.IntN(60), 1+r.IntN(8)
		data := make([]float32, rows*dim)
		for i := range data {
			data[i] = float32(r.IntN(5) - 2) // small integers make ties common
		}
		m, err := NewMatrix(dim, data)
		if err != nil {
			t.Fatal(err)
		}
		q := data[:dim]
		skip := func(id int32) bool { return id%7 == 3 }
		var all []Hit
		for id := int32(1); int(id) < rows; id++ {
			if !skip(id) {
				all = append(all, Hit{id, Dot(q, m.Row(id))})
			}
		}
		slices.SortFunc(all, func(a, b Hit) int {
			if Better(a, b) {
				return -1
			}
			return 1
		})
		k := r.IntN(rows + 2)
		want := all[:min(k, len(all))]
		if got := m.TopK(q, k, skip); len(got)+len(want) > 0 && !slices.Equal(got, want) {
			t.Fatalf("trial %d k=%d: got %v want %v", trial, k, got, want)
		}
	}
}

func TestDot32(t *testing.T) {
	r := rand.New(rand.NewPCG(5, 6))
	for n := range 40 {
		a, b := make([]float32, n), make([]float32, n+3) // b may be longer
		for i := range b {
			b[i] = float32(r.NormFloat64())
			if i < n {
				a[i] = float32(r.NormFloat64())
			}
		}
		want, scale := Dot(a, b), 0.0
		for i, v := range a {
			scale += math.Abs(float64(v) * float64(b[i]))
		}
		if got := float64(Dot32(a, b)); math.Abs(got-want) > 1e-6*scale {
			t.Fatalf("n=%d: Dot32 %g, Dot %g", n, got, want)
		}
	}
}

func TestInvNorms(t *testing.T) {
	m, err := NewMatrix(2, []float32{0, 0, 3, 4, 0, 0.5})
	if err != nil {
		t.Fatal(err)
	}
	if got := m.InvNorms(); !slices.Equal(got, []float32{0, 0.2, 2}) {
		t.Fatalf("got %v", got)
	}
}

func TestMean(t *testing.T) {
	m, err := NewMatrix(2, []float32{0, 0, 1, 2, 3, 6})
	if err != nil {
		t.Fatal(err)
	}
	if got := m.Mean([]int32{1, 2}); !slices.Equal(got, []float32{2, 4}) {
		t.Fatalf("got %v", got)
	}
	if m.Mean(nil) != nil {
		t.Fatal("mean of nothing")
	}
}
