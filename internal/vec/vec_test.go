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

func TestInvNorms(t *testing.T) {
	m, err := NewMatrix(2, []float32{0, 0, 3, 4, 0, 0.5})
	if err != nil {
		t.Fatal(err)
	}
	if got := m.InvNorms(); !slices.Equal(got, []float32{0, 0.2, 2}) {
		t.Fatalf("got %v", got)
	}
}

// TestInvNormsOfTinyRows covers rows whose norm, though not 0, is below
// 1/math.MaxFloat32: 1/norm is not a float32, so they count as zero rows.
func TestInvNormsOfTinyRows(t *testing.T) {
	m, err := NewMatrix(2, []float32{1e-45, 0, 1e-39, 1e-39, 1.1754942e-38, 0})
	if err != nil {
		t.Fatal(err)
	}
	got := m.InvNorms()
	if got[0] != 0 || got[1] != 0 {
		t.Errorf("rows of norm %g and %g: inverse norms %v, want 0", 1e-45, math.Sqrt2*1e-39, got[:2])
	}
	if want := float32(1 / float64(float32(1.1754942e-38))); got[2] != want {
		t.Errorf("row of norm 1.1754942e-38: inverse norm %g, want %g", got[2], want)
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
