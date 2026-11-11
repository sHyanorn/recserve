package vec

import (
	"math/rand/v2"
	"strconv"
	"testing"
)

// BenchmarkDot uses nothing but Dot, so that scripts/bench-compare.sh can
// run it on a commit from before Dot changed.
func BenchmarkDot(b *testing.B) {
	r := rand.New(rand.NewPCG(1, 1))
	for _, n := range []int{64, 100} {
		x, y := benchVector(r, n), benchVector(r, n)
		b.Run(strconv.Itoa(n), func(b *testing.B) {
			var s float64
			for b.Loop() {
				s += Dot(x, y)
			}
			_ = s
		})
	}
}

// BenchmarkDot32 uses nothing but Dot32, for the same reason.
func BenchmarkDot32(b *testing.B) {
	r := rand.New(rand.NewPCG(1, 1))
	for _, n := range []int{64, 100} {
		x, y := benchVector(r, n), benchVector(r, n)
		b.Run(strconv.Itoa(n), func(b *testing.B) {
			var s float32
			for b.Loop() {
				s += Dot32(x, y)
			}
			_ = s
		})
	}
}

func benchVector(r *rand.Rand, n int) []float32 {
	v := make([]float32, n)
	for i := range v {
		v[i] = float32(r.NormFloat64())
	}
	return v
}
