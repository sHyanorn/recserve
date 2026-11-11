package vec

import (
	"encoding/binary"
	"math"
	"math/big"
	"math/rand/v2"
	"testing"
)

// sixteenLanes is the definition Dot follows (docs/DECISIONS.md D053),
// written out element by element: element k of each full block of 16 goes
// to partial sum k mod 16, those of a last block of 4 to sums 0 to 3; the
// sums are added as (s_l + s_4+l) + (s_8+l + s_12+l) for each lane l, the
// lanes as (l0 + l2) + (l1 + l3), and the last elements one at a time.
func sixteenLanes(a, b []float32) float64 {
	var s [16]float64
	n16 := len(a) / 16 * 16
	n4 := len(a) / 4 * 4
	for k := range n4 {
		lane := k % 16
		if k >= n16 {
			lane = k % 4
		}
		s[lane] += float64(a[k]) * float64(b[k])
	}
	var l [4]float64
	for i := range l {
		l[i] = (s[i] + s[4+i]) + (s[8+i] + s[12+i])
	}
	sum := (l[0] + l[2]) + (l[1] + l[3])
	for k := n4; k < len(a); k++ {
		sum += float64(a[k]) * float64(b[k])
	}
	return sum
}

// randomVector draws values over many orders of magnitude, so that the
// order of the additions shows in the result.
func randomVector(r *rand.Rand, n int) []float32 {
	v := make([]float32, n)
	for i := range v {
		v[i] = float32(r.NormFloat64() * math.Pow(10, float64(r.IntN(12)-6)))
	}
	return v
}

// Dot, the Go code and the assembly (where the CPU runs it) all follow
// the definition to the bit.
func TestDotSumsInSixteenLanes(t *testing.T) {
	r := rand.New(rand.NewPCG(5, 5))
	for n := range 301 {
		for range 20 {
			a, b := randomVector(r, n), randomVector(r, n+r.IntN(3))
			want := sixteenLanes(a, b)
			for name, dot := range implementations() {
				if got := dot(a, b); math.Float64bits(got) != math.Float64bits(want) {
					t.Fatalf("n %d: %s %v, want %v", n, name, got, want)
				}
			}
		}
	}
}

func implementations() map[string]func(a, b []float32) float64 {
	m := map[string]func(a, b []float32) float64{"Dot": Dot, "Go": dotGo}
	if useAVX2 {
		m["AVX2"] = func(a, b []float32) float64 { return dotAVX2(a, b[:len(a)]) }
	}
	return m
}

func TestDotSpecialValues(t *testing.T) {
	inf, nan := float32(math.Inf(1)), float32(math.NaN())
	cases := []struct {
		a, b []float32
		want float64
	}{
		{nil, nil, 0},
		{[]float32{math.MaxFloat32}, []float32{math.MaxFloat32}, float64(math.MaxFloat32) * math.MaxFloat32},
		{[]float32{math.SmallestNonzeroFloat32}, []float32{math.SmallestNonzeroFloat32}, float64(math.SmallestNonzeroFloat32) * math.SmallestNonzeroFloat32},
		{[]float32{inf, 1}, []float32{1, 1}, math.Inf(1)},
		{[]float32{inf, -inf}, []float32{1, 1}, math.NaN()},
		{[]float32{nan, 1, 2, 3, 4}, []float32{1, 1, 1, 1, 1}, math.NaN()},
	}
	for i, c := range cases {
		for name, dot := range implementations() {
			got := dot(c.a, c.b)
			if !(got == c.want || math.IsNaN(got) && math.IsNaN(c.want)) {
				t.Errorf("case %d: %s %v, want %v", i, name, got, c.want)
			}
		}
	}
}

// The result is within the usual bound of a float64 sum of n exact
// products: n * 2^-53 * the sum of their magnitudes.
func TestDotAccuracy(t *testing.T) {
	r := rand.New(rand.NewPCG(9, 9))
	for range 200 {
		n := r.IntN(300)
		a, b := randomVector(r, n), randomVector(r, n)
		exact, magnitude := new(big.Float).SetPrec(2000), 0.0
		for i := range a {
			p := float64(a[i]) * float64(b[i])
			exact.Add(exact, big.NewFloat(p))
			magnitude += math.Abs(p)
		}
		want, _ := exact.Float64()
		if diff := math.Abs(Dot(a, b) - want); diff > float64(n)*0x1p-53*magnitude {
			t.Fatalf("n %d: off by %g, bound %g", n, diff, float64(n)*0x1p-53*magnitude)
		}
	}
}

func FuzzDot(f *testing.F) {
	f.Add([]byte{0, 0, 128, 63, 0, 0, 0, 64}, []byte{0, 0, 64, 64, 0, 0, 128, 64})
	f.Fuzz(func(t *testing.T, x, y []byte) {
		a, b := floats(x), floats(y)
		n := min(len(a), len(b))
		a, b = a[:n], b[:n]
		want := sixteenLanes(a, b)
		for name, dot := range implementations() {
			if got := dot(a, b); math.Float64bits(got) != math.Float64bits(want) && !(math.IsNaN(got) && math.IsNaN(want)) {
				t.Fatalf("%s %v, want %v", name, got, want)
			}
		}
	})
}

func floats(b []byte) []float32 {
	out := make([]float32, len(b)/4)
	for i := range out {
		out[i] = math.Float32frombits(binary.LittleEndian.Uint32(b[4*i:]))
	}
	return out
}
