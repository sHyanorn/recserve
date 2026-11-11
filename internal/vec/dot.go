package vec

// Dot is the inner product, accumulated in float64. len(b) must be at
// least len(a). The additions follow one fixed order, the one AVX2 runs
// fastest in (D053): 16 partial sums over the full blocks of 16 elements,
// the first 4 of them also over a last block of 4, then the lanes, then the
// last elements one at a time. On amd64 with AVX2 and FMA it runs in
// assembly, elsewhere in Go, with the same result to the bit: a product of
// two float32 values is exact in float64, so fusing it with the addition
// changes nothing.
func Dot(a, b []float32) float64 {
	b = b[:len(a)]
	if useAVX2 {
		return dotAVX2(a, b)
	}
	return dotGo(a, b)
}

func dotGo(a, b []float32) float64 {
	b = b[:len(a)]
	var acc [4][4]float64 // acc[j][l] sums elements 16i + 4j + l
	i := 0
	for ; i+16 <= len(a); i += 16 {
		x, y := a[i:i+16:i+16], b[i:i+16:i+16]
		for j := range 4 {
			for l := range 4 {
				acc[j][l] += float64(x[4*j+l]) * float64(y[4*j+l])
			}
		}
	}
	for ; i+4 <= len(a); i += 4 {
		for l := range 4 {
			acc[0][l] += float64(a[i+l]) * float64(b[i+l])
		}
	}
	var v [4]float64
	for l := range 4 {
		v[l] = (acc[0][l] + acc[1][l]) + (acc[2][l] + acc[3][l])
	}
	s := (v[0] + v[2]) + (v[1] + v[3])
	for ; i < len(a); i++ {
		s += float64(a[i]) * float64(b[i])
	}
	return s
}
