package vec

// Dot is the inner product, accumulated in float64. len(b) must be at
// least len(a). The additions follow one fixed order, the one AVX2 runs
// fastest in (D053): 16 partial sums over the full blocks of 16 elements,
// the first 4 of them also over the remaining blocks of 4, then the lanes,
// then the last elements one at a time. On amd64 with AVX2 and FMA it runs in
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

// Dot32 is the inner product in float32, for the HNSW index and MMR, where
// float32 rounding is enough. len(b) must be at least len(a). Each product
// is rounded to float32 and the additions follow one fixed order (D055): 32
// partial sums over the full blocks of 32 elements, the first 8 of them also
// over the last blocks of 8, then the lanes, then the last elements one at a
// time. On amd64 with AVX2 it runs in assembly, elsewhere in Go, with the
// same result to the bit; neither fuses a multiply with an add.
func Dot32(a, b []float32) float32 {
	b = b[:len(a)]
	if useAVX2 {
		return dot32AVX2(a, b)
	}
	return dot32Go(a, b)
}

// dot32Go converts every product to float32, which keeps the compiler from
// fusing it with the addition on CPUs that have FMA.
func dot32Go(a, b []float32) float32 {
	b = b[:len(a)]
	var acc [4][8]float32 // acc[j][l] sums elements 32i + 8j + l
	i := 0
	for ; i+32 <= len(a); i += 32 {
		x, y := a[i:i+32:i+32], b[i:i+32:i+32]
		for j := range 4 {
			for l := range 8 {
				acc[j][l] += float32(x[8*j+l] * y[8*j+l])
			}
		}
	}
	for ; i+8 <= len(a); i += 8 {
		for l := range 8 {
			acc[0][l] += float32(a[i+l] * b[i+l])
		}
	}
	var v [8]float32
	for l := range 8 {
		v[l] = (acc[0][l] + acc[1][l]) + (acc[2][l] + acc[3][l])
	}
	s := ((v[0] + v[4]) + (v[2] + v[6])) + ((v[1] + v[5]) + (v[3] + v[7]))
	for ; i < len(a); i++ {
		s += float32(a[i] * b[i])
	}
	return s
}
