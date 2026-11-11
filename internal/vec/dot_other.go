//go:build !amd64

package vec

const useAVX2 = false

func dotAVX2(a, b []float32) float64 { return dotGo(a, b) }

func dot32AVX2(a, b []float32) float32 { return dot32Go(a, b) }
