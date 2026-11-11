//go:build !amd64

package vec

const useAVX2 = false

func dotAVX2(a, b []float32) float64 { return dotGo(a, b) }
