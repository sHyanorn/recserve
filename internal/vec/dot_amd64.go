package vec

import "golang.org/x/sys/cpu"

var useAVX2 = cpu.X86.HasAVX2 && cpu.X86.HasFMA

// dotAVX2 is dotGo in AVX2 and FMA; len(b) == len(a).
//
//go:noescape
func dotAVX2(a, b []float32) float64
