package bench

import (
	"math/rand/v2"
	"runtime"
	"slices"
	"time"

	"golang.org/x/sys/unix"
)

const (
	noiseDim     = 64
	noiseVectors = 16384
	noisePasses  = 200
	noiseTimings = 30
	// NoiseLimit is the slowdown against the session reference that flags a run.
	NoiseLimit = 1.10
	// NoiseAttempts is the number of tries a flagged run gets in total.
	NoiseAttempts = 3
)

var (
	noiseData  = makeNoiseData()
	noiseSink  float32
	noiseQuery = noiseData[:noiseDim]
)

func makeNoiseData() []float32 {
	r := rand.New(rand.NewPCG(1, 1))
	v := make([]float32, noiseDim*noiseVectors)
	for i := range v {
		v[i] = r.Float32()*2 - 1
	}
	return v
}

func noisePass() float32 {
	var best float32
	for i := 0; i < noiseVectors; i++ {
		row := noiseData[i*noiseDim : (i+1)*noiseDim]
		var s float32
		for j, q := range noiseQuery {
			s += q * row[j]
		}
		best = max(best, s)
	}
	return best
}

// MeasureNoise runs the fixed workload pinned to cpu and returns the median
// nanoseconds of one pass.
func MeasureNoise(cpu int) float64 {
	runtime.LockOSThread()
	defer runtime.UnlockOSThread()
	var old, set unix.CPUSet
	if err := unix.SchedGetaffinity(0, &old); err == nil {
		set.Set(cpu)
		if unix.SchedSetaffinity(0, &set) == nil {
			defer unix.SchedSetaffinity(0, &old) //nolint:errcheck
		}
	}
	t := make([]time.Duration, noiseTimings)
	for i := range t {
		start := time.Now()
		for range noisePasses {
			noiseSink += noisePass()
		}
		t[i] = time.Since(start)
	}
	slices.Sort(t)
	return float64(t[len(t)/2]) / noisePasses
}

// Noise is the baseline record of one measured run.
type Noise struct {
	Reference float64 `json:"reference_ns"`
	Before    float64 `json:"before_ns"`
	After     float64 `json:"after_ns"`
	Attempt   int     `json:"attempt"`
	Flagged   bool    `json:"noisy"`
}

// Bracketed runs fn between two noise baselines on cpu and repeats it while a
// baseline is more than NoiseLimit slower than ref, up to NoiseAttempts tries.
// The last attempt is returned, flagged if it is still noisy.
func Bracketed[T any](ref float64, cpu int, fn func() T) (T, Noise) {
	for attempt := 1; ; attempt++ {
		before := MeasureNoise(cpu)
		r := fn()
		after := MeasureNoise(cpu)
		n := Noise{Reference: ref, Before: before, After: after, Attempt: attempt,
			Flagged: before > NoiseLimit*ref || after > NoiseLimit*ref}
		if !n.Flagged || attempt == NoiseAttempts {
			return r, n
		}
	}
}
