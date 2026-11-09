package bench

import (
	"fmt"
	"math/rand/v2"
	"runtime"
	"slices"
	"sync"
	"time"

	"golang.org/x/sys/unix"
)

// The noise baseline of docs/BENCHMARKS.md: a dot product of a 64-value
// query against 16,384 rows, 200 passes, timed 30 times.
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

// The workload's speed depends on where the linker places its inner loop:
// the same instructions ran about 20% faster in a binary where the loop
// fits in one 64-byte line than in one where it straddles two. Every
// baseline of a session is therefore taken by the same cmd/noisebase
// binary, never inside the measured program. The source follows
// experiments/m0/cmd/noise.
var (
	noiseOnce  sync.Once
	noiseData  []float32
	noiseQuery []float32
	noiseSink  float32
)

func makeNoiseData() {
	r := rand.New(rand.NewPCG(1, 1))
	noiseData = make([]float32, noiseDim*noiseVectors)
	for i := range noiseData {
		noiseData[i] = r.Float32()*2 - 1
	}
	noiseQuery = noiseData[:noiseDim]
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

// MeasureNoise runs the workload pinned to cpu and returns the median
// nanoseconds of one pass. It fails if the thread cannot be pinned, rather
// than measure unpinned.
func MeasureNoise(cpu int) (float64, error) {
	var ns float64
	err := pinned(cpu, func() { ns = noiseRun() })
	return ns, err
}

// setAffinity is unix.SchedSetaffinity; tests replace it.
var setAffinity = unix.SchedSetaffinity

// pinned runs fn on a goroutine of its own, locked to its thread and the
// thread pinned to cpu. If the thread's affinity cannot be restored
// afterwards, the goroutine exits still locked, so the runtime terminates
// the thread (or parks it for good, if it is the main thread) instead of
// returning it, pinned, to the pool.
func pinned(cpu int, fn func()) error {
	if cpu < 0 {
		return fmt.Errorf("CPU %d is negative", cpu)
	}
	done := make(chan error, 1)
	go func() {
		runtime.LockOSThread()
		var old, set unix.CPUSet
		if err := unix.SchedGetaffinity(0, &old); err != nil {
			runtime.UnlockOSThread()
			done <- fmt.Errorf("reading CPU affinity: %w", err)
			return
		}
		set.Set(cpu) // a CPU beyond the set leaves it empty, which the kernel rejects
		if err := setAffinity(0, &set); err != nil {
			runtime.UnlockOSThread()
			done <- fmt.Errorf("pinning to CPU %d: %w", cpu, err)
			return
		}
		fn()
		if err := setAffinity(0, &old); err != nil {
			done <- fmt.Errorf("restoring CPU affinity: %w", err)
			return // still locked: the thread is discarded
		}
		runtime.UnlockOSThread()
		done <- nil
	}()
	return <-done
}

func noiseRun() float64 {
	noiseOnce.Do(makeNoiseData)
	t := make([]time.Duration, noiseTimings)
	for i := range t {
		start := time.Now()
		for range noisePasses {
			noiseSink += noisePass()
		}
		t[i] = time.Since(start)
	}
	runtime.KeepAlive(noiseSink) // a read, for linters; the writes keep the passes
	return Median(t) / noisePasses
}

// Median returns the median of v, the mean of the two middle values when
// len(v) is even; 0 for no values.
func Median[T ~int64 | ~float64](v []T) float64 {
	if len(v) == 0 {
		return 0
	}
	s := slices.Clone(v)
	slices.Sort(s)
	n := len(s)
	if n%2 == 1 {
		return float64(s[n/2])
	}
	return (float64(s[n/2-1]) + float64(s[n/2])) / 2
}

// Noisy reports whether a baseline is more than NoiseLimit slower than ref;
// ref <= 0 disables the check.
func Noisy(baseline, ref float64) bool {
	return ref > 0 && baseline > NoiseLimit*ref
}

// Noise is the baseline record of one measured run.
type Noise struct {
	Reference float64 `json:"reference_ns"`
	Before    float64 `json:"before_ns"`
	After     float64 `json:"after_ns"`
	Attempt   int     `json:"attempt"`
	Flagged   bool    `json:"noisy"`
}

// Bracketed runs fn between two baselines taken by measure, and repeats it
// while either is more than NoiseLimit slower than ref, up to NoiseAttempts
// tries. It returns the last attempt, flagged if it is still noisy.
func Bracketed[T any](ref float64, measure func() float64, fn func() T) (T, Noise) {
	for attempt := 1; ; attempt++ {
		before := measure()
		r := fn()
		after := measure()
		n := Noise{Reference: ref, Before: before, After: after, Attempt: attempt,
			Flagged: Noisy(before, ref) || Noisy(after, ref)}
		if !n.Flagged || attempt == NoiseAttempts {
			return r, n
		}
	}
}
