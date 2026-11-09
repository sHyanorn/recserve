package bench

import (
	"math"
	"runtime"
	"runtime/metrics"
)

// GCSnap is the runtime's stop-the-world GC pause histogram and cycle count
// at one moment (D017).
type GCSnap struct {
	counts  []uint64
	buckets []float64 // seconds; len(counts)+1 boundaries
	cycles  uint64
}

// ReadGC takes a GCSnap.
func ReadGC() GCSnap {
	s := []metrics.Sample{
		{Name: "/sched/pauses/total/gc:seconds"},
		{Name: "/gc/cycles/total:gc-cycles"},
	}
	metrics.Read(s)
	h := s[0].Value.Float64Histogram()
	return GCSnap{counts: append([]uint64(nil), h.Counts...), buckets: h.Buckets, cycles: s[1].Value.Uint64()}
}

// GCStats are the GC pauses and cycles between two snapshots. The histogram
// gives a pause only as a bucket, so the longest pause is reported by the
// bounds of the highest bucket hit.
type GCStats struct {
	Cycles        uint64  `json:"num_gc"`
	Pauses        uint64  `json:"pauses"`
	MaxPauseLower float64 `json:"max_pause_lower_ms"`
	MaxPauseUpper float64 `json:"max_pause_upper_ms"`
}

// GCBetween returns the pauses and cycles from a to b.
func GCBetween(a, b GCSnap) GCStats {
	st := GCStats{Cycles: b.cycles - a.cycles}
	found := false
	for i := len(b.counts) - 1; i >= 0; i-- {
		d := b.counts[i] - a.counts[i]
		st.Pauses += d
		if d > 0 && !found {
			found = true
			lo, hi := b.buckets[i], b.buckets[i+1]
			if math.IsInf(lo, -1) {
				lo = 0
			}
			if math.IsInf(hi, 1) {
				hi = lo
			}
			st.MaxPauseLower, st.MaxPauseUpper = lo*1e3, hi*1e3
		}
	}
	return st
}

// HeapObjectsMiB collects garbage and returns the memory held by live and
// not yet swept heap objects.
func HeapObjectsMiB() float64 {
	runtime.GC()
	s := []metrics.Sample{{Name: "/memory/classes/heap/objects:bytes"}}
	metrics.Read(s)
	return float64(s[0].Value.Uint64()) / (1 << 20)
}
