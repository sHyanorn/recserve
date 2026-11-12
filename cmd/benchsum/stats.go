package main

import (
	"math"
	"slices"

	"github.com/sHyanorn/recserve/internal/bench"
)

// stats describes the samples due in a span of a run.
type stats struct {
	Answers  int                `json:"answers"` // recommendations served
	P50      float64            `json:"p50_ms"`
	P90      float64            `json:"p90_ms"`
	P99      float64            `json:"p99_ms"`
	P999     float64            `json:"p99_9_ms"`
	Max      float64            `json:"max_ms"`
	Errors   int                `json:"errors"` // transport errors and 5xx, either endpoint; loadgen's outcome "error"
	Busy     int                `json:"busy"`   // 503, also in Errors
	Degraded float64            `json:"degraded_share"`
	Reasons  map[string]float64 `json:"degraded_by_reason"` // share of the answers naming each reason
	Coverage float64            `json:"mean_coverage"`      // answered/shards over the answers that searched shards
	Events   int                `json:"events"`
	EventP99 float64            `json:"events_p99_ms"`
}

// describe computes stats of the samples due in [from, to) ms.
func describe(samples []sample, from, to float64) stats {
	var st stats
	st.Reasons = map[string]float64{}
	var lat, ev []int64 // µs
	var degraded, searched int
	var coverage float64
	for _, s := range samples {
		if s.due < from || s.due >= to {
			continue
		}
		if s.failed() {
			st.Errors++
			if s.status == 503 {
				st.Busy++
			}
		}
		if s.events {
			if s.accepted() {
				st.Events++
				ev = append(ev, micros(s.latency))
			}
			continue
		}
		if !s.answer() {
			continue
		}
		lat = append(lat, micros(s.latency))
		if len(s.degraded) > 0 {
			degraded++
		}
		for _, r := range s.degraded {
			st.Reasons[r]++
		}
		if s.shards > 0 {
			searched++
			coverage += float64(s.answered) / float64(s.shards)
		}
	}
	st.Answers = len(lat)
	if st.Answers > 0 {
		slices.Sort(lat)
		st.P50, st.P90, st.P99, st.P999 = ms(lat, 0.5), ms(lat, 0.9), ms(lat, 0.99), ms(lat, 0.999)
		st.Max = float64(lat[len(lat)-1]) / 1000
		st.Degraded = float64(degraded) / float64(st.Answers)
		for r, n := range st.Reasons {
			st.Reasons[r] = n / float64(st.Answers)
		}
	}
	if searched > 0 {
		st.Coverage = coverage / float64(searched)
	}
	if len(ev) > 0 {
		slices.Sort(ev)
		st.EventP99 = ms(ev, 0.99)
	}
	return st
}

func micros(ms float64) int64 { return int64(math.Round(ms * 1000)) }

func ms(sorted []int64, q float64) float64 { return float64(bench.Percentile(sorted, q)) / 1000 }

// excess is each reason's share in st beyond its share in base.
func excess(st, base stats) map[string]float64 {
	out := map[string]float64{}
	for r, v := range st.Reasons {
		out[r] = v - base.Reasons[r]
	}
	return out
}

// recovery returns the seconds from removal (ms) until the first 10 s
// window, starting a whole second after removal, whose p99 is within 1.2
// times healthy's and whose degraded share is within 1 percentage point of
// healthy's either way; ok is false if no such window starts before end
// (ms), the end of the recovery phase. A window that runs past end holds
// the responses due before it.
func recovery(samples []sample, healthy stats, removal, end float64) (seconds int, ok bool) {
	for t := 0; removal+float64(t)*1000 < end; t++ {
		from := removal + float64(t)*1000
		w := describe(samples, from, from+10_000)
		if w.Answers > 0 && w.P99 <= 1.2*healthy.P99 && math.Abs(w.Degraded-healthy.Degraded) <= 0.01 {
			return t, true
		}
	}
	return 0, false
}

// windows describes each whole second of [from, to) ms.
func windows(samples []sample, from, to float64) []stats {
	var out []stats
	for t := from; t+1000 <= to; t += 1000 {
		out = append(out, describe(samples, t, t+1000))
	}
	return out
}
