package main

import (
	"fmt"
	"math"
	"os"
	"strconv"
	"strings"

	"github.com/prometheus/common/expfmt"
	"github.com/prometheus/common/model"
)

// The replica whose share the M7 criteria judge: annshard-1, a replica of
// shard 0, by its address in the fault runs' -shard-addrs.
const (
	judgedReplica = "toxiproxy:19091"
	judgedShard   = "0"
)

// The shares of the M7 criteria: annshard-1's in [210, 270 s), and in each
// second of its return.
const (
	shareLate   = 0.40
	shareReturn = 0.30
)

// runReads are a fault run's per-second reads (scripts/bench-fault.sh).
type runReads struct {
	t0       float64 // Unix ms of the first request's due time
	list     []read
	bySecond map[int]read // the first read in each whole second from t0
}

// read is one of them: its time, the gateway's metrics of the fault
// criteria, and each container's CPU time.
type read struct {
	ms  float64 // Unix ms
	m   metrics
	cpu map[string]float64 // ns, by container
}

// readReads reads the reads of a run whose first request was due at t0
// (Unix ms): each a line "read <unix ms>", then lines of the gateway's
// /metrics, then lines "cpu <container> <ns>". A read without the
// gateway's lines, which did not answer, is left out.
func readReads(path string, t0 float64) (runReads, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return runReads{}, err
	}
	var blocks [][]string
	for line := range strings.Lines(string(data)) {
		line = strings.TrimSpace(line)
		switch {
		case strings.HasPrefix(line, "read "):
			blocks = append(blocks, []string{line})
		case line != "" && len(blocks) > 0:
			blocks[len(blocks)-1] = append(blocks[len(blocks)-1], line)
		}
	}
	r := runReads{t0: t0, bySecond: map[int]read{}}
	for _, b := range blocks {
		rd := read{cpu: map[string]float64{}}
		if rd.ms, err = strconv.ParseFloat(strings.TrimPrefix(b[0], "read "), 64); err != nil {
			return runReads{}, fmt.Errorf("%s: %w", path, err)
		}
		var gateway strings.Builder
		for _, l := range b[1:] {
			c, ok := strings.CutPrefix(l, "cpu ")
			if !ok {
				gateway.WriteString(l + "\n")
				continue
			}
			name, ns, _ := strings.Cut(c, " ")
			if rd.cpu[name], err = strconv.ParseFloat(ns, 64); err != nil {
				return runReads{}, fmt.Errorf("%s: %w", path, err)
			}
		}
		if gateway.Len() == 0 {
			continue
		}
		p := expfmt.NewTextParser(model.UTF8Validation)
		if rd.m, err = p.TextToMetricFamilies(strings.NewReader(gateway.String())); err != nil {
			return runReads{}, fmt.Errorf("%s: read at %.0f: %w", path, rd.ms, err)
		}
		r.list = append(r.list, rd)
		if s := r.second(rd); s >= 0 {
			if _, ok := r.bySecond[s]; !ok {
				r.bySecond[s] = rd
			}
		}
	}
	return r, nil
}

// second is the whole second from t0 in which rd was taken.
func (r runReads) second(rd read) int { return int(math.Floor((rd.ms - r.t0) / 1000)) }

// at returns the read of second s.
func (r runReads) at(s int) (read, bool) {
	rd, ok := r.bySecond[s]
	return rd, ok
}

// delta is the change of name's series with the labels want from the read
// of second from to that of second to; ok is false without both reads.
func (r runReads) delta(name string, want map[string]string, from, to int) (float64, bool) {
	a, ok0 := r.at(from)
	b, ok1 := r.at(to)
	if !ok0 || !ok1 {
		return 0, false
	}
	return b.m.sum(name, want) - a.m.sum(name, want), true
}

// deltaBefore is the change of name's series with the labels want from the
// last read taken before from to the last taken before to, both ms from t0;
// ok is false without both reads. A phase [from, to) so loses no change
// read just after from, and gains none read just after to.
func (r runReads) deltaBefore(name string, want map[string]string, from, to float64) (float64, bool) {
	a, ok0 := r.lastBefore(from)
	b, ok1 := r.lastBefore(to)
	if !ok0 || !ok1 {
		return 0, false
	}
	return b.m.sum(name, want) - a.m.sum(name, want), true
}

// lastBefore returns the last read taken before ms from t0.
func (r runReads) lastBefore(ms float64) (read, bool) {
	var last read
	ok := false
	for _, rd := range r.list {
		if rd.ms-r.t0 < ms && (!ok || rd.ms > last.ms) {
			last, ok = rd, true
		}
	}
	return last, ok
}

// share is annshard-1's answered calls over all those of its shard's
// replicas from the read of second from to that of second to; ok is false
// without both reads or with no answered call.
func (r runReads) share(from, to int) (float64, bool) {
	want := map[string]string{"shard": judgedShard, "result": "answered"}
	all, ok := r.delta("recserve_shard_calls_total", want, from, to)
	if !ok || all <= 0 {
		return 0, false
	}
	want["replica"] = judgedReplica
	one, _ := r.delta("recserve_shard_calls_total", want, from, to)
	return one / all, true
}

// shares is annshard-1's share in each second of [0, end s), nil where it
// has none.
func (r runReads) shares(end int) []*float64 {
	out := make([]*float64, end)
	for s := range out {
		if v, ok := r.share(s, s+1); ok {
			out[s] = &v
		}
	}
	return out
}

// returned is the time from removal (s from t0) to annshard-1's return: the
// first whole second t at or after removal, before end (s), the end of the
// recovery phase, such that its share is at least 30% in each second of
// [t, t + 10 s) before end; nil if there is none.
func (r runReads) returned(removal float64, end int) *float64 {
	for t := int(math.Ceil(removal)); t < end; t++ {
		back := true
		for s := t; s < min(t+10, end) && back; s++ {
			v, ok := r.share(s, s+1)
			back = ok && v >= shareReturn
		}
		if back {
			v := float64(t) - removal
			return &v
		}
	}
	return nil
}

// outlierDetection reports whether the gateway counted ejections, which it
// does only with outlier detection.
func (r runReads) outlierDetection() bool {
	for _, rd := range r.list {
		if len(rd.m["recserve_shard_ejections_total"].GetMetric()) > 0 {
			return true
		}
	}
	return false
}

// later is the later of the window recovery (whole s) and the replica's
// return (s), each from removal; nil if either is.
func later(window *int, back *float64) *float64 {
	if window == nil || back == nil {
		return nil
	}
	v := max(float64(*window), *back)
	return &v
}
