package main

import (
	"cmp"
	"compress/gzip"
	"encoding/csv"
	"encoding/json"
	"errors"
	"net/http"
	"os"
	"slices"
	"strconv"
	"strings"
	"time"
)

// sample is one measured request, for analysis after the run: when it was
// due, how long until its reply or failure, and what the reply said.
type sample struct {
	i                int
	path             string
	due, latency     time.Duration // due from the run's start; latency from due to reply
	status           int           // 0 for a transport error
	outcome          string
	degraded         []string
	answered, shards int // the recommendation's shard coverage; -1 without one
}

func newSample(j job, t0 time.Time, path string, status int, outcome string, data []byte, done time.Time) sample {
	s := sample{i: j.i, path: path, due: j.planned.Sub(t0), latency: done.Sub(j.planned), status: status, outcome: outcome, answered: -1, shards: -1}
	if path == "/v1/recommend" && status == http.StatusOK {
		var r struct {
			Degraded []string
			Coverage *struct{ Shards, Answered int }
		}
		if json.Unmarshal(data, &r) == nil {
			s.degraded = r.Degraded
			if r.Coverage != nil {
				s.answered, s.shards = r.Coverage.Answered, r.Coverage.Shards
			}
		}
	}
	return s
}

// writeSamples writes samples in request order as gzipped CSV, times in
// milliseconds and the degradation reasons joined by "|".
func writeSamples(path string, samples []sample) (err error) {
	slices.SortFunc(samples, func(a, b sample) int { return cmp.Compare(a.i, b.i) })
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	defer func() { err = errors.Join(err, f.Close()) }()
	z := gzip.NewWriter(f)
	w := csv.NewWriter(z)
	ms := func(d time.Duration) string { return strconv.FormatFloat(d.Seconds()*1000, 'f', 3, 64) }
	_ = w.Write([]string{"i", "path", "due_ms", "latency_ms", "status", "outcome", "degraded", "answered", "shards"}) //nolint:errcheck // reported by w.Error
	for _, s := range samples {
		_ = w.Write([]string{ //nolint:errcheck // reported by w.Error
			strconv.Itoa(s.i), s.path, ms(s.due), ms(s.latency), strconv.Itoa(s.status), s.outcome,
			strings.Join(s.degraded, "|"), strconv.Itoa(s.answered), strconv.Itoa(s.shards),
		})
	}
	w.Flush()
	return errors.Join(w.Error(), z.Close())
}
