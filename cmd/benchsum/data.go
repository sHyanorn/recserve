package main

import (
	"bufio"
	"compress/gzip"
	"encoding/csv"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"
	"time"

	dto "github.com/prometheus/client_model/go"
	"github.com/prometheus/common/expfmt"
	"github.com/prometheus/common/model"
)

// sample is one request as loadgen's -samples wrote it, times in ms.
type sample struct {
	events           bool // /v1/events, else /v1/recommend
	due, latency     float64
	status           int // 0 for a transport error
	degraded         []string
	answered, shards int // -1 without coverage
}

// failed is an error of the fault criteria: a transport error or any 5xx.
func (s sample) failed() bool { return s.status == 0 || s.status >= 500 }

// answer is a recommendation the gateway served.
func (s sample) answer() bool { return !s.events && s.status == 200 }

func readSamples(path string) ([]sample, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close() //nolint:errcheck // read only
	z, err := gzip.NewReader(f)
	if err != nil {
		return nil, err
	}
	r := csv.NewReader(bufio.NewReader(z))
	if _, err := r.Read(); err != nil { // header
		return nil, err
	}
	var out []sample
	for {
		rec, err := r.Read()
		if errors.Is(err, io.EOF) {
			return out, nil
		}
		if err != nil {
			return nil, err
		}
		s := sample{events: rec[1] == "/v1/events"}
		var errs [5]error
		s.due, errs[0] = strconv.ParseFloat(rec[2], 64)
		s.latency, errs[1] = strconv.ParseFloat(rec[3], 64)
		s.status, errs[2] = strconv.Atoi(rec[4])
		s.answered, errs[3] = strconv.Atoi(rec[7])
		s.shards, errs[4] = strconv.Atoi(rec[8])
		if err := errors.Join(errs[:]...); err != nil {
			return nil, fmt.Errorf("%s: sample %s: %w", path, rec[0], err)
		}
		if rec[6] != "" {
			s.degraded = strings.Split(rec[6], "|")
		}
		out = append(out, s)
	}
}

// loadReport is the part of loadgen's JSON report the summaries use.
type loadReport struct {
	Start    time.Time `json:"start"`
	Target   float64   `json:"target_rate"`
	Achieved float64   `json:"achieved_rate"`
	Warmup   float64   `json:"warmup_s"`
	Duration float64   `json:"duration_s"`
	Valid    bool      `json:"valid"`
	Problems []string  `json:"problems"`
}

func readJSON(path string, v any) error {
	data, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	return json.Unmarshal(data, v)
}

func writeJSON(path string, v any) error {
	data, err := json.MarshalIndent(v, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(path, append(data, '\n'), 0o644)
}

// noisy reports whether either noise baseline of a run is flagged.
func noisy(prefix string) (bool, error) {
	for _, side := range []string{"before", "after"} {
		var n struct{ Noisy bool }
		if err := readJSON(prefix+".noise-"+side+".json", &n); err != nil {
			return false, err
		}
		if n.Noisy {
			return true, nil
		}
	}
	return false, nil
}

// metrics is a scrape of /metrics.
type metrics map[string]*dto.MetricFamily

func readMetrics(path string) (metrics, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close() //nolint:errcheck // read only
	p := expfmt.NewTextParser(model.UTF8Validation)
	return p.TextToMetricFamilies(f)
}

// sum adds the values of name's counters whose labels include want.
func (m metrics) sum(name string, want map[string]string) float64 {
	var total float64
	for _, x := range m[name].GetMetric() {
		if has(x, want) {
			total += x.GetCounter().GetValue()
		}
	}
	return total
}

// by adds the values of name's counters by the value of label.
func (m metrics) by(name, label string) map[string]float64 {
	out := map[string]float64{}
	for _, x := range m[name].GetMetric() {
		for _, l := range x.GetLabel() {
			if l.GetName() == label {
				out[l.GetValue()] += x.GetCounter().GetValue()
			}
		}
	}
	return out
}

func has(x *dto.Metric, want map[string]string) bool {
	n := 0
	for _, l := range x.GetLabel() {
		if v, ok := want[l.GetName()]; ok && v == l.GetValue() {
			n++
		}
	}
	return n == len(want)
}

// histogramQuantile is q of the histogram name with the labels want, by
// linear interpolation within the bucket that holds it, as Prometheus's
// histogram_quantile; between two scrapes when before is not nil.
func histogramQuantile(after, before metrics, name string, want map[string]string, q float64) float64 {
	cum := func(m metrics) ([]float64, []float64) {
		for _, x := range m[name].GetMetric() {
			if has(x, want) {
				var bounds, counts []float64
				for _, b := range x.GetHistogram().GetBucket() {
					bounds = append(bounds, b.GetUpperBound())
					counts = append(counts, float64(b.GetCumulativeCount()))
				}
				return bounds, counts
			}
		}
		return nil, nil
	}
	bounds, counts := cum(after)
	if before != nil {
		_, c0 := cum(before)
		for i := range c0 {
			counts[i] -= c0[i]
		}
	}
	if len(counts) == 0 || counts[len(counts)-1] == 0 {
		return 0
	}
	rank := q * counts[len(counts)-1]
	lower, below := 0.0, 0.0
	for i, c := range counts {
		if c >= rank {
			return lower + (bounds[i]-lower)*(rank-below)/(c-below)
		}
		lower, below = bounds[i], c
	}
	return bounds[len(bounds)-1]
}

// readCPU reads a snapshot of the containers' CPU time: lines of a name and
// nanoseconds.
func readCPU(path string) (map[string]float64, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	out := map[string]float64{}
	for line := range strings.Lines(string(data)) {
		name, ns, ok := strings.Cut(strings.TrimSpace(line), " ")
		if !ok {
			continue
		}
		v, err := strconv.ParseFloat(ns, 64)
		if err != nil {
			return nil, fmt.Errorf("%s: %w", path, err)
		}
		out[name] = v
	}
	return out, nil
}
