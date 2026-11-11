package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math/rand/v2"
	"net/http"
	"runtime"
	"strconv"
	"sync"
	"time"
)

// config is one measurement: a workload offered to the gateway at addr at a fixed rate.
type config struct {
	addr     string
	rate     float64 // requests per second
	warmup   time.Duration
	duration time.Duration
	conns    int
	work     workload
	samples  string    // file for one sample per measured request; empty for none
	started  io.Writer // if set, told "start <unix ms>" once request 0's due time is fixed
}

// workload produces the body of request i.
type workload interface {
	request(i int, buf []byte) (path string, body []byte)
}

// report is the result of a measurement, written as JSON.
type report struct {
	Start      time.Time          `json:"start"` // when request 0 was due
	TargetRate float64            `json:"target_rate"`
	Achieved   float64            `json:"achieved_rate"`
	Warmup     float64            `json:"warmup_s"`
	Duration   float64            `json:"duration_s"`
	Conns      int                `json:"conns"`
	Go         string             `json:"go"`
	CPUs       int                `json:"cpus"`
	Valid      bool               `json:"valid"`
	Problems   []string           `json:"problems,omitempty"`
	Outcomes   map[string]int     `json:"outcomes"`
	Versions   map[string]int     `json:"versions"` // recommendations by the bundle version they name
	Latency    map[string]summary `json:"latency_us"`
}

// job is one request, due at planned.
type job struct {
	i        int
	planned  time.Time
	measured bool // planned inside the measurement window
}

// worker sends requests over one keep-alive connection; only its goroutine
// touches it until the run ends.
type worker struct {
	cfg    config
	base   string
	end    time.Time
	client *http.Client
	buf    []byte

	outcomes map[string]int
	versions map[string]int
	firstErr string
	sent     int // measured requests sent before the window ended
	t0       time.Time
	samples  []sample // when cfg.samples is set
	// Latency samples in nanoseconds, of measured requests that succeeded:
	// due to reply, sent to reply, due to sent.
	reply, service, lag []int64
}

func newWorker(ctx context.Context, cfg config, end time.Time) (*worker, error) {
	w := &worker{
		cfg: cfg, base: "http://" + cfg.addr, end: end,
		client: &http.Client{
			Transport: &http.Transport{MaxConnsPerHost: 1, MaxIdleConnsPerHost: 1, DisableCompression: true},
			Timeout:   30 * time.Second,
		},
		outcomes: map[string]int{},
		versions: map[string]int{},
	}
	status, _, err := w.do(ctx, http.MethodGet, "/readyz", nil)
	if err == nil && status != http.StatusOK {
		err = fmt.Errorf("/readyz answered %d", status)
	}
	return w, err
}

var (
	versionField  = []byte(`"model_version":"`)
	armField      = []byte(`"arm":"`)
	degradedField = []byte(`"degraded":["`)
)

// classify names a reply's outcome. A recommendation must carry its arm and
// the model version; one that reports a degradation is an answer, counted
// apart.
func classify(path string, status int, data []byte, err error) string {
	switch {
	case err != nil:
		return "error"
	case status == http.StatusServiceUnavailable:
		return "busy"
	case path == "/v1/events" && status == http.StatusAccepted:
		return "events_ok"
	case path == "/v1/recommend" && status == http.StatusOK && bytes.Contains(data, versionField) && bytes.Contains(data, armField):
		if bytes.Contains(data, degradedField) {
			return "recommend_degraded"
		}
		return "recommend_ok"
	}
	return "error"
}

// version returns the bundle version a recommendation names.
func version(data []byte) []byte {
	_, v, _ := bytes.Cut(data, versionField)
	v, _, _ = bytes.Cut(v, []byte(`"`))
	return v
}

func (w *worker) handle(ctx context.Context, j job) {
	path, body := w.cfg.work.request(j.i, w.buf[:0])
	w.buf = body
	sent := time.Now()
	status, data, err := w.do(ctx, http.MethodPost, path, body)
	done := time.Now()
	if !j.measured {
		return
	}
	outcome := classify(path, status, data, err)
	w.outcomes[outcome]++
	if outcome == "recommend_ok" || outcome == "recommend_degraded" {
		w.versions[string(version(data))]++
	}
	if sent.Before(w.end) {
		w.sent++
	}
	if w.cfg.samples != "" {
		w.samples = append(w.samples, newSample(j, w.t0, path, status, outcome, data, done))
	}
	if outcome == "busy" || outcome == "error" {
		if w.firstErr == "" {
			w.firstErr = fmt.Sprintf("%s: %d %s %v", path, status, bytes.TrimSpace(data), err)
		}
		return
	}
	w.reply = append(w.reply, int64(done.Sub(j.planned)))
	w.service = append(w.service, int64(done.Sub(sent)))
	w.lag = append(w.lag, int64(sent.Sub(j.planned)))
}

// do makes one request and reads the whole reply, so the connection can be reused.
func (w *worker) do(ctx context.Context, method, path string, body []byte) (int, []byte, error) {
	req, err := http.NewRequestWithContext(ctx, method, w.base+path, bytes.NewReader(body))
	if err != nil {
		return 0, nil, err
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	res, err := w.client.Do(req)
	if err != nil {
		return 0, nil, err
	}
	data, err := io.ReadAll(res.Body)
	return res.StatusCode, data, errors.Join(err, res.Body.Close())
}

// measure offers cfg's workload and measures it. Request i is due at
// t0 + i/rate whether or not earlier requests have been answered, and its
// latency counts from that time, as in wrk2: a server that stalls is charged
// for every request it delayed, not only for those in flight (D009).
// Requests due during the warmup are sent but not counted.
func measure(ctx context.Context, cfg config) (report, error) {
	total := int(cfg.rate * (cfg.warmup + cfg.duration).Seconds())
	period := float64(time.Second) / cfg.rate
	workers := make([]*worker, cfg.conns)
	t0 := time.Now() // provisional, for the workers' window end
	var err error
	for i := range workers {
		if workers[i], err = newWorker(ctx, cfg, t0); err != nil {
			return report{}, fmt.Errorf("opening connection %d: %w", i+1, err)
		}
	}
	t0 = time.Now()
	if cfg.started != nil {
		if _, err := fmt.Fprintf(cfg.started, "start %d\n", t0.UnixMilli()); err != nil {
			return report{}, err
		}
	}
	due := func(i int) time.Time { return t0.Add(time.Duration(float64(i) * period)) }
	start := t0.Add(cfg.warmup)
	end := start.Add(cfg.duration)
	for _, w := range workers {
		w.end, w.t0 = end, t0
	}
	hiccups := make(chan []int64, 1)
	go func() { hiccups <- sampleHiccups(start, end) }()

	jobs := make(chan job, cfg.conns)
	var wg sync.WaitGroup
	for _, w := range workers {
		wg.Go(func() {
			for j := range jobs {
				w.handle(ctx, j)
			}
		})
	}
	// Pace in small steps, sending every request that is due. If every
	// worker is busy, sends wait, and the delay shows in the latency.
send:
	for i := 0; i < total; {
		for n := min(total, int(float64(time.Since(t0))/period)+1); i < n; i++ {
			select {
			case jobs <- job{i: i, planned: due(i), measured: !due(i).Before(start)}:
			case <-ctx.Done():
				break send
			}
		}
		if i < total {
			time.Sleep(time.Until(due(i)))
		}
	}
	close(jobs)
	wg.Wait()
	for _, w := range workers {
		w.client.CloseIdleConnections()
	}
	if err := ctx.Err(); err != nil {
		return report{}, err
	}
	r := summarizeRun(cfg, workers)
	r.Start = t0
	r.Latency["hiccup"] = summarize(<-hiccups)
	if cfg.samples != "" {
		var all []sample
		for _, w := range workers {
			all = append(all, w.samples...)
		}
		if err := writeSamples(cfg.samples, all); err != nil {
			return report{}, err
		}
	}
	return r, nil
}

func summarizeRun(cfg config, workers []*worker) report {
	r := report{
		TargetRate: cfg.rate, Warmup: cfg.warmup.Seconds(), Duration: cfg.duration.Seconds(),
		Conns: cfg.conns, Go: runtime.Version(), CPUs: runtime.NumCPU(), Outcomes: map[string]int{}, Versions: map[string]int{},
	}
	var sent int
	var firstErr string
	var reply, service, lag []int64
	for _, w := range workers {
		for k, n := range w.outcomes {
			r.Outcomes[k] += n
		}
		for k, n := range w.versions {
			r.Versions[k] += n
		}
		sent += w.sent
		if firstErr == "" {
			firstErr = w.firstErr
		}
		reply = append(reply, w.reply...)
		service = append(service, w.service...)
		lag = append(lag, w.lag...)
	}
	r.Achieved = float64(sent) / cfg.duration.Seconds()
	if r.Achieved < 0.99*cfg.rate {
		r.Problems = append(r.Problems, fmt.Sprintf("achieved %.0f/s, below 99%% of the target", r.Achieved))
	}
	if n := r.Outcomes["busy"] + r.Outcomes["error"]; n > 0 {
		r.Problems = append(r.Problems, fmt.Sprintf("%d requests failed, the first: %s", n, firstErr))
	}
	r.Valid = len(r.Problems) == 0
	r.Latency = map[string]summary{"reply": summarize(reply), "service": summarize(service), "send_lag": summarize(lag)}
	return r
}

// sampleHiccups records, from start to end, how late each 1 ms sleep wakes
// up: a pause of loadgen or of the machine, which adds to every latency
// measured meanwhile.
func sampleHiccups(start, end time.Time) []int64 {
	var late []int64
	time.Sleep(time.Until(start))
	for now := time.Now(); now.Before(end); {
		time.Sleep(hiccupTick)
		t := time.Now()
		late = wokeLate(late, t.Sub(now)-hiccupTick)
		now = t
	}
	return late
}

const hiccupTick = time.Millisecond

// wokeLate appends a wakeup that came d late and, as jHiccup does, the
// wakeups the pause swallowed, each one tick less late, down to one tick.
func wokeLate(late []int64, d time.Duration) []int64 {
	late = append(late, int64(d))
	for m := d - hiccupTick; m >= hiccupTick; m -= hiccupTick {
		late = append(late, int64(m))
	}
	return late
}

// datasetWorkload replays dataset rows in order, cycling: one recommendation
// request per row at the row's time, and an event write instead for a
// writeShare of the requests, spread evenly. With repeats, a request may
// instead copy an earlier one (newDatasetWorkload).
type datasetWorkload struct {
	rows       []row
	k          int
	writeShare float64
	order      []int // row of each request; nil: request i takes row i mod len(rows)
}

// repeatWindow is how far back a repeated request may reach.
const repeatWindow = 1000

// newDatasetWorkload returns the workload of total requests in which each
// request, with probability repeat, takes the row of one of the repeatWindow
// requests before it, drawn uniformly; the draws follow from seed. Without
// writes such a copy has the same history, so the same cache key: a client
// asking again.
func newDatasetWorkload(rows []row, k int, writeShare, repeat float64, seed uint64, total int) *datasetWorkload {
	w := &datasetWorkload{rows: rows, k: k, writeShare: writeShare}
	if repeat > 0 {
		rng := rand.New(rand.NewPCG(seed, 0))
		w.order = make([]int, total)
		for i := range total {
			w.order[i] = i % len(rows)
			if i > 0 && rng.Float64() < repeat {
				w.order[i] = w.order[i-1-rng.IntN(min(i, repeatWindow))]
			}
		}
	}
	return w
}

type row struct {
	user, item string
	ts         int64
}

func (d *datasetWorkload) request(i int, b []byte) (string, []byte) {
	r := d.rows[i%len(d.rows)]
	if d.order != nil {
		r = d.rows[d.order[i]]
	}
	if int(float64(i+1)*d.writeShare) > int(float64(i)*d.writeShare) {
		b = append(b, `{"events":[{"user_id":`...)
		b = appendJSONString(b, r.user)
		b = append(b, `,"item_id":`...)
		b = appendJSONString(b, r.item)
		b = append(b, `,"ts":`...)
		b = strconv.AppendInt(b, r.ts, 10)
		b = append(b, `,"kind":"rating"}]}`...)
		return "/v1/events", b
	}
	b = append(b, `{"user_id":`...)
	b = appendJSONString(b, r.user)
	b = append(b, `,"k":`...)
	b = strconv.AppendInt(b, int64(d.k), 10)
	b = append(b, `,"ts":`...)
	b = strconv.AppendInt(b, r.ts, 10)
	b = append(b, '}')
	return "/v1/recommend", b
}

// appendJSONString appends s as a JSON string.
func appendJSONString(b []byte, s string) []byte {
	q, _ := json.Marshal(s) //nolint:errcheck // a string always marshals; invalid UTF-8 becomes U+FFFD
	return append(b, q...)
}
