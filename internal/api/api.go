// Package api serves the HTTP API of docs/SPEC.md §6.
package api

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strconv"
	"sync/atomic"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/sHyanorn/recserve/internal/events"
	"github.com/sHyanorn/recserve/internal/history"
	"github.com/sHyanorn/recserve/internal/recommend"
)

// MaxBody bounds a request body.
const MaxBody = 1 << 20

// MaxEvents bounds the events in one /v1/events request.
const MaxEvents = 1000

// DefaultEventsTimeout is the time /v1/events waits for its events to be
// accepted before it answers 503.
const DefaultEventsTimeout = 2 * time.Second

// Server holds the handlers' dependencies.
type Server struct {
	Live *recommend.Live
	// History is the store the pipelines read.
	History history.Store
	// Events takes the events of POST /v1/events; New sets one that stores
	// them in History before it answers.
	Events events.Producer
	// EventsTimeout bounds the time Events may take; 0 for none.
	EventsTimeout time.Duration
	// Prepare loads, verifies and indexes the bundle in a directory, for
	// POST /v1/admin/swap; nil refuses swaps.
	Prepare func(dir string) (*recommend.Pipeline, error)
	Now     func() time.Time
	Log     *slog.Logger

	ready           atomic.Bool
	requests        *prometheus.CounterVec
	latency         *prometheus.HistogramVec
	rankerTruncated prometheus.Counter
	rankerDropped   prometheus.Counter
	degraded        *prometheus.CounterVec
	cacheLookups    *prometheus.CounterVec
	swaps           *prometheus.CounterVec
	reg             *prometheus.Registry
}

// New returns a server that is not ready until SetReady(true).
func New(l *recommend.Live, h history.Store, log *slog.Logger) *Server {
	s := &Server{Live: l, History: h, Events: events.Direct{Store: h}, EventsTimeout: DefaultEventsTimeout, Now: time.Now, Log: log, reg: prometheus.NewRegistry()}
	s.requests = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "recserve_http_requests_total", Help: "HTTP requests by route and status code.",
	}, []string{"route", "code"})
	s.latency = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name: "recserve_http_request_duration_seconds", Help: "Server-side request duration by route.",
		Buckets: prometheus.ExponentialBuckets(0.0005, 2, 14),
	}, []string{"route"})
	s.rankerTruncated = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "recserve_ranker_truncated_requests_total", Help: "Requests with more candidates than the ranker takes.",
	})
	s.rankerDropped = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "recserve_ranker_candidates_dropped_total", Help: "Candidates left out of ranking for want of ranker rows.",
	})
	s.degraded = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "recserve_degraded_total", Help: "Recommend responses that report each degradation reason.",
	}, []string{"reason"})
	for _, r := range recommend.Reasons {
		s.degraded.WithLabelValues(r)
	}
	s.cacheLookups = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "recserve_cache_lookups_total", Help: "Result cache lookups by result: hit, miss, or error (the cache could not be read or written; answered as a miss).",
	}, []string{"result"})
	s.swaps = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "recserve_swaps_total", Help: "Bundle swaps by result: ok, rejected (the bundle failed to prepare or is already active) or busy (another swap was running).",
	}, []string{"result"})
	for _, r := range []string{swapOK, swapRejected, swapBusy} {
		s.swaps.WithLabelValues(r)
	}
	s.reg.MustRegister(s.requests, s.latency, s.rankerTruncated, s.rankerDropped, s.degraded, s.cacheLookups, s.swaps,
		collectors.NewGoCollector(), collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}))
	return s
}

// Register adds metrics of other parts of the gateway to /metrics.
func (s *Server) Register(cs ...prometheus.Collector) { s.reg.MustRegister(cs...) }

// SetReady sets the readiness reported by /readyz.
func (s *Server) SetReady(v bool) { s.ready.Store(v) }

// Handler returns the HTTP handler. While the server is not ready, as when
// it is stopping, each response closes its connection, so that a client
// that keeps connections open goes to another instance (D045).
func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.Handle("POST /v1/recommend", s.instrument("recommend", s.recommend))
	mux.Handle("POST /v1/events", s.instrument("events", s.events))
	mux.HandleFunc("GET /livez", func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusOK) })
	mux.HandleFunc("GET /readyz", func(w http.ResponseWriter, _ *http.Request) {
		if !s.ready.Load() {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		w.WriteHeader(http.StatusOK)
	})
	mux.Handle("GET /metrics", promhttp.HandlerFor(s.reg, promhttp.HandlerOpts{}))
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !s.ready.Load() {
			w.Header().Set("Connection", "close")
		}
		mux.ServeHTTP(w, r)
	})
}

// AdminHandler returns the handler of the admin API, for a listener of its
// own: it has no authentication.
func (s *Server) AdminHandler() http.Handler {
	mux := http.NewServeMux()
	mux.Handle("POST /v1/admin/swap", s.instrument("admin_swap", s.swap))
	return mux
}

type statusWriter struct {
	http.ResponseWriter
	code int
}

func (w *statusWriter) WriteHeader(code int) {
	w.code = code
	w.ResponseWriter.WriteHeader(code)
}

func (s *Server) instrument(route string, h func(http.ResponseWriter, *http.Request) error) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		sw := &statusWriter{ResponseWriter: w, code: http.StatusOK}
		r.Body = http.MaxBytesReader(w, r.Body, MaxBody)
		if err := h(sw, r); err != nil {
			code := http.StatusInternalServerError
			var be badRequest
			var he httpError
			var tooLarge *http.MaxBytesError
			switch {
			case errors.As(err, &he):
				code = he.code
			case errors.As(err, &tooLarge):
				code = http.StatusRequestEntityTooLarge
			case errors.As(err, &be):
				code = http.StatusBadRequest
			case errors.Is(err, recommend.ErrLiveClosed): // shutting down
				code = http.StatusServiceUnavailable
			case errors.Is(err, context.Canceled) && r.Context().Err() != nil:
				code = statusClientClosed
			case errors.Is(err, context.DeadlineExceeded) && r.Context().Err() != nil:
				code = http.StatusGatewayTimeout
			default:
				s.Log.Error("request failed", "route", route, "err", err)
			}
			if err := writeJSON(sw, code, map[string]string{"error": err.Error()}); err != nil {
				s.Log.Error("writing an error response failed", "route", route, "err", err)
			}
		}
		s.requests.WithLabelValues(route, strconv.Itoa(sw.code)).Inc()
		s.latency.WithLabelValues(route).Observe(time.Since(start).Seconds())
	})
}

type badRequest struct{ error }

func (b badRequest) Unwrap() error { return b.error }

// httpError is an error answered with its own status code, not logged.
type httpError struct {
	code int
	error
}

func (e httpError) Unwrap() error { return e.error }

// statusClientClosed is recorded when the client went away before the reply (nginx's 499).
const statusClientClosed = 499

// writeJSON writes v as the body with the given status, or nothing if v
// cannot be encoded.
func writeJSON(w http.ResponseWriter, code int, v any) error {
	body, err := json.Marshal(v)
	if err != nil {
		return err
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_, _ = w.Write(append(body, '\n')) //nolint:errcheck // the status line is sent; a failed write means the client has gone
	return nil
}

// decode reads exactly one JSON value with no unknown fields.
func decode(r io.Reader, v any) error {
	d := json.NewDecoder(r)
	d.DisallowUnknownFields()
	if err := d.Decode(v); err != nil {
		return badRequest{fmt.Errorf("body: %w", err)}
	}
	if _, err := d.Token(); !errors.Is(err, io.EOF) {
		return badRequest{errors.New("body: trailing data")}
	}
	return nil
}

// RecommendRequest is the body of POST /v1/recommend.
type RecommendRequest struct {
	UserID  string   `json:"user_id"`
	K       int      `json:"k"`
	TS      *int64   `json:"ts,omitempty"`
	Exclude []string `json:"exclude,omitempty"`
	Recent  []string `json:"recent,omitempty"` // the user's latest items, oldest first; the history when the stored one is unavailable
}

// ParseRecommend decodes and validates a recommend request; now is used when ts is absent.
func ParseRecommend(r io.Reader, now int64) (recommend.Request, error) {
	var in RecommendRequest
	if err := decode(r, &in); err != nil {
		return recommend.Request{}, err
	}
	if in.UserID == "" {
		return recommend.Request{}, badRequest{errors.New("user_id is required")}
	}
	if in.K < 1 || in.K > recommend.MaxK {
		return recommend.Request{}, badRequest{fmt.Errorf("k must be in 1..%d", recommend.MaxK)}
	}
	if len(in.Recent) > recommend.MaxRecent {
		return recommend.Request{}, badRequest{fmt.Errorf("recent must hold at most %d items", recommend.MaxRecent)}
	}
	ts := now
	if in.TS != nil {
		ts = *in.TS
	}
	return recommend.Request{User: in.UserID, K: in.K, TS: ts, Exclude: in.Exclude, Recent: in.Recent}, nil
}

func (s *Server) recommend(w http.ResponseWriter, r *http.Request) error {
	req, err := ParseRecommend(r.Body, s.Now().UnixMilli())
	if err != nil {
		return err
	}
	resp, err := s.Live.Recommend(r.Context(), req)
	if resp.Cached != nil { // also set when the request failed after the lookup
		result := "miss"
		switch {
		case resp.CacheErr != nil:
			result = "error"
			if !errors.Is(resp.CacheErr, context.DeadlineExceeded) {
				s.Log.Warn("result cache failed; answered as a miss", "err", resp.CacheErr)
			}
		case *resp.Cached:
			result = "hit"
		}
		s.cacheLookups.WithLabelValues(result).Inc()
	}
	if err != nil {
		return err
	}
	if resp.Dropped > 0 {
		s.rankerTruncated.Inc()
		s.rankerDropped.Add(float64(resp.Dropped))
	}
	for _, r := range resp.Degraded {
		s.degraded.WithLabelValues(r).Inc()
	}
	for _, f := range resp.Failures {
		s.Log.Warn("request degraded", "reason", f.Reason, "err", f.Err)
	}
	return writeJSON(w, http.StatusOK, resp)
}

// EventsRequest is the body of POST /v1/events.
type EventsRequest struct {
	Events []events.Event `json:"events"`
}

// EventsResponse reports how many events were accepted.
type EventsResponse struct {
	Accepted int `json:"accepted"`
}

// events hands the request's events to the producer and answers 202 once
// it has accepted them all: stored, or acknowledged by the broker (D034).
// It answers 503 if the producer fails or takes longer than EventsTimeout;
// some of the events may then have been accepted, and sending them again
// is safe.
func (s *Server) events(w http.ResponseWriter, r *http.Request) error {
	var in EventsRequest
	if err := decode(r.Body, &in); err != nil {
		return err
	}
	if len(in.Events) == 0 || len(in.Events) > MaxEvents {
		return badRequest{fmt.Errorf("events must hold 1..%d events", MaxEvents)}
	}
	for i, e := range in.Events {
		if err := e.Validate(); err != nil {
			return badRequest{fmt.Errorf("event %d: %w", i, err)}
		}
	}
	ctx := r.Context()
	if s.EventsTimeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, s.EventsTimeout)
		defer cancel()
	}
	if err := s.Events.Produce(ctx, in.Events); err != nil {
		if ctxErr := r.Context().Err(); ctxErr != nil && errors.Is(err, ctxErr) {
			return err
		}
		s.Log.Warn("events not accepted", "err", err)
		return httpError{http.StatusServiceUnavailable, fmt.Errorf("events not accepted: %w", err)}
	}
	return writeJSON(w, http.StatusAccepted, EventsResponse{Accepted: len(in.Events)})
}

// Results of a swap, as recserve_swaps_total counts them.
const (
	swapOK       = "ok"
	swapRejected = "rejected"
	swapBusy     = "busy"
)

// SwapRequest is the body of POST /v1/admin/swap.
type SwapRequest struct {
	Bundle string `json:"bundle"` // the bundle's directory on the gateway's file system
}

// SwapResponse reports a completed swap.
type SwapResponse struct {
	ModelVersion string  `json:"model_version"`
	Previous     string  `json:"previous"`
	PrepareMS    float64 `json:"prepare_ms"` // loading, verifying and indexing the bundle
	DrainMS      float64 `json:"drain_ms"`   // from activation until the previous bundle was closed
}

// swap prepares the bundle in the requested directory and activates it:
// 409 while another swap runs or if its version is active, 422 if it fails
// to load or verify; the active bundle then stays.
func (s *Server) swap(w http.ResponseWriter, r *http.Request) error {
	var in SwapRequest
	if err := decode(r.Body, &in); err != nil {
		return err
	}
	if in.Bundle == "" {
		return badRequest{errors.New("bundle is required")}
	}
	if s.Prepare == nil {
		return httpError{http.StatusNotImplemented, errors.New("swaps are not configured")}
	}
	res, err := s.Live.Swap(func() (*recommend.Pipeline, error) { return s.Prepare(in.Bundle) })
	switch {
	case errors.Is(err, recommend.ErrSwapInProgress):
		s.swaps.WithLabelValues(swapBusy).Inc()
		return httpError{http.StatusConflict, err}
	case errors.Is(err, recommend.ErrLiveClosed):
		return err
	case err != nil:
		s.swaps.WithLabelValues(swapRejected).Inc()
		s.Log.Warn("swap rejected", "bundle", in.Bundle, "err", err)
		code := http.StatusUnprocessableEntity
		if errors.Is(err, recommend.ErrActive) {
			code = http.StatusConflict
		}
		return httpError{code, err}
	}
	s.swaps.WithLabelValues(swapOK).Inc()
	s.Log.Info("bundle swapped", "bundle", in.Bundle, "model_version", res.Version, "previous", res.Previous,
		"prepare", res.Prepare, "drain", res.Drain)
	return writeJSON(w, http.StatusOK, SwapResponse{
		ModelVersion: res.Version, Previous: res.Previous,
		PrepareMS: ms(res.Prepare), DrainMS: ms(res.Drain),
	})
}

func ms(d time.Duration) float64 { return float64(d) / float64(time.Millisecond) }
