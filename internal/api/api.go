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

	"github.com/sHyanorn/recserve/internal/history"
	"github.com/sHyanorn/recserve/internal/recommend"
)

// MaxBody bounds a request body.
const MaxBody = 1 << 20

// MaxEvents bounds the events in one /v1/events request.
const MaxEvents = 1000

// Server holds the handlers' dependencies.
type Server struct {
	Pipeline *recommend.Pipeline
	History  history.Store
	Now      func() time.Time
	Log      *slog.Logger

	ready    atomic.Bool
	requests *prometheus.CounterVec
	latency  *prometheus.HistogramVec
	reg      *prometheus.Registry
}

// New returns a server that is not ready until SetReady(true).
func New(p *recommend.Pipeline, h history.Store, log *slog.Logger) *Server {
	s := &Server{Pipeline: p, History: h, Now: time.Now, Log: log, reg: prometheus.NewRegistry()}
	s.requests = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "recserve_http_requests_total", Help: "HTTP requests by route and status code.",
	}, []string{"route", "code"})
	s.latency = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name: "recserve_http_request_duration_seconds", Help: "Server-side request duration by route.",
		Buckets: prometheus.ExponentialBuckets(0.0005, 2, 14),
	}, []string{"route"})
	s.reg.MustRegister(s.requests, s.latency, collectors.NewGoCollector(), collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}))
	return s
}

// SetReady sets the readiness reported by /readyz.
func (s *Server) SetReady(v bool) { s.ready.Store(v) }

// Handler returns the HTTP handler.
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
			var tooLarge *http.MaxBytesError
			switch {
			case errors.As(err, &tooLarge):
				code = http.StatusRequestEntityTooLarge
			case errors.As(err, &be):
				code = http.StatusBadRequest
			case errors.Is(err, context.Canceled) && r.Context().Err() != nil:
				code = statusClientClosed
			default:
				s.Log.Error("request failed", "route", route, "err", err)
			}
			writeJSON(sw, code, map[string]string{"error": err.Error()})
		}
		s.requests.WithLabelValues(route, strconv.Itoa(sw.code)).Inc()
		s.latency.WithLabelValues(route).Observe(time.Since(start).Seconds())
	})
}

type badRequest struct{ error }

func (b badRequest) Unwrap() error { return b.error }

// statusClientClosed is recorded when the client went away before the reply (nginx's 499).
const statusClientClosed = 499

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(v)
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
	Recent  []string `json:"recent,omitempty"` // used from M3, when the stored history is unavailable
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
	ts := now
	if in.TS != nil {
		ts = *in.TS
	}
	return recommend.Request{User: in.UserID, K: in.K, TS: ts, Exclude: in.Exclude}, nil
}

func (s *Server) recommend(w http.ResponseWriter, r *http.Request) error {
	req, err := ParseRecommend(r.Body, s.Now().UnixMilli())
	if err != nil {
		return err
	}
	resp, err := s.Pipeline.Recommend(r.Context(), req)
	if err != nil {
		return err
	}
	writeJSON(w, http.StatusOK, resp)
	return nil
}

// Event is one event in POST /v1/events.
type Event struct {
	UserID string `json:"user_id"`
	ItemID string `json:"item_id"`
	TS     int64  `json:"ts"`
	Kind   string `json:"kind"`
}

// EventsRequest is the body of POST /v1/events.
type EventsRequest struct {
	Events []Event `json:"events"`
}

// EventsResponse reports how many events were stored and how many named unknown items.
type EventsResponse struct {
	Accepted     int `json:"accepted"`
	UnknownItems int `json:"unknown_items"`
}

func (s *Server) events(w http.ResponseWriter, r *http.Request) error {
	var in EventsRequest
	if err := decode(r.Body, &in); err != nil {
		return err
	}
	if len(in.Events) == 0 || len(in.Events) > MaxEvents {
		return badRequest{fmt.Errorf("events must hold 1..%d events", MaxEvents)}
	}
	var out EventsResponse
	evs := make([]history.Event, 0, len(in.Events))
	for i, e := range in.Events {
		if e.UserID == "" || e.ItemID == "" || e.TS <= 0 {
			return badRequest{fmt.Errorf("event %d: user_id, item_id and a positive ts are required", i)}
		}
		item, ok := s.Pipeline.Bundle.Lookup(e.ItemID)
		if !ok {
			out.UnknownItems++
			continue
		}
		evs = append(evs, history.Event{ID: history.EventID(e.UserID, e.ItemID, e.TS, e.Kind), User: e.UserID, Item: item, TS: e.TS})
	}
	if err := s.History.Append(r.Context(), evs); err != nil {
		return err
	}
	out.Accepted = len(evs)
	writeJSON(w, http.StatusAccepted, out)
	return nil
}
