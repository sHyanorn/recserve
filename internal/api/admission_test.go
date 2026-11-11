package api

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"

	"github.com/sHyanorn/recserve/internal/history"
)

// heldStore holds every history read until release is closed.
type heldStore struct {
	history.Store
	entered chan struct{}
	release chan struct{}
}

func (s heldStore) Before(ctx context.Context, user string, ts int64, n int) ([]string, error) {
	s.entered <- struct{}{}
	select {
	case <-s.release:
		return s.Store.Before(ctx, user, ts, n)
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

// do sends a request and returns its status and Retry-After header, or an
// error if it takes longer than a second.
func do(method, url, body string) (int, string, error) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, method, url, strings.NewReader(body))
	if err != nil {
		return 0, "", err
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return 0, "", fmt.Errorf("%s %s: %w", method, url, err)
	}
	_, err = io.Copy(io.Discard, resp.Body)
	return resp.StatusCode, resp.Header.Get("Retry-After"), errors.Join(err, resp.Body.Close())
}

const recommendBody = `{"user_id":"u","k":2}`

// checkInFlight checks the in-flight limit of a gateway that allows 3
// recommend requests at a time, served by handler: with 3 held in the
// pipeline, the next is answered 503 with Retry-After at once, while the
// probes, /metrics and /v1/events still answer; once the 3 are released
// they answer 200, and so does the next.
func checkInFlight(t *testing.T, handler func(*Server) http.Handler) error {
	store := heldStore{Store: history.NewMemory(), entered: make(chan struct{}, 3), release: make(chan struct{})}
	s := degradedServer(t, store, io.Discard)
	s.Limits = Limits{InFlight: 3}
	s.SetReady(true)
	ts := httptest.NewServer(handler(s))
	defer ts.Close()

	var held sync.WaitGroup
	codes := make(chan int, 3)
	for range 3 {
		held.Go(func() {
			code, _, err := do(http.MethodPost, ts.URL+"/v1/recommend", recommendBody)
			if err != nil {
				code = 0
			}
			codes <- code
		})
		<-store.entered
	}
	code, retry, err := do(http.MethodPost, ts.URL+"/v1/recommend", recommendBody)
	if err == nil && (code != http.StatusServiceUnavailable || retry != "1") {
		err = fmt.Errorf("a fourth request in flight: %d, Retry-After %q; want 503, 1", code, retry)
	}
	for _, path := range []string{"/livez", "/readyz", "/metrics"} {
		if c, _, e := do(http.MethodGet, ts.URL+path, ""); e != nil || c != http.StatusOK {
			err = errors.Join(err, e, fmt.Errorf("%s answered %d while full", path, c))
		}
	}
	if c, _, e := do(http.MethodPost, ts.URL+"/v1/events", `{"events":[{"user_id":"v","item_id":"a","ts":1,"kind":"view"}]}`); e != nil || c != http.StatusAccepted {
		err = errors.Join(err, e, fmt.Errorf("/v1/events answered %d while full", c))
	}
	close(store.release)
	held.Wait()
	close(codes)
	for c := range codes {
		if c != http.StatusOK {
			err = errors.Join(err, fmt.Errorf("a held request answered %d", c))
		}
	}
	if c, _, e := do(http.MethodPost, ts.URL+"/v1/recommend", recommendBody); e != nil || c != http.StatusOK {
		err = errors.Join(err, e, fmt.Errorf("a request after the release answered %d", c))
	}
	if err == nil && testutil.ToFloat64(s.shed.WithLabelValues(shedInFlight)) != 1 {
		err = errors.New("the request turned away is not counted")
	}
	return err
}

func TestInFlightLimit(t *testing.T) {
	if err := checkInFlight(t, (*Server).Handler); err != nil {
		t.Fatal(err)
	}
}

// The broken variant queues requests beyond the limit instead of turning
// them away, and holds the probes too.
func TestInFlightLimitCatchesQueueing(t *testing.T) {
	queueing := func(s *Server) http.Handler {
		slots := make(chan struct{}, s.Limits.InFlight)
		s.Limits = Limits{}
		h := s.Handler()
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			slots <- struct{}{}
			defer func() { <-slots }()
			h.ServeHTTP(w, r)
		})
	}
	if err := checkInFlight(t, queueing); err == nil {
		t.Fatal("the queueing variant passed")
	}
}

// A gateway allowing 5 requests a second takes 5 at once, the burst, then
// turns the next away until a token comes back, 200 ms later on the
// server's clock.
func TestRateLimit(t *testing.T) {
	s := degradedServer(t, history.NewMemory(), io.Discard)
	var now atomic.Int64 // ms
	now.Store(1000)
	s.Limits, s.Now = Limits{Rate: 5}, func() time.Time { return time.UnixMilli(now.Load()) }
	ts := httptest.NewServer(s.Handler())
	defer ts.Close()
	var codes []int
	for range 6 {
		code, _, err := do(http.MethodPost, ts.URL+"/v1/recommend", recommendBody)
		if err != nil {
			t.Fatal(err)
		}
		codes = append(codes, code)
	}
	if fmt.Sprint(codes) != "[200 200 200 200 200 503]" {
		t.Fatalf("codes %v, want five 200 then 503", codes)
	}
	now.Add(200)
	if code, _, err := do(http.MethodPost, ts.URL+"/v1/recommend", recommendBody); err != nil || code != http.StatusOK {
		t.Fatalf("after a token came back: %d, %v", code, err)
	}
	if n := testutil.ToFloat64(s.shed.WithLabelValues(shedRate)); n != 1 {
		t.Fatalf("%v requests counted as over the rate, want 1", n)
	}
}
