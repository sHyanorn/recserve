package main

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// fakeGateway answers like the gateway. Recommendations are served one at a
// time, as by a single busy core; stall, if set, runs before each of them.
func fakeGateway(t *testing.T, stall func(), status int) string {
	t.Helper()
	var mu sync.Mutex
	mux := http.NewServeMux()
	mux.HandleFunc("GET /readyz", func(http.ResponseWriter, *http.Request) {})
	mux.HandleFunc("POST /v1/recommend", func(w http.ResponseWriter, _ *http.Request) {
		mu.Lock()
		if stall != nil {
			stall()
		}
		mu.Unlock()
		w.WriteHeader(status)
		_, _ = w.Write([]byte(`{"items":[],"model_version":"v"}`)) //nolint:errcheck // loadgen may have given up on the request; the tests check what it received
	})
	mux.HandleFunc("POST /v1/events", func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusAccepted) })
	s := httptest.NewServer(mux)
	t.Cleanup(s.Close)
	return strings.TrimPrefix(s.URL, "http://")
}

// testConfig offers 500 requests/s: a rate the tests reach with the race
// detector on while other packages' tests use the CPUs.
func testConfig(addr string) config {
	return config{
		addr: addr, rate: 500, warmup: 200 * time.Millisecond, duration: time.Second, conns: 16,
		work: &datasetWorkload{rows: []row{{"u", "i", 1}, {"v", "j", 2}}, k: 5, writeShare: 0.1},
	}
}

func TestMeasure(t *testing.T) {
	r, err := measure(context.Background(), testConfig(fakeGateway(t, nil, http.StatusOK)))
	if err != nil {
		t.Fatal(err)
	}
	if !r.Valid {
		t.Fatalf("invalid: %v", r.Problems)
	}
	// 500 measured requests, a tenth of them writes.
	if n := r.Outcomes["recommend_ok"] + r.Outcomes["events_ok"]; n < 495 || n > 500 || r.Outcomes["events_ok"] < 47 || r.Outcomes["events_ok"] > 53 {
		t.Fatalf("outcomes %v", r.Outcomes)
	}
}

func TestFailuresMakeTheRunInvalid(t *testing.T) {
	r, err := measure(context.Background(), testConfig(fakeGateway(t, nil, http.StatusInternalServerError)))
	if err != nil {
		t.Fatal(err)
	}
	if r.Valid || r.Outcomes["error"] == 0 {
		t.Fatalf("valid %v, outcomes %v", r.Valid, r.Outcomes)
	}
}

// Property 7: a stall of the gateway delays every request due during it,
// not only those in flight, and the latency shows it because it counts from
// when each request was due. The service time, which counts from the actual
// send as a closed-loop generator would, hides most of it: it is the broken
// variant, and the check below fails on it.
func TestStallIsChargedToEveryDelayedRequest(t *testing.T) {
	const stall = 400 * time.Millisecond
	var n atomic.Int64
	addr := fakeGateway(t, func() {
		if n.Add(1) == 200 { // about 0.2 s into the measured second
			time.Sleep(stall)
		}
	}, http.StatusOK)
	r, err := measure(context.Background(), testConfig(addr))
	if err != nil {
		t.Fatal(err)
	}
	// About stall*rate = 200 requests were due during the stall, 40% of those
	// measured; most waited over 100 ms, while only 16 were in flight.
	const limit = 100_000 // µs
	if p := r.Latency["reply"].P90; p < limit {
		t.Errorf("p90 due to reply %.0f µs, want over %d", p, limit)
	}
	if p := r.Latency["send_lag"].P90; p < limit {
		t.Errorf("p90 send lag %.0f µs, want over %d", p, limit)
	}
	if p := r.Latency["service"].P90; p >= limit {
		t.Errorf("p90 service time %.0f µs, want under %d", p, limit)
	}
}

func TestWorkloadSpreadsWrites(t *testing.T) {
	w := &datasetWorkload{rows: []row{{`a"b`, "i", 7}}, k: 3, writeShare: 0.25}
	var writes []int
	for i := range 12 {
		path, body := w.request(i, nil)
		if path == "/v1/events" {
			writes = append(writes, i)
			if string(body) != `{"events":[{"user_id":"a\"b","item_id":"i","ts":7,"kind":"rating"}]}` {
				t.Fatalf("event body %s", body)
			}
		} else if string(body) != `{"user_id":"a\"b","k":3,"ts":7}` {
			t.Fatalf("recommend body %s", body)
		}
	}
	if len(writes) != 3 || writes[0] != 3 || writes[1] != 7 || writes[2] != 11 {
		t.Fatalf("writes at %v", writes)
	}
}

func TestWokeLate(t *testing.T) {
	got := wokeLate(nil, 3500*time.Microsecond)
	if len(got) != 3 || got[0] != 3_500_000 || got[2] != 1_500_000 {
		t.Fatalf("got %v", got)
	}
}
