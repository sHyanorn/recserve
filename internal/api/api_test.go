package api

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/sHyanorn/recserve/internal/bundle"
	"github.com/sHyanorn/recserve/internal/history"
	"github.com/sHyanorn/recserve/internal/recommend"
	"github.com/sHyanorn/recserve/internal/vec"
)

func testServer(t *testing.T) (*Server, *httptest.Server) {
	t.Helper()
	m, err := vec.NewMatrix(2, []float32{0, 0, 1, 0, 0.9, 0.1, 0, 1, -1, 0})
	if err != nil {
		t.Fatal(err)
	}
	b := bundle.New(bundle.Manifest{ModelVersion: "v1", ItemCount: 4, Dim: 2, MaxSeqLen: 50},
		[]string{"", "a", "b", "c", "d"}, m, nil, []int32{4, 3})
	store := history.NewMemory()
	s := New(&recommend.Pipeline{Bundle: b, History: store}, store, slog.New(slog.NewTextHandler(io.Discard, nil)))
	s.Now = func() time.Time { return time.UnixMilli(1000) }
	ts := httptest.NewServer(s.Handler())
	t.Cleanup(ts.Close)
	return s, ts
}

func post(t *testing.T, url, body string) (int, map[string]any) {
	t.Helper()
	resp, err := http.Post(url, "application/json", strings.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	var out map[string]any
	if err := json.Unmarshal(readBody(t, resp), &out); err != nil {
		t.Fatal(err)
	}
	return resp.StatusCode, out
}

// readBody reads and closes the body of resp.
func readBody(t *testing.T, resp *http.Response) []byte {
	t.Helper()
	b, err := io.ReadAll(resp.Body)
	if err := errors.Join(err, resp.Body.Close()); err != nil {
		t.Fatal(err)
	}
	return b
}

func ids(out map[string]any) []string {
	var r []string
	items, _ := out["items"].([]any)
	for _, it := range items {
		r = append(r, it.(map[string]any)["id"].(string))
	}
	return r
}

func TestEventsThenRecommend(t *testing.T) {
	_, ts := testServer(t)
	code, out := post(t, ts.URL+"/v1/events", `{"events":[{"user_id":"u","item_id":"a","ts":500,"kind":"view"},{"user_id":"u","item_id":"zz","ts":500,"kind":"view"}]}`)
	if code != http.StatusAccepted || out["accepted"] != 1.0 || out["unknown_items"] != 1.0 {
		t.Fatalf("events: %d %v", code, out)
	}
	code, out = post(t, ts.URL+"/v1/recommend", `{"user_id":"u","k":2}`)
	if code != http.StatusOK || out["model_version"] != "v1" {
		t.Fatalf("recommend: %d %v", code, out)
	}
	got := ids(out)
	if len(got) != 2 || got[0] != "b" {
		t.Fatalf("items %v; want b first (closest to a), a never", got)
	}
	// Before the event, the history is empty and popularity decides.
	_, out = post(t, ts.URL+"/v1/recommend", `{"user_id":"u","k":1,"ts":400}`)
	if got := ids(out); len(got) != 1 || got[0] != "d" {
		t.Fatalf("items at ts 400: %v", got)
	}
}

func TestBadRequests(t *testing.T) {
	_, ts := testServer(t)
	for _, tc := range []struct{ path, body string }{
		{"/v1/recommend", `{"user_id":"u","k":0}`},
		{"/v1/recommend", `{"user_id":"","k":5}`},
		{"/v1/recommend", `{"user_id":"u","k":5,"extra":1}`},
		{"/v1/recommend", `{"user_id":"u","k":5}{}`},
		{"/v1/recommend", `not json`},
		{"/v1/events", `{"events":[]}`},
		{"/v1/events", `{"events":[{"user_id":"u","item_id":"a","ts":0}]}`},
		{"/v1/recommend", `{"user_id":"u","k":5}]`},
	} {
		if code, _ := post(t, ts.URL+tc.path, tc.body); code != http.StatusBadRequest {
			t.Errorf("%s %.40s: status %d", tc.path, tc.body, code)
		}
	}
}

func TestOversizedBody(t *testing.T) {
	_, ts := testServer(t)
	if code, _ := post(t, ts.URL+"/v1/recommend", `{"user_id":"`+strings.Repeat("x", MaxBody)+`","k":1}`); code != http.StatusRequestEntityTooLarge {
		t.Fatalf("status %d", code)
	}
}

func TestProbesAndMetrics(t *testing.T) {
	s, ts := testServer(t)
	get := func(path string) (int, string) {
		resp, err := http.Get(ts.URL + path)
		if err != nil {
			t.Fatal(err)
		}
		return resp.StatusCode, string(readBody(t, resp))
	}
	if code, _ := get("/readyz"); code != http.StatusServiceUnavailable {
		t.Fatalf("readyz before ready: %d", code)
	}
	s.SetReady(true)
	if code, _ := get("/readyz"); code != http.StatusOK {
		t.Fatalf("readyz: %d", code)
	}
	if code, _ := get("/livez"); code != http.StatusOK {
		t.Fatalf("livez: %d", code)
	}
	post(t, ts.URL+"/v1/recommend", `{"user_id":"u","k":1}`)
	if _, body := get("/metrics"); !strings.Contains(body, `recserve_http_requests_total{code="200",route="recommend"} 1`) {
		t.Fatalf("metrics lack the request:\n%s", body)
	}
}

func FuzzParseRecommend(f *testing.F) {
	f.Add([]byte(`{"user_id":"u","k":5,"ts":10,"exclude":["a"]}`))
	f.Add([]byte(`{"user_id":"u","k":101}`))
	f.Fuzz(func(t *testing.T, body []byte) {
		req, err := ParseRecommend(bytes.NewReader(body), 7)
		if err != nil {
			return
		}
		if req.User == "" || req.K < 1 || req.K > recommend.MaxK {
			t.Fatalf("accepted invalid request %+v", req)
		}
	})
}
