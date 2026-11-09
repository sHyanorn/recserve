package api

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"

	"github.com/sHyanorn/recserve/internal/bundle"
	"github.com/sHyanorn/recserve/internal/history"
	"github.com/sHyanorn/recserve/internal/onnx"
	"github.com/sHyanorn/recserve/internal/onnx/onnxtest"
	"github.com/sHyanorn/recserve/internal/recommend"
	"github.com/sHyanorn/recserve/internal/shard"
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
	s := New(pipeline(t, b, store), store, slog.New(slog.NewTextHandler(io.Discard, nil)))
	s.Now = func() time.Time { return time.UnixMilli(1000) }
	ts := httptest.NewServer(s.Handler())
	t.Cleanup(ts.Close)
	return s, ts
}

func pipeline(t *testing.T, b *bundle.Bundle, h history.Store) *recommend.Pipeline {
	t.Helper()
	p, err := recommend.New(b, h, shard.DefaultConfig)
	if err != nil {
		t.Fatal(err)
	}
	return p
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

// A value that cannot be encoded fails writeJSON before anything is written,
// so the caller can still send an error status.
func TestWriteJSONWritesNothingOnFailure(t *testing.T) {
	rec := httptest.NewRecorder()
	if err := writeJSON(rec, http.StatusOK, map[string]float64{"score": math.NaN()}); err == nil {
		t.Fatal("NaN encoded")
	}
	if rec.Body.Len() != 0 || rec.Header().Get("Content-Type") != "" {
		t.Fatalf("wrote %q with headers %v", rec.Body.String(), rec.Header())
	}
}

// A request with more candidates than the ranker takes counts them: 10
// history items with 50 distinct co-occurring items each, 100 popular items
// and 200 from vector retrieval make more than bundle.RankerRows.
func TestRankerTruncationIsCounted(t *testing.T) {
	if err := onnx.Init(onnxtest.Lib(t)); err != nil {
		t.Fatal(err)
	}
	const n, dim = 3000, 2
	data := make([]float32, (n+1)*dim)
	for i := dim; i < len(data); i++ {
		data[i] = float32(math.Sin(float64(i)))
	}
	m, err := vec.NewMatrix(dim, data)
	if err != nil {
		t.Fatal(err)
	}
	raw := make([]string, n+1)
	cooc := make([]int32, (n+1)*bundle.CoocWidth)
	for i := 1; i <= n; i++ {
		raw[i] = fmt.Sprint("i", i)
		for j := range bundle.CoocWidth {
			cooc[i*bundle.CoocWidth+j] = int32(1 + (i*bundle.CoocWidth+j)%n)
		}
	}
	popular := make([]int32, recommend.PopularTop)
	for i := range popular {
		popular[i] = int32(n - i)
	}
	b := bundle.New(bundle.Manifest{ModelVersion: "v1", ItemCount: n, Dim: dim, MaxSeqLen: 50}, raw, m, cooc, popular)
	if b.Ranker, err = onnx.NewRanker(onnxtest.Model(t, "linear_ranker.onnx"), len(bundle.RankerFeatures), bundle.RankerRows); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := b.Close(); err != nil {
			t.Error(err)
		}
	})
	store := history.NewMemory()
	var evs []history.Event
	for i := range recommend.CoocSeeds {
		evs = append(evs, history.Event{ID: uint64(i), User: "u", Item: int32(1 + 97*i), TS: int64(i)})
	}
	if err := store.Append(context.Background(), evs); err != nil {
		t.Fatal(err)
	}
	s := New(pipeline(t, b, store), store, slog.New(slog.NewTextHandler(io.Discard, nil)))
	ts := httptest.NewServer(s.Handler())
	t.Cleanup(ts.Close)
	for range 2 {
		if code, out := post(t, ts.URL+"/v1/recommend", `{"user_id":"u","k":10,"ts":100}`); code != http.StatusOK || len(ids(out)) != 10 {
			t.Fatalf("%d %v", code, out)
		}
	}
	if got := testutil.ToFloat64(s.rankerTruncated); got != 2 {
		t.Fatalf("truncated requests %v, want 2", got)
	}
	if got := testutil.ToFloat64(s.rankerDropped); got <= 0 || math.Mod(got, 2) != 0 {
		t.Fatalf("dropped candidates %v, want a positive count, the same for both requests", got)
	}
}
