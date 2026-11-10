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
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"

	"github.com/sHyanorn/recserve/internal/bundle"
	"github.com/sHyanorn/recserve/internal/cache"
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
	s := New(live(t, b, store), store, slog.New(slog.NewTextHandler(io.Discard, nil)))
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

// live serves a pipeline over b and h.
func live(t *testing.T, b *bundle.Bundle, h history.Store) *recommend.Live {
	t.Helper()
	return recommend.NewLive(pipeline(t, b, h), slog.New(slog.NewTextHandler(io.Discard, nil)))
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
	// zz is stored too, though no bundle knows it; requests ignore it.
	if code != http.StatusAccepted || out["accepted"] != 2.0 || len(out) != 1 {
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
		{"/v1/recommend", `{"user_id":"u","k":5,"recent":["a"` + strings.Repeat(`,"a"`, recommend.MaxRecent) + `]}`},
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
		if req.User == "" || req.K < 1 || req.K > recommend.MaxK || len(req.Recent) > recommend.MaxRecent {
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
		evs = append(evs, history.Event{ID: uint64(i), User: "u", Item: fmt.Sprint("i", 1+97*i), TS: int64(i)})
	}
	if err := store.Append(context.Background(), evs); err != nil {
		t.Fatal(err)
	}
	s := New(live(t, b, store), store, slog.New(slog.NewTextHandler(io.Discard, nil)))
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

// A user with a history gets a user vector, so both shards are searched; a
// user without one gets no vector retrieval, and coverage is null.
func TestCoverage(t *testing.T) {
	_, ts := testServer(t)
	post(t, ts.URL+"/v1/events", `{"events":[{"user_id":"u","item_id":"a","ts":500,"kind":"view"}]}`)
	for _, c := range []struct {
		user string
		want string
	}{{"u", `{"shards":2,"answered":2}`}, {"new", `null`}} {
		resp, err := http.Post(ts.URL+"/v1/recommend", "application/json", strings.NewReader(`{"user_id":"`+c.user+`","k":2}`))
		if err != nil {
			t.Fatal(err)
		}
		var out map[string]json.RawMessage
		if err := json.Unmarshal(readBody(t, resp), &out); err != nil {
			t.Fatal(err)
		}
		if got := string(out["coverage"]); got != c.want {
			t.Fatalf("%s: coverage %s, want %s", c.user, got, c.want)
		}
	}
}

// failingStore stores events but cannot read them.
type failingStore struct{ history.Store }

func (failingStore) Before(context.Context, string, int64, int) ([]string, error) {
	return nil, errors.New("store down")
}

// blockingStore reads nothing until the request's context ends.
type blockingStore struct {
	history.Store
	entered chan struct{}
}

func (s blockingStore) Before(ctx context.Context, _ string, _ int64, _ int) ([]string, error) {
	close(s.entered)
	<-ctx.Done()
	return nil, ctx.Err()
}

// degradedServer returns a server over testServer's bundle and store h,
// logging into log.
func degradedServer(t *testing.T, h history.Store, log io.Writer) *Server {
	t.Helper()
	m, err := vec.NewMatrix(2, []float32{0, 0, 1, 0, 0.9, 0.1, 0, 1, -1, 0})
	if err != nil {
		t.Fatal(err)
	}
	b := bundle.New(bundle.Manifest{ModelVersion: "v1", ItemCount: 4, Dim: 2, MaxSeqLen: 50},
		[]string{"", "a", "b", "c", "d"}, m, nil, []int32{4, 3})
	return New(live(t, b, h), h, slog.New(slog.NewTextHandler(log, nil)))
}

// With the store failing, the response is 200, uses the request's recent
// items as the history, reports history_unavailable, counts it and logs the
// error.
func TestHistoryUnavailableUsesRecent(t *testing.T) {
	var log bytes.Buffer
	s := degradedServer(t, failingStore{history.NewMemory()}, &log)
	ts := httptest.NewServer(s.Handler())
	t.Cleanup(ts.Close)
	code, out := post(t, ts.URL+"/v1/recommend", `{"user_id":"u","k":3,"recent":["a","zz"]}`)
	if code != http.StatusOK {
		t.Fatalf("%d %v", code, out)
	}
	if got := ids(out); len(got) != 3 || got[0] != "b" || slices.Contains(got, "a") {
		t.Fatalf("items %v; want b first (closest to a), a never", got)
	}
	if d, _ := out["degraded"].([]any); len(d) != 1 || d[0] != recommend.HistoryUnavailable {
		t.Fatalf("degraded %v", out["degraded"])
	}
	if got := testutil.ToFloat64(s.degraded.WithLabelValues(recommend.HistoryUnavailable)); got != 1 {
		t.Fatalf("counted %v", got)
	}
	if !strings.Contains(log.String(), "store down") || !strings.Contains(log.String(), "level=WARN") {
		t.Fatalf("log: %s", log.String())
	}
}

// A request cancelled by its client is answered 499, one past its caller's
// deadline 504; neither is a degradation.
func TestEndedRequestStatus(t *testing.T) {
	for _, c := range []struct {
		deadline bool
		want     int
	}{{false, statusClientClosed}, {true, http.StatusGatewayTimeout}} {
		store := blockingStore{history.NewMemory(), make(chan struct{})}
		s := degradedServer(t, store, io.Discard)
		ctx, cancel := context.WithCancel(context.Background())
		if c.deadline {
			ctx, cancel = context.WithTimeout(context.Background(), 20*time.Millisecond)
		} else {
			go func() {
				<-store.entered
				cancel()
			}()
		}
		rec := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodPost, "/v1/recommend", strings.NewReader(`{"user_id":"u","k":3}`)).WithContext(ctx)
		s.Handler().ServeHTTP(rec, req)
		cancel()
		if rec.Code != c.want {
			t.Fatalf("deadline %v: status %d, want %d: %s", c.deadline, rec.Code, c.want, rec.Body.String())
		}
		for _, r := range recommend.Reasons {
			if got := testutil.ToFloat64(s.degraded.WithLabelValues(r)); got != 0 {
				t.Fatalf("%s counted %v", r, got)
			}
		}
	}
}

// An encoder that fails gives a 200 without vector retrieval: the
// response reports encoder_error and null coverage, and the error is
// counted and logged.
func TestEncoderErrorDegrades(t *testing.T) {
	if err := onnx.Init(onnxtest.Lib(t)); err != nil {
		t.Fatal(err)
	}
	const n = onnxtest.MeanItems
	m, err := vec.NewMatrix(onnxtest.MeanDim, onnxtest.MeanVectors())
	if err != nil {
		t.Fatal(err)
	}
	raw := make([]string, n+1)
	for i := 1; i <= n; i++ {
		raw[i] = fmt.Sprint("i", i)
	}
	b := bundle.New(bundle.Manifest{ModelVersion: "v1", ItemCount: n, Dim: onnxtest.MeanDim, MaxSeqLen: 5}, raw, m, nil, []int32{3, 7})
	if b.Encoder, err = onnx.NewEncoder(onnxtest.Model(t, "mean.onnx"), 5, onnxtest.MeanDim); err != nil {
		t.Fatal(err)
	}
	if err := b.Encoder.Close(); err != nil { // later calls fail with onnx.ErrClosed
		t.Fatal(err)
	}
	store := history.NewMemory()
	if err := store.Append(context.Background(), []history.Event{{ID: 1, User: "u", Item: "i1", TS: 1}}); err != nil {
		t.Fatal(err)
	}
	var log bytes.Buffer
	s := New(live(t, b, store), store, slog.New(slog.NewTextHandler(&log, nil)))
	ts := httptest.NewServer(s.Handler())
	t.Cleanup(ts.Close)
	code, out := post(t, ts.URL+"/v1/recommend", `{"user_id":"u","k":2,"ts":5}`)
	if code != http.StatusOK || out["coverage"] != nil {
		t.Fatalf("%d %v", code, out)
	}
	if got := ids(out); !slices.Equal(got, []string{"i3", "i7"}) {
		t.Fatalf("items %v, want the popular ones", got)
	}
	if d, _ := out["degraded"].([]any); len(d) != 1 || d[0] != recommend.EncoderError {
		t.Fatalf("degraded %v", out["degraded"])
	}
	if got := testutil.ToFloat64(s.degraded.WithLabelValues(recommend.EncoderError)); got != 1 {
		t.Fatalf("counted %v", got)
	}
	if !strings.Contains(log.String(), onnx.ErrClosed.Error()) {
		t.Fatalf("log: %s", log.String())
	}
}

// Lookups are counted by result; without a cache nothing is counted.
func TestCacheLookupsAreCounted(t *testing.T) {
	s, ts := testServer(t)
	post(t, ts.URL+"/v1/recommend", `{"user_id":"u","k":2}`)
	p, release, err := s.Live.Acquire()
	if err != nil {
		t.Fatal(err)
	}
	release()
	p.Cache = cache.New[recommend.Response](10, time.Hour)
	for range 3 {
		if code, out := post(t, ts.URL+"/v1/recommend", `{"user_id":"u","k":2}`); code != http.StatusOK || len(ids(out)) != 2 {
			t.Fatalf("%d %v", code, out)
		}
	}
	for result, want := range map[string]float64{"miss": 1, "hit": 2} {
		if got := testutil.ToFloat64(s.cacheLookups.WithLabelValues(result)); got != want {
			t.Fatalf("%s: %v, want %v", result, got, want)
		}
	}
}

// A request that fails after its cache lookup is counted like any other.
func TestCacheLookupOfFailedRequestIsCounted(t *testing.T) {
	s, _ := testServer(t)
	p, release, err := s.Live.Acquire()
	if err != nil {
		t.Fatal(err)
	}
	release()
	p.Cache = cache.New[recommend.Response](10, time.Hour)
	if err := s.History.Append(context.Background(), []history.Event{{ID: 1, User: "u", Item: "a", TS: 1}}); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel() // the history and the cache are read, then vector retrieval finds the context ended
	req := httptest.NewRequest(http.MethodPost, "/v1/recommend", strings.NewReader(`{"user_id":"u","k":2}`)).WithContext(ctx)
	rec := httptest.NewRecorder()
	s.Handler().ServeHTTP(rec, req)
	if rec.Code != 499 {
		t.Fatalf("status %d: %s", rec.Code, rec.Body)
	}
	if got := testutil.ToFloat64(s.cacheLookups.WithLabelValues("miss")); got != 1 {
		t.Fatalf("miss counted %v times, want 1", got)
	}
}
