package api

import (
	"bytes"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"sync"
	"testing"

	"github.com/prometheus/client_golang/prometheus/testutil"

	"github.com/sHyanorn/recserve/internal/bundle"
	"github.com/sHyanorn/recserve/internal/recommend"
	"github.com/sHyanorn/recserve/internal/vec"
)

// swapServer returns testServer's server with an admin API whose Prepare
// serves directories by name: "v1" a bundle of the active version, "v2"
// one of version v2 that holds item e in place of d, and anything else an
// error.
func swapServer(t *testing.T) (s *Server, api, admin *httptest.Server, logs *bytes.Buffer) {
	t.Helper()
	s, api = testServer(t)
	logs = &bytes.Buffer{}
	s.Log = slog.New(slog.NewTextHandler(logs, nil))
	s.Prepare = func(dir string) (*recommend.Pipeline, error) {
		ids := []string{"", "a", "b", "c", "d"}
		switch dir {
		case "v1":
		case "v2":
			ids[4] = "e"
		default:
			return nil, errors.New("checksum mismatch in " + dir)
		}
		m, err := vec.NewMatrix(2, []float32{0, 0, 0, 1, 1, 0, 0.9, 0.1, -1, 0})
		if err != nil {
			return nil, err
		}
		b := bundle.New(bundle.Manifest{ModelVersion: dir, ItemCount: 4, Dim: 2, MaxSeqLen: 50}, ids, m, nil, []int32{1, 2})
		return pipeline(t, b, s.History), nil
	}
	admin = httptest.NewServer(s.AdminHandler())
	t.Cleanup(admin.Close)
	return s, api, admin, logs
}

func TestAdminSwap(t *testing.T) {
	s, api, admin, logs := swapServer(t)
	for _, tc := range []struct {
		body string
		code int
		want string // in the response's error
	}{
		{`{"bundle":"broken"}`, http.StatusUnprocessableEntity, "checksum mismatch in broken"},
		{`{"bundle":"v1"}`, http.StatusConflict, recommend.ErrActive.Error()},
		{`{"bundle":""}`, http.StatusBadRequest, "bundle is required"},
		{`{"dir":"v2"}`, http.StatusBadRequest, "unknown field"},
	} {
		code, out := post(t, admin.URL+"/v1/admin/swap", tc.body)
		if msg, _ := out["error"].(string); code != tc.code || !strings.Contains(msg, tc.want) {
			t.Fatalf("%s: %d %v, want %d with %q", tc.body, code, out, tc.code, tc.want)
		}
		if _, out := post(t, api.URL+"/v1/recommend", `{"user_id":"u","k":2}`); out["model_version"] != "v1" {
			t.Fatalf("%s: serving %v after a failed swap", tc.body, out["model_version"])
		}
	}
	if !strings.Contains(logs.String(), "swap rejected") {
		t.Fatalf("rejections not logged: %s", logs)
	}

	// w's history holds d, which only v1 knows, and e, which only v2 knows.
	if code, out := post(t, api.URL+"/v1/events", `{"events":[{"user_id":"w","item_id":"d","ts":400,"kind":"view"},{"user_id":"w","item_id":"e","ts":401,"kind":"view"}]}`); code != http.StatusAccepted || out["accepted"] != 2.0 {
		t.Fatalf("events before the swap: %d %v", code, out)
	}
	if _, out := post(t, api.URL+"/v1/recommend", `{"user_id":"w","k":4}`); !slices.Equal(ids(out), []string{"c", "b", "a"}) {
		t.Fatalf("w on v1: %v; want c, b, a: the history is d", out)
	}

	code, out := post(t, admin.URL+"/v1/admin/swap", `{"bundle":"v2"}`)
	if code != http.StatusOK || out["model_version"] != "v2" || out["previous"] != "v1" {
		t.Fatalf("swap to a bundle with other items: %d %v", code, out)
	}
	// On v2 the stored history reads as e: e is seen and decides the order.
	if _, out := post(t, api.URL+"/v1/recommend", `{"user_id":"w","k":4}`); out["model_version"] != "v2" || !slices.Equal(ids(out), []string{"a", "c", "b"}) {
		t.Fatalf("w on v2: %v; want a, c, b: the history is e", out)
	}
	if _, ok := out["drain_ms"].(float64); !ok {
		t.Fatalf("swap: no drain_ms in %v", out)
	}
	// Requests are served by the new bundle.
	if code, out := post(t, api.URL+"/v1/events", `{"events":[{"user_id":"u","item_id":"b","ts":500,"kind":"view"}]}`); code != http.StatusAccepted || out["accepted"] != 1.0 {
		t.Fatalf("events after the swap: %d %v", code, out)
	}
	code, out = post(t, api.URL+"/v1/recommend", `{"user_id":"u","k":1}`)
	if got := ids(out); code != http.StatusOK || out["model_version"] != "v2" || len(got) != 1 || got[0] != "c" {
		t.Fatalf("recommend after the swap: %d %v; want c, closest to b in v2", code, out)
	}
	for result, want := range map[string]float64{swapOK: 1, swapRejected: 2, swapBusy: 0} {
		if got := testutil.ToFloat64(s.swaps.WithLabelValues(result)); got != want {
			t.Fatalf("swaps %s: %v, want %v", result, got, want)
		}
	}
	// The admin routes are not on the API's handler, nor the API's on the admin's.
	if code, _ := postRaw(t, api.URL+"/v1/admin/swap", `{"bundle":"v1"}`); code != http.StatusNotFound && code != http.StatusMethodNotAllowed {
		t.Fatalf("admin route on the API listener: %d", code)
	}
	if code, _ := postRaw(t, admin.URL+"/v1/recommend", `{"user_id":"u","k":1}`); code != http.StatusNotFound {
		t.Fatalf("API route on the admin listener: %d", code)
	}
}

// A swap requested while another prepares gets 409 and changes nothing.
func TestAdminSwapBusy(t *testing.T) {
	s, _, admin, _ := swapServer(t)
	prepare := s.Prepare
	entered, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	s.Prepare = func(dir string) (*recommend.Pipeline, error) {
		once.Do(func() { close(entered); <-release })
		return prepare(dir)
	}
	first := make(chan int, 1)
	go func() {
		resp, err := http.Post(admin.URL+"/v1/admin/swap", "application/json", strings.NewReader(`{"bundle":"v2"}`))
		if err != nil {
			first <- 0
			return
		}
		_, err = io.Copy(io.Discard, resp.Body)
		if err := errors.Join(err, resp.Body.Close()); err != nil {
			first <- 0
			return
		}
		first <- resp.StatusCode
	}()
	<-entered
	code, out := post(t, admin.URL+"/v1/admin/swap", `{"bundle":"v2"}`)
	close(release)
	if code != http.StatusConflict || !strings.Contains(out["error"].(string), recommend.ErrSwapInProgress.Error()) {
		t.Fatalf("second swap: %d %v", code, out)
	}
	if code := <-first; code != http.StatusOK {
		t.Fatalf("first swap: %d", code)
	}
	if got := testutil.ToFloat64(s.swaps.WithLabelValues(swapBusy)); got != 1 {
		t.Fatalf("busy swaps: %v", got)
	}
}

func TestAdminSwapNotConfigured(t *testing.T) {
	s, _ := testServer(t)
	admin := httptest.NewServer(s.AdminHandler())
	t.Cleanup(admin.Close)
	if code, _ := post(t, admin.URL+"/v1/admin/swap", `{"bundle":"v2"}`); code != http.StatusNotImplemented {
		t.Fatalf("%d", code)
	}
}

func postRaw(t *testing.T, url, body string) (int, []byte) {
	t.Helper()
	resp, err := http.Post(url, "application/json", strings.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	return resp.StatusCode, readBody(t, resp)
}

// After the gateway closes its bundle, every route that needs it answers
// 503, as the readiness probe does, and logs nothing.
func TestClosedLiveIs503(t *testing.T) {
	s, api, admin, logs := swapServer(t)
	if err := s.Live.Close(); err != nil {
		t.Fatal(err)
	}
	for _, c := range []struct{ url, body string }{
		{api.URL + "/v1/recommend", `{"user_id":"u","k":3}`},
		{admin.URL + "/v1/admin/swap", `{"bundle":"v2"}`},
	} {
		if code, out := post(t, c.url, c.body); code != http.StatusServiceUnavailable {
			t.Fatalf("%s: %d %v", c.url, code, out)
		}
	}
	// Events need no bundle and are still stored.
	if code, out := post(t, api.URL+"/v1/events", `{"events":[{"user_id":"u","item_id":"a","ts":5,"kind":"view"}]}`); code != http.StatusAccepted {
		t.Fatalf("events: %d %v", code, out)
	}
	if logs.Len() != 0 {
		t.Fatalf("logged: %s", logs)
	}
}

// The admin listener serves the runtime's profiles; the public one does not.
func TestAdminServesProfiles(t *testing.T) {
	s, _ := testServer(t)
	admin := httptest.NewServer(s.AdminHandler())
	t.Cleanup(admin.Close)
	public := httptest.NewServer(s.Handler())
	t.Cleanup(public.Close)
	for _, c := range []struct {
		url  string
		want int
	}{{admin.URL + "/debug/pprof/cmdline", http.StatusOK}, {admin.URL + "/debug/pprof/heap", http.StatusOK}, {public.URL + "/debug/pprof/cmdline", http.StatusNotFound}} {
		resp, err := http.Get(c.url)
		if err != nil {
			t.Fatal(err)
		}
		readBody(t, resp)
		if resp.StatusCode != c.want {
			t.Errorf("GET %s: %d, want %d", c.url, resp.StatusCode, c.want)
		}
	}
}
