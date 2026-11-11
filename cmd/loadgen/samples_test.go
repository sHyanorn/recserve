package main

import (
	"compress/gzip"
	"context"
	"encoding/csv"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
)

// Every measured request has a sample with its index, path, outcome, the
// reasons it was degraded and its shard coverage, as the gateway sent them.
func TestSamples(t *testing.T) {
	var n atomic.Int64
	mux := http.NewServeMux()
	mux.HandleFunc("GET /readyz", func(http.ResponseWriter, *http.Request) {})
	mux.HandleFunc("POST /v1/recommend", func(w http.ResponseWriter, _ *http.Request) {
		body := `{"items":[],"arm":"default","model_version":"b","degraded":[],"coverage":{"shards":2,"answered":2}}`
		if n.Add(1)%2 == 0 {
			body = `{"items":[],"arm":"default","model_version":"b","degraded":["history_unavailable","shards_partial"],"coverage":{"shards":2,"answered":1}}`
		}
		_, _ = w.Write([]byte(body)) //nolint:errcheck // loadgen may have given up on the request; the test checks what it received
	})
	mux.HandleFunc("POST /v1/events", func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusAccepted) })
	s := httptest.NewServer(mux)
	t.Cleanup(s.Close)
	cfg := testConfig(strings.TrimPrefix(s.URL, "http://"))
	cfg.samples = filepath.Join(t.TempDir(), "samples.csv.gz")
	r, err := measure(context.Background(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	rows := readSamples(t, cfg.samples)
	if len(rows) != r.Outcomes["recommend_ok"]+r.Outcomes["recommend_degraded"]+r.Outcomes["events_ok"] {
		t.Fatalf("%d samples, outcomes %v", len(rows), r.Outcomes)
	}
	want := map[string]bool{
		"/v1/recommend,200,recommend_ok,,2,2":                                         true,
		"/v1/recommend,200,recommend_degraded,history_unavailable|shards_partial,1,2": true,
		"/v1/events,202,events_ok,,-1,-1":                                             true,
	}
	seen := map[string]int{}
	first := 100 // the warmup's 100 requests are not measured
	for k, row := range rows {
		if i, err := strconv.Atoi(row[0]); err != nil || i != first+k {
			t.Fatalf("sample %d has index %s, want %d", k, row[0], first+k)
		}
		key := strings.Join([]string{row[1], row[4], row[5], row[6], row[7], row[8]}, ",")
		if !want[key] {
			t.Fatalf("sample %v", row)
		}
		seen[key]++
		due, err1 := strconv.ParseFloat(row[2], 64)
		latency, err2 := strconv.ParseFloat(row[3], 64)
		if err1 != nil || err2 != nil || due < 200 || due > 1300 || latency <= 0 {
			t.Fatalf("sample %v: due and latency in ms", row)
		}
	}
	if len(seen) != len(want) {
		t.Fatalf("kinds of samples %v", seen)
	}
}

func readSamples(t *testing.T, path string) [][]string {
	t.Helper()
	f, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close() //nolint:errcheck // read only
	z, err := gzip.NewReader(f)
	if err != nil {
		t.Fatal(err)
	}
	rows, err := csv.NewReader(z).ReadAll()
	if err != nil {
		t.Fatal(err)
	}
	if strings.Join(rows[0], ",") != "i,path,due_ms,latency_ms,status,outcome,degraded,answered,shards" {
		t.Fatalf("header %v", rows[0])
	}
	return rows[1:]
}

// A recommendation must name its arm, as every response of the gateway does.
func TestRecommendationWithoutArmIsAnError(t *testing.T) {
	if got := classify("/v1/recommend", http.StatusOK, []byte(`{"items":[],"model_version":"v"}`), nil); got != "error" {
		t.Fatalf("classified %s", got)
	}
}

// With repeats, about that share of the requests copies one of the 1,000
// requests before it; the others replay the rows in order. The same seed
// gives the same requests.
func TestRepeats(t *testing.T) {
	rows := make([]row, 10_000)
	for i := range rows {
		rows[i] = row{user: "u" + strconv.Itoa(i), item: "i", ts: int64(i)}
	}
	const total = 30_000
	w := newDatasetWorkload(rows, 5, 0, 0.3, 7, total)
	repeated := 0
	for i := range total {
		_, body := w.request(i, nil)
		got := string(body)
		if got == recommendBody(rows[i%len(rows)]) {
			continue
		}
		repeated++
		found := false
		for j := max(0, i-1000); j < i && !found; j++ {
			_, b := w.request(j, nil)
			found = string(b) == got
		}
		if !found {
			t.Fatalf("request %d, %s, copies none of the 1,000 before it", i, got)
		}
	}
	if share := float64(repeated) / total; share < 0.28 || share > 0.32 {
		t.Fatalf("%.3f of the requests repeat, want about 0.3", share)
	}
	again := newDatasetWorkload(rows, 5, 0, 0.3, 7, total)
	for i := range total {
		_, a := w.request(i, nil)
		_, b := again.request(i, nil)
		if string(a) != string(b) {
			t.Fatalf("request %d differs with the same seed", i)
		}
	}
}

func recommendBody(r row) string {
	return `{"user_id":"` + r.user + `","k":5,"ts":` + strconv.FormatInt(r.ts, 10) + `}`
}
