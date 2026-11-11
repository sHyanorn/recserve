package tracing_test

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace"
	"google.golang.org/grpc"

	"github.com/sHyanorn/recserve/internal/annshard"
	"github.com/sHyanorn/recserve/internal/annshard/annshardtest"
	"github.com/sHyanorn/recserve/internal/api"
	"github.com/sHyanorn/recserve/internal/bundle"
	"github.com/sHyanorn/recserve/internal/history"
	"github.com/sHyanorn/recserve/internal/recommend"
	"github.com/sHyanorn/recserve/internal/shard"
	"github.com/sHyanorn/recserve/internal/tracing"
	"github.com/sHyanorn/recserve/internal/vec"
)

// rec records every span of the package's tests: a tracer provider set
// once stays the one behind tracers taken before it, such as the
// pipeline's, so the tests share it.
var rec = tracetest.NewSpanRecorder()

func TestMain(m *testing.M) {
	tracing.Install(sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(rec)))
	os.Exit(m.Run())
}

// checkTrace sends one recommend request to a gateway over two shards on
// replicas, wired as cmd/gateway and cmd/annshard wire them when tracing;
// without propagate, the instrumentation is built with no propagator, as
// the global default is. It requires the
// replicas' spans of the request's searches to be in the trace of the
// gateway's HTTP span, each below a client call below the retrieval stage
// below the HTTP span, with the history and encoder stages beside
// retrieval, and the probe untraced.
func checkTrace(t *testing.T, propagate bool) error {
	if !propagate { // the instrumentation reads the propagator when it is built
		otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator())
		defer otel.SetTextMapPropagator(propagation.TraceContext{})
	}
	before := len(rec.Ended())

	m, err := vec.NewMatrix(2, []float32{0, 0, 1, 0, 0.9, 0.1, 0, 1, -1, 0})
	if err != nil {
		return err
	}
	root := t.TempDir()
	version := annshardtest.WriteVectors(t, root, m)
	addrs := [][]string{{annshardtest.Start(t, root, tracing.ServerOption())}, {annshardtest.Start(t, root, tracing.ServerOption())}}
	cfg := shard.Config{Shards: 2, Mode: shard.Exact}
	client := annshardtest.Dial(t, addrs, annshard.Options{Config: cfg, Dial: []grpc.DialOption{tracing.ClientOption()}})
	shards, err := client.Prepare(context.Background(), version, 4)
	if err != nil {
		return err
	}
	b := bundle.New(bundle.Manifest{ModelVersion: version, ItemCount: 4, Dim: 2, MaxSeqLen: 50}, []string{"", "a", "b", "c", "d"}, m, nil, []int32{4, 3})
	store := history.NewMemory()
	if err := store.Append(context.Background(), []history.Event{{ID: 1, User: "u", Item: "a", TS: 1}}); err != nil {
		return err
	}
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	srv := api.New(recommend.NewLive(recommend.New(b, store, shards), log), store, log)
	ts := httptest.NewServer(tracing.Handler(srv.Handler(), "/v1/recommend"))
	defer ts.Close()

	resp, err := http.Post(ts.URL+"/v1/recommend", "application/json", strings.NewReader(`{"user_id":"u","k":2,"ts":5}`))
	if err != nil {
		return err
	}
	if err := errors.Join(drain(resp), statusOK(resp)); err != nil {
		return err
	}
	probe, err := http.Get(ts.URL + "/readyz")
	if err != nil {
		return err
	}
	if err := drain(probe); err != nil {
		return err
	}

	// A server span may end just after its client has the answer.
	deadline := time.Now().Add(2 * time.Second)
	for {
		err := checkSpans(rec.Ended()[before:])
		if err == nil || time.Now().After(deadline) {
			return err
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func drain(resp *http.Response) error {
	_, err := io.Copy(io.Discard, resp.Body)
	return errors.Join(err, resp.Body.Close())
}

func statusOK(resp *http.Response) error {
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("recommend answered %d", resp.StatusCode)
	}
	return nil
}

func checkSpans(spans []sdktrace.ReadOnlySpan) error {
	byID := map[trace.SpanID]sdktrace.ReadOnlySpan{}
	var request sdktrace.ReadOnlySpan
	for _, s := range spans {
		byID[s.SpanContext().SpanID()] = s
		switch s.Name() {
		case "POST /v1/recommend":
			request = s
		case "GET /readyz":
			return errors.New("the probe was traced")
		}
	}
	if request == nil {
		return errors.New("no span for the request")
	}
	attrs := map[string]string{}
	for _, a := range request.Attributes() {
		attrs[string(a.Key)] = a.Value.String()
	}
	if attrs["recserve.arm"] != "default" || attrs["recserve.ranking"] != "retrieval" || attrs["recserve.model_version"] == "" {
		return fmt.Errorf("the request's span has attributes %v", attrs)
	}
	parent := func(s sdktrace.ReadOnlySpan) string {
		if p, ok := byID[s.Parent().SpanID()]; ok && s.SpanContext().TraceID() == request.SpanContext().TraceID() {
			return p.Name()
		}
		return ""
	}
	searches := 0
	stages := map[string]bool{}
	for _, s := range spans {
		switch {
		case strings.HasSuffix(s.Name(), "AnnShard/Search") && s.SpanKind() == trace.SpanKindServer:
			call, ok := byID[s.Parent().SpanID()]
			if !ok || s.SpanContext().TraceID() != request.SpanContext().TraceID() || call.SpanKind() != trace.SpanKindClient || parent(call) != "retrieval" {
				return fmt.Errorf("a replica's search span is not below a call of the request's retrieval (parent %q)", parent(s))
			}
			searches++
		case parent(s) == request.Name():
			stages[s.Name()] = true
		}
	}
	if searches != 2 {
		return fmt.Errorf("%d replica search spans in the request's trace, want one per shard", searches)
	}
	for _, stage := range []string{"history", "encoder", "retrieval"} { // no cache, no ranker
		if !stages[stage] {
			return fmt.Errorf("no %s span below the request's", stage)
		}
	}
	return nil
}

func TestTraceJoinsGatewayAndReplicas(t *testing.T) {
	if err := checkTrace(t, true); err != nil {
		t.Fatal(err)
	}
}

// The broken variant has no propagator: the replicas' spans start traces of
// their own.
func TestTraceCatchesMissingPropagator(t *testing.T) {
	err := checkTrace(t, false)
	if err == nil || !strings.Contains(err.Error(), "replica") {
		t.Fatalf("broken variant: %v", err)
	}
}
