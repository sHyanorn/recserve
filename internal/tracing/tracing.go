// Package tracing exports OpenTelemetry traces over OTLP when the
// environment names an endpoint, and traces the gateway's API and the gRPC
// calls between the gateways and the replicas (D051).
package tracing

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"slices"
	"time"

	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc/filters"
	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.43.0"
	"go.opentelemetry.io/otel/trace"
	"google.golang.org/grpc"
)

// Enabled reports whether the environment asks for traces, by setting
// OTEL_EXPORTER_OTLP_ENDPOINT or OTEL_EXPORTER_OTLP_TRACES_ENDPOINT.
// Without traces nothing is installed or wrapped.
func Enabled() bool {
	return os.Getenv("OTEL_EXPORTER_OTLP_ENDPOINT") != "" || os.Getenv("OTEL_EXPORTER_OTLP_TRACES_ENDPOINT") != ""
}

// FlushTimeout bounds the export of the spans left at exit.
const FlushTimeout = 2 * time.Second

// Start installs, if Enabled, a tracer provider that exports spans in
// batches over OTLP gRPC, configured by the standard environment: the
// endpoint, the sampler (OTEL_TRACES_SAMPLER; parent-based and always on by
// default) and the service name (OTEL_SERVICE_NAME, service otherwise). It
// does no I/O: the exporter connects in the background, and spans that
// cannot be exported are dropped, never waited for; failures are logged.
// stop, to call at exit, exports the spans left within FlushTimeout.
func Start(service string, log *slog.Logger) (stop func(), err error) {
	if !Enabled() {
		return func() {}, nil
	}
	ctx := context.Background()
	exp, err := otlptracegrpc.New(ctx)
	if err != nil {
		return nil, err
	}
	res, err := resource.New(ctx, resource.WithAttributes(semconv.ServiceName(service)), resource.WithFromEnv(), resource.WithTelemetrySDK())
	if err != nil {
		return nil, errors.Join(err, exp.Shutdown(ctx))
	}
	tp := sdktrace.NewTracerProvider(sdktrace.WithBatcher(exp), sdktrace.WithResource(res))
	otel.SetErrorHandler(otel.ErrorHandlerFunc(func(err error) { log.Warn("tracing failed", "err", err) }))
	Install(tp)
	return func() {
		ctx, cancel := context.WithTimeout(context.Background(), FlushTimeout)
		defer cancel()
		if err := tp.Shutdown(ctx); err != nil {
			log.Warn("exporting the last spans failed", "err", err)
		}
	}, nil
}

// Install makes tp the provider of the instrumentation and of the stage
// spans, and propagates the W3C trace context, so that a replica's spans
// join the trace of the gateway's request.
func Install(tp trace.TracerProvider) {
	otel.SetTracerProvider(tp)
	otel.SetTextMapPropagator(propagation.TraceContext{})
}

// Handler traces h's requests to the given paths, a span each named by the
// method and the path; other requests, as the probes', make none.
func Handler(h http.Handler, paths ...string) http.Handler {
	return otelhttp.NewHandler(h, "",
		otelhttp.WithFilter(func(r *http.Request) bool { return slices.Contains(paths, r.URL.Path) }),
		otelhttp.WithSpanNameFormatter(func(_ string, r *http.Request) string { return r.Method + " " + r.URL.Path }))
}

// ClientOption traces a gRPC client's calls and sends their trace context.
func ClientOption() grpc.DialOption {
	return grpc.WithStatsHandler(otelgrpc.NewClientHandler())
}

// ServerOption traces the calls a gRPC server answers, in the trace of the
// caller's span, except health checks.
func ServerOption() grpc.ServerOption {
	return grpc.StatsHandler(otelgrpc.NewServerHandler(otelgrpc.WithFilter(filters.Not(filters.HealthCheck()))))
}
