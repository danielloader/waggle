package ingest_test

// Mirror of sdk_integration_test.go but driving the gRPC ingest path:
// we register GRPCHandler on a real grpc.Server bound to a loopback port
// and use the OTel SDK's grpc exporters to push payloads through it.
// These tests are the compliance proof that the gRPC transport reaches
// SQLite via the same Writer.Enqueue path as HTTP.

import (
	"context"
	"log/slog"
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/exporters/otlp/otlplog/otlploggrpc"
	"go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetricgrpc"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	otellog "go.opentelemetry.io/otel/log"
	"go.opentelemetry.io/otel/metric"
	sdklog "go.opentelemetry.io/otel/sdk/log"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/trace"
	coltracepb "go.opentelemetry.io/proto/otlp/collector/trace/v1"
	tracepb "go.opentelemetry.io/proto/otlp/trace/v1"
	"google.golang.org/grpc"
	grpccodes "google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"

	"github.com/danielloader/waggle/internal/ingest"
	"github.com/danielloader/waggle/internal/store"
	"github.com/danielloader/waggle/internal/store/sqlite"
)

type grpcFixture struct {
	t      *testing.T
	ctx    context.Context
	cancel context.CancelFunc
	st     *sqlite.Store
	writer *ingest.Writer
	srv    *grpc.Server
	addr   string
}

func newGRPCFixture(t *testing.T) *grpcFixture {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())

	st, err := sqlite.Open(ctx, filepath.Join(t.TempDir(), "test.db"))
	if err != nil {
		t.Fatalf("sqlite.Open: %v", err)
	}

	log := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError}))
	w := ingest.NewWriter(st, log, ingest.WriterConfig{
		FlushEvery:  5 * time.Millisecond,
		FlushEvents: 10,
	})
	w.Start(ctx)

	h := ingest.NewGRPCHandler(w, log)
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	srv := grpc.NewServer()
	h.Register(srv)
	go func() { _ = srv.Serve(lis) }()

	return &grpcFixture{
		t:      t,
		ctx:    ctx,
		cancel: cancel,
		st:     st,
		writer: w,
		srv:    srv,
		addr:   lis.Addr().String(),
	}
}

func (f *grpcFixture) close() {
	f.srv.GracefulStop()
	sctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	_ = f.writer.Stop(sctx)
	_ = f.st.Close()
	f.cancel()
}

// -----------------------------------------------------------------------------
// Traces: real otlptracegrpc exporter
// -----------------------------------------------------------------------------

func TestSDK_GRPC_TracesRoundTrip(t *testing.T) {
	f := newGRPCFixture(t)
	defer f.close()

	exp, err := otlptracegrpc.New(f.ctx,
		otlptracegrpc.WithEndpoint(f.addr),
		otlptracegrpc.WithInsecure(),
	)
	if err != nil {
		t.Fatalf("otlptracegrpc.New: %v", err)
	}
	res, err := resource.New(f.ctx,
		resource.WithAttributes(
			attribute.String("service.name", "sdk-grpc-trace-test"),
			attribute.String("service.version", "1.2.3"),
		),
	)
	if err != nil {
		t.Fatalf("resource.New: %v", err)
	}
	tp := sdktrace.NewTracerProvider(
		sdktrace.WithBatcher(exp, sdktrace.WithBatchTimeout(50*time.Millisecond)),
		sdktrace.WithResource(res),
	)
	otel.SetTracerProvider(tp)
	tr := tp.Tracer("sdk-grpc-trace-test")

	ctx, root := tr.Start(f.ctx, "GET /things")
	root.SetAttributes(
		attribute.String("http.request.method", "GET"),
		attribute.Int("http.response.status_code", 200),
	)
	_, child := tr.Start(ctx, "do-thing")
	child.SetAttributes(attribute.String("rpc.service", "ThingService"))
	child.AddEvent("step", trace.WithAttributes(attribute.String("k", "v")))
	child.SetStatus(codes.Error, "boom")
	child.End()
	root.End()

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := tp.Shutdown(shutdownCtx); err != nil {
		t.Fatalf("tp.Shutdown: %v", err)
	}

	waitFor(t, 3*time.Second, "service row with 2 spans", func() bool {
		svcs, err := f.st.ListServices(f.ctx)
		if err != nil {
			return false
		}
		for _, s := range svcs {
			if s.ServiceName == "sdk-grpc-trace-test" && s.SpanCount == 2 {
				return true
			}
		}
		return false
	})

	traces, _, err := f.st.ListTraces(f.ctx, store.TraceFilter{})
	if err != nil {
		t.Fatalf("ListTraces: %v", err)
	}
	var traceID string
	for _, tr := range traces {
		if tr.RootName == "GET /things" {
			traceID = tr.TraceID
			break
		}
	}
	if traceID == "" {
		t.Fatalf("could not find root trace in %+v", traces)
	}

	detail, err := f.st.GetTrace(f.ctx, traceID)
	if err != nil {
		t.Fatalf("GetTrace: %v", err)
	}
	if len(detail.Spans) != 2 {
		t.Fatalf("expected 2 spans, got %d", len(detail.Spans))
	}
	var sawError bool
	for _, s := range detail.Spans {
		if s.Name == "do-thing" {
			if s.StatusCode != 2 {
				t.Errorf("child span status_code: want 2 (ERROR), got %d", s.StatusCode)
			}
			if len(s.Events) != 1 || s.Events[0].Name != "step" {
				t.Errorf("child span events: %+v", s.Events)
			}
			sawError = true
		}
	}
	if !sawError {
		t.Errorf("did not see do-thing span")
	}
}

// -----------------------------------------------------------------------------
// Logs: real otlploggrpc exporter
// -----------------------------------------------------------------------------

func TestSDK_GRPC_LogsRoundTrip(t *testing.T) {
	f := newGRPCFixture(t)
	defer f.close()

	exp, err := otlploggrpc.New(f.ctx,
		otlploggrpc.WithEndpoint(f.addr),
		otlploggrpc.WithInsecure(),
	)
	if err != nil {
		t.Fatalf("otlploggrpc.New: %v", err)
	}
	res, err := resource.New(f.ctx,
		resource.WithAttributes(
			attribute.String("service.name", "sdk-grpc-log-test"),
		),
	)
	if err != nil {
		t.Fatalf("resource.New: %v", err)
	}
	lp := sdklog.NewLoggerProvider(
		sdklog.WithProcessor(sdklog.NewBatchProcessor(exp, sdklog.WithExportInterval(50*time.Millisecond))),
		sdklog.WithResource(res),
	)
	lg := lp.Logger("sdk-grpc-log-test")

	emit := func(severity otellog.Severity, sevText, body string, kv ...otellog.KeyValue) {
		var r otellog.Record
		r.SetTimestamp(time.Now())
		r.SetSeverity(severity)
		r.SetSeverityText(sevText)
		r.SetBody(otellog.StringValue(body))
		if len(kv) > 0 {
			r.AddAttributes(kv...)
		}
		lg.Emit(f.ctx, r)
	}

	emit(otellog.SeverityInfo, "INFO", "grpc info line",
		otellog.String("component", "http"),
	)
	emit(otellog.SeverityError, "ERROR", "grpc error: connection refused",
		otellog.String("component", "db"),
	)

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := lp.Shutdown(shutdownCtx); err != nil {
		t.Fatalf("lp.Shutdown: %v", err)
	}

	waitFor(t, 3*time.Second, "two logs from sdk-grpc-log-test", func() bool {
		logs, _, err := f.st.SearchLogs(f.ctx, store.LogFilter{Service: "sdk-grpc-log-test"})
		return err == nil && len(logs) == 2
	})

	found, _, err := f.st.SearchLogs(f.ctx, store.LogFilter{Query: "connection"})
	if err != nil {
		t.Fatalf("FTS SearchLogs: %v", err)
	}
	if len(found) != 1 || !strings.Contains(found[0].Body, "connection refused") {
		t.Fatalf("FTS expected 1 match on 'connection', got %+v", found)
	}
}

// -----------------------------------------------------------------------------
// Metrics: real otlpmetricgrpc exporter
// -----------------------------------------------------------------------------

func TestSDK_GRPC_MetricsRoundTrip(t *testing.T) {
	f := newGRPCFixture(t)
	defer f.close()

	exp, err := otlpmetricgrpc.New(f.ctx,
		otlpmetricgrpc.WithEndpoint(f.addr),
		otlpmetricgrpc.WithInsecure(),
		otlpmetricgrpc.WithTemporalitySelector(func(sdkmetric.InstrumentKind) metricdata.Temporality {
			return metricdata.CumulativeTemporality
		}),
	)
	if err != nil {
		t.Fatalf("otlpmetricgrpc.New: %v", err)
	}
	res, err := resource.New(f.ctx, resource.WithAttributes(
		attribute.String("service.name", "sdk-grpc-metric-test"),
	))
	if err != nil {
		t.Fatalf("resource.New: %v", err)
	}
	mp := sdkmetric.NewMeterProvider(
		sdkmetric.WithResource(res),
		sdkmetric.WithReader(sdkmetric.NewPeriodicReader(exp,
			sdkmetric.WithInterval(50*time.Millisecond),
		)),
	)

	counter, err := mp.Meter("sdk-grpc").Int64Counter("requests.total",
		metric.WithUnit("1"),
		metric.WithDescription("total requests"),
	)
	if err != nil {
		t.Fatalf("Int64Counter: %v", err)
	}
	for range 4 {
		counter.Add(f.ctx, 1, metric.WithAttributes(attribute.String("http.method", "GET")))
	}

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := mp.Shutdown(shutdownCtx); err != nil {
		t.Fatalf("mp.Shutdown: %v", err)
	}

	waitFor(t, 3*time.Second, "metric_events row for sdk-grpc-metric-test", func() bool {
		var n int
		err := f.st.ReaderDB().QueryRowContext(f.ctx, `
			SELECT COUNT(*) FROM metric_events
			WHERE service_name = ?
			  AND json_extract(attributes, '$."requests.total"') IS NOT NULL`,
			"sdk-grpc-metric-test",
		).Scan(&n)
		return err == nil && n > 0
	})
}

// -----------------------------------------------------------------------------
// Backpressure: writer queue full → server returns codes.Unavailable
// -----------------------------------------------------------------------------

// TestGRPC_BackpressureUnavailable bypasses the SDK exporter (which retries
// internally) and calls Export directly via a low-level gRPC client so we
// can observe the raw status code on the very first rejection.
func TestGRPC_BackpressureUnavailable(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	st, err := sqlite.Open(ctx, filepath.Join(t.TempDir(), "test.db"))
	if err != nil {
		t.Fatalf("sqlite.Open: %v", err)
	}
	defer st.Close()

	log := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError}))
	// BufferSize: 1, and we deliberately do NOT call Start — so the channel
	// fills with the first request and rejects everything after.
	w := ingest.NewWriter(st, log, ingest.WriterConfig{BufferSize: 1})

	h := ingest.NewGRPCHandler(w, log)
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	srv := grpc.NewServer()
	h.Register(srv)
	go func() { _ = srv.Serve(lis) }()
	defer srv.GracefulStop()

	conn, err := grpc.NewClient(lis.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("grpc.NewClient: %v", err)
	}
	defer conn.Close()
	client := coltracepb.NewTraceServiceClient(conn)

	req := &coltracepb.ExportTraceServiceRequest{
		ResourceSpans: []*tracepb.ResourceSpans{{}},
	}

	// First call lands in the buffer-of-1.
	if _, err := client.Export(ctx, req); err != nil {
		t.Fatalf("first Export should succeed, got: %v", err)
	}
	// Second call must hit codes.Unavailable — the OTLP backpressure signal.
	_, err = client.Export(ctx, req)
	if err == nil {
		t.Fatalf("expected error on second Export, got nil")
	}
	if got := status.Code(err); got != grpccodes.Unavailable {
		t.Fatalf("expected codes.Unavailable, got %v (%v)", got, err)
	}
	if dropped := w.DroppedCount(); dropped != 1 {
		t.Errorf("DroppedCount: want 1, got %d", dropped)
	}
}
