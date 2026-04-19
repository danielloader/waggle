package ingest_test

// These tests exercise the real OpenTelemetry Go SDK exporters against our
// ingest HTTP handlers. They are the compliance tests that prove arbitrary
// OTel SDK-generated payloads (protobuf, gzip-compressed by default, batched,
// with resource detection and semconv attributes) round-trip correctly into
// SQLite.

import (
	"context"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/exporters/otlp/otlplog/otlploghttp"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	otellog "go.opentelemetry.io/otel/log"
	sdklog "go.opentelemetry.io/otel/sdk/log"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/trace"

	"github.com/danielloader/waggle/internal/ingest"
	"github.com/danielloader/waggle/internal/store"
	"github.com/danielloader/waggle/internal/store/sqlite"
)

type fixture struct {
	t      *testing.T
	ctx    context.Context
	cancel context.CancelFunc
	st     *sqlite.Store
	writer *ingest.Writer
	srv    *httptest.Server
}

func newFixture(t *testing.T) *fixture {
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

	h := ingest.NewHandler(w, log)
	mux := http.NewServeMux()
	h.Mount(mux)
	srv := httptest.NewServer(mux)

	return &fixture{t: t, ctx: ctx, cancel: cancel, st: st, writer: w, srv: srv}
}

func (f *fixture) close() {
	f.srv.Close()
	sctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	_ = f.writer.Stop(sctx)
	_ = f.st.Close()
	f.cancel()
}

func (f *fixture) endpointHost() string {
	u, err := url.Parse(f.srv.URL)
	if err != nil {
		f.t.Fatal(err)
	}
	return u.Host
}

func waitFor(t *testing.T, timeout time.Duration, msg string, pred func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if pred() {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("timeout waiting for: %s", msg)
}

// -----------------------------------------------------------------------------
// Traces: real otlptracehttp exporter (protobuf + gzip by default)
// -----------------------------------------------------------------------------

func TestSDK_TracesRoundTrip(t *testing.T) {
	f := newFixture(t)
	defer f.close()

	exp, err := otlptracehttp.New(f.ctx,
		otlptracehttp.WithEndpoint(f.endpointHost()),
		otlptracehttp.WithInsecure(),
	)
	if err != nil {
		t.Fatalf("otlptracehttp.New: %v", err)
	}
	res, err := resource.New(f.ctx,
		resource.WithAttributes(
			attribute.String("service.name", "sdk-int-test"),
			attribute.String("service.version", "1.2.3"),
			attribute.String("deployment.environment", "test"),
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
	tr := tp.Tracer("sdk-int-test")

	// Emit a realistic 3-span trace: root HTTP server span, a DB child which
	// records an error, and an RPC child with an event.
	ctx, root := tr.Start(f.ctx, "GET /users")
	root.SetAttributes(
		attribute.String("http.request.method", "GET"),
		attribute.String("url.path", "/users"),
		attribute.String("http.route", "/users"),
		attribute.Int("http.response.status_code", 200),
	)

	_, db := tr.Start(ctx, "SELECT users")
	db.SetAttributes(
		attribute.String("db.system", "postgresql"),
		attribute.String("db.statement", "SELECT * FROM users LIMIT 100"),
	)
	db.SetStatus(codes.Error, "connection refused")
	db.End()

	_, rpc := tr.Start(ctx, "UserService/List")
	rpc.SetAttributes(
		attribute.String("rpc.service", "UserService"),
		attribute.String("rpc.method", "List"),
	)
	rpc.AddEvent("cache_miss", trace.WithAttributes(attribute.String("key", "users:all")))
	rpc.End()

	root.End()

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := tp.Shutdown(shutdownCtx); err != nil {
		t.Fatalf("Shutdown: %v", err)
	}

	waitFor(t, 3*time.Second, "service row with 3 spans", func() bool {
		svcs, err := f.st.ListServices(f.ctx)
		if err != nil {
			return false
		}
		for _, s := range svcs {
			if s.ServiceName == "sdk-int-test" && s.SpanCount == 3 {
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
		if tr.RootName == "GET /users" {
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
	if len(detail.Spans) != 3 {
		t.Fatalf("expected 3 spans, got %d", len(detail.Spans))
	}

	names := map[string]store.SpanOut{}
	for _, s := range detail.Spans {
		names[s.Name] = s
	}
	if _, ok := names["GET /users"]; !ok {
		t.Errorf("missing root span")
	}
	if dbSpan, ok := names["SELECT users"]; !ok {
		t.Errorf("missing DB span")
	} else {
		if dbSpan.StatusCode != 2 {
			t.Errorf("DB span status_code: want 2 (ERROR), got %d", dbSpan.StatusCode)
		}
		if !strings.Contains(dbSpan.AttributesJSON, "db.statement") {
			t.Errorf("DB span missing db.statement in %s", dbSpan.AttributesJSON)
		}
	}
	if rpcSpan, ok := names["UserService/List"]; !ok {
		t.Errorf("missing RPC span")
	} else {
		if len(rpcSpan.Events) != 1 || rpcSpan.Events[0].Name != "cache_miss" {
			t.Errorf("RPC span events: %+v", rpcSpan.Events)
		}
	}

	if len(detail.Resources) == 0 {
		t.Errorf("expected at least one resource row")
	}
	var sawService bool
	for _, r := range detail.Resources {
		if r.ServiceName == "sdk-int-test" {
			sawService = true
			if !strings.Contains(r.AttributesJSON, "service.version") {
				t.Errorf("resource attributes missing service.version: %s", r.AttributesJSON)
			}
		}
	}
	if !sawService {
		t.Errorf("resource table missing sdk-int-test: %+v", detail.Resources)
	}

	fields, err := f.st.ListFields(f.ctx, store.FieldFilter{SignalType: "span", Service: "sdk-int-test"})
	if err != nil {
		t.Fatalf("ListFields: %v", err)
	}
	seen := map[string]bool{}
	for _, fi := range fields {
		seen[fi.Key] = true
	}
	for _, want := range []string{
		"http.request.method",
		"http.response.status_code",
		"http.route",
		"url.path",
		"db.statement",
		"rpc.service",
		"rpc.method",
	} {
		if !seen[want] {
			t.Errorf("field catalog missing key %q; got %+v", want, seen)
		}
	}
}

// -----------------------------------------------------------------------------
// Logs: real otlploghttp exporter
// -----------------------------------------------------------------------------

func TestSDK_LogsRoundTrip(t *testing.T) {
	f := newFixture(t)
	defer f.close()

	exp, err := otlploghttp.New(f.ctx,
		otlploghttp.WithEndpoint(f.endpointHost()),
		otlploghttp.WithInsecure(),
	)
	if err != nil {
		t.Fatalf("otlploghttp.New: %v", err)
	}
	res, err := resource.New(f.ctx,
		resource.WithAttributes(
			attribute.String("service.name", "sdk-log-test"),
		),
	)
	if err != nil {
		t.Fatalf("resource.New: %v", err)
	}
	lp := sdklog.NewLoggerProvider(
		sdklog.WithProcessor(sdklog.NewBatchProcessor(exp, sdklog.WithExportInterval(50*time.Millisecond))),
		sdklog.WithResource(res),
	)
	lg := lp.Logger("sdk-log-test")

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

	emit(otellog.SeverityInfo, "INFO", "handling request from 10.0.0.1",
		otellog.String("component", "http"),
		otellog.Int64("user.id", 42),
	)
	emit(otellog.SeverityError, "ERROR", "connection refused: postgres",
		otellog.String("component", "db"),
	)

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := lp.Shutdown(shutdownCtx); err != nil {
		t.Fatalf("lp.Shutdown: %v", err)
	}

	waitFor(t, 3*time.Second, "two logs from sdk-log-test", func() bool {
		logs, _, err := f.st.SearchLogs(f.ctx, store.LogFilter{Service: "sdk-log-test"})
		return err == nil && len(logs) == 2
	})

	logs, _, err := f.st.SearchLogs(f.ctx, store.LogFilter{Service: "sdk-log-test"})
	if err != nil {
		t.Fatalf("SearchLogs: %v", err)
	}
	bodies := map[string]store.LogOut{}
	for _, l := range logs {
		bodies[l.Body] = l
	}
	if _, ok := bodies["handling request from 10.0.0.1"]; !ok {
		t.Errorf("missing INFO log body; got %+v", bodies)
	}
	if errLog, ok := bodies["connection refused: postgres"]; !ok {
		t.Errorf("missing ERROR log body; got %+v", bodies)
	} else if errLog.SeverityNumber == 0 {
		t.Errorf("ERROR severity_number was not propagated: %+v", errLog)
	}

	found, _, err := f.st.SearchLogs(f.ctx, store.LogFilter{Query: "postgres"})
	if err != nil {
		t.Fatalf("FTS SearchLogs: %v", err)
	}
	if len(found) != 1 || found[0].Body != "connection refused: postgres" {
		t.Fatalf("FTS expected 1 match on 'postgres', got %+v", found)
	}

	fields, err := f.st.ListFields(f.ctx, store.FieldFilter{SignalType: "log", Service: "sdk-log-test"})
	if err != nil {
		t.Fatalf("ListFields: %v", err)
	}
	seen := map[string]bool{}
	for _, fi := range fields {
		seen[fi.Key] = true
	}
	if !seen["component"] {
		t.Errorf("log field catalog missing 'component'; got %+v", seen)
	}
}
