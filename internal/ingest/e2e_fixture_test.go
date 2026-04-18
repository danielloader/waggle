package ingest_test

// e2eFixture wires the full server-shape used by `cmd/waggle/main.go`:
// ingest.Handler on /v1/{traces,logs} + api.Router on /api/* against a
// single SQLite store, fronted by a single httptest.Server. Tests can
// then (a) emit via the real OTel Go SDK and (b) call the UI-facing
// /api/* endpoints, asserting round-trip correctness end-to-end.
//
// The existing sdk_integration_test.go `fixture` only mounts the ingest
// path — the tests below that need to query data go direct to the store.
// This richer fixture is what the new SDK-driven API integration tests
// consume, separate from the simpler fixture so we don't churn the older
// assertions.

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"testing"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/otlp/otlplog/otlploghttp"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	sdklog "go.opentelemetry.io/otel/sdk/log"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"

	"github.com/danielloader/waggle/internal/api"
	"github.com/danielloader/waggle/internal/ingest"
	"github.com/danielloader/waggle/internal/store/sqlite"
)

type e2eFixture struct {
	t      *testing.T
	ctx    context.Context
	cancel context.CancelFunc
	st     *sqlite.Store
	writer *ingest.Writer
	srv    *httptest.Server
}

func newE2EFixture(t *testing.T) *e2eFixture {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())

	st, err := sqlite.Open(ctx, filepath.Join(t.TempDir(), "test.db"))
	if err != nil {
		t.Fatalf("sqlite.Open: %v", err)
	}

	log := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError}))
	w := ingest.NewWriter(st, log, ingest.WriterConfig{
		FlushEvery: 5 * time.Millisecond,
		FlushSpans: 10,
		FlushLogs:  10,
	})
	w.Start(ctx)

	ih := ingest.NewHandler(w, log)
	apiRouter := api.NewRouter(st, log)

	mux := http.NewServeMux()
	ih.Mount(mux)
	apiRouter.Mount(mux)

	srv := httptest.NewServer(mux)
	return &e2eFixture{t: t, ctx: ctx, cancel: cancel, st: st, writer: w, srv: srv}
}

func (f *e2eFixture) close() {
	f.srv.Close()
	sctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	_ = f.writer.Stop(sctx)
	_ = f.st.Close()
	f.cancel()
}

// endpointHost returns "127.0.0.1:PORT" — the host:port form the OTLP/HTTP
// exporter wants via WithEndpoint().
func (f *e2eFixture) endpointHost() string {
	u, err := url.Parse(f.srv.URL)
	if err != nil {
		f.t.Fatal(err)
	}
	return u.Host
}

// apiURL builds the absolute URL for an /api path. Tests read from here
// via net/http so we're exercising the real handler chain, not the
// router directly.
func (f *e2eFixture) apiURL(path string) string {
	return f.srv.URL + path
}

// tracerProvider builds a fresh OTel TracerProvider pointing at this
// fixture's ingest endpoint, tagged with the given service.name. Short
// batch timeout keeps test runtime low.
func (f *e2eFixture) tracerProvider(service string) *sdktrace.TracerProvider {
	f.t.Helper()
	exp, err := otlptracehttp.New(f.ctx,
		otlptracehttp.WithEndpoint(f.endpointHost()),
		otlptracehttp.WithInsecure(),
	)
	if err != nil {
		f.t.Fatalf("otlptracehttp.New: %v", err)
	}
	res, err := resource.New(f.ctx, resource.WithAttributes(
		attribute.String("service.name", service),
		attribute.String("service.version", "e2e-test"),
		attribute.String("deployment.environment", "test"),
	))
	if err != nil {
		f.t.Fatalf("resource.New: %v", err)
	}
	return sdktrace.NewTracerProvider(
		sdktrace.WithBatcher(exp,
			sdktrace.WithBatchTimeout(50*time.Millisecond),
			sdktrace.WithMaxExportBatchSize(256),
		),
		sdktrace.WithResource(res),
	)
}

// loggerProvider is the log-side counterpart to tracerProvider.
func (f *e2eFixture) loggerProvider(service string) *sdklog.LoggerProvider {
	f.t.Helper()
	exp, err := otlploghttp.New(f.ctx,
		otlploghttp.WithEndpoint(f.endpointHost()),
		otlploghttp.WithInsecure(),
	)
	if err != nil {
		f.t.Fatalf("otlploghttp.New: %v", err)
	}
	res, err := resource.New(f.ctx, resource.WithAttributes(
		attribute.String("service.name", service),
		attribute.String("service.version", "e2e-test"),
		attribute.String("deployment.environment", "test"),
	))
	if err != nil {
		f.t.Fatalf("resource.New: %v", err)
	}
	return sdklog.NewLoggerProvider(
		sdklog.WithProcessor(sdklog.NewBatchProcessor(exp,
			sdklog.WithExportInterval(50*time.Millisecond),
			sdklog.WithExportMaxBatchSize(256),
		)),
		sdklog.WithResource(res),
	)
}

// shutdownTracer flushes then shuts down a tracer provider. Use after
// span emission so spans hit the ingest endpoint before assertions run.
func (f *e2eFixture) shutdownTracer(tp *sdktrace.TracerProvider) {
	f.t.Helper()
	sctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := tp.Shutdown(sctx); err != nil {
		f.t.Fatalf("tracer shutdown: %v", err)
	}
}

// shutdownLogger flushes + shuts down a logger provider.
func (f *e2eFixture) shutdownLogger(lp *sdklog.LoggerProvider) {
	f.t.Helper()
	sctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := lp.Shutdown(sctx); err != nil {
		f.t.Fatalf("logger shutdown: %v", err)
	}
}

// waitForSpanCount polls the store until service `svc` has at least n
// spans or the deadline expires.
func (f *e2eFixture) waitForSpanCount(svc string, n int) {
	f.t.Helper()
	waitFor(f.t, 3*time.Second, fmt.Sprintf("spans for %q >= %d", svc, n), func() bool {
		services, err := f.st.ListServices(f.ctx)
		if err != nil {
			return false
		}
		for _, s := range services {
			if s.ServiceName == svc && s.SpanCount >= int64(n) {
				return true
			}
		}
		return false
	})
}

// waitForTotalSpanCount polls until the sum of span counts across all
// services reaches at least n.
func (f *e2eFixture) waitForTotalSpanCount(n int) {
	f.t.Helper()
	waitFor(f.t, 3*time.Second, fmt.Sprintf("total spans >= %d", n), func() bool {
		services, err := f.st.ListServices(f.ctx)
		if err != nil {
			return false
		}
		var total int64
		for _, s := range services {
			total += s.SpanCount
		}
		return total >= int64(n)
	})
}

// waitForLogCount polls the /api/logs/search endpoint until it returns
// at least n records. Use this instead of poking the store directly so
// we also validate the HTTP-side behaviour.
func (f *e2eFixture) waitForLogCount(n int) {
	f.t.Helper()
	waitFor(f.t, 3*time.Second, fmt.Sprintf("logs >= %d", n), func() bool {
		var resp struct {
			Logs []map[string]any `json:"logs"`
		}
		if err := f.getJSON("/api/logs/search?limit=1000", &resp); err != nil {
			return false
		}
		return len(resp.Logs) >= n
	})
}

// getJSON performs a GET against /api and decodes into out. Fails the
// test on transport error or non-200 status.
func (f *e2eFixture) getJSON(path string, out any) error {
	f.t.Helper()
	resp, err := http.Get(f.apiURL(path))
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		b, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("GET %s: %d %s", path, resp.StatusCode, string(b))
	}
	return json.NewDecoder(resp.Body).Decode(out)
}

// postJSON posts the given body to /api and decodes the response. Any
// non-2xx status is returned as an error with the response body.
func (f *e2eFixture) postJSON(path string, body any, out any) error {
	f.t.Helper()
	buf, err := json.Marshal(body)
	if err != nil {
		return err
	}
	resp, err := http.Post(f.apiURL(path), "application/json", bytes.NewReader(buf))
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(resp.Body)
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fmt.Errorf("POST %s: %d %s", path, resp.StatusCode, string(raw))
	}
	if out == nil {
		return nil
	}
	return json.Unmarshal(raw, out)
}

// runQueryAPI is a thin typed wrapper around POST /api/query for tests
// that want to assert on structured results without re-declaring the
// result shape every time.
type apiQueryResult struct {
	Columns []struct {
		Name string `json:"name"`
		Type string `json:"type"`
	} `json:"columns"`
	Rows [][]any `json:"rows"`
}

func (f *e2eFixture) runQueryAPI(body map[string]any) (*apiQueryResult, error) {
	var out apiQueryResult
	if err := f.postJSON("/api/query", body, &out); err != nil {
		return nil, err
	}
	return &out, nil
}

// columnIdx returns the column index for a given name or -1.
func (r *apiQueryResult) columnIdx(name string) int {
	for i, c := range r.Columns {
		if c.Name == name {
			return i
		}
	}
	return -1
}

// rfc3339 formats a time for inclusion in an API query time_range.
func rfc3339(t time.Time) string { return t.UTC().Format(time.RFC3339Nano) }
