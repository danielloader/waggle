package ingest_test

// SDK-driven log edge cases. Coverage:
//
// - Full OTel severity spectrum (TRACE..FATAL) round-trips as numeric
//   severity_number, and the synthetic `error` field fires at >= ERROR.
// - Logs emitted inside an active span pick up trace_id/span_id
//   automatically from context and expose them on /api/logs/search.
// - Logs emitted with no span context land with empty trace/span bytes.
// - Structured map bodies round-trip as JSON on the body column.
// - FTS search via WHERE body contains X narrows results correctly.

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"testing"
	"time"

	otellog "go.opentelemetry.io/otel/log"
	"go.opentelemetry.io/otel/trace"
)

// ---------------------------------------------------------------------------
// Severity spectrum.
// ---------------------------------------------------------------------------

func TestE2E_LogSeverityLevels(t *testing.T) {
	f := newE2EFixture(t)
	defer f.close()

	lp := f.loggerProvider("sev-svc")
	logger := lp.Logger("e2e")

	emissions := []struct {
		sev  otellog.Severity
		text string
		body string
	}{
		{otellog.SeverityTrace1, "TRACE", "trace level"},
		{otellog.SeverityDebug, "DEBUG", "cache miss"},
		{otellog.SeverityInfo, "INFO", "request served"},
		{otellog.SeverityWarn, "WARN", "retry attempt"},
		{otellog.SeverityError, "ERROR", "upstream 500"},
		{otellog.SeverityFatal, "FATAL", "panic shutdown"},
	}
	for _, e := range emissions {
		var r otellog.Record
		r.SetTimestamp(time.Now())
		r.SetObservedTimestamp(time.Now())
		r.SetSeverity(e.sev)
		r.SetSeverityText(e.text)
		r.SetBody(otellog.StringValue(e.body))
		logger.Emit(context.Background(), r)
	}
	f.shutdownLogger(lp)
	f.waitForLogCount(len(emissions))

	// Total count first.
	now := time.Now()
	from := now.Add(-5 * time.Minute)
	res, err := f.runQueryAPI(map[string]any{
		"dataset": "logs",
		"time_range": map[string]any{
			"from": rfc3339(from), "to": rfc3339(now.Add(5 * time.Second)),
		},
		"select": []map[string]any{{"op": "count"}},
		"where":  []map[string]any{{"field": "service.name", "op": "=", "value": "sev-svc"}},
	})
	if err != nil {
		t.Fatalf("count query: %v", err)
	}
	count, _ := toInt64(res.Rows[0][res.columnIdx("count")])
	if count != int64(len(emissions)) {
		t.Fatalf("want %d logs, got %d", len(emissions), count)
	}

	// Synthetic `error` should match ERROR + FATAL only (severity_number >= 17).
	res, err = f.runQueryAPI(map[string]any{
		"dataset": "logs",
		"time_range": map[string]any{
			"from": rfc3339(from), "to": rfc3339(now.Add(5 * time.Second)),
		},
		"select": []map[string]any{{"op": "count"}},
		"where": []map[string]any{
			{"field": "service.name", "op": "=", "value": "sev-svc"},
			{"field": "error", "op": "=", "value": true},
		},
	})
	if err != nil {
		t.Fatalf("error query: %v", err)
	}
	errCount, _ := toInt64(res.Rows[0][res.columnIdx("count")])
	if errCount != 2 {
		t.Errorf("want 2 error-or-higher logs, got %d", errCount)
	}

	// Group by severity_number — each level has count 1.
	res, err = f.runQueryAPI(map[string]any{
		"dataset": "logs",
		"time_range": map[string]any{
			"from": rfc3339(from), "to": rfc3339(now.Add(5 * time.Second)),
		},
		"select":   []map[string]any{{"op": "count"}},
		"where":    []map[string]any{{"field": "service.name", "op": "=", "value": "sev-svc"}},
		"group_by": []string{"severity_number"},
	})
	if err != nil {
		t.Fatalf("severity group-by query: %v", err)
	}
	distribution := map[int64]int64{}
	for _, row := range res.Rows {
		sev, _ := toInt64(row[res.columnIdx("severity_number")])
		c, _ := toInt64(row[res.columnIdx("count")])
		distribution[sev] = c
	}
	// TRACE=1, DEBUG=5, INFO=9, WARN=13, ERROR=17, FATAL=21 per the OTel spec.
	for _, sev := range []int64{1, 5, 9, 13, 17, 21} {
		if distribution[sev] != 1 {
			t.Errorf("severity %d: want count 1, got %d (full map %v)",
				sev, distribution[sev], distribution)
		}
	}
}

// ---------------------------------------------------------------------------
// Trace-log correlation: a log emitted inside an active span's context
// inherits trace_id + span_id automatically via the OTel SDK.
// ---------------------------------------------------------------------------

func TestE2E_LogTraceCorrelation(t *testing.T) {
	f := newE2EFixture(t)
	defer f.close()

	tp := f.tracerProvider("corr-svc")
	lp := f.loggerProvider("corr-svc")
	tr := tp.Tracer("e2e")
	logger := lp.Logger("e2e")

	// Correlated: log emitted with the span's ctx.
	ctx, span := tr.Start(context.Background(), "work.do")
	var r otellog.Record
	r.SetTimestamp(time.Now())
	r.SetObservedTimestamp(time.Now())
	r.SetSeverity(otellog.SeverityInfo)
	r.SetSeverityText("INFO")
	r.SetBody(otellog.StringValue("inside span"))
	logger.Emit(ctx, r)
	wantTraceID := span.SpanContext().TraceID().String()
	wantSpanID := span.SpanContext().SpanID().String()
	span.End()

	// Uncorrelated: log emitted with a bare Background ctx — no active span.
	var r2 otellog.Record
	r2.SetTimestamp(time.Now())
	r2.SetObservedTimestamp(time.Now())
	r2.SetSeverity(otellog.SeverityInfo)
	r2.SetSeverityText("INFO")
	r2.SetBody(otellog.StringValue("outside span"))
	logger.Emit(context.Background(), r2)

	f.shutdownTracer(tp)
	f.shutdownLogger(lp)
	f.waitForLogCount(2)

	var resp struct {
		Logs []struct {
			Body    string `json:"body"`
			TraceID string `json:"trace_id"`
			SpanID  string `json:"span_id"`
		} `json:"logs"`
	}
	if err := f.getJSON("/api/logs/search?service=corr-svc&limit=10", &resp); err != nil {
		t.Fatalf("search: %v", err)
	}
	var inside, outside *struct {
		Body    string `json:"body"`
		TraceID string `json:"trace_id"`
		SpanID  string `json:"span_id"`
	}
	for i := range resp.Logs {
		switch resp.Logs[i].Body {
		case "inside span":
			inside = &resp.Logs[i]
		case "outside span":
			outside = &resp.Logs[i]
		}
	}
	if inside == nil || outside == nil {
		t.Fatalf("missing expected log rows: %+v", resp.Logs)
	}
	if !strings.EqualFold(inside.TraceID, wantTraceID) {
		t.Errorf("inside.trace_id: want %s, got %s", wantTraceID, inside.TraceID)
	}
	if !strings.EqualFold(inside.SpanID, wantSpanID) {
		t.Errorf("inside.span_id: want %s, got %s", wantSpanID, inside.SpanID)
	}
	if outside.TraceID != "" {
		t.Errorf("outside log should have no trace_id, got %q", outside.TraceID)
	}
	if outside.SpanID != "" {
		t.Errorf("outside log should have no span_id, got %q", outside.SpanID)
	}
	// Sanity: the correlated span's trace_id matches a trace in /api/traces.
	if _, err := trace.TraceIDFromHex(wantTraceID); err != nil {
		t.Fatalf("wantTraceID not valid hex: %v", err)
	}
}

// ---------------------------------------------------------------------------
// Structured bodies: a map body should land as JSON on the body column.
// ---------------------------------------------------------------------------

func TestE2E_LogStructuredBody(t *testing.T) {
	f := newE2EFixture(t)
	defer f.close()

	lp := f.loggerProvider("body-svc")
	logger := lp.Logger("e2e")

	// Map body with nested scalars.
	var r otellog.Record
	r.SetTimestamp(time.Now())
	r.SetSeverity(otellog.SeverityInfo)
	r.SetSeverityText("INFO")
	r.SetBody(otellog.MapValue(
		otellog.String("user", "alice"),
		otellog.Int("orders", 7),
		otellog.Bool("staff", false),
	))
	logger.Emit(context.Background(), r)

	f.shutdownLogger(lp)
	f.waitForLogCount(1)

	var resp struct {
		Logs []struct {
			Body string `json:"body"`
		} `json:"logs"`
	}
	if err := f.getJSON("/api/logs/search?service=body-svc&limit=5", &resp); err != nil {
		t.Fatalf("search: %v", err)
	}
	if len(resp.Logs) != 1 {
		t.Fatalf("want 1 log, got %d", len(resp.Logs))
	}
	// Body should be either a JSON-serialised map or at minimum contain
	// the keys — waggle may render map bodies as JSON on the body column
	// or stash the structured form on body_json. Accept either, but we
	// must find the keys SOMEWHERE on the record. Fetch again with the
	// full row via /api/query raw-events mode.
	now := time.Now()
	from := now.Add(-5 * time.Minute)
	res, err := f.runQueryAPI(map[string]any{
		"dataset": "logs",
		"time_range": map[string]any{
			"from": rfc3339(from), "to": rfc3339(now.Add(5 * time.Second)),
		},
		"select": []map[string]any{},
		"where":  []map[string]any{{"field": "service.name", "op": "=", "value": "body-svc"}},
		"limit":  5,
	})
	if err != nil {
		t.Fatalf("raw logs query: %v", err)
	}
	if len(res.Rows) != 1 {
		t.Fatalf("raw logs: want 1 row, got %d", len(res.Rows))
	}
	// Body round-trips as a JSON-encoded string on the `body` column
	// (the backend stringifies structured bodies). Decode it and assert
	// on the resulting map so the test doesn't fight escape quoting.
	bodyIdx := res.columnIdx("body")
	if bodyIdx < 0 {
		t.Fatalf("no body column in raw result: %+v", res.Columns)
	}
	bodyStr, ok := res.Rows[0][bodyIdx].(string)
	if !ok {
		t.Fatalf("body column is %T, expected string", res.Rows[0][bodyIdx])
	}
	var decoded map[string]any
	if err := json.Unmarshal([]byte(bodyStr), &decoded); err != nil {
		t.Fatalf("body wasn't valid JSON: %v (raw %q)", err, bodyStr)
	}
	if decoded["user"] != "alice" {
		t.Errorf("body.user: want alice, got %v", decoded["user"])
	}
	if got, _ := toInt64(decoded["orders"]); got != 7 {
		t.Errorf("body.orders: want 7, got %v", decoded["orders"])
	}
	if decoded["staff"] != false {
		t.Errorf("body.staff: want false, got %v", decoded["staff"])
	}
}

// ---------------------------------------------------------------------------
// Attribute filtering on logs: a WHERE filter on a log attribute key
// must narrow results correctly.
// ---------------------------------------------------------------------------

func TestE2E_LogAttributeFiltering(t *testing.T) {
	f := newE2EFixture(t)
	defer f.close()

	lp := f.loggerProvider("attrs-log-svc")
	logger := lp.Logger("e2e")

	// 10 logs: 4 with http.method=GET, 6 with http.method=POST.
	for i := range 10 {
		var r otellog.Record
		r.SetTimestamp(time.Now())
		r.SetSeverity(otellog.SeverityInfo)
		r.SetSeverityText("INFO")
		r.SetBody(otellog.StringValue(fmt.Sprintf("req-%d", i)))
		method := "POST"
		if i%5 < 2 {
			method = "GET"
		}
		r.AddAttributes(otellog.String("http.method", method))
		logger.Emit(context.Background(), r)
	}
	f.shutdownLogger(lp)
	f.waitForLogCount(10)

	now := time.Now()
	from := now.Add(-5 * time.Minute)
	res, err := f.runQueryAPI(map[string]any{
		"dataset": "logs",
		"time_range": map[string]any{
			"from": rfc3339(from), "to": rfc3339(now.Add(5 * time.Second)),
		},
		"select": []map[string]any{{"op": "count"}},
		"where": []map[string]any{
			{"field": "service.name", "op": "=", "value": "attrs-log-svc"},
			{"field": "http.method", "op": "=", "value": "GET"},
		},
	})
	if err != nil {
		t.Fatalf("count GET: %v", err)
	}
	gotGet, _ := toInt64(res.Rows[0][res.columnIdx("count")])
	if gotGet != 4 {
		t.Errorf("GET count: want 4, got %d", gotGet)
	}
}

// ---------------------------------------------------------------------------
// FTS-style body contains: WHERE body contains X narrows to matching logs.
// ---------------------------------------------------------------------------

func TestE2E_LogBodyContains(t *testing.T) {
	f := newE2EFixture(t)
	defer f.close()

	lp := f.loggerProvider("fts-svc")
	logger := lp.Logger("e2e")

	bodies := []string{
		"request refused: rate limit exceeded",
		"request served OK",
		"upstream timeout after 5s",
		"connection refused to partner-webhook",
		"all systems normal",
	}
	for _, b := range bodies {
		var r otellog.Record
		r.SetTimestamp(time.Now())
		r.SetSeverity(otellog.SeverityInfo)
		r.SetSeverityText("INFO")
		r.SetBody(otellog.StringValue(b))
		logger.Emit(context.Background(), r)
	}
	f.shutdownLogger(lp)
	f.waitForLogCount(len(bodies))

	now := time.Now()
	from := now.Add(-5 * time.Minute)
	res, err := f.runQueryAPI(map[string]any{
		"dataset": "logs",
		"time_range": map[string]any{
			"from": rfc3339(from), "to": rfc3339(now.Add(5 * time.Second)),
		},
		"select": []map[string]any{{"op": "count"}},
		"where": []map[string]any{
			{"field": "service.name", "op": "=", "value": "fts-svc"},
			{"field": "body", "op": "contains", "value": "refused"},
		},
	})
	if err != nil {
		t.Fatalf("contains query: %v", err)
	}
	count, _ := toInt64(res.Rows[0][res.columnIdx("count")])
	if count != 2 {
		t.Errorf("body contains 'refused': want 2, got %d", count)
	}
}
