package ingest_test

// E2E coverage for the management + catalogue surface:
//
// - POST /api/clear wipes every signal table.
// - store.Retain() enforces the time-based retention cutoff.
// - /api/fields surfaces both JSONB-observed keys and the promoted /
//   synthetic fields the query builder resolves natively, with and
//   without a service scope.
// - /api/fields/{key}/values returns actual observed values.
// - /api/span-names returns the span names for a service.
// - /api/traces has_error filter.
// - POST /api/query returns 400 on malformed payloads (cheap negative
//   path so parsing regressions can't silently slip through).

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"testing"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
)

// ---------------------------------------------------------------------------
// Clear nukes everything.
// ---------------------------------------------------------------------------

func TestE2E_ClearEndpoint(t *testing.T) {
	f := newE2EFixture(t)
	defer f.close()

	tp := f.tracerProvider("clear-svc")
	tr := tp.Tracer("e2e")
	_, span := tr.Start(context.Background(), "op")
	span.End()
	f.shutdownTracer(tp)
	f.waitForSpanCount("clear-svc", 1)

	if err := f.postJSON("/api/clear", map[string]any{}, nil); err != nil {
		t.Fatalf("clear: %v", err)
	}

	// /api/services should now be empty.
	var svcResp struct {
		Services []any `json:"services"`
	}
	if err := f.getJSON("/api/services", &svcResp); err != nil {
		t.Fatalf("services: %v", err)
	}
	if len(svcResp.Services) != 0 {
		t.Errorf("services after clear: want empty, got %d", len(svcResp.Services))
	}

	// A query for the same window should also return zero count.
	now := time.Now()
	res, err := f.runQueryAPI(map[string]any{
		"dataset": "spans",
		"time_range": map[string]any{
			"from": rfc3339(now.Add(-5 * time.Minute)),
			"to":   rfc3339(now.Add(5 * time.Second)),
		},
		"select": []map[string]any{{"op": "count"}},
	})
	if err != nil {
		t.Fatalf("post-clear query: %v", err)
	}
	c, _ := toInt64(res.Rows[0][res.columnIdx("count")])
	if c != 0 {
		t.Errorf("count after clear: want 0, got %d", c)
	}
}

// ---------------------------------------------------------------------------
// Retention cutoff drops old data but keeps anything newer.
// ---------------------------------------------------------------------------

func TestE2E_Retention(t *testing.T) {
	f := newE2EFixture(t)
	defer f.close()

	tp := f.tracerProvider("retain-svc")
	tr := tp.Tracer("e2e")

	// Two spans. We emit, shut down the SDK, then capture a "now" cutoff
	// and sleep before emitting a second batch so the two groups straddle
	// it in wall-clock time.
	for i := 0; i < 2; i++ {
		_, span := tr.Start(context.Background(), "old")
		span.End()
	}
	f.shutdownTracer(tp)
	f.waitForSpanCount("retain-svc", 2)

	// Nanosecond cutoff captured AFTER the old spans flushed.
	cutoff := time.Now().UnixNano()
	time.Sleep(3 * time.Millisecond)

	tp2 := f.tracerProvider("retain-svc")
	tr2 := tp2.Tracer("e2e")
	for i := 0; i < 3; i++ {
		_, span := tr2.Start(context.Background(), "new")
		span.End()
	}
	f.shutdownTracer(tp2)
	f.waitForSpanCount("retain-svc", 5)

	// Directly invoke retain with our cutoff; old spans should vanish.
	if err := f.st.Retain(f.ctx, cutoff); err != nil {
		t.Fatalf("retain: %v", err)
	}

	var svcResp struct {
		Services []struct {
			Service   string `json:"service"`
			SpanCount int64  `json:"span_count"`
		} `json:"services"`
	}
	if err := f.getJSON("/api/services", &svcResp); err != nil {
		t.Fatalf("services: %v", err)
	}
	var svcCount int64
	for _, s := range svcResp.Services {
		if s.Service == "retain-svc" {
			svcCount = s.SpanCount
		}
	}
	if svcCount != 3 {
		t.Errorf("retain-svc span_count after cutoff: want 3 (new), got %d", svcCount)
	}
}

// ---------------------------------------------------------------------------
// /api/fields returns both JSONB keys and synthetic fields, and scopes
// correctly by service.
// ---------------------------------------------------------------------------

func TestE2E_FieldCatalog(t *testing.T) {
	f := newE2EFixture(t)
	defer f.close()

	apiTP := f.tracerProvider("gw-fc")
	dbTP := f.tracerProvider("db-fc")
	api := apiTP.Tracer("e2e")
	db := dbTP.Tracer("e2e")

	// gw-fc spans get http.route; db-fc spans get db.system. No overlap
	// in attribute keys so we can assert scoping holds.
	_, s1 := api.Start(context.Background(), "GET /x")
	s1.SetAttributes(attribute.String("http.route", "/x"))
	s1.End()
	_, s2 := db.Start(context.Background(), "SELECT")
	s2.SetAttributes(attribute.String("db.system", "postgresql"))
	s2.End()
	f.shutdownTracer(apiTP)
	f.shutdownTracer(dbTP)
	f.waitForTotalSpanCount(2)

	type fieldInfo struct {
		Key       string `json:"key"`
		ValueType string `json:"type"`
	}
	// Cross-service call: synthetic fields present; both attribute keys visible.
	var all struct {
		Fields []fieldInfo `json:"fields"`
	}
	if err := f.getJSON("/api/fields?dataset=span&limit=100", &all); err != nil {
		t.Fatalf("fields (all): %v", err)
	}
	got := map[string]string{}
	for _, fi := range all.Fields {
		got[fi.Key] = fi.ValueType
	}
	// Synthetic — always present.
	for _, k := range []string{"name", "service.name", "duration_ns", "duration_ms", "is_root", "error"} {
		if _, ok := got[k]; !ok {
			t.Errorf("synthetic field %q missing (cross-service)", k)
		}
	}
	// JSONB attribute keys — both services' keys should appear.
	for _, k := range []string{"http.route", "db.system"} {
		if _, ok := got[k]; !ok {
			t.Errorf("attribute key %q missing (cross-service)", k)
		}
	}

	// Service-scoped: db-fc should have db.system but NOT http.route.
	var dbOnly struct {
		Fields []fieldInfo `json:"fields"`
	}
	if err := f.getJSON("/api/fields?dataset=span&service=db-fc&limit=100", &dbOnly); err != nil {
		t.Fatalf("fields (db-fc): %v", err)
	}
	seen := map[string]bool{}
	for _, fi := range dbOnly.Fields {
		seen[fi.Key] = true
	}
	if !seen["db.system"] {
		t.Errorf("db.system missing on db-fc scope: %+v", dbOnly.Fields)
	}
	if seen["http.route"] {
		t.Errorf("http.route leaked into db-fc scope: %+v", dbOnly.Fields)
	}
}

// ---------------------------------------------------------------------------
// /api/fields/{key}/values: returns observed values, ordered by frequency.
// ---------------------------------------------------------------------------

func TestE2E_FieldValues(t *testing.T) {
	f := newE2EFixture(t)
	defer f.close()

	tp := f.tracerProvider("values-svc")
	tr := tp.Tracer("e2e")

	tiers := []string{"free", "silver", "gold", "gold", "gold", "free"}
	for _, tier := range tiers {
		_, span := tr.Start(context.Background(), "op")
		span.SetAttributes(attribute.String("customer.tier", tier))
		span.End()
	}
	f.shutdownTracer(tp)
	f.waitForSpanCount("values-svc", len(tiers))

	var resp struct {
		Values []string `json:"values"`
	}
	if err := f.getJSON("/api/fields/customer.tier/values?dataset=span&limit=10", &resp); err != nil {
		t.Fatalf("values: %v", err)
	}
	want := map[string]bool{"free": false, "silver": false, "gold": false}
	for _, v := range resp.Values {
		if _, ok := want[v]; ok {
			want[v] = true
		}
	}
	for k, saw := range want {
		if !saw {
			t.Errorf("value %q missing from /api/fields/customer.tier/values: %+v", k, resp.Values)
		}
	}
}

// ---------------------------------------------------------------------------
// /api/span-names returns distinct span names for a service.
// ---------------------------------------------------------------------------

func TestE2E_SpanNames(t *testing.T) {
	f := newE2EFixture(t)
	defer f.close()

	tp := f.tracerProvider("names-svc")
	tr := tp.Tracer("e2e")

	for _, n := range []string{"auth", "auth", "checkout", "refund"} {
		_, span := tr.Start(context.Background(), n)
		span.End()
	}
	f.shutdownTracer(tp)
	f.waitForSpanCount("names-svc", 4)

	var resp struct {
		Names []string `json:"names"`
	}
	if err := f.getJSON("/api/span-names?service=names-svc&limit=10", &resp); err != nil {
		t.Fatalf("span-names: %v", err)
	}
	seen := map[string]bool{}
	for _, n := range resp.Names {
		seen[n] = true
	}
	for _, n := range []string{"auth", "checkout", "refund"} {
		if !seen[n] {
			t.Errorf("span name %q missing: %+v", n, resp.Names)
		}
	}
}

// ---------------------------------------------------------------------------
// has_error filter on ListTraces.
// ---------------------------------------------------------------------------

func TestE2E_HasErrorFilter(t *testing.T) {
	f := newE2EFixture(t)
	defer f.close()

	tp := f.tracerProvider("err-svc")
	tr := tp.Tracer("e2e")

	// 2 clean traces.
	for i := 0; i < 2; i++ {
		_, s := tr.Start(context.Background(), "ok.op", trace.WithNewRoot())
		s.End()
		time.Sleep(200 * time.Microsecond)
	}
	// 3 errored traces (status=Error).
	for i := 0; i < 3; i++ {
		_, s := tr.Start(context.Background(), "bad.op", trace.WithNewRoot())
		s.SetStatus(codes.Error, "bad")
		s.End()
		time.Sleep(200 * time.Microsecond)
	}
	f.shutdownTracer(tp)
	f.waitForSpanCount("err-svc", 5)

	var allTraces struct {
		Traces []struct {
			HasError bool `json:"has_error"`
		} `json:"traces"`
	}
	if err := f.getJSON("/api/traces?service=err-svc&limit=20", &allTraces); err != nil {
		t.Fatalf("all: %v", err)
	}
	if len(allTraces.Traces) != 5 {
		t.Errorf("total traces: want 5, got %d", len(allTraces.Traces))
	}

	var errored struct {
		Traces []struct {
			HasError bool `json:"has_error"`
		} `json:"traces"`
	}
	if err := f.getJSON("/api/traces?service=err-svc&limit=20&has_error=true", &errored); err != nil {
		t.Fatalf("errored: %v", err)
	}
	if len(errored.Traces) != 3 {
		t.Errorf("has_error=true: want 3 traces, got %d", len(errored.Traces))
	}
	for _, tr := range errored.Traces {
		if !tr.HasError {
			t.Errorf("returned trace with has_error=false under has_error=true filter: %+v", tr)
		}
	}
}

// ---------------------------------------------------------------------------
// Negative: /api/query with an invalid operator returns 400 + a helpful
// message. Catches parser regressions without needing a dataset.
// ---------------------------------------------------------------------------

func TestE2E_InvalidQuery(t *testing.T) {
	f := newE2EFixture(t)
	defer f.close()

	now := time.Now()
	body := map[string]any{
		"dataset": "spans",
		"time_range": map[string]any{
			"from": rfc3339(now.Add(-1 * time.Minute)),
			"to":   rfc3339(now),
		},
		"select": []map[string]any{{"op": "bogus", "field": "whatever"}},
	}
	resp, err := http.Post(f.apiURL("/api/query"),
		"application/json", bytes.NewReader(mustJSON(t, body)))
	if err != nil {
		t.Fatalf("post: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("want 400, got %d", resp.StatusCode)
	}
}

// ---------------------------------------------------------------------------
// Negative: malformed OTLP body → decode error.
// ---------------------------------------------------------------------------

func TestE2E_MalformedOTLP(t *testing.T) {
	f := newE2EFixture(t)
	defer f.close()

	// Garbage protobuf bytes against the trace ingest.
	resp, err := http.Post(f.srv.URL+"/v1/traces",
		"application/x-protobuf", bytes.NewReader([]byte{0xde, 0xad, 0xbe, 0xef}))
	if err != nil {
		t.Fatalf("post: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("garbage OTLP: want 400, got %d", resp.StatusCode)
	}

	// Unsupported content type should hit the 415 path.
	resp2, err := http.Post(f.srv.URL+"/v1/traces",
		"text/plain", bytes.NewReader([]byte("hi")))
	if err != nil {
		t.Fatalf("post: %v", err)
	}
	defer resp2.Body.Close()
	if resp2.StatusCode != http.StatusUnsupportedMediaType {
		t.Fatalf("bad content type: want 415, got %d", resp2.StatusCode)
	}
}

// ---------------------------------------------------------------------------
// helpers
// ---------------------------------------------------------------------------

func mustJSON(t *testing.T, v any) []byte {
	t.Helper()
	b, err := json.Marshal(v)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	return b
}
