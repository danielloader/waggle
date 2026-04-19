package ingest_test

// SDK-driven span edge cases. Every test here emits via the real OTel Go
// SDK, lets it pipeline through the OTLP/HTTP exporter into waggle's
// ingest handler + SQLite writer, and then asserts on the public /api/*
// surface. The goal is to exercise parts of the OTel data model that the
// older sdk_integration_test doesn't touch.

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
)

// ---------------------------------------------------------------------------
// Span kinds: all five OTel SpanKind values should land in the store with
// their numeric codes preserved and be addressable via `group_by: ["kind"]`.
// ---------------------------------------------------------------------------

func TestE2E_SpanKinds(t *testing.T) {
	f := newE2EFixture(t)
	defer f.close()

	tp := f.tracerProvider("kinds-svc")
	tr := tp.Tracer("e2e")

	kinds := []struct {
		name string
		kind trace.SpanKind
	}{
		{"internal", trace.SpanKindInternal},
		{"server", trace.SpanKindServer},
		{"client", trace.SpanKindClient},
		{"producer", trace.SpanKindProducer},
		{"consumer", trace.SpanKindConsumer},
	}
	for _, k := range kinds {
		_, span := tr.Start(context.Background(), "op."+k.name, trace.WithSpanKind(k.kind))
		span.End()
	}
	f.shutdownTracer(tp)
	f.waitForSpanCount("kinds-svc", len(kinds))

	// Group-by kind + count — one row per kind, count=1 each.
	now := time.Now()
	from := now.Add(-5 * time.Minute)
	res, err := f.runQueryAPI(map[string]any{
		"dataset": "spans",
		"time_range": map[string]any{
			"from": rfc3339(from), "to": rfc3339(now.Add(5 * time.Second)),
		},
		"select":   []map[string]any{{"op": "count"}},
		"where":    []map[string]any{{"field": "service.name", "op": "=", "value": "kinds-svc"}},
		"group_by": []string{"kind"},
	})
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	kindIdx := res.columnIdx("kind")
	countIdx := res.columnIdx("count")
	if kindIdx < 0 || countIdx < 0 {
		t.Fatalf("missing columns in result: %+v", res.Columns)
	}
	got := map[string]int64{}
	for _, row := range res.Rows {
		k, _ := row[kindIdx].(string)
		c, _ := toInt64(row[countIdx])
		got[k] = c
	}
	// meta.span_kind is the OTel enum name: INTERNAL, SERVER, CLIENT, PRODUCER, CONSUMER.
	want := map[string]int64{"INTERNAL": 1, "SERVER": 1, "CLIENT": 1, "PRODUCER": 1, "CONSUMER": 1}
	if len(want) != len(got) {
		t.Errorf("kind distribution: want %v, got %v", want, got)
	}
	for k, v := range want {
		if got[k] != v {
			t.Errorf("kind %q: want %d, got %d", k, v, got[k])
		}
	}
}

// ---------------------------------------------------------------------------
// Span status: three values (unset / ok / error). Error flows through
// both `status_code=2` and the synthetic `error` field.
// ---------------------------------------------------------------------------

func TestE2E_SpanStatus(t *testing.T) {
	f := newE2EFixture(t)
	defer f.close()

	tp := f.tracerProvider("status-svc")
	tr := tp.Tracer("e2e")

	// Unset
	_, s1 := tr.Start(context.Background(), "unset.op")
	s1.End()
	// Ok
	_, s2 := tr.Start(context.Background(), "ok.op")
	s2.SetStatus(codes.Ok, "")
	s2.End()
	// Error (status only, no exception event)
	_, s3 := tr.Start(context.Background(), "error.op")
	s3.SetStatus(codes.Error, "boom")
	s3.End()
	// Error via exception event only (status stays unset) — the synthetic
	// `error` field should still fire.
	_, s4 := tr.Start(context.Background(), "exception.op")
	s4.AddEvent("exception", trace.WithAttributes(attribute.String("exception.type", "Oops")))
	s4.End()

	f.shutdownTracer(tp)
	f.waitForSpanCount("status-svc", 4)

	now := time.Now()
	from := now.Add(-5 * time.Minute)
	// Error count via synthetic field: should be 2 (error.op + exception.op).
	res, err := f.runQueryAPI(map[string]any{
		"dataset": "spans",
		"time_range": map[string]any{
			"from": rfc3339(from), "to": rfc3339(now.Add(5 * time.Second)),
		},
		"select": []map[string]any{{"op": "count"}},
		"where": []map[string]any{
			{"field": "service.name", "op": "=", "value": "status-svc"},
			{"field": "error", "op": "=", "value": true},
		},
	})
	if err != nil {
		t.Fatalf("error-count query: %v", err)
	}
	count, _ := toInt64(res.Rows[0][res.columnIdx("count")])
	if count != 2 {
		t.Errorf("expected 2 error-flagged spans, got %d", count)
	}

	// Group-by status_code — 3 distinct rows (0=unset×2, 1=ok, 2=error).
	res, err = f.runQueryAPI(map[string]any{
		"dataset": "spans",
		"time_range": map[string]any{
			"from": rfc3339(from), "to": rfc3339(now.Add(5 * time.Second)),
		},
		"select":   []map[string]any{{"op": "count"}},
		"where":    []map[string]any{{"field": "service.name", "op": "=", "value": "status-svc"}},
		"group_by": []string{"status_code"},
	})
	if err != nil {
		t.Fatalf("status group-by query: %v", err)
	}
	perStatus := map[int64]int64{}
	for _, row := range res.Rows {
		k, _ := toInt64(row[res.columnIdx("status_code")])
		c, _ := toInt64(row[res.columnIdx("count")])
		perStatus[k] = c
	}
	// Exactly one Ok span and one Error span; the two exception+unset
	// spans share status 0 so that bucket has count=2.
	if perStatus[0] != 2 || perStatus[1] != 1 || perStatus[2] != 1 {
		t.Errorf("status distribution: want {0:2, 1:1, 2:1}, got %v", perStatus)
	}
}

// ---------------------------------------------------------------------------
// Events: multiple events per span, each with their own attributes, must
// round-trip through the GetTrace handler.
// ---------------------------------------------------------------------------

func TestE2E_SpanEvents(t *testing.T) {
	f := newE2EFixture(t)
	defer f.close()

	tp := f.tracerProvider("events-svc")
	tr := tp.Tracer("e2e")

	_, span := tr.Start(context.Background(), "op.with.events")
	span.AddEvent("cache.lookup", trace.WithAttributes(
		attribute.Bool("cache.hit", true),
		attribute.String("cache.key", "sessions:42"),
	))
	span.AddEvent("db.query", trace.WithAttributes(
		attribute.String("db.statement", "SELECT 1"),
		attribute.Int("db.rows", 1),
	))
	span.AddEvent("done")
	span.End()
	f.shutdownTracer(tp)
	f.waitForSpanCount("events-svc", 1)

	// Find the trace and pull its detail.
	var listResp struct {
		Traces []struct {
			TraceID string `json:"trace_id"`
		} `json:"traces"`
	}
	if err := f.getJSON("/api/traces?service=events-svc&limit=5", &listResp); err != nil {
		t.Fatalf("list: %v", err)
	}
	if len(listResp.Traces) == 0 {
		t.Fatal("no trace returned")
	}
	var detail struct {
		Spans []struct {
			Name   string `json:"name"`
			Events []struct {
				Name       string `json:"name"`
				Attributes string `json:"attributes"`
			} `json:"events"`
		} `json:"spans"`
	}
	if err := f.getJSON("/api/traces/"+listResp.Traces[0].TraceID, &detail); err != nil {
		t.Fatalf("detail: %v", err)
	}
	if len(detail.Spans) != 1 {
		t.Fatalf("expected 1 span, got %d", len(detail.Spans))
	}
	evs := detail.Spans[0].Events
	if len(evs) != 3 {
		t.Fatalf("expected 3 events, got %d (%+v)", len(evs), evs)
	}
	names := make([]string, len(evs))
	for i, e := range evs {
		names[i] = e.Name
	}
	wantNames := []string{"cache.lookup", "db.query", "done"}
	for i, n := range wantNames {
		if names[i] != n {
			t.Errorf("event[%d]: want %q, got %q (all: %v)", i, n, names[i], names)
		}
	}
	// First event carries cache.hit=true and cache.key in its attributes.
	if !strings.Contains(evs[0].Attributes, `"cache.hit":true`) ||
		!strings.Contains(evs[0].Attributes, `"cache.key":"sessions:42"`) {
		t.Errorf("event[0] attrs lost: %s", evs[0].Attributes)
	}
}

// ---------------------------------------------------------------------------
// Links: a consumer span carrying a link back to a producer's span
// context (the classic async / queue pattern). GetTrace should surface
// the linked trace/span IDs.
// ---------------------------------------------------------------------------

func TestE2E_SpanLinks(t *testing.T) {
	f := newE2EFixture(t)
	defer f.close()

	tp := f.tracerProvider("linker-svc")
	tr := tp.Tracer("e2e")

	// Producer trace.
	_, producer := tr.Start(context.Background(), "queue.publish",
		trace.WithSpanKind(trace.SpanKindProducer))
	producerSC := producer.SpanContext()
	producer.End()

	// Consumer trace: new root that links to the producer.
	_, consumer := tr.Start(context.Background(), "queue.consume",
		trace.WithSpanKind(trace.SpanKindConsumer),
		trace.WithNewRoot(),
		trace.WithLinks(trace.Link{
			SpanContext: producerSC,
			Attributes: []attribute.KeyValue{
				attribute.String("messaging.operation", "receive"),
			},
		}),
	)
	consumerSID := consumer.SpanContext().SpanID().String()
	consumerTID := consumer.SpanContext().TraceID().String()
	consumer.End()
	f.shutdownTracer(tp)
	f.waitForSpanCount("linker-svc", 2)

	var detail struct {
		Spans []struct {
			SpanID string `json:"span_id"`
			Links  []struct {
				LinkedTraceID string `json:"linked_trace_id"`
				LinkedSpanID  string `json:"linked_span_id"`
				Attributes    string `json:"attributes"`
			} `json:"links"`
		} `json:"spans"`
	}
	if err := f.getJSON("/api/traces/"+consumerTID, &detail); err != nil {
		t.Fatalf("detail: %v", err)
	}
	var consumerSpan *struct {
		Links []struct {
			LinkedTraceID string `json:"linked_trace_id"`
			LinkedSpanID  string `json:"linked_span_id"`
			Attributes    string `json:"attributes"`
		} `json:"links"`
	}
	for i := range detail.Spans {
		if detail.Spans[i].SpanID == consumerSID {
			consumerSpan = &struct {
				Links []struct {
					LinkedTraceID string `json:"linked_trace_id"`
					LinkedSpanID  string `json:"linked_span_id"`
					Attributes    string `json:"attributes"`
				} `json:"links"`
			}{Links: detail.Spans[i].Links}
			break
		}
	}
	if consumerSpan == nil {
		t.Fatalf("consumer span %s missing from trace detail", consumerSID)
	}
	if len(consumerSpan.Links) != 1 {
		t.Fatalf("want 1 link, got %d", len(consumerSpan.Links))
	}
	link := consumerSpan.Links[0]
	if !strings.EqualFold(link.LinkedTraceID, producerSC.TraceID().String()) {
		t.Errorf("linked_trace_id: want %s, got %s",
			producerSC.TraceID().String(), link.LinkedTraceID)
	}
	if !strings.EqualFold(link.LinkedSpanID, producerSC.SpanID().String()) {
		t.Errorf("linked_span_id: want %s, got %s",
			producerSC.SpanID().String(), link.LinkedSpanID)
	}
	if !strings.Contains(link.Attributes, `"messaging.operation":"receive"`) {
		t.Errorf("link attrs missing: %s", link.Attributes)
	}
}

// ---------------------------------------------------------------------------
// Attribute types: OTel supports string, int64, float64, bool, plus
// slices of each. Every scalar type should survive the round-trip and be
// filterable via /api/query.
// ---------------------------------------------------------------------------

func TestE2E_AttributeTypes(t *testing.T) {
	f := newE2EFixture(t)
	defer f.close()

	tp := f.tracerProvider("attrs-svc")
	tr := tp.Tracer("e2e")

	_, span := tr.Start(context.Background(), "typed.op")
	span.SetAttributes(
		attribute.String("attr.string", "hello"),
		attribute.Int("attr.int", 42),
		attribute.Int64("attr.int64", 1<<40),
		attribute.Float64("attr.float", 3.14),
		attribute.Bool("attr.bool", true),
		attribute.StringSlice("attr.strslice", []string{"a", "b", "c"}),
		attribute.IntSlice("attr.intslice", []int{1, 2, 3}),
	)
	span.End()
	f.shutdownTracer(tp)
	f.waitForSpanCount("attrs-svc", 1)

	// Filter on each scalar type — all should match exactly one span.
	cases := []struct {
		name  string
		op    string
		value any
	}{
		{"attr.string", "=", "hello"},
		{"attr.int", "=", 42},
		{"attr.int64", ">=", int64(1 << 40)},
		{"attr.float", ">=", 3.0},
		{"attr.bool", "=", true},
	}
	now := time.Now()
	from := now.Add(-5 * time.Minute)
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			res, err := f.runQueryAPI(map[string]any{
				"dataset": "spans",
				"time_range": map[string]any{
					"from": rfc3339(from), "to": rfc3339(now.Add(5 * time.Second)),
				},
				"select": []map[string]any{{"op": "count"}},
				"where": []map[string]any{
					{"field": "service.name", "op": "=", "value": "attrs-svc"},
					{"field": c.name, "op": c.op, "value": c.value},
				},
			})
			if err != nil {
				t.Fatalf("query: %v", err)
			}
			count, _ := toInt64(res.Rows[0][res.columnIdx("count")])
			if count != 1 {
				t.Errorf("filter on %s %s %v: want 1, got %d", c.name, c.op, c.value, count)
			}
		})
	}

	// Slice attributes don't filter via =, but they must appear in the
	// raw span attributes blob on GetTrace. Fetch the one span.
	var listResp struct {
		Traces []struct {
			TraceID string `json:"trace_id"`
		} `json:"traces"`
	}
	if err := f.getJSON("/api/traces?service=attrs-svc&limit=5", &listResp); err != nil {
		t.Fatalf("list: %v", err)
	}
	if len(listResp.Traces) == 0 {
		t.Fatal("no trace")
	}
	var detail struct {
		Spans []struct {
			Attributes string `json:"attributes"`
		} `json:"spans"`
	}
	if err := f.getJSON("/api/traces/"+listResp.Traces[0].TraceID, &detail); err != nil {
		t.Fatalf("detail: %v", err)
	}
	attrs := detail.Spans[0].Attributes
	for _, frag := range []string{
		`"attr.strslice":["a","b","c"]`,
		`"attr.intslice":[1,2,3]`,
	} {
		if !strings.Contains(attrs, frag) {
			t.Errorf("missing %s in attrs: %s", frag, attrs)
		}
	}
}

// ---------------------------------------------------------------------------
// Parent/child + cross-service: a single trace_id spanning 2 services
// composes into one trace on GetTrace with all spans present.
// ---------------------------------------------------------------------------

func TestE2E_CrossServiceTrace(t *testing.T) {
	f := newE2EFixture(t)
	defer f.close()

	apiTP := f.tracerProvider("gateway")
	payTP := f.tracerProvider("payments")
	apiTracer := apiTP.Tracer("e2e")
	payTracer := payTP.Tracer("e2e")

	ctx, root := apiTracer.Start(context.Background(), "POST /checkout",
		trace.WithSpanKind(trace.SpanKindServer))
	traceID := root.SpanContext().TraceID().String()

	// Child on the OTHER service but inheriting the same trace_id via ctx.
	_, child := payTracer.Start(ctx, "PaymentService/Authorize",
		trace.WithSpanKind(trace.SpanKindClient))
	child.End()
	root.End()

	f.shutdownTracer(apiTP)
	f.shutdownTracer(payTP)
	f.waitForTotalSpanCount(2)

	var detail struct {
		Spans []struct {
			ServiceName string `json:"service_name"`
			Name        string `json:"name"`
		} `json:"spans"`
	}
	if err := f.getJSON("/api/traces/"+traceID, &detail); err != nil {
		t.Fatalf("detail: %v", err)
	}
	if len(detail.Spans) != 2 {
		t.Fatalf("cross-service trace: want 2 spans, got %d", len(detail.Spans))
	}
	got := map[string]string{}
	for _, s := range detail.Spans {
		got[s.ServiceName] = s.Name
	}
	if got["gateway"] != "POST /checkout" {
		t.Errorf("gateway span missing / wrong: %v", got)
	}
	if got["payments"] != "PaymentService/Authorize" {
		t.Errorf("payments span missing / wrong: %v", got)
	}

	// Service list must show both with span_count = 1 each.
	var svcResp struct {
		Services []struct {
			Service   string `json:"service"`
			SpanCount int64  `json:"span_count"`
		} `json:"services"`
	}
	if err := f.getJSON("/api/services", &svcResp); err != nil {
		t.Fatalf("services: %v", err)
	}
	per := map[string]int64{}
	for _, s := range svcResp.Services {
		per[s.Service] = s.SpanCount
	}
	if per["gateway"] != 1 || per["payments"] != 1 {
		t.Errorf("service counts want gateway=1 payments=1, got %v", per)
	}
}

// ---------------------------------------------------------------------------
// helpers shared by span tests
// ---------------------------------------------------------------------------

func toInt64(v any) (int64, bool) {
	switch n := v.(type) {
	case int64:
		return n, true
	case int:
		return int64(n), true
	case float64:
		return int64(n), true
	case string:
		var x int64
		if _, err := fmt.Sscan(n, &x); err == nil {
			return x, true
		}
	}
	return 0, false
}

func equalIntMap(a, b map[int64]int64) bool {
	if len(a) != len(b) {
		return false
	}
	for k, v := range a {
		if b[k] != v {
			return false
		}
	}
	return true
}
