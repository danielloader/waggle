package ingest_test

// Two properties that the wide-events unification exposes:
//
//   1) meta.* whitelist: reserved keys (meta.signal_type, meta.span_kind,
//      meta.metric_kind, meta.annotation_type, meta.metric_*) are always
//      stamped by the ingest transform. Any user-sent value on those keys
//      is overwritten. Non-whitelisted meta.foo user attributes pass
//      through as ordinary attributes.
//
//   2) Cross-signal queries: a user attribute shared by spans and logs
//      (e.g. `user_id`) can be grouped by on either dataset and lives on
//      the same events row regardless of signal.

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"go.opentelemetry.io/otel/attribute"
	otellog "go.opentelemetry.io/otel/log"
)

// TestE2E_MetaReservedIsOverwritten — an SDK that tries to set
// meta.signal_type="bogus" gets it overwritten by our system stamp.
// meta.tenant_id (non-whitelisted) passes through untouched.
func TestE2E_MetaReservedIsOverwritten(t *testing.T) {
	f := newE2EFixture(t)
	defer f.close()

	tp := f.tracerProvider("meta-svc")
	tr := tp.Tracer("e2e")

	_, span := tr.Start(context.Background(), "op")
	// A misguided user SDK setting the reserved key directly.
	span.SetAttributes(
		attribute.String("meta.signal_type", "bogus"),
		attribute.String("meta.span_kind", "FAKE"),
		attribute.String("meta.tenant_id", "acme"), // not reserved
	)
	span.End()
	f.shutdownTracer(tp)
	f.waitForSpanCount("meta-svc", 1)

	// meta.signal_type should still be 'span'; meta.span_kind should be
	// the real kind; meta.tenant_id should pass through.
	var signalType, spanKind, attrs string
	err := f.st.ReaderDB().QueryRowContext(f.ctx, `
		SELECT signal_type, COALESCE(span_kind, ''), attributes
		  FROM events WHERE service_name = 'meta-svc' LIMIT 1
	`).Scan(&signalType, &spanKind, &attrs)
	if err != nil {
		t.Fatalf("select: %v", err)
	}
	if signalType != "span" {
		t.Errorf("meta.signal_type: want 'span', got %q", signalType)
	}
	if spanKind == "FAKE" {
		t.Errorf("meta.span_kind: want real kind, got FAKE (user override not blocked)")
	}
	if !strings.Contains(attrs, `"meta.tenant_id":"acme"`) {
		t.Errorf("meta.tenant_id should pass through; attrs=%s", attrs)
	}
}

// TestE2E_CrossSignalAttribute — same `user_id` attribute on a span and a
// log, then group by user_id in both datasets.
func TestE2E_CrossSignalAttribute(t *testing.T) {
	f := newE2EFixture(t)
	defer f.close()

	tp := f.tracerProvider("xsig-svc")
	tr := tp.Tracer("e2e")
	for i, u := range []string{"alice", "alice", "bob"} {
		_, span := tr.Start(context.Background(), fmt.Sprintf("op-%d", i))
		span.SetAttributes(attribute.String("user_id", u))
		span.End()
	}
	f.shutdownTracer(tp)

	lp := f.loggerProvider("xsig-svc")
	logger := lp.Logger("e2e")
	for _, u := range []string{"alice", "bob", "bob"} {
		var rec otellog.Record
		rec.SetTimestamp(time.Now())
		rec.SetBody(otellog.StringValue("hello"))
		rec.SetSeverity(otellog.SeverityInfo)
		rec.AddAttributes(otellog.String("user_id", u))
		logger.Emit(context.Background(), rec)
	}
	f.shutdownLogger(lp)
	f.waitForSpanCount("xsig-svc", 3)

	now := time.Now()
	from := now.Add(-5 * time.Minute)

	countPerUser := func(dataset string) map[string]int64 {
		res, err := f.runQueryAPI(map[string]any{
			"dataset":    dataset,
			"time_range": map[string]any{"from": rfc3339(from), "to": rfc3339(now.Add(5 * time.Second))},
			"select":     []map[string]any{{"op": "count"}},
			"where":      []map[string]any{{"field": "service.name", "op": "=", "value": "xsig-svc"}},
			"group_by":   []string{"user_id"},
		})
		if err != nil {
			t.Fatalf("%s query: %v", dataset, err)
		}
		uIdx := res.columnIdx("user_id")
		cIdx := res.columnIdx("count")
		got := map[string]int64{}
		for _, row := range res.Rows {
			u, _ := row[uIdx].(string)
			c, _ := toInt64(row[cIdx])
			got[u] = c
		}
		return got
	}

	spans := countPerUser("spans")
	if spans["alice"] != 2 || spans["bob"] != 1 {
		t.Errorf("span group by user_id: want alice=2 bob=1, got %+v", spans)
	}
	logs := countPerUser("logs")
	if logs["alice"] != 1 || logs["bob"] != 2 {
		t.Errorf("log group by user_id: want alice=1 bob=2, got %+v", logs)
	}
}
