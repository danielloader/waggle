package sqlite

import (
	"context"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/danielloader/waggle/internal/store"
)

func TestOpenAppliesSchema(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "test.db")
	s, err := Open(ctx, path)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer s.Close()

	// Confirm schema tables exist.
	var name string
	err = s.ReaderDB().QueryRowContext(ctx,
		`SELECT name FROM sqlite_master WHERE type='table' AND name='spans'`).Scan(&name)
	if err != nil {
		t.Fatalf("spans table missing: %v", err)
	}
}

func TestWriteAndReadSpan(t *testing.T) {
	ctx := context.Background()
	s, err := Open(ctx, filepath.Join(t.TempDir(), "test.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	now := time.Now().UnixNano()
	tid := []byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16}
	sid := []byte{1, 2, 3, 4, 5, 6, 7, 8}

	batch := store.Batch{
		Resources: []store.Resource{{
			ID: 1, ServiceName: "test", AttributesJSON: `{"service.name":"test"}`,
			FirstSeenNS: now, LastSeenNS: now,
		}},
		Scopes: []store.Scope{{ID: 1, Name: "go.test", Version: "v1"}},
		Spans: []store.Span{{
			TraceID: tid, SpanID: sid, ResourceID: 1, ScopeID: 1,
			ServiceName: "test", Name: "root", Kind: 1,
			StartTimeNS: now, EndTimeNS: now + 1_000_000,
			AttributesJSON: `{"http.route":"/x","http.response.status_code":200}`,
		}},
		AttrKeys: []store.AttrKeyDelta{
			{SignalType: "span", ServiceName: "test", Key: "http.route", ValueType: "str", Count: 1, LastSeenNS: now},
		},
	}

	if err := s.WriteBatch(ctx, batch); err != nil {
		t.Fatalf("WriteBatch: %v", err)
	}

	services, err := s.ListServices(ctx)
	if err != nil {
		t.Fatalf("ListServices: %v", err)
	}
	if len(services) != 1 || services[0].ServiceName != "test" || services[0].SpanCount != 1 {
		t.Fatalf("unexpected services: %+v", services)
	}

	traces, _, err := s.ListTraces(ctx, store.TraceFilter{})
	if err != nil {
		t.Fatalf("ListTraces: %v", err)
	}
	if len(traces) != 1 || traces[0].RootService != "test" {
		t.Fatalf("unexpected traces: %+v", traces)
	}

	detail, err := s.GetTrace(ctx, traces[0].TraceID)
	if err != nil {
		t.Fatalf("GetTrace: %v", err)
	}
	if len(detail.Spans) != 1 {
		t.Fatalf("expected 1 span, got %d", len(detail.Spans))
	}

	// ListFields should surface the attribute-keys row we just wrote
	// ("http.route" with a non-zero count) and the promoted / synthetic
	// fields the query builder resolves natively ("name", "service.name",
	// "duration_ns", ...). Synthetic fields carry count=0; attribute-keys
	// rows carry the actual observation count.
	fields, err := s.ListFields(ctx, store.FieldFilter{SignalType: "span", Service: "test"})
	if err != nil {
		t.Fatalf("ListFields: %v", err)
	}
	keys := make(map[string]store.FieldInfo)
	for _, fi := range fields {
		keys[fi.Key] = fi
	}
	if fi, ok := keys["http.route"]; !ok || fi.Count == 0 {
		t.Fatalf("expected http.route attribute_keys row: %+v", fields)
	}
	for _, k := range []string{"name", "service.name", "duration_ns", "is_root", "error"} {
		if _, ok := keys[k]; !ok {
			t.Fatalf("expected synthetic field %q in results: %+v", k, fields)
		}
	}

	// Prefix-filter narrows both synthetic + attribute_keys rows.
	prefixed, err := s.ListFields(ctx, store.FieldFilter{SignalType: "span", Prefix: "http."})
	if err != nil {
		t.Fatalf("ListFields prefix: %v", err)
	}
	for _, fi := range prefixed {
		if !strings.HasPrefix(fi.Key, "http.") {
			t.Fatalf("prefix leak: %+v", fi)
		}
	}
	if len(prefixed) == 0 {
		t.Fatalf("expected at least one http.* field")
	}

	// With no service scope, ListFields still returns data (regression
	// guard for the previous bug where service_name = '' matched nothing).
	fieldsAll, err := s.ListFields(ctx, store.FieldFilter{SignalType: "span"})
	if err != nil {
		t.Fatalf("ListFields (no service): %v", err)
	}
	foundRoute := false
	for _, fi := range fieldsAll {
		if fi.Key == "http.route" {
			foundRoute = true
			break
		}
	}
	if !foundRoute {
		t.Fatalf("expected http.route in cross-service fields: %+v", fieldsAll)
	}
}

func TestPercentileUDF(t *testing.T) {
	ctx := context.Background()
	s, err := Open(ctx, filepath.Join(t.TempDir(), "test.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	var p50, p95 float64
	err = s.ReaderDB().QueryRowContext(ctx, `
		WITH vs(v) AS (
		  SELECT 1 UNION ALL SELECT 2 UNION ALL SELECT 3 UNION ALL SELECT 4
		  UNION ALL SELECT 5 UNION ALL SELECT 6 UNION ALL SELECT 7 UNION ALL SELECT 8
		  UNION ALL SELECT 9 UNION ALL SELECT 10
		)
		SELECT percentile(v, 0.5), percentile(v, 0.95) FROM vs`).Scan(&p50, &p95)
	if err != nil {
		t.Fatalf("percentile: %v", err)
	}
	if p50 < 5.0 || p50 > 6.0 {
		t.Errorf("p50 expected ~5.5, got %v", p50)
	}
	if p95 < 9.0 || p95 > 10.0 {
		t.Errorf("p95 expected ~9.5, got %v", p95)
	}
}
