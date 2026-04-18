package sqlite

import (
	"context"
	"path/filepath"
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

	fields, err := s.ListFields(ctx, store.FieldFilter{SignalType: "span", Service: "test"})
	if err != nil {
		t.Fatalf("ListFields: %v", err)
	}
	if len(fields) != 1 || fields[0].Key != "http.route" {
		t.Fatalf("unexpected fields: %+v", fields)
	}

	// With no service scope, ListFields should pool across every service
	// so the UI's "no WHERE filter yet" state can still populate
	// group-by / autocomplete dropdowns.
	fieldsAll, err := s.ListFields(ctx, store.FieldFilter{SignalType: "span"})
	if err != nil {
		t.Fatalf("ListFields (no service): %v", err)
	}
	if len(fieldsAll) != 1 || fieldsAll[0].Key != "http.route" {
		t.Fatalf("unexpected cross-service fields: %+v", fieldsAll)
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
