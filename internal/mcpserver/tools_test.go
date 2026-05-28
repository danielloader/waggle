package mcpserver

import (
	"context"
	"io"
	"log/slog"
	"path/filepath"
	"testing"
	"time"

	"github.com/danielloader/waggle/internal/query"
	"github.com/danielloader/waggle/internal/store"
	"github.com/danielloader/waggle/internal/store/sqlite"
)

func TestClampLimit(t *testing.T) {
	cases := []struct {
		req, def, max, want int
	}{
		{0, 100, 1000, 100},     // unset -> default
		{-5, 100, 1000, 100},    // negative -> default
		{50, 100, 1000, 50},     // in range -> itself
		{5000, 100, 1000, 1000}, // over -> max
		{1000, 100, 1000, 1000}, // exactly max
	}
	for _, c := range cases {
		if got := clampLimit(c.req, c.def, c.max); got != c.want {
			t.Errorf("clampLimit(%d,%d,%d) = %d, want %d", c.req, c.def, c.max, got, c.want)
		}
	}
}

func TestSignalFor(t *testing.T) {
	cases := map[string]string{
		"spans":   store.SignalSpan,
		"logs":    store.SignalLog,
		"metrics": store.SignalMetric,
		"log":     store.SignalLog,
		"metric":  store.SignalMetric,
		"":        store.SignalSpan, // default
		"bogus":   store.SignalSpan, // unknown -> span
	}
	for in, want := range cases {
		if got := signalFor(in); got != want {
			t.Errorf("signalFor(%q) = %q, want %q", in, got, want)
		}
	}
}

func TestIsHexID(t *testing.T) {
	cases := map[string]bool{
		"abcdef0123456789": true,
		"ABCDEF":           false, // uppercase rejected (caller lowercases first)
		"xyz":              false,
		"":                 true, // empty has no invalid chars; length is checked separately
	}
	for in, want := range cases {
		if got := isHexID(in); got != want {
			t.Errorf("isHexID(%q) = %v, want %v", in, got, want)
		}
	}
}

func TestParseAbsTime(t *testing.T) {
	if _, err := parseAbsTime(""); err == nil {
		t.Error("parseAbsTime(\"\") should error")
	}
	if _, err := parseAbsTime("not-a-time"); err == nil {
		t.Error("parseAbsTime(garbage) should error")
	}
	got, err := parseAbsTime("1700000000000000000")
	if err != nil || got.UnixNano() != 1700000000000000000 {
		t.Errorf("parseAbsTime(ns) = %v, %v", got.UnixNano(), err)
	}
	got, err = parseAbsTime("2026-05-15T09:00:00Z")
	if err != nil || got.UTC().Hour() != 9 {
		t.Errorf("parseAbsTime(RFC3339) = %v, %v", got, err)
	}
}

func TestResolveTimeRange(t *testing.T) {
	// Relative "last": to == now, from == now - d, so the span is exactly d.
	r, err := resolveTimeRange(timeRange{Last: "2h"})
	if err != nil {
		t.Fatalf("last: %v", err)
	}
	if d := r.To.Sub(r.From.Time); d != 2*time.Hour {
		t.Errorf("last=2h span = %v, want 2h", d)
	}

	// Empty defaults to the last hour.
	r, _ = resolveTimeRange(timeRange{})
	if d := r.To.Sub(r.From.Time); d != time.Hour {
		t.Errorf("empty span = %v, want 1h", d)
	}

	// Absolute from/to round-trips exactly.
	r, err = resolveTimeRange(timeRange{From: "2026-05-15T09:00:00Z", To: "2026-05-15T10:00:00Z"})
	if err != nil {
		t.Fatalf("abs: %v", err)
	}
	if d := r.To.Sub(r.From.Time); d != time.Hour {
		t.Errorf("abs span = %v, want 1h", d)
	}

	// from set, to omitted -> to defaults to ~now.
	r, _ = resolveTimeRange(timeRange{From: "2026-05-15T09:00:00Z"})
	if time.Since(r.To.Time) > 5*time.Second {
		t.Errorf("to should default to ~now, got %v", r.To.Time)
	}

	// Invalid / non-positive durations error.
	if _, err := resolveTimeRange(timeRange{Last: "nope"}); err == nil {
		t.Error("invalid last should error")
	}
	if _, err := resolveTimeRange(timeRange{Last: "-1h"}); err == nil {
		t.Error("negative last should error")
	}
}

// --- store-backed handler tests ---------------------------------------------

const (
	testTraceHex = "0102030405060708090a0b0c0d0e0f10"
)

func newSeededTools(t *testing.T) *tools {
	t.Helper()
	ctx := context.Background()
	s, err := sqlite.Open(ctx, filepath.Join(t.TempDir(), "mcp.db"))
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { _ = s.Close() })

	now := time.Now().UnixNano()
	tid := []byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16}
	root := []byte{1, 1, 1, 1, 1, 1, 1, 1}
	child := []byte{2, 2, 2, 2, 2, 2, 2, 2}
	end := now + 1_000_000
	ok := int32(0)
	var flags uint32

	batch := store.Batch{
		Resources: []store.Resource{{
			ID: 1, ServiceName: "svc-a", AttributesJSON: `{"service.name":"svc-a"}`,
			FirstSeenNS: now, LastSeenNS: now,
		}},
		Scopes: []store.Scope{{ID: 1, Name: "go.test", Version: "v1"}},
		Events: []store.Event{
			{
				TimeNS: now, EndTimeNS: &end, ResourceID: 1, ScopeID: 1,
				ServiceName: "svc-a", Name: "GET /x", TraceID: tid, SpanID: root,
				StatusCode: &ok, Flags: &flags,
				AttributesJSON: `{"meta.signal_type":"span","meta.span_kind":"SERVER"}`,
			},
			{
				TimeNS: now + 1000, EndTimeNS: &end, ResourceID: 1, ScopeID: 1,
				ServiceName: "svc-a", Name: "db.query", TraceID: tid, SpanID: child, ParentSpanID: root,
				StatusCode: &ok, Flags: &flags,
				AttributesJSON: `{"meta.signal_type":"span","meta.span_kind":"CLIENT"}`,
			},
			{
				TimeNS: now, ResourceID: 1, ScopeID: 1, ServiceName: "svc-a",
				Name: "log", Body: "card declined for order 42",
				AttributesJSON: `{"meta.signal_type":"log"}`,
			},
		},
		MetricEvents: []store.MetricEvent{{
			TimeNS: now, ResourceID: 1, ScopeID: 1, ServiceName: "svc-a",
			AttributesJSON: `{"meta.signal_type":"metric","requests.total":17}`,
		}},
	}
	if err := s.WriteBatch(ctx, batch); err != nil {
		t.Fatalf("write: %v", err)
	}
	return &tools{store: s, log: slog.New(slog.NewTextHandler(io.Discard, nil))}
}

func TestQueryToolCount(t *testing.T) {
	tl := newSeededTools(t)
	_, out, err := tl.runQuery(context.Background(), nil, queryInput{
		Dataset: "spans",
		Time:    timeRange{Last: "1h"},
		Select:  []query.Aggregation{{Op: query.OpCount}},
	})
	if err != nil {
		t.Fatalf("runQuery: %v", err)
	}
	if out.RowCount != 1 || len(out.Rows) != 1 {
		t.Fatalf("want 1 aggregate row, got %d: %+v", out.RowCount, out.Rows)
	}
	// COUNT over the two spans in svc-a.
	if got := toInt(out.Rows[0][0]); got != 2 {
		t.Errorf("span count = %d, want 2", got)
	}
}

func TestQueryToolTruncated(t *testing.T) {
	tl := newSeededTools(t)
	// Raw-rows mode (empty select) over 2 spans, limit 1 -> truncated.
	_, out, err := tl.runQuery(context.Background(), nil, queryInput{
		Dataset: "spans",
		Time:    timeRange{Last: "1h"},
		Limit:   1,
	})
	if err != nil {
		t.Fatalf("runQuery: %v", err)
	}
	if !out.Truncated || out.Note == "" {
		t.Errorf("expected truncated with a note, got truncated=%v note=%q", out.Truncated, out.Note)
	}
}

func TestQueryToolInvalidDataset(t *testing.T) {
	tl := newSeededTools(t)
	_, _, err := tl.runQuery(context.Background(), nil, queryInput{Dataset: "bogus", Time: timeRange{Last: "1h"}})
	if err == nil {
		t.Fatal("expected error for invalid dataset")
	}
}

func TestListServicesTool(t *testing.T) {
	tl := newSeededTools(t)
	_, out, err := tl.listServices(context.Background(), nil, struct{}{})
	if err != nil {
		t.Fatalf("listServices: %v", err)
	}
	if len(out.Services) != 1 || out.Services[0].ServiceName != "svc-a" {
		t.Fatalf("unexpected services: %+v", out.Services)
	}
}

func TestGetTraceTool(t *testing.T) {
	tl := newSeededTools(t)

	_, out, err := tl.getTrace(context.Background(), nil, traceInput{TraceID: testTraceHex})
	if err != nil {
		t.Fatalf("getTrace: %v", err)
	}
	if len(out.Spans) != 2 {
		t.Errorf("want 2 spans, got %d", len(out.Spans))
	}
	if len(out.Resources) == 0 {
		t.Error("expected resources keyed by string id")
	}

	if _, _, err := tl.getTrace(context.Background(), nil, traceInput{TraceID: "tooshort"}); err == nil {
		t.Error("expected error for malformed trace id")
	}
	if _, _, err := tl.getTrace(context.Background(), nil, traceInput{TraceID: "ffffffffffffffffffffffffffffffff"}); err == nil {
		t.Error("expected not-found error for missing trace")
	}
}

func TestSearchLogsTool(t *testing.T) {
	tl := newSeededTools(t)
	_, out, err := tl.searchLogs(context.Background(), nil, logsInput{
		Query: "declined",
		Time:  timeRange{Last: "1h"},
	})
	if err != nil {
		t.Fatalf("searchLogs: %v", err)
	}
	if len(out.Logs) != 1 {
		t.Fatalf("want 1 matching log, got %d", len(out.Logs))
	}
}

func toInt(v any) int64 {
	switch n := v.(type) {
	case int64:
		return n
	case int:
		return int64(n)
	case float64:
		return int64(n)
	default:
		return -1
	}
}
