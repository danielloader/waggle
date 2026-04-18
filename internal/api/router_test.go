package api_test

import (
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
	"strings"
	"testing"
	"time"

	"github.com/danielloader/waggle/internal/api"
	"github.com/danielloader/waggle/internal/store"
	"github.com/danielloader/waggle/internal/store/sqlite"
)

// apiFixture wires a real SQLite store into the API router behind an
// httptest.Server so tests exercise the full HTTP path (path routing,
// query-string parsing, JSON encoding).
type apiFixture struct {
	t   *testing.T
	ctx context.Context
	st  *sqlite.Store
	srv *httptest.Server
}

func newAPIFixture(t *testing.T) *apiFixture {
	t.Helper()
	ctx := context.Background()
	st, err := sqlite.Open(ctx, filepath.Join(t.TempDir(), "api.db"))
	if err != nil {
		t.Fatalf("sqlite.Open: %v", err)
	}
	log := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError}))
	mux := http.NewServeMux()
	mux.HandleFunc("GET /api/health", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"ok":true}`))
	})
	api.NewRouter(st, log).Mount(mux)
	srv := httptest.NewServer(mux)
	t.Cleanup(func() {
		srv.Close()
		_ = st.Close()
	})
	return &apiFixture{t: t, ctx: ctx, st: st, srv: srv}
}

// seed writes a deterministic batch of spans + logs so read endpoints have
// something to return. Trace with 2 spans, one log record, and a single
// service. Returns the hex trace_id for later assertions.
func (f *apiFixture) seed() string {
	t := f.t
	t.Helper()
	now := time.Now().UnixNano()
	tid := []byte{
		0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08,
		0x09, 0x0a, 0x0b, 0x0c, 0x0d, 0x0e, 0x0f, 0x10,
	}
	root := []byte{0x20, 0x21, 0x22, 0x23, 0x24, 0x25, 0x26, 0x27}
	child := []byte{0x30, 0x31, 0x32, 0x33, 0x34, 0x35, 0x36, 0x37}
	batch := store.Batch{
		Resources: []store.Resource{{
			ID: 1, ServiceName: "widget",
			AttributesJSON: `{"service.name":"widget"}`,
			FirstSeenNS:    now, LastSeenNS: now,
		}},
		Scopes: []store.Scope{{ID: 1, Name: "lib", Version: "1.0"}},
		Spans: []store.Span{
			{
				TraceID: tid, SpanID: root, ResourceID: 1, ScopeID: 1,
				ServiceName: "widget", Name: "GET /", Kind: 2,
				StartTimeNS: now, EndTimeNS: now + 10_000_000,
				AttributesJSON: `{"http.route":"/","http.response.status_code":200,"customer.tier":"gold"}`,
			},
			{
				TraceID: tid, SpanID: child, ParentSpanID: root, ResourceID: 1, ScopeID: 1,
				ServiceName: "widget", Name: "db.query", Kind: 3,
				StartTimeNS: now + 1_000_000, EndTimeNS: now + 8_000_000,
				StatusCode:  2, StatusMessage: "boom",
				AttributesJSON: `{"db.system":"postgresql"}`,
			},
		},
		Logs: []store.LogRecord{{
			ResourceID: 1, ScopeID: 1, ServiceName: "widget",
			TimeNS: now, SeverityNumber: 9, SeverityText: "INFO",
			Body: "hello world", AttributesJSON: `{"component":"http"}`,
		}},
		AttrKeys: []store.AttrKeyDelta{
			{SignalType: "span", ServiceName: "widget", Key: "http.route", ValueType: "str", Count: 1, LastSeenNS: now},
			{SignalType: "span", ServiceName: "widget", Key: "customer.tier", ValueType: "str", Count: 1, LastSeenNS: now},
			{SignalType: "span", ServiceName: "widget", Key: "db.system", ValueType: "str", Count: 1, LastSeenNS: now},
			{SignalType: "log", ServiceName: "widget", Key: "component", ValueType: "str", Count: 1, LastSeenNS: now},
		},
		AttrValues: []store.AttrValueDelta{
			{SignalType: "span", ServiceName: "widget", Key: "http.route", Value: "/", Count: 1, LastSeenNS: now},
			{SignalType: "span", ServiceName: "widget", Key: "customer.tier", Value: "gold", Count: 1, LastSeenNS: now},
		},
	}
	if err := f.st.WriteBatch(f.ctx, batch); err != nil {
		t.Fatalf("seed: %v", err)
	}
	// hex.EncodeToString(tid)
	return "0102030405060708090a0b0c0d0e0f10"
}

func (f *apiFixture) getJSON(path string, into any) int {
	f.t.Helper()
	resp, err := http.Get(f.srv.URL + path)
	if err != nil {
		f.t.Fatalf("GET %s: %v", path, err)
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode == http.StatusOK && into != nil {
		if err := json.Unmarshal(body, into); err != nil {
			f.t.Fatalf("decode %s: %v\nbody: %s", path, err, body)
		}
	}
	return resp.StatusCode
}

// -----------------------------------------------------------------------------
// Tests
// -----------------------------------------------------------------------------

func TestAPI_Health(t *testing.T) {
	f := newAPIFixture(t)
	var body struct {
		OK bool `json:"ok"`
	}
	if got := f.getJSON("/api/health", &body); got != http.StatusOK {
		t.Fatalf("status: %d", got)
	}
	if !body.OK {
		t.Errorf("health not ok: %+v", body)
	}
}

func TestAPI_Services(t *testing.T) {
	f := newAPIFixture(t)

	// Empty store.
	var empty struct {
		Services []store.ServiceSummary `json:"services"`
	}
	if got := f.getJSON("/api/services", &empty); got != http.StatusOK {
		t.Fatalf("status: %d", got)
	}
	if len(empty.Services) != 0 {
		t.Errorf("want empty services, got %+v", empty.Services)
	}

	// Seed and re-query.
	f.seed()
	var populated struct {
		Services []store.ServiceSummary `json:"services"`
	}
	f.getJSON("/api/services", &populated)
	if len(populated.Services) != 1 {
		t.Fatalf("want 1 service, got %d", len(populated.Services))
	}
	if populated.Services[0].ServiceName != "widget" {
		t.Errorf("service name: %q", populated.Services[0].ServiceName)
	}
	if populated.Services[0].SpanCount != 2 {
		t.Errorf("span count: %d", populated.Services[0].SpanCount)
	}
	if populated.Services[0].ErrorCount != 1 {
		t.Errorf("error count: %d", populated.Services[0].ErrorCount)
	}
}

func TestAPI_TracesList_AndFilters(t *testing.T) {
	f := newAPIFixture(t)
	traceID := f.seed()

	// Unfiltered list.
	var list struct {
		Traces []store.TraceSummary `json:"traces"`
	}
	f.getJSON("/api/traces", &list)
	if len(list.Traces) != 1 || list.Traces[0].TraceID != traceID {
		t.Errorf("unexpected trace list: %+v", list.Traces)
	}
	if list.Traces[0].SpanCount != 2 {
		t.Errorf("span count: %d", list.Traces[0].SpanCount)
	}
	if !list.Traces[0].HasError {
		t.Errorf("expected has_error=true")
	}

	// Service filter matches.
	f.getJSON("/api/traces?service=widget", &list)
	if len(list.Traces) != 1 {
		t.Errorf("service=widget: want 1, got %d", len(list.Traces))
	}

	// Service filter misses.
	f.getJSON("/api/traces?service=missing", &list)
	if len(list.Traces) != 0 {
		t.Errorf("service=missing: want 0, got %d", len(list.Traces))
	}

	// has_error=true matches.
	f.getJSON("/api/traces?has_error=true", &list)
	if len(list.Traces) != 1 {
		t.Errorf("has_error=true: want 1, got %d", len(list.Traces))
	}

	// has_error=false excludes the seed trace (which has an error span).
	f.getJSON("/api/traces?has_error=false", &list)
	if len(list.Traces) != 0 {
		t.Errorf("has_error=false: want 0, got %d", len(list.Traces))
	}
}

func TestAPI_GetTrace(t *testing.T) {
	f := newAPIFixture(t)
	traceID := f.seed()

	var detail store.TraceDetail
	if got := f.getJSON("/api/traces/"+traceID, &detail); got != http.StatusOK {
		t.Fatalf("status: %d", got)
	}
	if detail.TraceID != traceID {
		t.Errorf("trace_id: %q", detail.TraceID)
	}
	if len(detail.Spans) != 2 {
		t.Fatalf("spans: want 2, got %d", len(detail.Spans))
	}
	names := map[string]bool{}
	for _, s := range detail.Spans {
		names[s.Name] = true
	}
	if !names["GET /"] || !names["db.query"] {
		t.Errorf("missing spans: %+v", names)
	}
	if len(detail.Resources) == 0 {
		t.Errorf("no resources in detail")
	}
}

func TestAPI_GetTrace_InvalidID(t *testing.T) {
	f := newAPIFixture(t)
	// Non-hex or wrong-length input is a client error.
	if got := f.getJSON("/api/traces/not-a-hex-id", nil); got != http.StatusBadRequest {
		t.Errorf("malformed id: want 400, got %d", got)
	}
	if got := f.getJSON("/api/traces/abcd", nil); got != http.StatusBadRequest {
		t.Errorf("short id: want 400, got %d", got)
	}
	if got := f.getJSON("/api/traces/0123456789abcdef0123456789abcdeg", nil); got != http.StatusBadRequest {
		t.Errorf("non-hex char: want 400, got %d", got)
	}
}

func TestAPI_GetTrace_NotFound(t *testing.T) {
	f := newAPIFixture(t)
	// Valid hex, correct length, but the trace doesn't exist in the store.
	got := f.getJSON("/api/traces/00000000000000000000000000000000", nil)
	if got != http.StatusNotFound {
		t.Errorf("missing id: want 404, got %d", got)
	}
}

func TestAPI_Fields(t *testing.T) {
	f := newAPIFixture(t)
	f.seed()

	var resp struct {
		Fields []store.FieldInfo `json:"fields"`
	}
	f.getJSON("/api/fields?dataset=span&service=widget", &resp)
	seen := map[string]string{}
	for _, fi := range resp.Fields {
		seen[fi.Key] = fi.ValueType
	}
	for _, k := range []string{"http.route", "customer.tier", "db.system"} {
		if _, ok := seen[k]; !ok {
			t.Errorf("field catalog missing %q; got %+v", k, seen)
		}
	}

	// Prefix filter.
	f.getJSON("/api/fields?dataset=span&service=widget&prefix=http.", &resp)
	for _, fi := range resp.Fields {
		if len(fi.Key) < 5 || fi.Key[:5] != "http." {
			t.Errorf("prefix filter leaked %q", fi.Key)
		}
	}

	// Log signal.
	f.getJSON("/api/fields?dataset=log&service=widget", &resp)
	var sawComponent bool
	for _, fi := range resp.Fields {
		if fi.Key == "component" {
			sawComponent = true
		}
	}
	if !sawComponent {
		t.Errorf("log catalog missing 'component': %+v", resp.Fields)
	}
}

func TestAPI_FieldValues(t *testing.T) {
	f := newAPIFixture(t)
	f.seed()

	q := url.Values{"dataset": {"span"}, "service": {"widget"}}
	var resp struct {
		Values []string `json:"values"`
	}
	f.getJSON("/api/fields/"+url.PathEscape("customer.tier")+"/values?"+q.Encode(), &resp)
	if len(resp.Values) != 1 || resp.Values[0] != "gold" {
		t.Errorf("customer.tier values: want [gold], got %+v", resp.Values)
	}

	// Cross-service autocomplete: omit the service parameter and expect
	// values pooled across every service with a matching key.
	qAll := url.Values{"dataset": {"span"}}
	f.getJSON("/api/fields/"+url.PathEscape("customer.tier")+"/values?"+qAll.Encode(), &resp)
	if len(resp.Values) != 1 || resp.Values[0] != "gold" {
		t.Errorf("customer.tier values (cross-service): want [gold], got %+v", resp.Values)
	}

	// Real-column fallback: `name` isn't captured by the attribute sampler
	// (it's a dedicated span column), but the DISTINCT scan against spans
	// should still surface the seeded values "GET /" and "db.query".
	f.getJSON("/api/fields/"+url.PathEscape("name")+"/values?"+qAll.Encode(), &resp)
	seen := map[string]bool{}
	for _, v := range resp.Values {
		seen[v] = true
	}
	if !seen["GET /"] || !seen["db.query"] {
		t.Errorf("name values (real-column fallback): want GET / + db.query; got %+v", resp.Values)
	}

	// service.name also lives in a dedicated column.
	f.getJSON("/api/fields/"+url.PathEscape("service.name")+"/values?"+qAll.Encode(), &resp)
	if len(resp.Values) != 1 || resp.Values[0] != "widget" {
		t.Errorf("service.name values: want [widget], got %+v", resp.Values)
	}
}

func TestAPI_SpanNames(t *testing.T) {
	f := newAPIFixture(t)
	f.seed()

	var resp struct {
		Names []string `json:"names"`
	}
	f.getJSON("/api/span-names?service=widget", &resp)
	want := map[string]bool{"GET /": true, "db.query": true}
	for _, n := range resp.Names {
		delete(want, n)
	}
	if len(want) != 0 {
		t.Errorf("missing span names: %+v", want)
	}

	// Prefix filter.
	f.getJSON("/api/span-names?service=widget&prefix=GET", &resp)
	if len(resp.Names) != 1 || resp.Names[0] != "GET /" {
		t.Errorf("prefix=GET names: %+v", resp.Names)
	}
}

func TestAPI_LogsSearch(t *testing.T) {
	f := newAPIFixture(t)
	f.seed()

	var resp struct {
		Logs []store.LogOut `json:"logs"`
	}
	f.getJSON("/api/logs/search?service=widget", &resp)
	if len(resp.Logs) != 1 || resp.Logs[0].Body != "hello world" {
		t.Errorf("logs: %+v", resp.Logs)
	}

	// FTS match.
	f.getJSON("/api/logs/search?q=hello", &resp)
	if len(resp.Logs) != 1 {
		t.Errorf("FTS hello: want 1, got %d", len(resp.Logs))
	}

	// FTS miss.
	f.getJSON("/api/logs/search?q=unrelated_word", &resp)
	if len(resp.Logs) != 0 {
		t.Errorf("FTS unrelated_word: want 0, got %+v", resp.Logs)
	}
}

func TestAPI_Clear(t *testing.T) {
	f := newAPIFixture(t)
	f.seed()

	// Precondition: data exists.
	var before struct {
		Services []store.ServiceSummary `json:"services"`
	}
	f.getJSON("/api/services", &before)
	if len(before.Services) != 1 {
		t.Fatalf("seed failed: %+v", before.Services)
	}

	// Clear.
	resp, err := http.Post(f.srv.URL+"/api/clear", "application/json", nil)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("clear status: %d", resp.StatusCode)
	}

	// Postcondition: store is empty.
	var after struct {
		Services []store.ServiceSummary `json:"services"`
	}
	f.getJSON("/api/services", &after)
	if len(after.Services) != 0 {
		t.Errorf("expected empty services after clear, got %+v", after.Services)
	}
}

func TestAPI_Query_StructuredAggregation(t *testing.T) {
	f := newAPIFixture(t)

	// Seed 30 spans across 2 services + 3 http.routes so aggregations have
	// something to reduce.
	now := time.Now().UnixNano()
	batch := store.Batch{
		Resources: []store.Resource{
			{ID: 1, ServiceName: "api", AttributesJSON: `{"service.name":"api"}`, FirstSeenNS: now, LastSeenNS: now},
			{ID: 2, ServiceName: "payments", AttributesJSON: `{"service.name":"payments"}`, FirstSeenNS: now, LastSeenNS: now},
		},
		Scopes: []store.Scope{{ID: 1, Name: "lib"}},
	}
	routes := []string{"/users", "/orders", "/products"}
	for i := range 24 {
		tid := make([]byte, 16)
		tid[0] = byte(i)
		tid[1] = 0xA
		sid := []byte{byte(i), 0, 0, 0, 0, 0, 0, 1}
		route := routes[i%3]
		status := 200
		if i%5 == 0 {
			status = 500
		}
		batch.Spans = append(batch.Spans, store.Span{
			TraceID: tid, SpanID: sid, ResourceID: 1, ScopeID: 1,
			ServiceName: "api", Name: "GET " + route, Kind: 2,
			StartTimeNS:    now - int64(i)*1_000_000,
			EndTimeNS:      now - int64(i)*1_000_000 + int64(i+1)*500_000,
			AttributesJSON: fmt.Sprintf(`{"http.route":"%s","http.response.status_code":%d}`, route, status),
		})
	}
	for i := range 6 {
		tid := make([]byte, 16)
		tid[0] = byte(i)
		tid[1] = 0xB
		batch.Spans = append(batch.Spans, store.Span{
			TraceID: tid, SpanID: []byte{byte(i), 0, 0, 0, 0, 0, 0, 2},
			ResourceID: 2, ScopeID: 1, ServiceName: "payments",
			Name: "Authorize", Kind: 2,
			StartTimeNS:    now - int64(i)*1_000_000,
			EndTimeNS:      now - int64(i)*1_000_000 + 2_000_000,
			AttributesJSON: `{"rpc.service":"PaymentService"}`,
		})
	}
	if err := f.st.WriteBatch(f.ctx, batch); err != nil {
		t.Fatal(err)
	}

	// -- COUNT grouped by service, filtered to a time range -------------------
	body := fmt.Sprintf(`{
		"dataset":"spans",
		"time_range":{"from":%d,"to":%d},
		"select":[{"op":"count"}],
		"group_by":["service.name"],
		"order_by":[{"field":"count","dir":"desc"}]
	}`, now-1_000_000_000_000, now+1_000_000_000_000)

	var result store.QueryResult
	if status := postJSON(t, f.srv.URL+"/api/query", body, &result); status != http.StatusOK {
		t.Fatalf("status %d", status)
	}
	if len(result.Rows) != 2 {
		t.Fatalf("want 2 service rows, got %d (%+v)", len(result.Rows), result.Rows)
	}
	got := map[string]int64{}
	for _, row := range result.Rows {
		got[row[0].(string)] = anyInt(row[1])
	}
	if got["api"] != 24 {
		t.Errorf("api count: want 24, got %d", got["api"])
	}
	if got["payments"] != 6 {
		t.Errorf("payments count: want 6, got %d", got["payments"])
	}

	// -- COUNT + p95 grouped by http.route, error filter ---------------------
	body2 := fmt.Sprintf(`{
		"dataset":"spans",
		"time_range":{"from":%d,"to":%d},
		"select":[{"op":"count","alias":"n"},{"op":"p95","field":"duration_ns"}],
		"where":[
			{"field":"service.name","op":"=","value":"api"},
			{"field":"http.response.status_code","op":">=","value":500}
		],
		"group_by":["http.route"],
		"having":[{"field":"n","op":">","value":0}]
	}`, now-1_000_000_000_000, now+1_000_000_000_000)

	var result2 store.QueryResult
	if status := postJSON(t, f.srv.URL+"/api/query", body2, &result2); status != http.StatusOK {
		t.Fatalf("status %d", status)
	}
	if len(result2.Rows) == 0 {
		t.Fatalf("expected rows with 500s; got none")
	}
	for _, row := range result2.Rows {
		if anyInt(row[1]) <= 0 {
			t.Errorf("row count should be > 0: %+v", row)
		}
	}

	// -- Time bucketing ------------------------------------------------------
	body3 := fmt.Sprintf(`{
		"dataset":"spans",
		"time_range":{"from":%d,"to":%d},
		"select":[{"op":"count"}],
		"group_by":["service.name"],
		"bucket_ms":1000
	}`, now-30_000_000_000, now+30_000_000_000)

	var result3 store.QueryResult
	if status := postJSON(t, f.srv.URL+"/api/query", body3, &result3); status != http.StatusOK {
		t.Fatalf("status %d", status)
	}
	if !result3.HasBucket {
		t.Errorf("expected HasBucket=true")
	}
	if len(result3.Columns) < 3 {
		t.Fatalf("expected [bucket_ns, service_name, count]; got %+v", result3.Columns)
	}
	if result3.Columns[0].Name != "bucket_ns" {
		t.Errorf("first column should be bucket_ns; got %q", result3.Columns[0].Name)
	}
}

func TestAPI_Query_RejectsBadInput(t *testing.T) {
	f := newAPIFixture(t)

	// Invalid JSON.
	if s := postJSON(t, f.srv.URL+"/api/query", `{not json`, nil); s != http.StatusBadRequest {
		t.Errorf("bad JSON: want 400, got %d", s)
	}
	// Missing dataset.
	if s := postJSON(t, f.srv.URL+"/api/query",
		`{"time_range":{"from":0,"to":1000000},"select":[{"op":"count"}]}`, nil); s != http.StatusBadRequest {
		t.Errorf("missing dataset: want 400, got %d", s)
	}
	// Malicious field name — contains a space, rejected by keyPattern.
	body := `{"dataset":"spans","time_range":{"from":0,"to":1000000},"select":[{"op":"count"}],"group_by":["bad key"]}`
	if s := postJSON(t, f.srv.URL+"/api/query", body, nil); s != http.StatusBadRequest {
		t.Errorf("bad group_by: want 400, got %d", s)
	}
}

func postJSON(t *testing.T, url, body string, into any) int {
	t.Helper()
	resp, err := http.Post(url, "application/json", strings.NewReader(body))
	if err != nil {
		t.Fatalf("POST: %v", err)
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(resp.Body)
	if resp.StatusCode == http.StatusOK && into != nil {
		if err := json.Unmarshal(raw, into); err != nil {
			t.Fatalf("decode: %v\nbody: %s", err, raw)
		}
	}
	return resp.StatusCode
}

func anyInt(v any) int64 {
	switch t := v.(type) {
	case float64:
		return int64(t)
	case int64:
		return t
	case int:
		return int64(t)
	case json.Number:
		n, _ := t.Int64()
		return n
	}
	return 0
}

func TestAPI_Traces_Pagination(t *testing.T) {
	f := newAPIFixture(t)

	// Seed many roots across a known time range so cursor pagination kicks in.
	now := time.Now().UnixNano()
	batch := store.Batch{
		Resources: []store.Resource{{ID: 1, ServiceName: "widget",
			AttributesJSON: `{"service.name":"widget"}`, FirstSeenNS: now, LastSeenNS: now}},
		Scopes: []store.Scope{{ID: 1, Name: "lib"}},
	}
	for i := range 15 {
		tid := make([]byte, 16)
		tid[0] = byte(i)
		batch.Spans = append(batch.Spans, store.Span{
			TraceID: tid, SpanID: []byte{byte(i), 0, 0, 0, 0, 0, 0, 1},
			ResourceID: 1, ScopeID: 1, ServiceName: "widget",
			Name: fmt.Sprintf("r%d", i), Kind: 2,
			StartTimeNS: now - int64(i)*1_000_000, EndTimeNS: now - int64(i)*1_000_000 + 1_000_000,
			AttributesJSON: "{}",
		})
	}
	if err := f.st.WriteBatch(f.ctx, batch); err != nil {
		t.Fatal(err)
	}

	var page struct {
		Traces     []store.TraceSummary `json:"traces"`
		NextCursor string               `json:"next_cursor"`
	}
	f.getJSON("/api/traces?limit=5", &page)
	if len(page.Traces) != 5 {
		t.Fatalf("page 1: want 5, got %d", len(page.Traces))
	}
	if page.NextCursor == "" {
		t.Fatalf("expected non-empty next_cursor")
	}

	f.getJSON("/api/traces?limit=5&cursor="+page.NextCursor, &page)
	if len(page.Traces) != 5 {
		t.Fatalf("page 2: want 5, got %d", len(page.Traces))
	}
}
