package mcpserver

import (
	"context"
	"fmt"
	"log/slog"
	"strconv"
	"strings"
	"time"

	"github.com/modelcontextprotocol/go-sdk/mcp"

	"github.com/danielloader/waggle/internal/query"
	"github.com/danielloader/waggle/internal/store"
)

// Result-size guardrails. MCP results are fed back into a model's context, so
// the defaults are deliberately smaller than the UI's and every list tool
// caps hard. A query that hits its limit reports Truncated so the model knows
// to narrow rather than assuming it saw everything.
const (
	defaultQueryLimit = 100
	maxQueryLimit     = 1000
	defaultTraceLimit = 25
	maxTraceLimit     = 100
	defaultLogLimit   = 50
	maxLogLimit       = 200
	defaultFieldLimit = 100
	maxFieldLimit     = 500
	defaultValueLimit = 50
	maxValueLimit     = 200
	defaultNameLimit  = 50
	maxNameLimit      = 200
	defaultHistLimit  = 50
	maxHistLimit      = 200
)

type tools struct {
	store store.Store
	log   *slog.Logger
}

// timeRange is the relative-or-absolute time window accepted by query-style
// tools. The internal/query layer is intentionally absolute-only (it keeps
// the store stateless), so this adapter resolves "last" against the wall
// clock before building a query. Precedence: last > from/to > default 1h.
type timeRange struct {
	Last string `json:"last,omitempty" jsonschema:"relative window ending now, as a Go duration e.g. 15m, 1h, 24h; takes precedence over from/to"`
	From string `json:"from,omitempty" jsonschema:"absolute start: RFC3339 timestamp or integer unix nanoseconds"`
	To   string `json:"to,omitempty" jsonschema:"absolute end: RFC3339 or unix nanoseconds; defaults to now when from is set"`
}

func registerTools(s *mcp.Server, t *tools) {
	mcp.AddTool(s, &mcp.Tool{
		Name:        "list_services",
		Description: "List services that have reported spans, with span and error counts. Use this first to discover what is being observed.",
	}, t.listServices)

	mcp.AddTool(s, &mcp.Tool{
		Name:        "list_fields",
		Description: "List queryable attribute keys (the schema) for a dataset, optionally scoped to a service or key prefix. Use before building a query to learn what fields exist. Metric names are themselves attribute keys on the metrics dataset.",
	}, t.listFields)

	mcp.AddTool(s, &mcp.Tool{
		Name:        "list_field_values",
		Description: "List observed values for one attribute key, to help build valid filters. Backed by the ingest-time value catalog (str/int/bool keys only).",
	}, t.listFieldValues)

	mcp.AddTool(s, &mcp.Tool{
		Name:        "list_span_names",
		Description: "List distinct span names, optionally scoped to a service or prefix. Useful for finding operations to query or trace.",
	}, t.listSpanNames)

	mcp.AddTool(s, &mcp.Tool{
		Name: "query",
		Description: "Run a structured Honeycomb-style query over one dataset (spans, logs, or metrics). " +
			"Aggregate with select (count, sum, avg, min, max, p50/p90/p95/p99, rate_sum/rate_avg/rate_max), " +
			"filter with where, break down with group_by, and time-series with bucket_ms. " +
			"Leave select empty for raw matching rows. Prefer aggregations over raw rows to keep results small. " +
			"Unknown fields resolve against event attributes automatically.",
	}, t.runQuery)

	mcp.AddTool(s, &mcp.Tool{
		Name:        "list_traces",
		Description: "List recent root spans (traces) matching a service / time / error filter. Returns trace ids to pass to get_trace.",
	}, t.listTraces)

	mcp.AddTool(s, &mcp.Tool{
		Name:        "get_trace",
		Description: "Fetch a full trace waterfall (all spans, events, and links) by 32-char hex trace id.",
	}, t.getTrace)

	mcp.AddTool(s, &mcp.Tool{
		Name:        "search_logs",
		Description: "Full-text search log bodies (FTS5) with optional service and time filters. The query field is an FTS5 match expression; leave empty to browse by time.",
	}, t.searchLogs)

	mcp.AddTool(s, &mcp.Tool{
		Name:        "recent_queries",
		Description: "List recently run structured queries (deduplicated, most recent first) — useful context on what a human has been investigating.",
	}, t.recentQueries)
}

// ---- list_services ----------------------------------------------------------

type servicesOutput struct {
	Services []store.ServiceSummary `json:"services"`
}

func (t *tools) listServices(ctx context.Context, _ *mcp.CallToolRequest, _ struct{}) (*mcp.CallToolResult, servicesOutput, error) {
	svcs, err := t.store.ListServices(ctx)
	if err != nil {
		return nil, servicesOutput{}, fmt.Errorf("list services: %w", err)
	}
	if svcs == nil {
		svcs = []store.ServiceSummary{}
	}
	return nil, servicesOutput{Services: svcs}, nil
}

// ---- list_fields ------------------------------------------------------------

type fieldsInput struct {
	Dataset string `json:"dataset,omitempty" jsonschema:"one of spans, logs, metrics (default spans)"`
	Service string `json:"service,omitempty" jsonschema:"restrict to one service.name"`
	Prefix  string `json:"prefix,omitempty" jsonschema:"only keys starting with this prefix"`
	Limit   int    `json:"limit,omitempty"`
}

type fieldsOutput struct {
	Fields []store.FieldInfo `json:"fields"`
}

func (t *tools) listFields(ctx context.Context, _ *mcp.CallToolRequest, in fieldsInput) (*mcp.CallToolResult, fieldsOutput, error) {
	fields, err := t.store.ListFields(ctx, store.FieldFilter{
		SignalType: signalFor(in.Dataset),
		Service:    in.Service,
		Prefix:     in.Prefix,
		Limit:      clampLimit(in.Limit, defaultFieldLimit, maxFieldLimit),
	})
	if err != nil {
		return nil, fieldsOutput{}, fmt.Errorf("list fields: %w", err)
	}
	if fields == nil {
		fields = []store.FieldInfo{}
	}
	return nil, fieldsOutput{Fields: fields}, nil
}

// ---- list_field_values ------------------------------------------------------

type fieldValuesInput struct {
	Key     string `json:"key" jsonschema:"attribute key to list values for"`
	Dataset string `json:"dataset,omitempty" jsonschema:"one of spans, logs, metrics (default spans)"`
	Service string `json:"service,omitempty"`
	Prefix  string `json:"prefix,omitempty" jsonschema:"only values starting with this prefix"`
	Limit   int    `json:"limit,omitempty"`
}

type fieldValuesOutput struct {
	Values []string `json:"values"`
}

func (t *tools) listFieldValues(ctx context.Context, _ *mcp.CallToolRequest, in fieldValuesInput) (*mcp.CallToolResult, fieldValuesOutput, error) {
	if strings.TrimSpace(in.Key) == "" {
		return nil, fieldValuesOutput{}, fmt.Errorf("key is required")
	}
	values, err := t.store.ListFieldValues(ctx, store.ValueFilter{
		SignalType: signalFor(in.Dataset),
		Service:    in.Service,
		Key:        in.Key,
		Prefix:     in.Prefix,
		Limit:      clampLimit(in.Limit, defaultValueLimit, maxValueLimit),
	})
	if err != nil {
		return nil, fieldValuesOutput{}, fmt.Errorf("list field values: %w", err)
	}
	if values == nil {
		values = []string{}
	}
	return nil, fieldValuesOutput{Values: values}, nil
}

// ---- list_span_names --------------------------------------------------------

type spanNamesInput struct {
	Service string `json:"service,omitempty"`
	Prefix  string `json:"prefix,omitempty"`
	Limit   int    `json:"limit,omitempty"`
}

type spanNamesOutput struct {
	Names []string `json:"names"`
}

func (t *tools) listSpanNames(ctx context.Context, _ *mcp.CallToolRequest, in spanNamesInput) (*mcp.CallToolResult, spanNamesOutput, error) {
	names, err := t.store.ListSpanNames(ctx, in.Service, in.Prefix, clampLimit(in.Limit, defaultNameLimit, maxNameLimit))
	if err != nil {
		return nil, spanNamesOutput{}, fmt.Errorf("list span names: %w", err)
	}
	if names == nil {
		names = []string{}
	}
	return nil, spanNamesOutput{Names: names}, nil
}

// ---- query ------------------------------------------------------------------

type queryInput struct {
	Dataset  string              `json:"dataset" jsonschema:"one of spans, logs, metrics"`
	Time     timeRange           `json:"time_range,omitempty" jsonschema:"query window; defaults to the last 1h"`
	Select   []query.Aggregation `json:"select,omitempty" jsonschema:"aggregations; empty means return raw matching rows"`
	Where    []query.Filter      `json:"where,omitempty" jsonschema:"filter predicates ANDed together"`
	GroupBy  []string            `json:"group_by,omitempty" jsonschema:"attribute keys to break down by"`
	OrderBy  []query.Order       `json:"order_by,omitempty"`
	Having   []query.Filter      `json:"having,omitempty" jsonschema:"predicates on aggregated columns"`
	Limit    int                 `json:"limit,omitempty" jsonschema:"max rows (default 100, capped at 1000)"`
	BucketMS int64               `json:"bucket_ms,omitempty" jsonschema:"time-bucket width in ms for time-series; required by rate_* ops"`
}

type queryOutput struct {
	Columns   []query.Column `json:"columns"`
	Rows      [][]any        `json:"rows"`
	RowCount  int            `json:"row_count"`
	Truncated bool           `json:"truncated"`
	Note      string         `json:"note,omitempty"`
}

func (t *tools) runQuery(ctx context.Context, _ *mcp.CallToolRequest, in queryInput) (*mcp.CallToolResult, queryOutput, error) {
	tr, err := resolveTimeRange(in.Time)
	if err != nil {
		return nil, queryOutput{}, err
	}
	limit := clampLimit(in.Limit, defaultQueryLimit, maxQueryLimit)
	q := query.Query{
		Dataset:   query.Dataset(in.Dataset),
		TimeRange: tr,
		Select:    in.Select,
		Where:     in.Where,
		GroupBy:   in.GroupBy,
		OrderBy:   in.OrderBy,
		Having:    in.Having,
		Limit:     limit,
		BucketMS:  in.BucketMS,
	}
	if err := q.Validate(); err != nil {
		return nil, queryOutput{}, fmt.Errorf("invalid query: %w", err)
	}
	compiled, err := query.Build(&q)
	if err != nil {
		return nil, queryOutput{}, fmt.Errorf("build query: %w", err)
	}
	res, err := t.store.RunQuery(ctx, compiled.SQL, compiled.Args, compiled.Columns, compiled.HasBucket, compiled.GroupKeys, compiled.Rates)
	if err != nil {
		return nil, queryOutput{}, fmt.Errorf("run query: %w", err)
	}
	out := queryOutput{Columns: res.Columns, Rows: res.Rows, RowCount: len(res.Rows)}
	if out.Columns == nil {
		out.Columns = []query.Column{}
	}
	if out.Rows == nil {
		out.Rows = [][]any{}
	}
	if len(res.Rows) >= limit {
		out.Truncated = true
		out.Note = fmt.Sprintf("results hit limit=%d; narrow time_range or add filters/group_by to see the rest", limit)
	}
	return nil, out, nil
}

// ---- list_traces ------------------------------------------------------------

type tracesInput struct {
	Service  string    `json:"service,omitempty"`
	Time     timeRange `json:"time_range,omitempty" jsonschema:"defaults to the last 1h"`
	HasError *bool     `json:"has_error,omitempty" jsonschema:"true: only error traces; false: only non-error"`
	Limit    int       `json:"limit,omitempty"`
	Cursor   string    `json:"cursor,omitempty" jsonschema:"next_cursor from a previous call to page"`
}

type tracesOutput struct {
	Traces     []store.TraceSummary `json:"traces"`
	NextCursor string               `json:"next_cursor,omitempty"`
}

func (t *tools) listTraces(ctx context.Context, _ *mcp.CallToolRequest, in tracesInput) (*mcp.CallToolResult, tracesOutput, error) {
	fromNS, toNS, err := resolveTimeNS(in.Time)
	if err != nil {
		return nil, tracesOutput{}, err
	}
	traces, cursor, err := t.store.ListTraces(ctx, store.TraceFilter{
		Service:  in.Service,
		FromNS:   fromNS,
		ToNS:     toNS,
		HasError: in.HasError,
		Limit:    clampLimit(in.Limit, defaultTraceLimit, maxTraceLimit),
		Cursor:   in.Cursor,
	})
	if err != nil {
		return nil, tracesOutput{}, fmt.Errorf("list traces: %w", err)
	}
	if traces == nil {
		traces = []store.TraceSummary{}
	}
	return nil, tracesOutput{Traces: traces, NextCursor: cursor}, nil
}

// ---- get_trace --------------------------------------------------------------

type traceInput struct {
	TraceID string `json:"trace_id" jsonschema:"32-character lowercase hex trace id"`
}

// traceOutput mirrors store.TraceDetail but keys Resources by a decimal
// string instead of uint64. Go's JSON encoder already emits uint64 map keys
// as strings, so the wire shape is identical to the /api/traces response —
// the string key just lets the SDK generate an output schema (JSON Schema
// has no integer-keyed object type).
type traceOutput struct {
	TraceID   string                    `json:"trace_id"`
	Spans     []store.SpanOut           `json:"spans"`
	Resources map[string]store.Resource `json:"resources"`
}

func (t *tools) getTrace(ctx context.Context, _ *mcp.CallToolRequest, in traceInput) (*mcp.CallToolResult, traceOutput, error) {
	id := strings.ToLower(strings.TrimSpace(in.TraceID))
	if len(id) != 32 || !isHexID(id) {
		return nil, traceOutput{}, fmt.Errorf("invalid trace_id %q: expected 32 lowercase hex characters", in.TraceID)
	}
	detail, err := t.store.GetTrace(ctx, id)
	if err != nil {
		return nil, traceOutput{}, fmt.Errorf("get trace: %w", err)
	}
	if len(detail.Spans) == 0 {
		return nil, traceOutput{}, fmt.Errorf("trace not found: %s", id)
	}
	resources := make(map[string]store.Resource, len(detail.Resources))
	for rid, r := range detail.Resources {
		resources[strconv.FormatUint(rid, 10)] = r
	}
	return nil, traceOutput{TraceID: detail.TraceID, Spans: detail.Spans, Resources: resources}, nil
}

// ---- search_logs ------------------------------------------------------------

type logsInput struct {
	Query   string    `json:"query,omitempty" jsonschema:"FTS5 match expression over log body; empty browses by time"`
	Service string    `json:"service,omitempty"`
	Time    timeRange `json:"time_range,omitempty" jsonschema:"defaults to the last 1h"`
	Limit   int       `json:"limit,omitempty"`
	Cursor  string    `json:"cursor,omitempty" jsonschema:"next_cursor from a previous call to page"`
}

type logsOutput struct {
	Logs       []store.LogOut `json:"logs"`
	NextCursor string         `json:"next_cursor,omitempty"`
}

func (t *tools) searchLogs(ctx context.Context, _ *mcp.CallToolRequest, in logsInput) (*mcp.CallToolResult, logsOutput, error) {
	fromNS, toNS, err := resolveTimeNS(in.Time)
	if err != nil {
		return nil, logsOutput{}, err
	}
	logs, cursor, err := t.store.SearchLogs(ctx, store.LogFilter{
		Query:   in.Query,
		Service: in.Service,
		FromNS:  fromNS,
		ToNS:    toNS,
		Limit:   clampLimit(in.Limit, defaultLogLimit, maxLogLimit),
		Cursor:  in.Cursor,
	})
	if err != nil {
		return nil, logsOutput{}, fmt.Errorf("search logs: %w", err)
	}
	if logs == nil {
		logs = []store.LogOut{}
	}
	return nil, logsOutput{Logs: logs, NextCursor: cursor}, nil
}

// ---- recent_queries ---------------------------------------------------------

type historyInput struct {
	Limit int `json:"limit,omitempty"`
}

type historyOutput struct {
	Entries []store.QueryHistoryEntry `json:"entries"`
}

func (t *tools) recentQueries(ctx context.Context, _ *mcp.CallToolRequest, in historyInput) (*mcp.CallToolResult, historyOutput, error) {
	entries, err := t.store.ListQueryHistory(ctx, clampLimit(in.Limit, defaultHistLimit, maxHistLimit))
	if err != nil {
		return nil, historyOutput{}, fmt.Errorf("list query history: %w", err)
	}
	if entries == nil {
		entries = []store.QueryHistoryEntry{}
	}
	return nil, historyOutput{Entries: entries}, nil
}

// ---- helpers ----------------------------------------------------------------

// signalFor maps a query-style dataset name ("spans"/"logs"/"metrics") to the
// singular signal_type the field/value catalog is keyed by. Defaults to span.
func signalFor(dataset string) string {
	switch dataset {
	case string(query.DatasetLogs), store.SignalLog:
		return store.SignalLog
	case string(query.DatasetMetrics), store.SignalMetric:
		return store.SignalMetric
	default:
		return store.SignalSpan
	}
}

func clampLimit(req, def, max int) int {
	if req <= 0 {
		return def
	}
	if req > max {
		return max
	}
	return req
}

// resolveTimeRange turns the relative-or-absolute timeRange into the absolute
// query.TimeRange the builder needs. Defaults to the last hour when empty.
func resolveTimeRange(tr timeRange) (query.TimeRange, error) {
	now := time.Now()
	var from, to time.Time

	switch {
	case tr.Last != "":
		d, err := time.ParseDuration(tr.Last)
		if err != nil {
			return query.TimeRange{}, fmt.Errorf("invalid time_range.last %q: %w", tr.Last, err)
		}
		if d <= 0 {
			return query.TimeRange{}, fmt.Errorf("time_range.last must be positive, got %q", tr.Last)
		}
		to, from = now, now.Add(-d)
	case tr.From == "" && tr.To == "":
		to, from = now, now.Add(-time.Hour)
	default:
		var err error
		if from, err = parseAbsTime(tr.From); err != nil {
			return query.TimeRange{}, fmt.Errorf("time_range.from: %w", err)
		}
		if tr.To == "" {
			to = now
		} else if to, err = parseAbsTime(tr.To); err != nil {
			return query.TimeRange{}, fmt.Errorf("time_range.to: %w", err)
		}
	}
	return query.TimeRange{From: query.JSONTime{Time: from}, To: query.JSONTime{Time: to}}, nil
}

// resolveTimeNS is resolveTimeRange flattened to the from/to nanosecond pair
// the store's TraceFilter/LogFilter expect.
func resolveTimeNS(tr timeRange) (int64, int64, error) {
	r, err := resolveTimeRange(tr)
	if err != nil {
		return 0, 0, err
	}
	return r.From.UnixNano(), r.To.UnixNano(), nil
}

// parseAbsTime accepts integer unix nanoseconds or an RFC3339(/Nano) string.
func parseAbsTime(s string) (time.Time, error) {
	s = strings.TrimSpace(s)
	if s == "" {
		return time.Time{}, fmt.Errorf("empty")
	}
	if n, err := strconv.ParseInt(s, 10, 64); err == nil {
		return time.Unix(0, n), nil
	}
	if t, err := time.Parse(time.RFC3339Nano, s); err == nil {
		return t, nil
	}
	t, err := time.Parse(time.RFC3339, s)
	if err != nil {
		return time.Time{}, fmt.Errorf("want RFC3339 or unix nanoseconds, got %q", s)
	}
	return t, nil
}

func isHexID(s string) bool {
	for i := 0; i < len(s); i++ {
		c := s[i]
		if !((c >= '0' && c <= '9') || (c >= 'a' && c <= 'f')) {
			return false
		}
	}
	return true
}
