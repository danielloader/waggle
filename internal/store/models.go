package store

import (
	"time"

	"github.com/danielloader/waggle/internal/query"
)

// Resource is a deduplicated process/service identity extracted from an OTLP
// ResourceSpans/ResourceLogs/ResourceMetrics envelope.
type Resource struct {
	ID                uint64
	ServiceName       string
	ServiceNamespace  string
	ServiceVersion    string
	ServiceInstanceID string
	SDKName           string
	SDKLanguage       string
	SDKVersion        string
	AttributesJSON    string
	FirstSeenNS       int64
	LastSeenNS        int64
}

// Scope is a deduplicated instrumentation scope identity.
type Scope struct {
	ID             uint64
	Name           string
	Version        string
	AttributesJSON string
}

// SignalType values stamped into meta.signal_type.
const (
	SignalSpan   = "span"
	SignalLog    = "log"
	SignalMetric = "metric"
)

// Event is a single row in the `events` table — a span or a log record.
// Metrics have their own table (see MetricEvent). Nullable scalar fields
// carry signal-specific data; meta.signal_type in AttributesJSON tells
// the reader which.
type Event struct {
	TimeNS         int64
	EndTimeNS      *int64 // spans only
	ResourceID     uint64
	ScopeID        uint64
	ServiceName    string
	Name           string

	TraceID      []byte
	SpanID       []byte
	ParentSpanID []byte

	// Span-only scalars (nil when not a span)
	StatusCode    *int32
	StatusMessage string
	TraceState    string
	Flags         *uint32

	// Log-only scalars
	SeverityNumber *int32
	SeverityText   string
	Body           string
	ObservedTimeNS *int64

	AttributesJSON string

	// Span events + links ride alongside when this event is a span. Writer
	// inserts them into span_events / span_links after the parent Event.
	SpanEvents []SpanEvent
	SpanLinks  []SpanLink
}

// MetricEvent is one row in the `metric_events` table. The Honeycomb-style
// folding means one row = one unique (resource, scope, time_ns, label set)
// tuple per OTel export cycle, and every scalar metric observed at that
// moment lands as an attribute key in AttributesJSON (e.g. the key
// "requests.total" with value 1423). Histograms unpack to sibling keys
// like "<name>.p50", "<name>.p99", "<name>.sum", "<name>.count".
type MetricEvent struct {
	TimeNS         int64
	ResourceID     uint64
	ScopeID        uint64
	ServiceName    string
	AttributesJSON string
}

type SpanEvent struct {
	TraceID           []byte
	SpanID            []byte
	Seq               int
	TimeNS            int64
	Name              string
	AttributesJSON    string
	DroppedAttrsCount uint32
}

type SpanLink struct {
	TraceID           []byte
	SpanID            []byte
	Seq               int
	LinkedTraceID     []byte
	LinkedSpanID      []byte
	TraceState        string
	Flags             uint32
	AttributesJSON    string
	DroppedAttrsCount uint32
}

// AttrKeyDelta captures a single per-batch observation of an attribute key.
// The writer aggregates these into multi-row UPSERTs on attribute_keys.
type AttrKeyDelta struct {
	SignalType  string // 'span' | 'log' | 'metric' | 'resource' | 'scope' | 'event' | 'link'
	ServiceName string // "" for cross-service keys
	Key         string
	ValueType   string // 'str' | 'int' | 'flt' | 'bool' | 'arr' | 'kv' | 'bytes'
	Count       int64
	LastSeenNS  int64
}

// AttrValueDelta captures a single per-batch observation of an attribute value.
// Only populated for str/int/bool types.
type AttrValueDelta struct {
	SignalType  string
	ServiceName string
	Key         string
	Value       string
	Count       int64
	LastSeenNS  int64
}

// Batch is the unit of work for the writer goroutine.
type Batch struct {
	Resources    []Resource
	Scopes       []Scope
	Events       []Event
	MetricEvents []MetricEvent
	AttrKeys     []AttrKeyDelta
	AttrValues   []AttrValueDelta
	EnqueuedAt   time.Time

	// MetaOverwrites counts ingest events whose attributes collided with a
	// reserved meta.* key and got overwritten. Surfaced in logs for SDK
	// debugging.
	MetaOverwrites int64
}

// ServiceSummary is a row for the service/dataset selector.
type ServiceSummary struct {
	ServiceName string  `json:"service"`
	SpanCount   int64   `json:"span_count"`
	ErrorCount  int64   `json:"error_count"`
	ErrorRate   float64 `json:"error_rate"`
}

// TraceSummary is a row for the trace list view.
type TraceSummary struct {
	TraceID     string `json:"trace_id"`
	RootService string `json:"root_service"`
	RootName    string `json:"root_name"`
	StartTimeNS int64  `json:"start_ns"`
	DurationNS  int64  `json:"duration_ns"`
	SpanCount   int64  `json:"span_count"`
	HasError    bool   `json:"has_error"`
}

type TraceDetail struct {
	TraceID   string              `json:"trace_id"`
	Spans     []SpanOut           `json:"spans"`
	Resources map[uint64]Resource `json:"resources"`
}

type SpanOut struct {
	TraceID        string         `json:"trace_id"`
	SpanID         string         `json:"span_id"`
	ParentSpanID   string         `json:"parent_span_id,omitempty"`
	ResourceID     uint64         `json:"resource_id"`
	ServiceName    string         `json:"service_name"`
	Name           string         `json:"name"`
	Kind           string         `json:"kind"` // OTel enum name: SERVER|CLIENT|INTERNAL|PRODUCER|CONSUMER|UNSPECIFIED
	StartTimeNS    int64          `json:"start_ns"`
	EndTimeNS      int64          `json:"end_ns"`
	DurationNS     int64          `json:"duration_ns"`
	StatusCode     int32          `json:"status_code"`
	StatusMessage  string         `json:"status_message,omitempty"`
	AttributesJSON string         `json:"attributes"`
	Events         []SpanEventOut `json:"events,omitempty"`
	Links          []SpanLinkOut  `json:"links,omitempty"`
}

type SpanEventOut struct {
	TimeNS         int64  `json:"time_ns"`
	Name           string `json:"name"`
	AttributesJSON string `json:"attributes"`
}

type SpanLinkOut struct {
	LinkedTraceID  string `json:"linked_trace_id"`
	LinkedSpanID   string `json:"linked_span_id"`
	AttributesJSON string `json:"attributes,omitempty"`
}

type FieldInfo struct {
	Key       string `json:"key"`
	ValueType string `json:"type"`
	Count     int64  `json:"count"`
}

// QueryColumn / QueryRateSpec are aliases of the query-builder types so
// the executor can accept the builder's output directly — no struct-to-
// struct copying in the HTTP handler.
type (
	QueryColumn   = query.Column
	QueryRateSpec = query.RateSpec
)

// QueryResult is returned by Store.RunQuery.
type QueryResult struct {
	Columns   []QueryColumn `json:"columns"`
	Rows      [][]any       `json:"rows"`
	HasBucket bool          `json:"has_bucket"`
	GroupKeys []string      `json:"group_keys,omitempty"`
}

type LogOut struct {
	LogID          int64  `json:"log_id"`
	TimeNS         int64  `json:"time_ns"`
	ServiceName    string `json:"service"`
	SeverityText   string `json:"severity"`
	SeverityNumber int32  `json:"severity_number"`
	Body           string `json:"body"`
	TraceID        string `json:"trace_id,omitempty"`
	SpanID         string `json:"span_id,omitempty"`
	AttributesJSON string `json:"attributes"`
}
