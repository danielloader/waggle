package store

import "time"

// Resource is a deduplicated process/service identity extracted from an OTLP
// ResourceSpans/ResourceLogs envelope.
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

type Span struct {
	TraceID            []byte
	SpanID             []byte
	ParentSpanID       []byte
	ResourceID         uint64
	ScopeID            uint64
	ServiceName        string
	Name               string
	Kind               int32
	StartTimeNS        int64
	EndTimeNS          int64
	StatusCode         int32
	StatusMessage      string
	TraceState         string
	Flags              uint32
	DroppedAttrsCount  uint32
	DroppedEventsCount uint32
	DroppedLinksCount  uint32
	AttributesJSON     string

	Events []SpanEvent
	Links  []SpanLink
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

type LogRecord struct {
	ResourceID        uint64
	ScopeID           uint64
	ServiceName       string
	TimeNS            int64
	ObservedTimeNS    int64
	SeverityNumber    int32
	SeverityText      string
	Body              string
	BodyJSON          string
	TraceID           []byte
	SpanID            []byte
	Flags             uint32
	DroppedAttrsCount uint32
	AttributesJSON    string
}

// AttrKeyDelta captures a single per-batch observation of an attribute key.
// The writer aggregates these into multi-row UPSERTs on attribute_keys.
type AttrKeyDelta struct {
	SignalType  string // 'span' | 'log' | 'resource' | 'scope' | 'event' | 'link'
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
	Resources   []Resource
	Scopes      []Scope
	Spans       []Span
	Logs        []LogRecord
	AttrKeys    []AttrKeyDelta
	AttrValues  []AttrValueDelta
	EnqueuedAt  time.Time
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
	TraceID      string `json:"trace_id"`
	RootService  string `json:"root_service"`
	RootName     string `json:"root_name"`
	StartTimeNS  int64  `json:"start_ns"`
	DurationNS   int64  `json:"duration_ns"`
	SpanCount    int64  `json:"span_count"`
	HasError     bool   `json:"has_error"`
}

type TraceDetail struct {
	TraceID   string             `json:"trace_id"`
	Spans     []SpanOut          `json:"spans"`
	Resources map[uint64]Resource `json:"resources"`
}

type SpanOut struct {
	TraceID        string            `json:"trace_id"`
	SpanID         string            `json:"span_id"`
	ParentSpanID   string            `json:"parent_span_id,omitempty"`
	ResourceID     uint64            `json:"resource_id"`
	ServiceName    string            `json:"service_name"`
	Name           string            `json:"name"`
	Kind           int32             `json:"kind"`
	StartTimeNS    int64             `json:"start_ns"`
	EndTimeNS      int64             `json:"end_ns"`
	DurationNS     int64             `json:"duration_ns"`
	StatusCode     int32             `json:"status_code"`
	StatusMessage  string            `json:"status_message,omitempty"`
	AttributesJSON string            `json:"attributes"`
	Events         []SpanEventOut    `json:"events,omitempty"`
	Links          []SpanLinkOut     `json:"links,omitempty"`
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

// QueryColumn is the output of one select/group-by expression.
type QueryColumn struct {
	Name string `json:"name"`
	Type string `json:"type"` // "string" | "int" | "float" | "bool" | "time"
}

// QueryResult is returned by Store.RunQuery.
type QueryResult struct {
	Columns   []QueryColumn `json:"columns"`
	Rows      [][]any       `json:"rows"`
	HasBucket bool          `json:"has_bucket"`
	GroupKeys []string      `json:"group_keys,omitempty"`
}

// QueryRateSpec tells the executor which output columns must be transformed
// into a per-second delta after scan. The underlying SQL emitted by the
// builder produces the raw SUM/AVG/MAX; the executor walks the rows,
// groups by the non-bucket/non-rate-column tuple, and replaces each value
// with (value - previous_value) / bucket_secs for consecutive buckets.
type QueryRateSpec struct {
	ColumnIndex int
	BucketSecs  float64
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
