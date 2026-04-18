package store

import "context"

// TraceFilter scopes the trace list query.
type TraceFilter struct {
	Service  string
	FromNS   int64
	ToNS     int64
	HasError *bool
	Limit    int
	Cursor   string
}

// LogFilter scopes the log search query.
type LogFilter struct {
	Query   string // FTS5 expression; empty means no text filter
	Service string
	FromNS  int64
	ToNS    int64
	Limit   int
	Cursor  string
}

// FieldFilter scopes the attribute key catalog query.
type FieldFilter struct {
	SignalType string // "span" | "log"
	Service    string
	Prefix     string
	Limit      int
}

// ValueFilter scopes the attribute value catalog query.
type ValueFilter struct {
	SignalType string
	Service    string
	Key        string
	Prefix     string
	Limit      int
}

// Store is the sole seam between the HTTP/ingest layers and the SQLite
// implementation. Read and write paths are exposed via the same interface;
// implementations may route them to different connection pools internally.
type Store interface {
	WriteBatch(ctx context.Context, b Batch) error

	ListServices(ctx context.Context) ([]ServiceSummary, error)
	ListTraces(ctx context.Context, f TraceFilter) ([]TraceSummary, string, error)
	GetTrace(ctx context.Context, traceID string) (TraceDetail, error)

	ListFields(ctx context.Context, f FieldFilter) ([]FieldInfo, error)
	ListFieldValues(ctx context.Context, f ValueFilter) ([]string, error)
	ListSpanNames(ctx context.Context, service, prefix string, limit int) ([]string, error)

	SearchLogs(ctx context.Context, f LogFilter) ([]LogOut, string, error)

	// RunQuery executes a pre-compiled structured query (SQL + args + column
	// schema) against the read pool. All user-provided inputs have been
	// whitelisted by the query package before we get here; sqlite parameter
	// binding handles the values. Rates, when non-empty, are applied as a
	// post-scan transform.
	RunQuery(ctx context.Context, sql string, args []any, columns []QueryColumn, hasBucket bool, groupKeys []string, rates []QueryRateSpec) (QueryResult, error)

	Retain(ctx context.Context, olderThanNS int64) error
	Clear(ctx context.Context) error

	Close() error
}
