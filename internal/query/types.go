// Package query implements the structured Honeycomb-style query builder:
// a JSON AST that callers send, translated by Build into parameterized SQL
// over the spans or logs tables.
//
// The wire shape is the same struct, so a POST /api/query handler can decode
// straight into it with encoding/json.
package query

import (
	"encoding/json"
	"fmt"
	"time"
)

// Dataset selects which table a query runs against. One query targets
// exactly one signal type — there's no UNION across datasets in the first
// pass.
type Dataset string

const (
	DatasetSpans Dataset = "spans"
	DatasetLogs  Dataset = "logs"
)

// Query is the wire-level structured query.
type Query struct {
	Dataset   Dataset       `json:"dataset"`
	TimeRange TimeRange     `json:"time_range"`
	Select    []Aggregation `json:"select"`
	Where     []Filter      `json:"where,omitempty"`
	GroupBy   []string      `json:"group_by,omitempty"`
	OrderBy   []Order       `json:"order_by,omitempty"`
	Having    []Filter      `json:"having,omitempty"`
	Limit     int           `json:"limit,omitempty"`
	// BucketMS, when > 0, groups rows into time buckets of that size and
	// emits a `bucket_ns` column as the first SELECT entry. Use this for
	// time-series charts.
	BucketMS int64 `json:"bucket_ms,omitempty"`
}

// TimeRange is absolute — relative ranges like "last 1h" must be resolved
// by the caller before the query is sent. That keeps the server
// stateless and the request reproducible.
type TimeRange struct {
	From JSONTime `json:"from"`
	To   JSONTime `json:"to"`
}

// JSONTime accepts RFC3339Nano strings or integer nanoseconds in the payload.
// The underlying storage is a time.Time; Unix() extracts nanoseconds for the
// SQL layer.
type JSONTime struct {
	time.Time
}

func (t JSONTime) UnixNano() int64 { return t.Time.UnixNano() }

func (t *JSONTime) UnmarshalJSON(b []byte) error {
	// Try integer ns first (covers "1713383000000000000" and unquoted 1713...).
	var n int64
	if err := json.Unmarshal(b, &n); err == nil {
		t.Time = time.Unix(0, n)
		return nil
	}
	var s string
	if err := json.Unmarshal(b, &s); err != nil {
		return fmt.Errorf("time: want RFC3339 string or int ns, got %s", b)
	}
	if s == "" {
		return nil
	}
	// Try RFC3339Nano, fall back to RFC3339.
	if tm, err := time.Parse(time.RFC3339Nano, s); err == nil {
		t.Time = tm
		return nil
	}
	tm, err := time.Parse(time.RFC3339, s)
	if err != nil {
		return fmt.Errorf("time: %w", err)
	}
	t.Time = tm
	return nil
}

// Aggregation is one entry in SELECT.
type Aggregation struct {
	Op    AggOp  `json:"op"`
	Field string `json:"field,omitempty"`
	Alias string `json:"alias,omitempty"`
}

// AggOp is the aggregation function. Names mirror Honeycomb's query builder.
type AggOp string

const (
	OpCount         AggOp = "count"           // COUNT(*)
	OpCountField    AggOp = "count_field"     // COUNT(field) — rows where field IS NOT NULL
	OpCountDistinct AggOp = "count_distinct"  // COUNT(DISTINCT field)
	OpSum           AggOp = "sum"
	OpAvg           AggOp = "avg"
	OpMin           AggOp = "min"
	OpMax           AggOp = "max"
	OpP001          AggOp = "p001"
	OpP01           AggOp = "p01"
	OpP05           AggOp = "p05"
	OpP10           AggOp = "p10"
	OpP25           AggOp = "p25"
	OpP50           AggOp = "p50"
	OpP75           AggOp = "p75"
	OpP90           AggOp = "p90"
	OpP95           AggOp = "p95"
	OpP99           AggOp = "p99"
	OpP999          AggOp = "p999"

	// Rate aggregations compute the per-second delta of the underlying
	// aggregation between consecutive time buckets. They require bucket_ms
	// to be set; the first bucket in each group emits NULL (no prior point
	// to diff against). The underlying aggregations are SUM/AVG/MAX
	// respectively — the rate layer is a post-processing step in the
	// executor, not a SQL expression.
	OpRateSum AggOp = "rate_sum"
	OpRateAvg AggOp = "rate_avg"
	OpRateMax AggOp = "rate_max"
)

// rateUnderlying maps a rate op to the aggregation it wraps. Builder emits
// the underlying SQL; the executor applies the per-bucket diff afterwards.
var rateUnderlying = map[AggOp]AggOp{
	OpRateSum: OpSum,
	OpRateAvg: OpAvg,
	OpRateMax: OpMax,
}

func isRateOp(op AggOp) bool {
	_, ok := rateUnderlying[op]
	return ok
}

// percentileFraction maps a percentile op to its 0..1 fraction.
var percentileFraction = map[AggOp]string{
	OpP001: "0.001",
	OpP01:  "0.01",
	OpP05:  "0.05",
	OpP10:  "0.10",
	OpP25:  "0.25",
	OpP50:  "0.50",
	OpP75:  "0.75",
	OpP90:  "0.90",
	OpP95:  "0.95",
	OpP99:  "0.99",
	OpP999: "0.999",
}

func isPercentileOp(op AggOp) bool {
	_, ok := percentileFraction[op]
	return ok
}

// Filter is one WHERE/HAVING predicate.
type Filter struct {
	Field string     `json:"field"`
	Op    FilterOp   `json:"op"`
	Value any        `json:"value,omitempty"`
}

// FilterOp is the comparison operator. For SQL codegen, the equality-style
// operators are used literally as the operator between field and "?".
type FilterOp string

const (
	FilterEq           FilterOp = "="
	FilterNe           FilterOp = "!="
	FilterGt           FilterOp = ">"
	FilterGe           FilterOp = ">="
	FilterLt           FilterOp = "<"
	FilterLe           FilterOp = "<="
	FilterIn           FilterOp = "in"
	FilterNotIn        FilterOp = "!in"
	FilterExists       FilterOp = "exists"
	FilterNotExist     FilterOp = "!exists"
	FilterContains     FilterOp = "contains"
	FilterNotContain   FilterOp = "!contains"
	FilterStartsWith   FilterOp = "starts-with"
	FilterNotStartWith FilterOp = "!starts-with"
	FilterEndsWith     FilterOp = "ends-with"
	FilterNotEndWith   FilterOp = "!ends-with"
)

// Order is one ORDER BY entry. The field may be a raw attribute name or
// one of the SELECT aliases.
type Order struct {
	Field string `json:"field"`
	Dir   string `json:"dir,omitempty"` // "asc" | "desc" (default desc)
}

// Column describes one output column of a compiled query — used by both the
// executor (for scanning) and the HTTP response.
type Column struct {
	Name string `json:"name"`
	Type string `json:"type"` // "string" | "int" | "float" | "bool" | "time"
}
