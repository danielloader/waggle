package query

import (
	"encoding/json"
	"fmt"
	"sort"
	"strings"
)

// FormatDisplay produces a single-line, Honeycomb-style one-glance summary
// of a query for the history list view — "SELECT COUNT WHERE x = y GROUP
// BY z". It's intentionally lossy: SELECTs collapse multiple aggregations
// into a comma list, filter values render via %v, HAVING/ORDER BY aren't
// surfaced. If you want the full AST, rehydrate from query_json.
func FormatDisplay(q *Query) string {
	var parts []string
	parts = append(parts, "SELECT "+formatSelect(q.Select))
	if len(q.Where) > 0 {
		parts = append(parts, "WHERE "+formatFilters(q.Where))
	}
	if len(q.GroupBy) > 0 {
		parts = append(parts, "GROUP BY "+strings.Join(q.GroupBy, ", "))
	}
	return strings.Join(parts, " ")
}

func formatSelect(sels []Aggregation) string {
	if len(sels) == 0 {
		return "*" // raw-rows mode — no aggregation
	}
	out := make([]string, 0, len(sels))
	for _, a := range sels {
		if a.Field == "" {
			out = append(out, strings.ToUpper(string(a.Op)))
			continue
		}
		out = append(out, fmt.Sprintf("%s(%s)", strings.ToUpper(string(a.Op)), a.Field))
	}
	return strings.Join(out, ", ")
}

func formatFilters(fs []Filter) string {
	out := make([]string, 0, len(fs))
	for _, f := range fs {
		out = append(out, formatFilter(f))
	}
	return strings.Join(out, " AND ")
}

func formatFilter(f Filter) string {
	switch f.Op {
	case FilterExists:
		return f.Field + " exists"
	case FilterNotExist:
		return f.Field + " !exists"
	case FilterIn, FilterNotIn:
		return fmt.Sprintf("%s %s %v", f.Field, f.Op, f.Value)
	default:
		return fmt.Sprintf("%s %s %v", f.Field, f.Op, f.Value)
	}
}

// HashQuery returns a stable content hash of the query suitable for
// query_history deduplication. Time-range bounds are excluded — two runs
// of "same query, different wall-clock window" dedupe to one history
// row. BucketMS and granularity (if present) are included because
// they're meaningful to the result shape.
func HashQuery(q *Query) ([]byte, error) {
	canonical, err := canonicalize(q)
	if err != nil {
		return nil, err
	}
	return sha256Sum(canonical), nil
}

// canonicalize emits a deterministic JSON representation suitable for
// hashing: stable field ordering, time-range stripped, filters and group
// keys sorted.
func canonicalize(q *Query) ([]byte, error) {
	type canonFilter struct {
		Field string `json:"field"`
		Op    string `json:"op"`
		Value any    `json:"value,omitempty"`
	}
	type canonAgg struct {
		Op    string `json:"op"`
		Field string `json:"field,omitempty"`
		Alias string `json:"alias,omitempty"`
	}
	type canonOrder struct {
		Field string `json:"field"`
		Dir   string `json:"dir,omitempty"`
	}
	type canonQuery struct {
		Dataset  string        `json:"dataset"`
		Select   []canonAgg    `json:"select,omitempty"`
		Where    []canonFilter `json:"where,omitempty"`
		GroupBy  []string      `json:"group_by,omitempty"`
		OrderBy  []canonOrder  `json:"order_by,omitempty"`
		Having   []canonFilter `json:"having,omitempty"`
		Limit    int           `json:"limit,omitempty"`
		BucketMS int64         `json:"bucket_ms,omitempty"`
	}
	c := canonQuery{
		Dataset:  string(q.Dataset),
		GroupBy:  append([]string(nil), q.GroupBy...),
		Limit:    q.Limit,
		BucketMS: q.BucketMS,
	}
	sort.Strings(c.GroupBy)
	for _, a := range q.Select {
		c.Select = append(c.Select, canonAgg{Op: string(a.Op), Field: a.Field, Alias: a.Alias})
	}
	// SELECT is order-sensitive (drives output columns), do NOT sort.
	for _, f := range q.Where {
		c.Where = append(c.Where, canonFilter{Field: f.Field, Op: string(f.Op), Value: f.Value})
	}
	sort.Slice(c.Where, func(i, j int) bool {
		if c.Where[i].Field != c.Where[j].Field {
			return c.Where[i].Field < c.Where[j].Field
		}
		return c.Where[i].Op < c.Where[j].Op
	})
	for _, f := range q.Having {
		c.Having = append(c.Having, canonFilter{Field: f.Field, Op: string(f.Op), Value: f.Value})
	}
	sort.Slice(c.Having, func(i, j int) bool {
		if c.Having[i].Field != c.Having[j].Field {
			return c.Having[i].Field < c.Having[j].Field
		}
		return c.Having[i].Op < c.Having[j].Op
	})
	for _, o := range q.OrderBy {
		c.OrderBy = append(c.OrderBy, canonOrder{Field: o.Field, Dir: o.Dir})
	}
	return json.Marshal(c)
}
