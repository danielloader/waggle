package query

import (
	"fmt"
	"regexp"
)

// keyPattern limits attribute keys to a safe subset — identifiers, dots,
// underscores, hyphens, slashes. This is the identifier we embed inside
// json_extract's path string; rejecting anything else is belt-and-braces
// defence even though the value is always parameterised at the SQL layer.
var keyPattern = regexp.MustCompile(`^[a-zA-Z0-9._\-/]+$`)

// Validate rejects obviously malformed queries before we start generating
// SQL. The builder performs its own field-name resolution against the
// per-dataset whitelist; this is the coarse first line of defense.
func (q *Query) Validate() error {
	switch q.Dataset {
	case DatasetSpans, DatasetLogs, DatasetMetrics, DatasetEvents:
	default:
		return fmt.Errorf("dataset must be one of %q / %q / %q / %q, got %q",
			DatasetSpans, DatasetLogs, DatasetMetrics, DatasetEvents, q.Dataset)
	}
	if q.TimeRange.From.IsZero() || q.TimeRange.To.IsZero() {
		return fmt.Errorf("time_range.from and time_range.to are required")
	}
	if !q.TimeRange.To.After(q.TimeRange.From.Time) {
		return fmt.Errorf("time_range.to must be after time_range.from")
	}
	// An empty select is the "raw rows" mode: return matching events rather
	// than an aggregation. It pairs with no GROUP BY, no HAVING, and no time
	// bucketing — those are all rollup-level concepts.
	if len(q.Select) == 0 {
		if len(q.GroupBy) > 0 {
			return fmt.Errorf("raw-rows mode (empty select) is incompatible with group_by")
		}
		if len(q.Having) > 0 {
			return fmt.Errorf("raw-rows mode (empty select) is incompatible with having")
		}
		if q.BucketMS > 0 {
			return fmt.Errorf("raw-rows mode (empty select) is incompatible with bucket_ms")
		}
	}
	hasRate := false
	for i, s := range q.Select {
		if err := validateAggregation(s); err != nil {
			return fmt.Errorf("select[%d]: %w", i, err)
		}
		if isRateOp(s.Op) {
			hasRate = true
		}
	}
	if hasRate && q.BucketMS == 0 {
		return fmt.Errorf("rate_* aggregations require bucket_ms > 0")
	}
	for i, f := range q.Where {
		if err := validateFilter(f); err != nil {
			return fmt.Errorf("where[%d]: %w", i, err)
		}
	}
	for i, f := range q.Having {
		if err := validateFilter(f); err != nil {
			return fmt.Errorf("having[%d]: %w", i, err)
		}
	}
	for i, g := range q.GroupBy {
		if !keyPattern.MatchString(g) {
			return fmt.Errorf("group_by[%d]: invalid field name %q", i, g)
		}
	}
	for i, o := range q.OrderBy {
		if !keyPattern.MatchString(o.Field) {
			return fmt.Errorf("order_by[%d]: invalid field name %q", i, o.Field)
		}
		if o.Dir != "" && o.Dir != "asc" && o.Dir != "desc" {
			return fmt.Errorf("order_by[%d]: dir must be asc|desc, got %q", i, o.Dir)
		}
	}
	if q.Limit < 0 || q.Limit > 10_000 {
		return fmt.Errorf("limit must be in [0, 10000]")
	}
	if q.BucketMS < 0 {
		return fmt.Errorf("bucket_ms must be >= 0")
	}
	return nil
}

func validateAggregation(a Aggregation) error {
	switch {
	case a.Op == OpCount:
		// count takes no field.
	case a.Op == OpCountField, a.Op == OpCountDistinct,
		a.Op == OpSum, a.Op == OpAvg, a.Op == OpMin, a.Op == OpMax,
		isPercentileOp(a.Op), isRateOp(a.Op):
		if a.Field == "" {
			return fmt.Errorf("op %q requires a field", a.Op)
		}
		if !keyPattern.MatchString(a.Field) {
			return fmt.Errorf("invalid field name %q", a.Field)
		}
	default:
		return fmt.Errorf("unknown op %q", a.Op)
	}
	if a.Alias != "" && !keyPattern.MatchString(a.Alias) {
		return fmt.Errorf("invalid alias %q", a.Alias)
	}
	return nil
}

func validateFilter(f Filter) error {
	if !keyPattern.MatchString(f.Field) {
		return fmt.Errorf("invalid field name %q", f.Field)
	}
	switch f.Op {
	case FilterEq, FilterNe, FilterGt, FilterGe, FilterLt, FilterLe:
		if f.Value == nil {
			return fmt.Errorf("op %q requires a value", f.Op)
		}
	case FilterIn, FilterNotIn:
		arr, ok := f.Value.([]any)
		if !ok {
			return fmt.Errorf("op %q requires an array value", f.Op)
		}
		if len(arr) == 0 {
			return fmt.Errorf("op %q requires a non-empty array", f.Op)
		}
	case FilterContains, FilterNotContain,
		FilterStartsWith, FilterNotStartWith,
		FilterEndsWith, FilterNotEndWith:
		if _, ok := f.Value.(string); !ok {
			return fmt.Errorf("op %q requires a string value", f.Op)
		}
	case FilterExists, FilterNotExist:
		// No value required.
	default:
		return fmt.Errorf("unknown op %q", f.Op)
	}
	return nil
}
