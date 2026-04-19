package query

import (
	"fmt"
	"strconv"
	"strings"
)

// Build translates a validated Query into a parameterized SQL statement
// against the unified `events` table. Dataset selects a signal_type='…'
// preset filter prepended to the WHERE clause. Field references route to
// real columns when the key is whitelisted; otherwise they fall through
// to json_extract(attributes, '$."<key>"').
//
// An empty Select means raw-rows mode: Build emits SELECT <fixed columns>
// and lets the caller iterate events instead of aggregates.
//
// The caller must have invoked Query.Validate() first — Build assumes
// its input is well-formed.
func Build(q *Query) (Compiled, error) {
	b := newBuilder(q)
	if len(q.Select) == 0 {
		return b.buildRaw()
	}
	return b.build()
}

// buildRaw emits SELECT <fixed columns> FROM events WHERE <filters>
// ORDER BY time_ns DESC LIMIT ?. Used when the user wants the matching
// events rather than an aggregation — the UI's "results" table mode.
func (b *builder) buildRaw() (Compiled, error) {
	cols, colSQL := rawColumnsFor(b.q.Dataset)
	for _, c := range cols {
		b.cols = append(b.cols, c)
	}
	for i, expr := range colSQL {
		b.selects = append(b.selects, fmt.Sprintf("%s AS %s", expr, cols[i].Name))
	}

	timeCol := b.datasetTimeColumn()
	whereParts := []string{b.signalTypeFilter(),
		fmt.Sprintf("%s >= ? AND %s < ?", timeCol, timeCol)}
	b.args = append(b.args, b.q.TimeRange.From.UnixNano(), b.q.TimeRange.To.UnixNano())
	for _, f := range b.q.Where {
		clause, err := b.filter(f)
		if err != nil {
			return Compiled{}, err
		}
		whereParts = append(whereParts, clause)
	}

	limit := b.q.Limit
	if limit == 0 {
		limit = 500
	}

	var sql strings.Builder
	sql.WriteString("SELECT ")
	sql.WriteString(strings.Join(b.selects, ", "))
	sql.WriteString(" FROM ")
	sql.WriteString(b.datasetFromClause())
	sql.WriteString(" WHERE ")
	sql.WriteString(strings.Join(whereParts, " AND "))

	// Honor the user's ORDER BY when supplied; otherwise default to time
	// descending (newest first, like a typical events feed).
	if len(b.q.OrderBy) > 0 {
		orderParts := make([]string, 0, len(b.q.OrderBy))
		for _, o := range b.q.OrderBy {
			expr, err := b.resolveOrderField(o.Field)
			if err != nil {
				return Compiled{}, err
			}
			dir := "DESC"
			if o.Dir == "asc" {
				dir = "ASC"
			}
			orderParts = append(orderParts, expr+" "+dir)
		}
		sql.WriteString(" ORDER BY ")
		sql.WriteString(strings.Join(orderParts, ", "))
	} else {
		sql.WriteString(" ORDER BY ")
		sql.WriteString(timeCol)
		sql.WriteString(" DESC")
	}
	sql.WriteString(" LIMIT ")
	sql.WriteString(strconv.Itoa(limit))

	return Compiled{
		SQL:     sql.String(),
		Args:    b.args,
		Columns: b.cols,
	}, nil
}

// rawColumnsFor returns the UI-friendly column set for raw-rows mode on
// each dataset. The column order is significant because the frontend maps
// positional output into typed rows.
func rawColumnsFor(d Dataset) ([]Column, []string) {
	switch d {
	case DatasetMetrics:
		cols := []Column{
			{Name: "time_ns", Type: "time"},
			{Name: "service_name", Type: "string"},
			{Name: "name", Type: "string"},
			{Name: "kind", Type: "string"},
			{Name: "value", Type: "float"},
			{Name: "attributes", Type: "string"},
		}
		exprs := []string{
			"time_ns",
			"service_name",
			"name",
			"metric_kind",
			"value",
			"attributes",
		}
		return cols, exprs
	case DatasetLogs:
		cols := []Column{
			{Name: "time_ns", Type: "time"},
			{Name: "service_name", Type: "string"},
			{Name: "severity_number", Type: "int"},
			{Name: "severity_text", Type: "string"},
			{Name: "body", Type: "string"},
			{Name: "trace_id", Type: "string"},
			{Name: "span_id", Type: "string"},
			{Name: "attributes", Type: "string"},
		}
		exprs := []string{
			"time_ns",
			"service_name",
			"severity_number",
			"severity_text",
			"body",
			"hex(trace_id)",
			"hex(span_id)",
			"attributes",
		}
		return cols, exprs
	default: // DatasetSpans
		cols := []Column{
			{Name: "trace_id", Type: "string"},
			{Name: "span_id", Type: "string"},
			{Name: "parent_span_id", Type: "string"},
			{Name: "service_name", Type: "string"},
			{Name: "name", Type: "string"},
			{Name: "kind", Type: "string"},
			{Name: "start_time_ns", Type: "time"},
			{Name: "duration_ns", Type: "int"},
			{Name: "status_code", Type: "int"},
			{Name: "error", Type: "bool"},
			{Name: "attributes", Type: "string"},
		}
		exprs := []string{
			"hex(trace_id)",
			"hex(span_id)",
			"hex(parent_span_id)",
			"service_name",
			"name",
			"span_kind",
			"time_ns",
			"duration_ns",
			"COALESCE(status_code, 0)",
			// Keep in sync with the synthetic `error` field in realColumn().
			`(status_code = 2 OR EXISTS (SELECT 1 FROM span_events se WHERE se.trace_id = events.trace_id AND se.span_id = events.span_id AND se.name = 'exception'))`,
			"attributes",
		}
		return cols, exprs
	}
}

// Compiled is the output of Build — a parameterized SQL statement plus the
// column metadata for decoding the result rows.
type Compiled struct {
	SQL     string
	Args    []any
	Columns []Column
	// HasBucket reports whether the query included a time bucket; when true,
	// the first column is the bucket time and callers should group rows into
	// time-series lines keyed by the GROUP BY tuple.
	HasBucket bool
	// GroupKeys holds the aliases of GROUP BY expressions (used to split
	// rows into series).
	GroupKeys []string
	// Rates describes post-processing transforms that turn underlying SUM/
	// AVG/MAX columns into per-second deltas. Executor applies them after
	// scanning the rows from SQLite.
	Rates []RateSpec
}

// RateSpec identifies a column that should be transformed into a rate after
// execution. ColumnIndex is the 0-based index into the result's Columns/Rows.
type RateSpec struct {
	ColumnIndex int
	BucketSecs  float64
}

// -----------------------------------------------------------------------------

type builder struct {
	q       *Query
	args    []any
	cols    []Column
	selects []string // SELECT expressions
	groups  []string // GROUP BY expressions (for grouping rows + emitting GROUP BY clause)

	groupKeys []string // aliases for GROUP BY fields (bucket_ns + each GROUP BY)
	hasBucket bool
}

func newBuilder(q *Query) *builder { return &builder{q: q} }

func (b *builder) build() (Compiled, error) {
	// 1) Time bucket column (prepended if requested)
	if b.q.BucketMS > 0 {
		bucketNS := b.q.BucketMS * 1_000_000
		timeCol := b.datasetTimeColumn()
		expr := fmt.Sprintf("(%s / %d) * %d", timeCol, bucketNS, bucketNS)
		b.addSelect(expr, "bucket_ns", "time")
		b.groups = append(b.groups, expr)
		b.groupKeys = append(b.groupKeys, "bucket_ns")
		b.hasBucket = true
	}

	// 2) GROUP BY fields
	for _, g := range b.q.GroupBy {
		sqlExpr, err := b.resolveField(g)
		if err != nil {
			return Compiled{}, err
		}
		alias := sanitizeAlias(g)
		b.addSelect(sqlExpr.SQL, alias, sqlExpr.Type)
		b.groups = append(b.groups, sqlExpr.SQL)
		b.groupKeys = append(b.groupKeys, alias)
	}

	// 3) Aggregations
	var rates []RateSpec
	bucketSecs := float64(b.q.BucketMS) / 1000.0
	for _, a := range b.q.Select {
		// Rate ops share codegen with their underlying aggregation; the
		// per-second delta is applied by the executor after scan. Preserve
		// the user-visible alias so the rate column is named correctly.
		if isRateOp(a.Op) {
			underlying := Aggregation{Op: rateUnderlying[a.Op], Field: a.Field, Alias: a.Alias}
			aggSQL, _, err := b.aggregation(underlying)
			if err != nil {
				return Compiled{}, err
			}
			alias := a.Alias
			if alias == "" {
				alias = defaultAggAlias(a)
			}
			colIdx := len(b.cols)
			b.addSelect(aggSQL, alias, "float")
			rates = append(rates, RateSpec{ColumnIndex: colIdx, BucketSecs: bucketSecs})
			continue
		}

		aggSQL, aggType, err := b.aggregation(a)
		if err != nil {
			return Compiled{}, err
		}
		alias := a.Alias
		if alias == "" {
			alias = defaultAggAlias(a)
		}
		b.addSelect(aggSQL, alias, aggType)
	}

	// 4) WHERE
	// Start with the signal_type prefix (events-table discriminator) +
	// the time range; both always apply.
	whereParts := []string{b.signalTypeFilter(),
		fmt.Sprintf("%s >= ? AND %s < ?", b.datasetTimeColumn(), b.datasetTimeColumn())}
	b.args = append(b.args, b.q.TimeRange.From.UnixNano(), b.q.TimeRange.To.UnixNano())

	for _, f := range b.q.Where {
		clause, err := b.filter(f)
		if err != nil {
			return Compiled{}, err
		}
		whereParts = append(whereParts, clause)
	}

	var sql strings.Builder
	sql.WriteString("SELECT ")
	sql.WriteString(strings.Join(b.selects, ", "))
	sql.WriteString(" FROM ")
	sql.WriteString(b.datasetFromClause())
	sql.WriteString(" WHERE ")
	sql.WriteString(strings.Join(whereParts, " AND "))

	if len(b.groups) > 0 {
		sql.WriteString(" GROUP BY ")
		sql.WriteString(strings.Join(b.groups, ", "))
	}

	// 5) HAVING
	if len(b.q.Having) > 0 {
		havingParts := make([]string, 0, len(b.q.Having))
		for _, f := range b.q.Having {
			clause, err := b.havingFilter(f)
			if err != nil {
				return Compiled{}, err
			}
			havingParts = append(havingParts, clause)
		}
		sql.WriteString(" HAVING ")
		sql.WriteString(strings.Join(havingParts, " AND "))
	}

	// 6) ORDER BY
	orderParts := []string{}
	if b.hasBucket {
		// Time-bucketed queries must always order by time first so the
		// series points come back in order.
		orderParts = append(orderParts, "bucket_ns ASC")
	}
	for _, o := range b.q.OrderBy {
		dir := "DESC"
		if o.Dir == "asc" {
			dir = "ASC"
		}
		expr, err := b.resolveOrderField(o.Field)
		if err != nil {
			return Compiled{}, err
		}
		orderParts = append(orderParts, expr+" "+dir)
	}
	if len(orderParts) > 0 {
		sql.WriteString(" ORDER BY ")
		sql.WriteString(strings.Join(orderParts, ", "))
	}

	// 7) LIMIT (default 1000 if unspecified; clamped at 10k by Validate)
	limit := b.q.Limit
	if limit == 0 {
		limit = 1000
	}
	sql.WriteString(" LIMIT ")
	sql.WriteString(strconv.Itoa(limit))

	return Compiled{
		SQL:       sql.String(),
		Args:      b.args,
		Columns:   b.cols,
		HasBucket: b.hasBucket,
		GroupKeys: b.groupKeys[min(len(b.groupKeys), iff(b.hasBucket, 1, 0)):],
		Rates:     rates,
	}, nil
}

// resolvedField is the SQL expression + inferred value type for a user-facing
// field name. Real columns route directly; everything else goes through
// json_extract.
type resolvedField struct {
	SQL  string
	Type string // "string" | "int" | "float" | "bool" | "time"
}

// resolveField is the core whitelist / json_extract router used for GROUP BY
// and filter left-hand-sides. The value types we return drive downstream
// column-typing in the Result.
func (b *builder) resolveField(name string) (resolvedField, error) {
	if col, ok := b.realColumn(name); ok {
		return col, nil
	}
	// json_extract — the key goes in the JSON path, which is a string literal.
	// We've already validated name against keyPattern, so no SQL injection via
	// the path; use a literal to keep the expression deterministic.
	path := fmt.Sprintf(`'$."%s"'`, name)
	return resolvedField{
		SQL:  fmt.Sprintf("json_extract(%s, %s)", b.attributesColumn(), path),
		Type: "string",
	}, nil
}

// realColumn returns the events-table column for a user-facing field name,
// if one exists. Generated-column shortcuts are preferred over raw JSON
// access so the planner picks up the corresponding indexes.
//
// The resolver is dataset-aware only for fields whose SQL expression
// differs by signal (e.g. synthetic `error` combines status_code for spans
// with severity_number for logs). Most fields resolve identically across
// signals — name, service.name, trace_id, http.route, etc. all live in the
// same columns regardless of signal type.
func (b *builder) realColumn(name string) (resolvedField, bool) {
	// Shared across all signals.
	switch name {
	case "service.name":
		return resolvedField{SQL: "service_name", Type: "string"}, true
	case "name":
		return resolvedField{SQL: "name", Type: "string"}, true
	case "time_ns":
		return resolvedField{SQL: "time_ns", Type: "time"}, true
	case "trace_id":
		return resolvedField{SQL: "trace_id", Type: "string"}, true
	case "span_id":
		return resolvedField{SQL: "span_id", Type: "string"}, true
	case "parent_span_id":
		return resolvedField{SQL: "parent_span_id", Type: "string"}, true
	case "meta.signal_type":
		return resolvedField{SQL: "signal_type", Type: "string"}, true
	case "meta.span_kind":
		return resolvedField{SQL: "span_kind", Type: "string"}, true
	case "meta.annotation_type":
		return resolvedField{SQL: "annotation_type", Type: "string"}, true
	case "meta.metric_kind":
		return resolvedField{SQL: "metric_kind", Type: "string"}, true
	case "meta.metric_unit":
		return resolvedField{SQL: "metric_unit", Type: "string"}, true
	case "meta.metric_temporality":
		return resolvedField{SQL: "metric_temporality", Type: "string"}, true
	case "meta.metric_monotonic":
		return resolvedField{SQL: "metric_monotonic", Type: "int"}, true
	case "http.request.method":
		return resolvedField{SQL: "http_method", Type: "string"}, true
	case "http.response.status_code":
		return resolvedField{SQL: "http_status_code", Type: "int"}, true
	case "http.route":
		return resolvedField{SQL: "http_route", Type: "string"}, true
	case "rpc.service":
		return resolvedField{SQL: "rpc_service", Type: "string"}, true
	case "db.system":
		return resolvedField{SQL: "db_system", Type: "string"}, true
	}

	// Signal-specific synthetic fields.
	switch b.q.Dataset {
	case DatasetSpans:
		switch name {
		case "status_code":
			return resolvedField{SQL: "COALESCE(status_code, 0)", Type: "int"}, true
		case "duration_ns":
			return resolvedField{SQL: "duration_ns", Type: "int"}, true
		case "duration_ms":
			return resolvedField{SQL: "(duration_ns / 1000000)", Type: "int"}, true
		case "start_time_ns":
			// Alias — spans emit time_ns as their start timestamp.
			return resolvedField{SQL: "time_ns", Type: "time"}, true
		case "is_root":
			return resolvedField{SQL: "(parent_span_id IS NULL)", Type: "bool"}, true
		case "error":
			return resolvedField{SQL: `(status_code = 2 OR EXISTS (` +
				`SELECT 1 FROM span_events se ` +
				`WHERE se.trace_id = events.trace_id ` +
				`AND se.span_id = events.span_id ` +
				`AND se.name = 'exception'))`, Type: "bool"}, true
		}
	case DatasetLogs:
		switch name {
		case "severity_number":
			return resolvedField{SQL: "severity_number", Type: "int"}, true
		case "severity_text":
			return resolvedField{SQL: "severity_text", Type: "string"}, true
		case "body":
			return resolvedField{SQL: "body", Type: "string"}, true
		case "error":
			return resolvedField{SQL: "(severity_number >= 17)", Type: "bool"}, true
		}
	case DatasetMetrics:
		switch name {
		case "value":
			return resolvedField{SQL: "value", Type: "float"}, true
		// Legacy aliases for back-compat with earlier metric-only queries.
		case "kind":
			return resolvedField{SQL: "metric_kind", Type: "string"}, true
		case "unit":
			return resolvedField{SQL: "metric_unit", Type: "string"}, true
		case "temporality":
			return resolvedField{SQL: "metric_temporality", Type: "string"}, true
		}
	}
	return resolvedField{}, false
}

// signalTypeFilter emits the WHERE prefix that pins queries to their
// dataset's events rows. Hits idx_events_signal_time.
func (b *builder) signalTypeFilter() string {
	switch b.q.Dataset {
	case DatasetSpans:
		return "signal_type = 'span'"
	case DatasetLogs:
		return "signal_type = 'log'"
	case DatasetMetrics:
		return "signal_type = 'metric'"
	}
	return "1=1"
}

func (b *builder) datasetTimeColumn() string { return "time_ns" }

func (b *builder) datasetFromClause() string { return "events" }

func (b *builder) attributesColumn() string { return "attributes" }

func (b *builder) aggregation(a Aggregation) (string, string, error) {
	if isPercentileOp(a.Op) {
		field, err := b.resolveField(a.Field)
		if err != nil {
			return "", "", err
		}
		return fmt.Sprintf("percentile(%s, %s)", field.SQL, percentileFraction[a.Op]), "float", nil
	}
	switch a.Op {
	case OpCount:
		return "COUNT(*)", "int", nil
	case OpCountField:
		field, err := b.resolveField(a.Field)
		if err != nil {
			return "", "", err
		}
		return fmt.Sprintf("COUNT(%s)", field.SQL), "int", nil
	case OpCountDistinct:
		field, err := b.resolveField(a.Field)
		if err != nil {
			return "", "", err
		}
		return fmt.Sprintf("COUNT(DISTINCT %s)", field.SQL), "int", nil
	case OpSum, OpAvg, OpMin, OpMax:
		field, err := b.resolveField(a.Field)
		if err != nil {
			return "", "", err
		}
		upper := strings.ToUpper(string(a.Op))
		return fmt.Sprintf("%s(%s)", upper, field.SQL), "float", nil
	}
	return "", "", fmt.Errorf("unsupported aggregation %q", a.Op)
}

// filter emits one parameterized WHERE clause.
func (b *builder) filter(f Filter) (string, error) {
	field, err := b.resolveField(f.Field)
	if err != nil {
		return "", err
	}
	return b.clauseFor(field.SQL, f)
}

// havingFilter looks in the select-alias namespace first (that's the normal
// case for HAVING) before falling back to a fresh field resolution.
func (b *builder) havingFilter(f Filter) (string, error) {
	if hasAlias(b.cols, f.Field) {
		return b.clauseFor(f.Field, f)
	}
	return b.filter(f)
}

func (b *builder) clauseFor(lhs string, f Filter) (string, error) {
	switch f.Op {
	case FilterEq, FilterNe, FilterGt, FilterGe, FilterLt, FilterLe:
		b.args = append(b.args, f.Value)
		return fmt.Sprintf("%s %s ?", lhs, f.Op), nil
	case FilterIn, FilterNotIn:
		arr := f.Value.([]any)
		placeholders := strings.Repeat("?,", len(arr))
		placeholders = placeholders[:len(placeholders)-1]
		b.args = append(b.args, arr...)
		op := "IN"
		if f.Op == FilterNotIn {
			op = "NOT IN"
		}
		return fmt.Sprintf("%s %s (%s)", lhs, op, placeholders), nil
	case FilterExists:
		return fmt.Sprintf("%s IS NOT NULL", lhs), nil
	case FilterNotExist:
		return fmt.Sprintf("%s IS NULL", lhs), nil
	case FilterContains, FilterNotContain:
		b.args = append(b.args, "%"+f.Value.(string)+"%")
		op := "LIKE"
		if f.Op == FilterNotContain {
			op = "NOT LIKE"
		}
		return fmt.Sprintf("%s %s ?", lhs, op), nil
	case FilterStartsWith, FilterNotStartWith:
		b.args = append(b.args, f.Value.(string)+"%")
		op := "LIKE"
		if f.Op == FilterNotStartWith {
			op = "NOT LIKE"
		}
		return fmt.Sprintf("%s %s ?", lhs, op), nil
	case FilterEndsWith, FilterNotEndWith:
		b.args = append(b.args, "%"+f.Value.(string))
		op := "LIKE"
		if f.Op == FilterNotEndWith {
			op = "NOT LIKE"
		}
		return fmt.Sprintf("%s %s ?", lhs, op), nil
	}
	return "", fmt.Errorf("unsupported filter op %q", f.Op)
}

func (b *builder) resolveOrderField(name string) (string, error) {
	if hasAlias(b.cols, name) {
		return name, nil
	}
	field, err := b.resolveField(name)
	if err != nil {
		return "", err
	}
	return field.SQL, nil
}

// -----------------------------------------------------------------------------

func (b *builder) addSelect(expr, alias, typ string) {
	b.selects = append(b.selects, fmt.Sprintf("%s AS %s", expr, alias))
	b.cols = append(b.cols, Column{Name: alias, Type: typ})
}

func hasAlias(cols []Column, name string) bool {
	for _, c := range cols {
		if c.Name == name {
			return true
		}
	}
	return false
}

func defaultAggAlias(a Aggregation) string {
	if a.Op == OpCount {
		return "count"
	}
	if isRateOp(a.Op) {
		return string(a.Op) + "_" + sanitizeAlias(a.Field)
	}
	return string(a.Op) + "_" + sanitizeAlias(a.Field)
}

// sanitizeAlias turns user-provided field names into SQL-safe column aliases.
// The key has already passed the keyPattern check, so this is a straight
// map of dots/slashes/hyphens to underscores.
func sanitizeAlias(s string) string {
	var out strings.Builder
	for _, r := range s {
		switch {
		case r >= 'a' && r <= 'z', r >= 'A' && r <= 'Z', r >= '0' && r <= '9', r == '_':
			out.WriteRune(r)
		default:
			out.WriteByte('_')
		}
	}
	res := out.String()
	if res == "" || (res[0] >= '0' && res[0] <= '9') {
		res = "_" + res
	}
	return res
}

func iff(cond bool, a, b int) int {
	if cond {
		return a
	}
	return b
}
