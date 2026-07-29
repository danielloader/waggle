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
//
// Every field reference is resolved before any SQL is assembled, because
// resolution is what decides whether the FROM clause needs the resources
// join (see resolveField).
func (b *builder) buildRaw() (Compiled, error) {
	cols, colSQL := rawColumnsFor(b.q.Dataset, b.baseTable())
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

	// Honor the user's ORDER BY when supplied; otherwise default to time
	// descending (newest first, like a typical events feed).
	orderClause := " ORDER BY " + timeCol + " DESC"
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
		orderClause = " ORDER BY " + strings.Join(orderParts, ", ")
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
	sql.WriteString(orderClause)
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
//
// `base` is the base table name, used to qualify the columns that also
// exist on `resources` (service_name, attributes) so the expressions stay
// unambiguous when the resources join is present.
func rawColumnsFor(d Dataset, base string) ([]Column, []string) {
	switch d {
	case DatasetMetrics:
		// Metrics are folded wide-events: one row per unique (time, label
		// set) with metric names as attribute keys. No dedicated name /
		// kind / value columns — the UI unpacks attributes client-side
		// and shows (time, service, attributes) by default.
		cols := []Column{
			{Name: "time_ns", Type: "time"},
			{Name: "service_name", Type: "string"},
			{Name: "dataset", Type: "string"},
			{Name: "attributes", Type: "string"},
		}
		exprs := []string{
			"time_ns",
			base + ".service_name",
			"dataset",
			base + ".attributes",
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
			base + ".service_name",
			"severity_number",
			"severity_text",
			"body",
			"hex(trace_id)",
			"hex(span_id)",
			base + ".attributes",
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
			base + ".service_name",
			"name",
			"span_kind",
			"time_ns",
			"duration_ns",
			"COALESCE(status_code, 0)",
			// Keep in sync with the synthetic `error` field in realColumn().
			`(status_code = 2 OR EXISTS (SELECT 1 FROM span_events se WHERE se.trace_id = events.trace_id AND se.span_id = events.span_id AND se.name = 'exception'))`,
			base + ".attributes",
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
	// joinResource is set by resolveField the first time a field falls
	// through to attribute JSON, because such a field may be a resource
	// attribute. It gates the resources join in datasetFromClause, so a
	// query that only touches real columns emits no join at all.
	joinResource bool
	// aliasByRef maps a select entry's user-facing name — the field name for
	// a GROUP BY, the alias (explicit or defaulted) for an aggregation — to
	// the SQL alias it was emitted under. HAVING and ORDER BY resolve through
	// this rather than by re-sanitizing the reference, so a reference always
	// binds to the column the user meant even when two different field names
	// flatten to the same identifier.
	aliasByRef map[string]string
}

func newBuilder(q *Query) *builder {
	return &builder{q: q, aliasByRef: map[string]string{}}
}

func (b *builder) build() (Compiled, error) {
	// 1) Time bucket column (prepended if requested)
	if b.q.BucketMS > 0 {
		bucketNS := b.q.BucketMS * 1_000_000
		timeCol := b.datasetTimeColumn()
		expr := fmt.Sprintf("(%s / %d) * %d", timeCol, bucketNS, bucketNS)
		b.addSelect(expr, "bucket_ns", "bucket_ns", "time")
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
		alias := b.uniqueAlias(sanitizeAlias(g), g)
		// Label is the field name as the user wrote it — `db.client.port`,
		// not the SQL-safe `db_client_port` alias.
		b.addSelect(sqlExpr.SQL, alias, g, sqlExpr.Type)
		b.groups = append(b.groups, sqlExpr.SQL)
		b.groupKeys = append(b.groupKeys, alias)
	}

	// 3) Aggregations
	rates, err := b.buildAggregations()
	if err != nil {
		return Compiled{}, err
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

	// 5) HAVING and 6) ORDER BY are resolved before any SQL is assembled:
	// resolving a field is what decides whether the FROM clause needs the
	// resources join, and FROM is written first. Resolution order still
	// matches the emitted placeholder order (WHERE, then HAVING — ORDER BY
	// binds no arguments), so the args slice stays aligned.
	havingClause, err := b.buildHavingSQL()
	if err != nil {
		return Compiled{}, err
	}
	orderClause, err := b.buildOrderBySQL()
	if err != nil {
		return Compiled{}, err
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
	sql.WriteString(havingClause)
	sql.WriteString(orderClause)

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

func (b *builder) buildAggregations() ([]RateSpec, error) {
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
				return nil, err
			}
			alias := a.Alias
			if alias == "" {
				alias = defaultAggAlias(a)
			}
			colIdx := len(b.cols)
			b.addSelect(aggSQL, b.uniqueAlias(sanitizeAlias(alias), alias), aggLabel(a), "float")
			rates = append(rates, RateSpec{ColumnIndex: colIdx, BucketSecs: bucketSecs})
			continue
		}
		aggSQL, aggType, err := b.aggregation(a)
		if err != nil {
			return nil, err
		}
		alias := a.Alias
		if alias == "" {
			alias = defaultAggAlias(a)
		}
		b.addSelect(aggSQL, b.uniqueAlias(sanitizeAlias(alias), alias), aggLabel(a), aggType)
	}
	return rates, nil
}

func (b *builder) buildHavingSQL() (string, error) {
	if len(b.q.Having) == 0 {
		return "", nil
	}
	havingParts := make([]string, 0, len(b.q.Having))
	for _, f := range b.q.Having {
		clause, err := b.havingFilter(f)
		if err != nil {
			return "", err
		}
		havingParts = append(havingParts, clause)
	}
	return " HAVING " + strings.Join(havingParts, " AND "), nil
}

func (b *builder) buildOrderBySQL() (string, error) {
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
			return "", err
		}
		orderParts = append(orderParts, expr+" "+dir)
	}
	if len(orderParts) == 0 {
		return "", nil
	}
	return " ORDER BY " + strings.Join(orderParts, ", "), nil
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
	// An unknown key can be either a row attribute or a resource attribute
	// (`service.version`, `host.name`, `deployment.environment`, …), and the
	// user shouldn't have to know which: both resolve under the plain key.
	//
	// Precedence is row-first — the narrower scope shadows the envelope it
	// was emitted under. That matches the flat field list the span-detail
	// panel already renders, where a span's own `service.name` wins over the
	// resource's. COALESCE short-circuits, so rows carrying the key
	// themselves never touch the resource JSON. (A row attribute explicitly
	// set to JSON null is indistinguishable from absent here and falls
	// through to the resource value.)
	b.joinResource = true
	return resolvedField{
		SQL: fmt.Sprintf("COALESCE(json_extract(%s, %s), json_extract(%s.attributes, %s))",
			b.attributesColumn(), path, resourceAlias, path),
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
	if col, ok := b.commonRealColumn(name); ok {
		return col, true
	}
	// Metric queries run against metric_events which doesn't have the
	// span/log-specific columns. Everything else falls through to a
	// json_extract on attributes — including metric-name keys like
	// "requests.total" which the user references as first-class fields.
	if b.q.Dataset == DatasetMetrics {
		return resolvedField{}, false
	}
	if col, ok := eventsRealColumn(name); ok {
		return col, true
	}
	switch b.q.Dataset {
	case DatasetSpans:
		return spanRealColumn(name)
	case DatasetLogs:
		return logRealColumn(name)
	}
	return resolvedField{}, false
}

// commonRealColumn resolves fields present on both events and metric_events.
// service_name is qualified because `resources` carries a column of the same
// name; the rest are unique to the base table.
func (b *builder) commonRealColumn(name string) (resolvedField, bool) {
	switch name {
	case "service.name":
		return resolvedField{SQL: b.baseTable() + ".service_name", Type: "string"}, true
	case "time_ns":
		return resolvedField{SQL: "time_ns", Type: "time"}, true
	case "meta.dataset", "dataset":
		return resolvedField{SQL: "dataset", Type: "string"}, true
	}
	return resolvedField{}, false
}

// eventsRealColumn resolves fields present on the events table (spans + logs).
func eventsRealColumn(name string) (resolvedField, bool) {
	switch name {
	case "name":
		return resolvedField{SQL: "name", Type: "string"}, true
	case "trace_id":
		return resolvedField{SQL: "trace_id", Type: "string"}, true
	case "span_id":
		return resolvedField{SQL: "span_id", Type: "string"}, true
	case "parent_span_id":
		return resolvedField{SQL: "parent_span_id", Type: "string"}, true
	case "meta.signal_type", "signal_type":
		return resolvedField{SQL: "signal_type", Type: "string"}, true
	case "meta.span_kind":
		return resolvedField{SQL: "span_kind", Type: "string"}, true
	case "meta.annotation_type":
		return resolvedField{SQL: "annotation_type", Type: "string"}, true
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
	return resolvedField{}, false
}

// spanRealColumn resolves span-specific synthetic fields.
func spanRealColumn(name string) (resolvedField, bool) {
	switch name {
	case "kind":
		// Shorthand for meta.span_kind — returns the same string column.
		return resolvedField{SQL: "span_kind", Type: "string"}, true
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
	return resolvedField{}, false
}

// logRealColumn resolves log-specific synthetic fields.
func logRealColumn(name string) (resolvedField, bool) {
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
	return resolvedField{}, false
}

// signalTypeFilter emits the WHERE prefix that pins queries to the rows
// a dataset cares about. For spans/logs this is a signal_type predicate
// on the events table (hits idx_events_signal_time). For metrics the
// table itself (metric_events) is the filter, so no predicate is needed.
func (b *builder) signalTypeFilter() string {
	switch b.q.Dataset {
	case DatasetSpans:
		return "signal_type = 'span'"
	case DatasetLogs:
		return "signal_type = 'log'"
	}
	return "1=1"
}

func (b *builder) datasetTimeColumn() string { return "time_ns" }

// resourceAlias is the alias the joined `resources` row goes by. Only the
// resource-attribute fallthrough in resolveField references it.
const resourceAlias = "res"

// baseTable is the table the dataset's rows live in. Used to qualify the
// three column names `resources` also defines — resource_id, service_name
// and attributes — so those references stay unambiguous under the join.
func (b *builder) baseTable() string {
	if b.q.Dataset == DatasetMetrics {
		return "metric_events"
	}
	return "events"
}

func (b *builder) datasetFromClause() string {
	base := b.baseTable()
	if !b.joinResource {
		return base
	}
	// Resource attributes live in one deduplicated row per resource identity,
	// reachable from every event/metric row by resource_id — the table's
	// INTEGER PRIMARY KEY, so this is a rowid seek into a table with as many
	// rows as there are distinct processes reporting (tens, typically).
	//
	// LEFT, not INNER: retention deletes `resources` rows once unreferenced,
	// and an INNER join would turn any such gap into silently missing events
	// rather than a NULL resource attribute.
	return base + " LEFT JOIN resources AS " + resourceAlias +
		" ON " + resourceAlias + ".resource_id = " + base + ".resource_id"
}

func (b *builder) attributesColumn() string { return b.baseTable() + ".attributes" }

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
	if alias, ok := b.aliasRef(f.Field); ok {
		return b.clauseFor(alias, f)
	}
	return b.filter(f)
}

// aliasRef matches a HAVING / ORDER BY reference against the SELECT output
// aliases. A reference written the way the user named it ("p95.duration",
// "quotech.org.id") resolves through the recorded mapping rather than by
// re-sanitizing, so it can't bind to a different column that happens to
// flatten to the same identifier.
func (b *builder) aliasRef(name string) (string, bool) {
	if hasAlias(b.cols, name) {
		return name, true
	}
	if alias, ok := b.aliasByRef[name]; ok {
		return alias, true
	}
	return "", false
}

// uniqueAlias makes `candidate` unique among the SELECT aliases emitted so
// far and records `ref` (the user-facing name that produced it) as pointing at
// it. Flattening dots to underscores is lossy — `a.b` and `a-b` both become
// `a_b` — and duplicate output aliases make any HAVING / ORDER BY reference to
// them ambiguous, so collisions get a numeric suffix instead.
func (b *builder) uniqueAlias(candidate, ref string) string {
	alias := candidate
	for n := 2; hasAlias(b.cols, alias); n++ {
		alias = candidate + "_" + strconv.Itoa(n)
	}
	if _, taken := b.aliasByRef[ref]; !taken {
		b.aliasByRef[ref] = alias
	}
	return alias
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
	if alias, ok := b.aliasRef(name); ok {
		return alias, nil
	}
	field, err := b.resolveField(name)
	if err != nil {
		return "", err
	}
	return field.SQL, nil
}

// -----------------------------------------------------------------------------

// addSelect appends one output column. `alias` is the SQL-safe identifier the
// column is keyed by everywhere downstream (GROUP BY splitting, HAVING and
// ORDER BY references, the JSON response); `label` is the same column written
// the way the user asked for it — a dotted attribute key or an aggregation
// like "MAX(db.client.network.sent)" — and is display-only.
func (b *builder) addSelect(expr, alias, label, typ string) {
	b.selects = append(b.selects, fmt.Sprintf("%s AS %s", expr, alias))
	col := Column{Name: alias, Type: typ}
	if label != alias {
		col.Label = label
	}
	b.cols = append(b.cols, col)
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
	return string(a.Op) + "_" + sanitizeAlias(a.Field)
}

// aggLabel renders an aggregation the way the user expressed it, so a header
// can be read back into a query without translation: COUNT,
// MAX(db.client.network.sent), P95(duration_ms). An explicit alias is the
// user's own wording and is used verbatim.
func aggLabel(a Aggregation) string {
	if a.Alias != "" {
		return a.Alias
	}
	if a.Op == OpCount {
		return "COUNT"
	}
	return strings.ToUpper(string(a.Op)) + "(" + a.Field + ")"
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
