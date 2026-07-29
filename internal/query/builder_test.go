package query

import (
	"strings"
	"testing"
	"time"
)

func tr() TimeRange {
	// Wide enough range to be irrelevant to unit tests.
	from := JSONTime{Time: time.Unix(0, 1000).UTC()}
	to := JSONTime{Time: time.Unix(0, 10_000_000_000_000).UTC()}
	return TimeRange{From: from, To: to}
}

func TestValidate_RequiresDatasetAndSelect(t *testing.T) {
	cases := []struct {
		name    string
		q       Query
		wantErr string
	}{
		{"missing dataset", Query{TimeRange: tr(), Select: []Aggregation{{Op: OpCount}}}, "dataset"},
		{"invalid dataset", Query{Dataset: "gibberish", TimeRange: tr(), Select: []Aggregation{{Op: OpCount}}}, "dataset"},
		{"missing time", Query{Dataset: DatasetSpans, Select: []Aggregation{{Op: OpCount}}}, "time_range"},
		// "no selects" is now valid — it triggers raw-rows mode. Removed.
		{"unknown op", Query{Dataset: DatasetSpans, TimeRange: tr(), Select: []Aggregation{{Op: "weird"}}}, "op"},
		{"p95 missing field", Query{Dataset: DatasetSpans, TimeRange: tr(), Select: []Aggregation{{Op: OpP95}}}, "field"},
		{"group_by bad identifier", Query{Dataset: DatasetSpans, TimeRange: tr(), Select: []Aggregation{{Op: OpCount}}, GroupBy: []string{"bad key"}}, "invalid field name"},
		{"order dir bogus", Query{Dataset: DatasetSpans, TimeRange: tr(), Select: []Aggregation{{Op: OpCount}}, OrderBy: []Order{{Field: "count", Dir: "up"}}}, "asc|desc"},
		{"limit too big", Query{Dataset: DatasetSpans, TimeRange: tr(), Select: []Aggregation{{Op: OpCount}}, Limit: 99999}, "limit"},
		{"filter in empty", Query{Dataset: DatasetSpans, TimeRange: tr(), Select: []Aggregation{{Op: OpCount}}, Where: []Filter{{Field: "service.name", Op: FilterIn, Value: []any{}}}}, "non-empty array"},
		{"filter contains non-string", Query{Dataset: DatasetSpans, TimeRange: tr(), Select: []Aggregation{{Op: OpCount}}, Where: []Filter{{Field: "service.name", Op: FilterContains, Value: 5}}}, "string"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			err := c.q.Validate()
			if err == nil {
				t.Fatalf("expected error containing %q, got nil", c.wantErr)
			}
			if !strings.Contains(err.Error(), c.wantErr) {
				t.Errorf("error %q does not contain %q", err.Error(), c.wantErr)
			}
		})
	}
}

func TestBuild_CountAll(t *testing.T) {
	q := Query{
		Dataset:   DatasetSpans,
		TimeRange: tr(),
		Select:    []Aggregation{{Op: OpCount}},
	}
	if err := q.Validate(); err != nil {
		t.Fatalf("validate: %v", err)
	}
	c, err := Build(&q)
	if err != nil {
		t.Fatalf("build: %v", err)
	}
	if !strings.Contains(c.SQL, "COUNT(*) AS count") {
		t.Errorf("SQL missing COUNT(*): %s", c.SQL)
	}
	if !strings.Contains(c.SQL, "FROM events") {
		t.Errorf("SQL missing FROM events: %s", c.SQL)
	}
	if !strings.Contains(c.SQL, "signal_type = 'span'") {
		t.Errorf("SQL missing signal_type prefix: %s", c.SQL)
	}
	if !strings.Contains(c.SQL, "time_ns >= ?") {
		t.Errorf("SQL missing time filter: %s", c.SQL)
	}
	if len(c.Args) != 2 {
		t.Errorf("want 2 args (from,to), got %d", len(c.Args))
	}
}

func TestBuild_GeneratedColumnForHotField(t *testing.T) {
	// http.route should route to the generated column `http_route`, not a
	// json_extract expression, so the planner picks up idx_spans_http_route.
	q := Query{
		Dataset:   DatasetSpans,
		TimeRange: tr(),
		Select:    []Aggregation{{Op: OpCount}},
		GroupBy:   []string{"http.route"},
	}
	_ = q.Validate()
	c, err := Build(&q)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(c.SQL, "http_route AS http_route") {
		t.Errorf("expected http_route generated column; got %s", c.SQL)
	}
	if strings.Contains(c.SQL, "json_extract(") {
		t.Errorf("unexpected json_extract fallthrough: %s", c.SQL)
	}
	// A query that resolves entirely to real columns pays for no join.
	if strings.Contains(c.SQL, "LEFT JOIN resources") {
		t.Errorf("unexpected resources join for a column-backed field: %s", c.SQL)
	}
}

func TestBuild_UnknownAttributeFallsToJSONExtract(t *testing.T) {
	q := Query{
		Dataset:   DatasetSpans,
		TimeRange: tr(),
		Select:    []Aggregation{{Op: OpCount}},
		GroupBy:   []string{"customer.tier"},
	}
	_ = q.Validate()
	c, err := Build(&q)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(c.SQL, `json_extract(events.attributes, '$."customer.tier"')`) {
		t.Errorf("expected json_extract for customer.tier; got %s", c.SQL)
	}
}

func TestBuild_FilterAndPercentile(t *testing.T) {
	q := Query{
		Dataset:   DatasetSpans,
		TimeRange: tr(),
		Select: []Aggregation{
			{Op: OpCount, Alias: "n"},
			{Op: OpP95, Field: "duration_ns"},
		},
		Where: []Filter{
			{Field: "service.name", Op: FilterEq, Value: "api-gateway"},
			{Field: "http.response.status_code", Op: FilterGe, Value: 500},
		},
		GroupBy: []string{"http.route"},
		OrderBy: []Order{{Field: "p95_duration_ns", Dir: "desc"}},
		Having:  []Filter{{Field: "n", Op: FilterGt, Value: 10}},
		Limit:   50,
	}
	if err := q.Validate(); err != nil {
		t.Fatalf("validate: %v", err)
	}
	c, err := Build(&q)
	if err != nil {
		t.Fatal(err)
	}
	want := []string{
		"percentile(duration_ns, 0.95) AS p95_duration_ns",
		"COUNT(*) AS n",
		"http_route AS http_route",
		"events.service_name = ?",
		"http_status_code >= ?",
		"GROUP BY",
		"HAVING n > ?",
		"ORDER BY p95_duration_ns DESC",
		"LIMIT 50",
	}
	for _, s := range want {
		if !strings.Contains(c.SQL, s) {
			t.Errorf("SQL missing %q; got:\n%s", s, c.SQL)
		}
	}
	// Args: time from, time to, service=api-gateway, status_code=500, having n>10 = 5 entries.
	if len(c.Args) != 5 {
		t.Errorf("args: want 5, got %d (%v)", len(c.Args), c.Args)
	}
}

func TestBuild_TimeBucketing(t *testing.T) {
	q := Query{
		Dataset:   DatasetSpans,
		TimeRange: tr(),
		Select:    []Aggregation{{Op: OpCount}},
		BucketMS:  60000, // 1 minute
	}
	_ = q.Validate()
	c, err := Build(&q)
	if err != nil {
		t.Fatal(err)
	}
	if !c.HasBucket {
		t.Error("HasBucket false")
	}
	if !strings.Contains(c.SQL, "60000000000) * 60000000000 AS bucket_ns") {
		t.Errorf("bucket expression missing: %s", c.SQL)
	}
	if !strings.Contains(c.SQL, "ORDER BY bucket_ns ASC") {
		t.Errorf("bucket ORDER BY missing: %s", c.SQL)
	}
}

func TestBuild_LogsDatasetUsesTimeNS(t *testing.T) {
	q := Query{
		Dataset:   DatasetLogs,
		TimeRange: tr(),
		Select:    []Aggregation{{Op: OpCount}},
	}
	_ = q.Validate()
	c, err := Build(&q)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(c.SQL, "FROM events") {
		t.Errorf("FROM events missing: %s", c.SQL)
	}
	if !strings.Contains(c.SQL, "signal_type = 'log'") {
		t.Errorf("logs signal prefix missing: %s", c.SQL)
	}
	if !strings.Contains(c.SQL, "time_ns >= ?") {
		t.Errorf("time filter should use time_ns for logs: %s", c.SQL)
	}
}

func TestBuild_FilterInWithArray(t *testing.T) {
	q := Query{
		Dataset:   DatasetSpans,
		TimeRange: tr(),
		Select:    []Aggregation{{Op: OpCount}},
		Where: []Filter{
			{Field: "service.name", Op: FilterIn, Value: []any{"a", "b", "c"}},
		},
	}
	if err := q.Validate(); err != nil {
		t.Fatal(err)
	}
	c, err := Build(&q)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(c.SQL, "events.service_name IN (?,?,?)") {
		t.Errorf("IN expansion missing: %s", c.SQL)
	}
	// time(2) + in(3) = 5 args.
	if len(c.Args) != 5 {
		t.Errorf("args: want 5, got %d", len(c.Args))
	}
}

func TestBuild_ExistsFilter(t *testing.T) {
	q := Query{
		Dataset:   DatasetSpans,
		TimeRange: tr(),
		Select:    []Aggregation{{Op: OpCount}},
		Where: []Filter{
			{Field: "db.system", Op: FilterExists},
			{Field: "customer.tier", Op: FilterNotExist},
		},
	}
	if err := q.Validate(); err != nil {
		t.Fatal(err)
	}
	c, err := Build(&q)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(c.SQL, "db_system IS NOT NULL") {
		t.Errorf("exists missing: %s", c.SQL)
	}
	if !strings.Contains(c.SQL,
		`COALESCE(json_extract(events.attributes, '$."customer.tier"'), `+
			`json_extract(res.attributes, '$."customer.tier"')) IS NULL`) {
		t.Errorf("!exists missing: %s", c.SQL)
	}
}

func TestBuild_ExtraPercentilesAndCountField(t *testing.T) {
	q := Query{
		Dataset:   DatasetSpans,
		TimeRange: tr(),
		Select: []Aggregation{
			{Op: OpCountField, Field: "http.route"},
			{Op: OpP10, Field: "duration_ns"},
			{Op: OpP25, Field: "duration_ns"},
			{Op: OpP75, Field: "duration_ns"},
			{Op: OpP90, Field: "duration_ns"},
			{Op: OpP999, Field: "duration_ns"},
		},
	}
	if err := q.Validate(); err != nil {
		t.Fatalf("validate: %v", err)
	}
	c, err := Build(&q)
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{
		"COUNT(http_route) AS",
		"percentile(duration_ns, 0.10)",
		"percentile(duration_ns, 0.25)",
		"percentile(duration_ns, 0.75)",
		"percentile(duration_ns, 0.90)",
		"percentile(duration_ns, 0.999)",
	} {
		if !strings.Contains(c.SQL, want) {
			t.Errorf("SQL missing %q; got:\n%s", want, c.SQL)
		}
	}
}

func TestBuild_NegativeStringFilters(t *testing.T) {
	q := Query{
		Dataset:   DatasetSpans,
		TimeRange: tr(),
		Select:    []Aggregation{{Op: OpCount}},
		Where: []Filter{
			{Field: "http.route", Op: FilterStartsWith, Value: "/api"},
			{Field: "http.route", Op: FilterNotStartWith, Value: "/internal"},
			{Field: "http.route", Op: FilterEndsWith, Value: ".json"},
			{Field: "http.route", Op: FilterNotEndWith, Value: ".html"},
			{Field: "body", Op: FilterNotContain, Value: "noise"},
			{Field: "service.name", Op: FilterNotIn, Value: []any{"a", "b"}},
		},
	}
	if err := q.Validate(); err != nil {
		t.Fatalf("validate: %v", err)
	}
	c, err := Build(&q)
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{
		"http_route LIKE ?",     // starts-with
		"http_route NOT LIKE ?", // !starts-with or !ends-with
		"NOT LIKE ?",            // !contains
		"events.service_name NOT IN (?,?)",
	} {
		if !strings.Contains(c.SQL, want) {
			t.Errorf("SQL missing %q; got:\n%s", want, c.SQL)
		}
	}
	// Arg wrapping sanity checks: starts-with wraps suffix only, ends-with wraps prefix only.
	found := map[string]bool{}
	for _, a := range c.Args {
		if s, ok := a.(string); ok {
			found[s] = true
		}
	}
	if !found["/api%"] || !found["/internal%"] {
		t.Errorf("starts-with args missing trailing %%: %+v", c.Args)
	}
	if !found["%.json"] || !found["%.html"] {
		t.Errorf("ends-with args missing leading %%: %+v", c.Args)
	}
	if !found["%noise%"] {
		t.Errorf("contains wrapping missing: %+v", c.Args)
	}
}

func TestBuild_RawModeEmitsRowSelect(t *testing.T) {
	q := Query{
		Dataset:   DatasetSpans,
		TimeRange: tr(),
		Where: []Filter{
			{Field: "name", Op: FilterEq, Value: "POST /orders"},
		},
	}
	if err := q.Validate(); err != nil {
		t.Fatalf("validate: %v", err)
	}
	c, err := Build(&q)
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{
		"hex(trace_id) AS trace_id",
		"hex(span_id) AS span_id",
		"name = ?",
		"ORDER BY time_ns DESC",
		"FROM events",
		"signal_type = 'span'",
	} {
		if !strings.Contains(c.SQL, want) {
			t.Errorf("raw-mode SQL missing %q; got:\n%s", want, c.SQL)
		}
	}
	if !strings.Contains(c.SQL, "LIMIT 500") {
		t.Errorf("default raw-mode limit should be 500; got:\n%s", c.SQL)
	}
	// 2 time args + 1 filter arg.
	if len(c.Args) != 3 {
		t.Errorf("args: want 3, got %d (%v)", len(c.Args), c.Args)
	}
}

func TestBuild_RawModeIsRootAndCustomOrder(t *testing.T) {
	// The shape the frontend's Traces tab compiles: "root spans only,
	// ordered by duration desc, top 10". is_root is a synthetic meta
	// field that resolves to (parent_span_id IS NULL).
	q := Query{
		Dataset:   DatasetSpans,
		TimeRange: tr(),
		Where: []Filter{
			{Field: "is_root", Op: FilterEq, Value: true},
		},
		OrderBy: []Order{{Field: "duration_ns", Dir: "desc"}},
		Limit:   10,
	}
	if err := q.Validate(); err != nil {
		t.Fatalf("validate: %v", err)
	}
	c, err := Build(&q)
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{
		"(parent_span_id IS NULL) = ?",
		"ORDER BY duration_ns DESC",
		"LIMIT 10",
	} {
		if !strings.Contains(c.SQL, want) {
			t.Errorf("SQL missing %q; got:\n%s", want, c.SQL)
		}
	}
	// Arg order: time-from, time-to, is_root value.
	if len(c.Args) != 3 {
		t.Fatalf("args: want 3, got %d (%v)", len(c.Args), c.Args)
	}
	if v, ok := c.Args[2].(bool); !ok || v != true {
		t.Errorf("is_root arg: want true, got %v (%T)", c.Args[2], c.Args[2])
	}
}

func TestBuild_IsRootInAggregationGroupBy(t *testing.T) {
	// Using is_root as a GROUP BY key: splits counts into root vs child spans.
	q := Query{
		Dataset:   DatasetSpans,
		TimeRange: tr(),
		Select:    []Aggregation{{Op: OpCount}},
		GroupBy:   []string{"is_root"},
	}
	if err := q.Validate(); err != nil {
		t.Fatalf("validate: %v", err)
	}
	c, err := Build(&q)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(c.SQL, "(parent_span_id IS NULL) AS is_root") {
		t.Errorf("SQL missing is_root group alias; got:\n%s", c.SQL)
	}
}

func TestBuild_ErrorSyntheticFieldOnSpans(t *testing.T) {
	q := Query{
		Dataset:   DatasetSpans,
		TimeRange: tr(),
		Select:    []Aggregation{{Op: OpCount}},
		Where:     []Filter{{Field: "error", Op: FilterEq, Value: true}},
		GroupBy:   []string{"error"},
	}
	if err := q.Validate(); err != nil {
		t.Fatalf("validate: %v", err)
	}
	c, err := Build(&q)
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{
		"status_code = 2",
		"EXISTS (SELECT 1 FROM span_events se",
		"se.name = 'exception'",
		// Comparison form generated by clauseFor with the synthetic expr.
		"OR EXISTS (",
	} {
		if !strings.Contains(c.SQL, want) {
			t.Errorf("SQL missing %q; got:\n%s", want, c.SQL)
		}
	}
}

func TestBuild_ErrorSyntheticFieldOnLogs(t *testing.T) {
	q := Query{
		Dataset:   DatasetLogs,
		TimeRange: tr(),
		Select:    []Aggregation{{Op: OpCount}},
		Where:     []Filter{{Field: "error", Op: FilterEq, Value: true}},
	}
	if err := q.Validate(); err != nil {
		t.Fatalf("validate: %v", err)
	}
	c, err := Build(&q)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(c.SQL, "(severity_number >= 17) = ?") {
		t.Errorf("expected severity-based error expression; got:\n%s", c.SQL)
	}
}

func TestBuild_RawModeIncludesErrorColumn(t *testing.T) {
	q := Query{
		Dataset:   DatasetSpans,
		TimeRange: tr(),
	}
	if err := q.Validate(); err != nil {
		t.Fatal(err)
	}
	c, err := Build(&q)
	if err != nil {
		t.Fatal(err)
	}
	found := false
	for _, col := range c.Columns {
		if col.Name == "error" {
			found = true
			if col.Type != "bool" {
				t.Errorf("error column type: want bool, got %q", col.Type)
			}
		}
	}
	if !found {
		t.Errorf("raw-mode columns missing `error`: %+v", c.Columns)
	}
	if !strings.Contains(c.SQL, "status_code = 2 OR EXISTS") {
		t.Errorf("raw SQL missing error expression; got:\n%s", c.SQL)
	}
}

func TestBuild_RawModeRejectsGroupByAndHaving(t *testing.T) {
	cases := []struct {
		name string
		q    Query
		want string
	}{
		{"group_by", Query{Dataset: DatasetSpans, TimeRange: tr(), GroupBy: []string{"service.name"}}, "group_by"},
		{"having", Query{Dataset: DatasetSpans, TimeRange: tr(), Having: []Filter{{Field: "count", Op: FilterGt, Value: 0}}}, "having"},
		{"bucket", Query{Dataset: DatasetSpans, TimeRange: tr(), BucketMS: 1000}, "bucket_ms"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			err := c.q.Validate()
			if err == nil || !strings.Contains(err.Error(), c.want) {
				t.Errorf("expected error containing %q, got %v", c.want, err)
			}
		})
	}
}

func TestBuild_RateRequiresBucket(t *testing.T) {
	q := Query{
		Dataset:   DatasetSpans,
		TimeRange: tr(),
		Select:    []Aggregation{{Op: OpRateSum, Field: "duration_ns"}},
	}
	err := q.Validate()
	if err == nil || !strings.Contains(err.Error(), "bucket_ms") {
		t.Fatalf("want bucket_ms error, got %v", err)
	}
}

func TestBuild_RateEmitsUnderlyingAndTracksRateSpec(t *testing.T) {
	q := Query{
		Dataset:   DatasetSpans,
		TimeRange: tr(),
		Select: []Aggregation{
			{Op: OpCount},
			{Op: OpRateSum, Field: "duration_ns"},
			{Op: OpRateAvg, Field: "duration_ns"},
		},
		BucketMS: 5_000, // 5 seconds per bucket
	}
	if err := q.Validate(); err != nil {
		t.Fatalf("validate: %v", err)
	}
	c, err := Build(&q)
	if err != nil {
		t.Fatal(err)
	}
	// Rates are emitted as their underlying aggregates; diffing is deferred
	// to the executor. So the SQL should contain SUM(...) and AVG(...) but
	// NOT any "rate_" SQL token.
	if !strings.Contains(c.SQL, "SUM(duration_ns) AS rate_sum_duration_ns") {
		t.Errorf("want SUM aliased as rate_sum_...; got %s", c.SQL)
	}
	if !strings.Contains(c.SQL, "AVG(duration_ns) AS rate_avg_duration_ns") {
		t.Errorf("want AVG aliased as rate_avg_...; got %s", c.SQL)
	}
	if len(c.Rates) != 2 {
		t.Fatalf("want 2 rate specs, got %d", len(c.Rates))
	}
	for _, rs := range c.Rates {
		if rs.BucketSecs != 5.0 {
			t.Errorf("bucket secs: want 5.0, got %v", rs.BucketSecs)
		}
		if rs.ColumnIndex < 1 || rs.ColumnIndex >= len(c.Columns) {
			t.Errorf("column index out of range: %d", rs.ColumnIndex)
		}
	}
}

func TestBuild_ContainsFilter(t *testing.T) {
	q := Query{
		Dataset:   DatasetLogs,
		TimeRange: tr(),
		Select:    []Aggregation{{Op: OpCount}},
		Where:     []Filter{{Field: "body", Op: FilterContains, Value: "refused"}},
	}
	if err := q.Validate(); err != nil {
		t.Fatal(err)
	}
	c, err := Build(&q)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(c.SQL, "body LIKE ?") {
		t.Errorf("LIKE missing: %s", c.SQL)
	}
	// Time(2) + contains(1) = 3 args; the contains arg is wrapped with %s.
	if got := c.Args[len(c.Args)-1]; got != "%refused%" {
		t.Errorf("contains arg wrapping: want %%refused%%, got %v", got)
	}
}

// duration_ms is a synthetic field derived from duration_ns / 1_000_000. It
// should compile as an expression (not a raw column), and the percentile
// aggregation's output alias should use the user-supplied field name so the
// column shows up as p99_duration_ms rather than p99_duration_ns.
func TestBuild_DurationMSField(t *testing.T) {
	q := Query{
		Dataset:   DatasetSpans,
		TimeRange: tr(),
		Select: []Aggregation{
			{Op: OpP99, Field: "duration_ms"},
		},
		Where:   []Filter{{Field: "duration_ms", Op: FilterGt, Value: 100}},
		GroupBy: []string{"service.name"},
		OrderBy: []Order{{Field: "p99_duration_ms", Dir: "desc"}},
	}
	if err := q.Validate(); err != nil {
		t.Fatalf("validate: %v", err)
	}
	c, err := Build(&q)
	if err != nil {
		t.Fatal(err)
	}
	want := []string{
		"percentile((duration_ns / 1000000), 0.99) AS p99_duration_ms",
		"(duration_ns / 1000000) > ?",
		"ORDER BY p99_duration_ms DESC",
	}
	for _, s := range want {
		if !strings.Contains(c.SQL, s) {
			t.Errorf("SQL missing %q; got:\n%s", s, c.SQL)
		}
	}
}

// ---------------------------------------------------------------------------
// Resource attributes
// ---------------------------------------------------------------------------

// An unknown key resolves against the row's attributes *and* the resource's,
// row-first, on every dataset. The join is what makes the resource side
// reachable, so assert both halves together.
func TestBuild_ResourceAttributeFallthroughPerDataset(t *testing.T) {
	cases := []struct {
		dataset Dataset
		base    string
	}{
		{DatasetSpans, "events"},
		{DatasetLogs, "events"},
		{DatasetMetrics, "metric_events"},
	}
	for _, c := range cases {
		t.Run(string(c.dataset), func(t *testing.T) {
			q := Query{
				Dataset:   c.dataset,
				TimeRange: tr(),
				Select:    []Aggregation{{Op: OpCount}},
				Where:     []Filter{{Field: "quotech.runtime", Op: FilterEq, Value: "server"}},
				GroupBy:   []string{"quotech.runtime"},
			}
			if err := q.Validate(); err != nil {
				t.Fatalf("validate: %v", err)
			}
			compiled, err := Build(&q)
			if err != nil {
				t.Fatal(err)
			}
			want := []string{
				// Row attributes first, resource attributes as the fallback.
				`COALESCE(json_extract(` + c.base + `.attributes, '$."quotech.runtime"'), ` +
					`json_extract(res.attributes, '$."quotech.runtime"'))`,
				"LEFT JOIN resources AS res ON res.resource_id = " + c.base + ".resource_id",
			}
			for _, s := range want {
				if !strings.Contains(compiled.SQL, s) {
					t.Errorf("SQL missing %q; got:\n%s", s, compiled.SQL)
				}
			}
			// The group-by column keeps the dotted key as its display label
			// while the SQL alias stays an identifier.
			var found bool
			for _, col := range compiled.Columns {
				if col.Name == "quotech_runtime" {
					found = true
					if col.Label != "quotech.runtime" {
						t.Errorf("group-by label: want %q, got %q", "quotech.runtime", col.Label)
					}
				}
			}
			if !found {
				t.Errorf("missing quotech_runtime column: %+v", compiled.Columns)
			}
		})
	}
}

// The join is emitted from the FROM clause, which is assembled before HAVING
// and ORDER BY are written — so a resource attribute referenced *only* there
// has to still bring the join with it.
func TestBuild_ResourceJoinFromHavingAndOrderBy(t *testing.T) {
	for _, c := range []struct {
		name string
		q    Query
	}{
		{"having", Query{
			Dataset:   DatasetSpans,
			TimeRange: tr(),
			Select:    []Aggregation{{Op: OpCount}},
			GroupBy:   []string{"name"},
			Having:    []Filter{{Field: "host.name", Op: FilterEq, Value: "box-1"}},
		}},
		{"order_by", Query{
			Dataset:   DatasetSpans,
			TimeRange: tr(),
			Select:    []Aggregation{{Op: OpCount}},
			OrderBy:   []Order{{Field: "host.name", Dir: "asc"}},
		}},
		{"aggregation", Query{
			Dataset:   DatasetMetrics,
			TimeRange: tr(),
			Select:    []Aggregation{{Op: OpCountDistinct, Field: "host.name"}},
		}},
		{"raw mode filter", Query{
			Dataset:   DatasetSpans,
			TimeRange: tr(),
			Where:     []Filter{{Field: "host.name", Op: FilterExists}},
		}},
	} {
		t.Run(c.name, func(t *testing.T) {
			if err := c.q.Validate(); err != nil {
				t.Fatalf("validate: %v", err)
			}
			compiled, err := Build(&c.q)
			if err != nil {
				t.Fatal(err)
			}
			if !strings.Contains(compiled.SQL, "LEFT JOIN resources AS res") {
				t.Errorf("resources join missing; got:\n%s", compiled.SQL)
			}
			if !strings.Contains(compiled.SQL, "json_extract(res.attributes") {
				t.Errorf("resource extraction missing; got:\n%s", compiled.SQL)
			}
		})
	}
}

// Raw-rows mode selects `service_name` and `attributes`, both of which
// `resources` also defines — they must be qualified or SQLite rejects the
// statement as ambiguous once the join is in play.
func TestBuild_RawModeQualifiesAmbiguousColumns(t *testing.T) {
	for _, d := range []Dataset{DatasetSpans, DatasetLogs, DatasetMetrics} {
		t.Run(string(d), func(t *testing.T) {
			q := Query{
				Dataset:   d,
				TimeRange: tr(),
				Where:     []Filter{{Field: "deployment.environment", Op: FilterEq, Value: "prod"}},
			}
			if err := q.Validate(); err != nil {
				t.Fatalf("validate: %v", err)
			}
			compiled, err := Build(&q)
			if err != nil {
				t.Fatal(err)
			}
			base := "events"
			if d == DatasetMetrics {
				base = "metric_events"
			}
			for _, s := range []string{
				base + ".service_name AS service_name",
				base + ".attributes AS attributes",
			} {
				if !strings.Contains(compiled.SQL, s) {
					t.Errorf("SQL missing %q; got:\n%s", s, compiled.SQL)
				}
			}
		})
	}
}

// ---------------------------------------------------------------------------
// Column labels
// ---------------------------------------------------------------------------

// Dotted attribute keys have to survive into the response so a header reads
// back as the thing the user typed. The SQL alias is necessarily flattened;
// Label carries the original. Built-in snake_case columns get no label.
func TestBuild_ColumnLabelsPreserveDottedKeys(t *testing.T) {
	q := Query{
		Dataset:   DatasetMetrics,
		TimeRange: tr(),
		Select: []Aggregation{
			{Op: OpCount},
			{Op: OpMax, Field: "db.client.network.sent"},
			{Op: OpP95, Field: "db.client.operation.duration"},
			{Op: OpSum, Field: "requests.total", Alias: "total.requests"},
		},
		GroupBy: []string{"quotech.org.id", "service.name"},
	}
	if err := q.Validate(); err != nil {
		t.Fatalf("validate: %v", err)
	}
	c, err := Build(&q)
	if err != nil {
		t.Fatal(err)
	}
	want := map[string]string{
		"quotech_org_id":                   "quotech.org.id",
		"service_name":                     "service.name",
		"count":                            "COUNT",
		"max_db_client_network_sent":       "MAX(db.client.network.sent)",
		"p95_db_client_operation_duration": "P95(db.client.operation.duration)",
		// An explicit alias is the user's own wording — used verbatim.
		"total_requests": "total.requests",
	}
	got := map[string]string{}
	for _, col := range c.Columns {
		got[col.Name] = col.Label
	}
	for name, label := range want {
		if got[name] != label {
			t.Errorf("column %q label: want %q, got %q (all: %+v)", name, label, got[name], c.Columns)
		}
	}
	// A user-supplied alias containing dots is not a valid bare SQL
	// identifier; it has to be flattened in the statement itself.
	if !strings.Contains(c.SQL, "AS total_requests") {
		t.Errorf("dotted alias should be sanitized in SQL; got:\n%s", c.SQL)
	}
}

// Built-in columns really are snake_case in the store, so labelling them
// would be noise: Label stays empty and clients fall back to Name.
func TestBuild_NoLabelForBuiltinColumns(t *testing.T) {
	q := Query{
		Dataset:   DatasetSpans,
		TimeRange: tr(),
		Select:    []Aggregation{{Op: OpMax, Field: "duration_ns"}},
		GroupBy:   []string{"service_name"},
		BucketMS:  60_000,
	}
	if err := q.Validate(); err != nil {
		t.Fatalf("validate: %v", err)
	}
	c, err := Build(&q)
	if err != nil {
		t.Fatal(err)
	}
	for _, col := range c.Columns {
		switch col.Name {
		case "bucket_ns", "service_name":
			if col.Label != "" {
				t.Errorf("built-in column %q should carry no label, got %q", col.Name, col.Label)
			}
		case "max_duration_ns":
			if col.Label != "MAX(duration_ns)" {
				t.Errorf("aggregation label: want MAX(duration_ns), got %q", col.Label)
			}
		}
	}
}

// A HAVING or ORDER BY reference written the way the user aliased it
// ("total.requests") still binds to the SELECT output rather than falling
// through to an attribute lookup.
func TestBuild_HavingMatchesDottedAlias(t *testing.T) {
	q := Query{
		Dataset:   DatasetMetrics,
		TimeRange: tr(),
		Select:    []Aggregation{{Op: OpSum, Field: "requests.total", Alias: "total.requests"}},
		GroupBy:   []string{"service.name"},
		Having:    []Filter{{Field: "total.requests", Op: FilterGt, Value: 10}},
		OrderBy:   []Order{{Field: "total.requests", Dir: "desc"}},
	}
	if err := q.Validate(); err != nil {
		t.Fatalf("validate: %v", err)
	}
	c, err := Build(&q)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(c.SQL, "HAVING total_requests > ?") {
		t.Errorf("HAVING should reference the select alias; got:\n%s", c.SQL)
	}
	if !strings.Contains(c.SQL, "ORDER BY total_requests DESC") {
		t.Errorf("ORDER BY should reference the select alias; got:\n%s", c.SQL)
	}
}

// Flattening dots to underscores is lossy: `deploy.runtime` and
// `deploy-runtime` both sanitize to `deploy_runtime`. Two columns sharing an
// output alias make any HAVING / ORDER BY reference to them ambiguous, so the
// second gets a suffix — and each reference still binds to the column the user
// named rather than to whichever one came first.
func TestBuild_CollidingAliasesStayDistinct(t *testing.T) {
	q := Query{
		Dataset:   DatasetSpans,
		TimeRange: tr(),
		Select: []Aggregation{
			{Op: OpCount},
			{Op: OpMax, Field: "queue.depth", Alias: "peak.depth"},
			{Op: OpMin, Field: "queue.depth", Alias: "peak-depth"},
		},
		GroupBy: []string{"deploy.runtime", "deploy-runtime"},
		Having:  []Filter{{Field: "peak-depth", Op: FilterGt, Value: 1}},
		OrderBy: []Order{{Field: "peak.depth", Dir: "desc"}},
	}
	if err := q.Validate(); err != nil {
		t.Fatalf("validate: %v", err)
	}
	c, err := Build(&q)
	if err != nil {
		t.Fatal(err)
	}

	seen := map[string]int{}
	for _, col := range c.Columns {
		seen[col.Name]++
	}
	for name, n := range seen {
		if n > 1 {
			t.Errorf("duplicate output alias %q (%d times): %+v", name, n, c.Columns)
		}
	}
	for _, want := range []string{
		"deploy_runtime",
		"deploy_runtime_2",
		"peak_depth",
		"peak_depth_2",
	} {
		if !hasAlias(c.Columns, want) {
			t.Errorf("missing alias %q: %+v", want, c.Columns)
		}
	}
	// Labels still distinguish them for the reader.
	labels := map[string]string{}
	for _, col := range c.Columns {
		labels[col.Name] = col.Label
	}
	if labels["deploy_runtime"] != "deploy.runtime" || labels["deploy_runtime_2"] != "deploy-runtime" {
		t.Errorf("group-by labels lost the original keys: %+v", labels)
	}
	// The HAVING reference named the *second* alias's field, and the ORDER BY
	// the first: neither may bind to the other.
	if !strings.Contains(c.SQL, "HAVING peak_depth_2 > ?") {
		t.Errorf("HAVING bound to the wrong column; got:\n%s", c.SQL)
	}
	if !strings.Contains(c.SQL, "ORDER BY peak_depth DESC") {
		t.Errorf("ORDER BY bound to the wrong column; got:\n%s", c.SQL)
	}
}

// Two identical aggregations are a degenerate case of the same collision:
// SQLite tolerates duplicate output aliases, the group-splitting on the other
// side of the wire does not.
func TestBuild_RepeatedAggregationGetsDistinctAlias(t *testing.T) {
	q := Query{
		Dataset:   DatasetSpans,
		TimeRange: tr(),
		Select:    []Aggregation{{Op: OpCount}, {Op: OpCount}},
	}
	if err := q.Validate(); err != nil {
		t.Fatalf("validate: %v", err)
	}
	c, err := Build(&q)
	if err != nil {
		t.Fatal(err)
	}
	if !hasAlias(c.Columns, "count") || !hasAlias(c.Columns, "count_2") {
		t.Errorf("want count + count_2, got %+v", c.Columns)
	}
}
