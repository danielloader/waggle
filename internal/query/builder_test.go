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
		{"invalid dataset", Query{Dataset: "metrics", TimeRange: tr(), Select: []Aggregation{{Op: OpCount}}}, "dataset"},
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
	if !strings.Contains(c.SQL, "FROM spans") {
		t.Errorf("SQL missing FROM spans: %s", c.SQL)
	}
	if !strings.Contains(c.SQL, "start_time_ns >= ?") {
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
	if strings.Contains(c.SQL, "json_extract(attributes") {
		t.Errorf("unexpected json_extract fallthrough: %s", c.SQL)
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
	if !strings.Contains(c.SQL, `json_extract(attributes, '$."customer.tier"')`) {
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
		"service_name = ?",
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
	if !strings.Contains(c.SQL, "FROM logs") {
		t.Errorf("FROM logs missing: %s", c.SQL)
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
	if !strings.Contains(c.SQL, "service_name IN (?,?,?)") {
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
	if !strings.Contains(c.SQL, `json_extract(attributes, '$."customer.tier"') IS NULL`) {
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
		"service_name NOT IN (?,?)",
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
		"ORDER BY start_time_ns DESC",
		"FROM spans",
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
		"EXISTS (SELECT 1 FROM span_events e",
		"e.name = 'exception'",
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
