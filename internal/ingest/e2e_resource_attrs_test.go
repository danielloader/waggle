package ingest_test

// Resource attributes are ingested into their own deduplicated `resources`
// row and referenced by every event and metric row. These tests drive real
// SDK exporters and then query through POST /api/query, so they cover the
// whole path: resource attribute in -> queryable field out, on all three
// datasets, in WHERE / GROUP BY / SELECT and in the field catalogue.

import (
	"context"
	"testing"
	"time"

	"go.opentelemetry.io/otel/attribute"
	otellog "go.opentelemetry.io/otel/log"
	"go.opentelemetry.io/otel/metric"
)

// runtimeKey mirrors the shape that motivated this: one service reporting
// from two different runtimes, distinguishable only by a resource attribute.
const runtimeKey = "waggle.runtime"

// ---------------------------------------------------------------------------
// Spans
// ---------------------------------------------------------------------------

func TestE2E_ResourceAttributeQueryableOnSpans(t *testing.T) {
	f := newE2EFixture(t)
	defer f.close()

	// Same service.name, two resources differing only in waggle.runtime.
	for _, tc := range []struct {
		runtime string
		spans   int
	}{
		{"server", 5},
		{"job-run", 2},
	} {
		tp := f.tracerProvider("res-span-svc", attribute.String(runtimeKey, tc.runtime))
		tr := tp.Tracer("e2e")
		for i := 0; i < tc.spans; i++ {
			_, span := tr.Start(context.Background(), "op")
			span.End()
		}
		f.shutdownTracer(tp)
	}
	f.waitForSpanCount("res-span-svc", 7)

	window := f.window()

	// GROUP BY a resource attribute splits one service into its runtimes.
	res, err := f.runQueryAPI(map[string]any{
		"dataset":    "spans",
		"time_range": window,
		"select":     []map[string]any{{"op": "count"}},
		"where":      []map[string]any{{"field": "service.name", "op": "=", "value": "res-span-svc"}},
		"group_by":   []string{runtimeKey},
	})
	if err != nil {
		t.Fatalf("group-by query: %v", err)
	}
	counts := groupCounts(t, res, "waggle_runtime", "count")
	if counts["server"] != 5 || counts["job-run"] != 2 {
		t.Errorf("group-by counts: want server=5 job-run=2, got %v", counts)
	}

	// The group-by column keeps the dotted key as its display label.
	if got := columnLabel(res, "waggle_runtime"); got != runtimeKey {
		t.Errorf("column label: want %q, got %q", runtimeKey, got)
	}

	// And WHERE on the same attribute selects one of them.
	res, err = f.runQueryAPI(map[string]any{
		"dataset":    "spans",
		"time_range": window,
		"select":     []map[string]any{{"op": "count"}},
		"where": []map[string]any{
			{"field": "service.name", "op": "=", "value": "res-span-svc"},
			{"field": runtimeKey, "op": "=", "value": "server"},
		},
	})
	if err != nil {
		t.Fatalf("filter query: %v", err)
	}
	if n := singleCount(t, res); n != 5 {
		t.Errorf("filtered count: want 5, got %d", n)
	}
}

// A span attribute and a resource attribute can carry the same key. The row's
// own value wins — the narrower scope shadows the envelope — matching the flat
// field list the span-detail panel renders.
func TestE2E_SpanAttributeShadowsResourceAttribute(t *testing.T) {
	f := newE2EFixture(t)
	defer f.close()

	tp := f.tracerProvider("shadow-svc", attribute.String(runtimeKey, "server"))
	tr := tp.Tracer("e2e")
	// One span overrides the resource-level value on itself; one doesn't.
	_, override := tr.Start(context.Background(), "override")
	override.SetAttributes(attribute.String(runtimeKey, "overridden"))
	override.End()
	_, plain := tr.Start(context.Background(), "plain")
	plain.End()
	f.shutdownTracer(tp)
	f.waitForSpanCount("shadow-svc", 2)

	res, err := f.runQueryAPI(map[string]any{
		"dataset":    "spans",
		"time_range": f.window(),
		"select":     []map[string]any{{"op": "count"}},
		"where":      []map[string]any{{"field": "service.name", "op": "=", "value": "shadow-svc"}},
		"group_by":   []string{runtimeKey},
	})
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	counts := groupCounts(t, res, "waggle_runtime", "count")
	if counts["overridden"] != 1 {
		t.Errorf("span attribute should win over the resource value: %v", counts)
	}
	if counts["server"] != 1 {
		t.Errorf("span without its own value should fall through to the resource: %v", counts)
	}
}

// ---------------------------------------------------------------------------
// Logs
// ---------------------------------------------------------------------------

func TestE2E_ResourceAttributeQueryableOnLogs(t *testing.T) {
	f := newE2EFixture(t)
	defer f.close()

	for _, tc := range []struct {
		runtime string
		logs    int
	}{
		{"server", 4},
		{"job-run", 1},
	} {
		lp := f.loggerProvider("res-log-svc", attribute.String(runtimeKey, tc.runtime))
		lg := lp.Logger("e2e")
		for i := 0; i < tc.logs; i++ {
			var r otellog.Record
			r.SetTimestamp(time.Now())
			r.SetObservedTimestamp(time.Now())
			r.SetSeverity(otellog.SeverityInfo)
			r.SetSeverityText("INFO")
			r.SetBody(otellog.StringValue("hello"))
			lg.Emit(context.Background(), r)
		}
		f.shutdownLogger(lp)
	}
	f.waitForLogCount(5)

	res, err := f.runQueryAPI(map[string]any{
		"dataset":    "logs",
		"time_range": f.window(),
		"select":     []map[string]any{{"op": "count"}},
		"where":      []map[string]any{{"field": "service.name", "op": "=", "value": "res-log-svc"}},
		"group_by":   []string{runtimeKey},
	})
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	counts := groupCounts(t, res, "waggle_runtime", "count")
	if counts["server"] != 4 || counts["job-run"] != 1 {
		t.Errorf("group-by counts: want server=4 job-run=1, got %v", counts)
	}
}

// ---------------------------------------------------------------------------
// Metrics
// ---------------------------------------------------------------------------

// The metrics dataset is the one where resource attributes matter most: a
// folded metric row carries only its label set, so the runtime that produced
// it is knowable *only* from the resource.
func TestE2E_ResourceAttributeQueryableOnMetrics(t *testing.T) {
	f := newE2EFixture(t)
	defer f.close()

	for _, tc := range []struct {
		runtime string
		value   int64
	}{
		{"server", 7},
		{"job-run", 3},
	} {
		mp := f.meterProvider("res-metric-svc", attribute.String(runtimeKey, tc.runtime))
		ctr, err := mp.Meter("e2e").Int64Counter("requests.total")
		if err != nil {
			t.Fatalf("counter: %v", err)
		}
		ctr.Add(context.Background(), tc.value, metric.WithAttributes(attribute.String("route", "/x")))
		f.shutdownMeter(mp)
	}
	f.waitForMetricEvents("res-metric-svc", 2)

	window := f.window()

	// SELECT an aggregation, GROUP BY the resource attribute.
	res, err := f.runQueryAPI(map[string]any{
		"dataset":    "metrics",
		"time_range": window,
		"select":     []map[string]any{{"op": "max", "field": "requests.total"}},
		"where":      []map[string]any{{"field": "service.name", "op": "=", "value": "res-metric-svc"}},
		"group_by":   []string{runtimeKey},
	})
	if err != nil {
		t.Fatalf("group-by query: %v", err)
	}
	maxima := map[string]float64{}
	runtimeIdx, valIdx := res.columnIdx("waggle_runtime"), res.columnIdx("max_requests_total")
	if runtimeIdx < 0 || valIdx < 0 {
		t.Fatalf("missing columns: %+v", res.Columns)
	}
	for _, row := range res.Rows {
		key, _ := row[runtimeIdx].(string)
		v, _ := toFloat(row[valIdx])
		maxima[key] = v
	}
	if maxima["server"] != 7 || maxima["job-run"] != 3 {
		t.Errorf("MAX(requests.total) by runtime: want server=7 job-run=3, got %v", maxima)
	}
	// The aggregation header reads back as what was asked for.
	if got := columnLabel(res, "max_requests_total"); got != "MAX(requests.total)" {
		t.Errorf("aggregation label: want MAX(requests.total), got %q", got)
	}

	// WHERE on the resource attribute narrows to one runtime.
	res, err = f.runQueryAPI(map[string]any{
		"dataset":    "metrics",
		"time_range": window,
		"select":     []map[string]any{{"op": "max", "field": "requests.total"}},
		"where": []map[string]any{
			{"field": "service.name", "op": "=", "value": "res-metric-svc"},
			{"field": runtimeKey, "op": "=", "value": "job-run"},
		},
	})
	if err != nil {
		t.Fatalf("filter query: %v", err)
	}
	if len(res.Rows) != 1 {
		t.Fatalf("want 1 row, got %d", len(res.Rows))
	}
	if v, _ := toFloat(res.Rows[0][res.columnIdx("max_requests_total")]); v != 3 {
		t.Errorf("filtered MAX: want 3, got %v", v)
	}

	// Raw-rows mode has to survive the join too: `service_name` and
	// `attributes` exist on both metric_events and resources.
	raw, err := f.runQueryAPI(map[string]any{
		"dataset":    "metrics",
		"time_range": window,
		"where":      []map[string]any{{"field": runtimeKey, "op": "=", "value": "server"}},
	})
	if err != nil {
		t.Fatalf("raw query: %v", err)
	}
	if len(raw.Rows) == 0 {
		t.Error("raw-rows query on a resource attribute returned nothing")
	}
}

// ---------------------------------------------------------------------------
// Field catalogue
// ---------------------------------------------------------------------------

// The pickers in the UI are driven by /api/fields and /api/fields/{key}/values.
// A resource attribute that can be queried but can't be discovered is only
// half fixed, so assert both, on the dataset that has no other way to see it.
func TestE2E_ResourceAttributeInFieldCatalogue(t *testing.T) {
	f := newE2EFixture(t)
	defer f.close()

	mp := f.meterProvider("cat-res-svc", attribute.String(runtimeKey, "server"))
	ctr, err := mp.Meter("e2e").Int64Counter("requests.total")
	if err != nil {
		t.Fatalf("counter: %v", err)
	}
	ctr.Add(context.Background(), 1)
	f.shutdownMeter(mp)
	f.waitForMetricEvents("cat-res-svc", 1)

	var fields struct {
		Fields []struct {
			Key       string `json:"key"`
			ValueType string `json:"type"`
		} `json:"fields"`
	}
	if err := f.getJSON("/api/fields?dataset=metric&service=cat-res-svc", &fields); err != nil {
		t.Fatalf("GET /api/fields: %v", err)
	}
	found := map[string]string{}
	for _, fi := range fields.Fields {
		found[fi.Key] = fi.ValueType
	}
	// Every resource attribute the exporter set, none of which is a column
	// on metric_events or a per-datapoint label.
	for _, key := range []string{runtimeKey, "deployment.environment", "service.version"} {
		if _, ok := found[key]; !ok {
			t.Errorf("metric field catalogue missing resource attribute %q: %v", key, found)
		}
	}

	var values struct {
		Values []string `json:"values"`
	}
	if err := f.getJSON("/api/fields/"+runtimeKey+"/values?dataset=metric&service=cat-res-svc", &values); err != nil {
		t.Fatalf("GET /api/fields/{key}/values: %v", err)
	}
	if len(values.Values) != 1 || values.Values[0] != "server" {
		t.Errorf("resource attribute values: want [server], got %v", values.Values)
	}
}

// ---------------------------------------------------------------------------
// helpers
// ---------------------------------------------------------------------------

// window is a time range wide enough to cover anything a test just emitted.
func (f *e2eFixture) window() map[string]any {
	now := time.Now()
	return map[string]any{
		"from": rfc3339(now.Add(-5 * time.Minute)),
		"to":   rfc3339(now.Add(5 * time.Second)),
	}
}

// groupCounts collapses a grouped result into groupValue -> count.
func groupCounts(t *testing.T, res *apiQueryResult, groupCol, countCol string) map[string]int64 {
	t.Helper()
	gi, ci := res.columnIdx(groupCol), res.columnIdx(countCol)
	if gi < 0 || ci < 0 {
		t.Fatalf("missing columns %q/%q in %+v", groupCol, countCol, res.Columns)
	}
	out := map[string]int64{}
	for _, row := range res.Rows {
		key, _ := row[gi].(string)
		n, _ := toInt64(row[ci])
		out[key] = n
	}
	return out
}

func singleCount(t *testing.T, res *apiQueryResult) int64 {
	t.Helper()
	if len(res.Rows) != 1 {
		t.Fatalf("want exactly 1 row, got %d", len(res.Rows))
	}
	n, _ := toInt64(res.Rows[0][res.columnIdx("count")])
	return n
}

func columnLabel(res *apiQueryResult, name string) string {
	for _, c := range res.Columns {
		if c.Name == name {
			return c.Label
		}
	}
	return ""
}
