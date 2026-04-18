package ingest_test

// SDK-driven metric ingest round-trips. Stage 1 only covers Sum + Gauge
// (see plans/metrics.md). These tests exercise the full path:
//
//   OTel SDK meter → otlpmetrichttp → POST /v1/metrics → ingest.Handler
//     → ingest.Writer → store.WriteBatch → sqlite metric_series + metric_points
//
// We don't have /api/metrics yet, so the assertions go straight against
// the SQLite store via its reader DB handle.

import (
	"context"
	"database/sql"
	"fmt"
	"testing"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetrichttp"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/resource"
	semattr "go.opentelemetry.io/otel/attribute"
)

// meterProvider builds a MeterProvider that exports to the fixture's
// ingest endpoint on the default cumulative temporality. Uses a periodic
// reader with a short interval so tests don't wait long.
func (f *e2eFixture) meterProvider(service string) *sdkmetric.MeterProvider {
	f.t.Helper()
	exp, err := otlpmetrichttp.New(f.ctx,
		otlpmetrichttp.WithEndpoint(f.endpointHost()),
		otlpmetrichttp.WithInsecure(),
		// Explicit cumulative — we exercise delta later when stage 4
		// brings histograms with explicit temporality selection.
		otlpmetrichttp.WithTemporalitySelector(func(sdkmetric.InstrumentKind) metricdata.Temporality {
			return metricdata.CumulativeTemporality
		}),
	)
	if err != nil {
		f.t.Fatalf("otlpmetrichttp.New: %v", err)
	}
	res, err := resource.New(f.ctx, resource.WithAttributes(
		semattr.String("service.name", service),
		semattr.String("service.version", "e2e-test"),
		semattr.String("deployment.environment", "test"),
	))
	if err != nil {
		f.t.Fatalf("resource.New: %v", err)
	}
	return sdkmetric.NewMeterProvider(
		sdkmetric.WithResource(res),
		sdkmetric.WithReader(sdkmetric.NewPeriodicReader(exp,
			sdkmetric.WithInterval(50*time.Millisecond),
		)),
	)
}

// shutdownMeter flushes + shuts down a MeterProvider with a deadline.
// The periodic reader has fired at least one export cycle by the time
// Shutdown returns, but we also wait for the store to actually see the
// writes before asserting.
func (f *e2eFixture) shutdownMeter(mp *sdkmetric.MeterProvider) {
	f.t.Helper()
	sctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := mp.Shutdown(sctx); err != nil {
		f.t.Fatalf("meter shutdown: %v", err)
	}
}

// waitForMetricSeries polls the store until at least n series exist for
// the given service.
func (f *e2eFixture) waitForMetricSeries(service string, n int) {
	f.t.Helper()
	waitFor(f.t, 3*time.Second, fmt.Sprintf("metric_series for %q >= %d", service, n), func() bool {
		var count int
		err := f.st.ReaderDB().QueryRowContext(f.ctx,
			"SELECT COUNT(*) FROM metric_series WHERE service_name = ?", service,
		).Scan(&count)
		return err == nil && count >= n
	})
}

// waitForMetricPoints polls until at least n points exist for the
// series named `metric` under the given service.
func (f *e2eFixture) waitForMetricPoints(service, metricName string, n int) {
	f.t.Helper()
	waitFor(f.t, 3*time.Second, fmt.Sprintf("metric_points for %q/%q >= %d", service, metricName, n), func() bool {
		var count int
		err := f.st.ReaderDB().QueryRowContext(f.ctx, `
			SELECT COUNT(*) FROM metric_points p
			  JOIN metric_series s ON s.series_id = p.series_id
			WHERE s.service_name = ? AND s.name = ?
		`, service, metricName).Scan(&count)
		return err == nil && count >= n
	})
}

// ---------------------------------------------------------------------------
// Counter (Sum / cumulative / monotonic)
// ---------------------------------------------------------------------------

func TestE2E_MetricCounter(t *testing.T) {
	f := newE2EFixture(t)
	defer f.close()

	mp := f.meterProvider("counter-svc")
	meter := mp.Meter("e2e")
	counter, err := meter.Int64Counter("requests.total",
		metric.WithUnit("1"),
		metric.WithDescription("total HTTP requests"),
	)
	if err != nil {
		t.Fatalf("counter: %v", err)
	}
	// Two distinct series via the label set — GET vs POST.
	for i := 0; i < 5; i++ {
		counter.Add(context.Background(), 1,
			metric.WithAttributes(attribute.String("http.method", "GET")))
	}
	for i := 0; i < 3; i++ {
		counter.Add(context.Background(), 1,
			metric.WithAttributes(attribute.String("http.method", "POST")))
	}

	f.shutdownMeter(mp)
	f.waitForMetricSeries("counter-svc", 2)
	f.waitForMetricPoints("counter-svc", "requests.total", 2)

	// Series-level assertions: kind=sum, monotonic=1, temporality=cumulative.
	rows, err := f.st.ReaderDB().QueryContext(f.ctx, `
		SELECT kind, temporality, monotonic, attributes
		  FROM metric_series
		 WHERE service_name = ? AND name = ?
		 ORDER BY attributes
	`, "counter-svc", "requests.total")
	if err != nil {
		t.Fatalf("series query: %v", err)
	}
	defer rows.Close()
	var seen int
	for rows.Next() {
		var kind, temp, attrs string
		var mono int
		if err := rows.Scan(&kind, &temp, &mono, &attrs); err != nil {
			t.Fatal(err)
		}
		if kind != "sum" {
			t.Errorf("kind: want sum, got %q", kind)
		}
		if temp != "cumulative" {
			t.Errorf("temporality: want cumulative, got %q", temp)
		}
		if mono != 1 {
			t.Errorf("monotonic: want 1, got %d", mono)
		}
		seen++
	}
	if seen != 2 {
		t.Errorf("series count: want 2, got %d", seen)
	}

	// Each series' last-seen cumulative value matches what we added.
	perMethod := map[string]float64{}
	pointRows, err := f.st.ReaderDB().QueryContext(f.ctx, `
		SELECT s.attributes, p.value
		  FROM metric_points p
		  JOIN metric_series s ON s.series_id = p.series_id
		 WHERE s.service_name = ? AND s.name = ?
	`, "counter-svc", "requests.total")
	if err != nil {
		t.Fatalf("point query: %v", err)
	}
	defer pointRows.Close()
	for pointRows.Next() {
		var attrs string
		var v float64
		if err := pointRows.Scan(&attrs, &v); err != nil {
			t.Fatal(err)
		}
		if v > perMethod[attrs] {
			perMethod[attrs] = v
		}
	}
	var get, post float64
	for attrs, v := range perMethod {
		switch {
		case containsMethod(attrs, "GET"):
			get = v
		case containsMethod(attrs, "POST"):
			post = v
		}
	}
	if get != 5 {
		t.Errorf("GET cumulative total: want 5, got %v", get)
	}
	if post != 3 {
		t.Errorf("POST cumulative total: want 3, got %v", post)
	}
}

// ---------------------------------------------------------------------------
// Gauge (sampled current value — async instrument)
// ---------------------------------------------------------------------------

func TestE2E_MetricGauge(t *testing.T) {
	f := newE2EFixture(t)
	defer f.close()

	mp := f.meterProvider("gauge-svc")
	meter := mp.Meter("e2e")

	// Observed async gauge — the callback returns the current value on
	// each collection cycle. Simulates memory.used_bytes.
	called := 0
	gauge, err := meter.Float64ObservableGauge("memory.used",
		metric.WithUnit("By"),
		metric.WithFloat64Callback(func(_ context.Context, obs metric.Float64Observer) error {
			called++
			obs.Observe(float64(1_000_000+called*1000),
				metric.WithAttributes(attribute.String("process", "main")))
			return nil
		}),
	)
	if err != nil {
		t.Fatalf("gauge: %v", err)
	}
	_ = gauge

	// Wait for at least one collection cycle. 50ms reader interval; 200ms
	// gives some slack for CI.
	time.Sleep(250 * time.Millisecond)
	f.shutdownMeter(mp)
	f.waitForMetricSeries("gauge-svc", 1)
	f.waitForMetricPoints("gauge-svc", "memory.used", 1)

	// Kind=gauge, monotonic=NULL, temporality empty (gauge has no
	// temporality in OTLP's model; the transformer writes it as NULL).
	var kind string
	var temp sql.NullString
	var mono sql.NullInt64
	if err := f.st.ReaderDB().QueryRowContext(f.ctx, `
		SELECT kind, temporality, monotonic
		  FROM metric_series
		 WHERE service_name = ? AND name = ?
	`, "gauge-svc", "memory.used").Scan(&kind, &temp, &mono); err != nil {
		t.Fatalf("series row: %v", err)
	}
	if kind != "gauge" {
		t.Errorf("kind: want gauge, got %q", kind)
	}
	if temp.Valid && temp.String != "" {
		t.Errorf("temporality for gauge: want NULL/empty, got %q", temp.String)
	}
	if mono.Valid {
		t.Errorf("monotonic for gauge: want NULL, got %d", mono.Int64)
	}

	// Value should match the callback output (roughly — reader cycles
	// are non-deterministic; just ensure it's in the expected band).
	var value float64
	if err := f.st.ReaderDB().QueryRowContext(f.ctx, `
		SELECT p.value FROM metric_points p
		  JOIN metric_series s ON s.series_id = p.series_id
		 WHERE s.service_name = ? AND s.name = ?
		 ORDER BY p.time_ns DESC LIMIT 1
	`, "gauge-svc", "memory.used").Scan(&value); err != nil {
		t.Fatalf("latest value: %v", err)
	}
	if value < 1_000_000 || value > 2_000_000 {
		t.Errorf("gauge value out of expected band: %v", value)
	}
}

// ---------------------------------------------------------------------------
// Catalog: metric attribute keys land under signal_type=metric so the
// existing /api/fields machinery lights up when we build the UI.
// ---------------------------------------------------------------------------

func TestE2E_MetricAttributeCatalog(t *testing.T) {
	f := newE2EFixture(t)
	defer f.close()

	mp := f.meterProvider("cat-svc")
	meter := mp.Meter("e2e")
	counter, err := meter.Int64Counter("hits")
	if err != nil {
		t.Fatal(err)
	}
	counter.Add(context.Background(), 1,
		metric.WithAttributes(
			attribute.String("route", "/users"),
			attribute.Int("status_code", 200),
		))
	f.shutdownMeter(mp)
	f.waitForMetricPoints("cat-svc", "hits", 1)

	var routeCount, statusCount int
	if err := f.st.ReaderDB().QueryRowContext(f.ctx,
		"SELECT COUNT(*) FROM attribute_keys WHERE signal_type = 'metric' AND service_name = ? AND key = 'route'",
		"cat-svc").Scan(&routeCount); err != nil {
		t.Fatal(err)
	}
	if err := f.st.ReaderDB().QueryRowContext(f.ctx,
		"SELECT COUNT(*) FROM attribute_keys WHERE signal_type = 'metric' AND service_name = ? AND key = 'status_code'",
		"cat-svc").Scan(&statusCount); err != nil {
		t.Fatal(err)
	}
	if routeCount == 0 {
		t.Errorf("route key missing from metric attribute_keys catalog")
	}
	if statusCount == 0 {
		t.Errorf("status_code key missing from metric attribute_keys catalog")
	}
}

// containsMethod checks whether the JSON-encoded attrs column has
// `"http.method": "<wanted>"`. Quick substring check is safe because
// the attribute value has no escapes for these inputs.
func containsMethod(attrs, wanted string) bool {
	return fmt.Sprintf(`"http.method":"%s"`, wanted) != "" &&
		(indexString(attrs, fmt.Sprintf(`"http.method":"%s"`, wanted)) >= 0)
}

func indexString(s, sub string) int {
	for i := 0; i+len(sub) <= len(s); i++ {
		if s[i:i+len(sub)] == sub {
			return i
		}
	}
	return -1
}

// ---------------------------------------------------------------------------
// /api/metrics + /api/metrics/{name}/series
// ---------------------------------------------------------------------------

func TestE2E_MetricsAPIListing(t *testing.T) {
	f := newE2EFixture(t)
	defer f.close()

	mp := f.meterProvider("picker-svc")
	meter := mp.Meter("e2e")

	reqs, _ := meter.Int64Counter("requests.total", metric.WithUnit("1"),
		metric.WithDescription("total HTTP requests"))
	reqs.Add(context.Background(), 1, metric.WithAttributes(attribute.String("route", "/a")))
	reqs.Add(context.Background(), 1, metric.WithAttributes(attribute.String("route", "/b")))

	_, err := meter.Float64ObservableGauge("memory.used", metric.WithUnit("By"),
		metric.WithFloat64Callback(func(_ context.Context, obs metric.Float64Observer) error {
			obs.Observe(1024)
			return nil
		}))
	if err != nil {
		t.Fatal(err)
	}
	time.Sleep(250 * time.Millisecond)
	f.shutdownMeter(mp)
	f.waitForMetricSeries("picker-svc", 3) // 2 routes on requests + 1 gauge

	var listing struct {
		Metrics []struct {
			Name        string `json:"name"`
			Kind        string `json:"kind"`
			Unit        string `json:"unit"`
			Description string `json:"description"`
			SeriesCount int64  `json:"series_count"`
		} `json:"metrics"`
	}
	if err := f.getJSON("/api/metrics?service=picker-svc", &listing); err != nil {
		t.Fatalf("GET /api/metrics: %v", err)
	}
	byName := map[string]struct {
		Kind        string
		Unit        string
		Count       int64
	}{}
	for _, m := range listing.Metrics {
		byName[m.Name] = struct {
			Kind  string
			Unit  string
			Count int64
		}{m.Kind, m.Unit, m.SeriesCount}
	}
	if e := byName["requests.total"]; e.Kind != "sum" || e.Count != 2 {
		t.Errorf("requests.total: want sum / series_count=2, got %+v", e)
	}
	if e := byName["memory.used"]; e.Kind != "gauge" || e.Unit != "By" {
		t.Errorf("memory.used: want gauge unit=By, got %+v", e)
	}

	// /api/metrics/{name}/series
	var series struct {
		Series []struct {
			SeriesID    int64  `json:"series_id"`
			ServiceName string `json:"service_name"`
			Kind        string `json:"kind"`
			Attributes  string `json:"attributes"`
		} `json:"series"`
	}
	if err := f.getJSON("/api/metrics/requests.total/series?service=picker-svc", &series); err != nil {
		t.Fatalf("series endpoint: %v", err)
	}
	if len(series.Series) != 2 {
		t.Fatalf("want 2 series, got %d (%+v)", len(series.Series), series.Series)
	}
	for _, s := range series.Series {
		if s.Kind != "sum" {
			t.Errorf("series kind: want sum, got %q", s.Kind)
		}
		if s.ServiceName != "picker-svc" {
			t.Errorf("service_name: want picker-svc, got %q", s.ServiceName)
		}
	}
}

// ---------------------------------------------------------------------------
// /api/query with dataset=metrics — the shared query path works
// against the metric_points + metric_series join.
// ---------------------------------------------------------------------------

func TestE2E_MetricsQuery(t *testing.T) {
	f := newE2EFixture(t)
	defer f.close()

	mp := f.meterProvider("q-svc")
	counter, err := mp.Meter("e2e").Int64Counter("events")
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 5; i++ {
		counter.Add(context.Background(), 1, metric.WithAttributes(attribute.String("k", "a")))
	}
	for i := 0; i < 3; i++ {
		counter.Add(context.Background(), 1, metric.WithAttributes(attribute.String("k", "b")))
	}
	f.shutdownMeter(mp)
	f.waitForMetricSeries("q-svc", 2)
	f.waitForMetricPoints("q-svc", "events", 2)

	now := time.Now()
	from := now.Add(-5 * time.Minute)

	// Aggregated MAX(value) group-by k — cumulative sum instruments
	// report their total-to-date, so MAX over a window reflects the
	// last-seen cumulative level per series. Should equal the total
	// we added.
	res, err := f.runQueryAPI(map[string]any{
		"dataset":    "metrics",
		"time_range": map[string]any{"from": rfc3339(from), "to": rfc3339(now.Add(5 * time.Second))},
		"select":     []map[string]any{{"op": "max", "field": "value"}},
		"where": []map[string]any{
			{"field": "service.name", "op": "=", "value": "q-svc"},
			{"field": "name", "op": "=", "value": "events"},
		},
		"group_by": []string{"k"},
	})
	if err != nil {
		t.Fatalf("metrics query: %v", err)
	}
	got := map[string]float64{}
	kIdx := res.columnIdx("k")
	maxIdx := res.columnIdx("max_value")
	for _, row := range res.Rows {
		key, _ := row[kIdx].(string)
		v, _ := toFloat(row[maxIdx])
		got[key] = v
	}
	if got["a"] != 5 {
		t.Errorf("max(value) where k=a: want 5, got %v", got["a"])
	}
	if got["b"] != 3 {
		t.Errorf("max(value) where k=b: want 3, got %v", got["b"])
	}

	// Raw-rows mode (empty SELECT) — each point comes back with
	// service_name, name, kind, value, and the series attributes blob.
	rawRes, err := f.runQueryAPI(map[string]any{
		"dataset":    "metrics",
		"time_range": map[string]any{"from": rfc3339(from), "to": rfc3339(now.Add(5 * time.Second))},
		"select":     []map[string]any{},
		"where":      []map[string]any{{"field": "service.name", "op": "=", "value": "q-svc"}},
		"limit":      50,
	})
	if err != nil {
		t.Fatalf("raw metrics query: %v", err)
	}
	if len(rawRes.Rows) == 0 {
		t.Fatal("raw metrics query returned no rows")
	}
	// Expect the five shape columns documented in rawColumnsFor.
	for _, c := range []string{"time_ns", "service_name", "name", "kind", "value", "attributes"} {
		if rawRes.columnIdx(c) < 0 {
			t.Errorf("raw metric columns missing %q (got %+v)", c, rawRes.Columns)
		}
	}
}
