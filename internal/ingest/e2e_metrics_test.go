package ingest_test

// SDK-driven metric ingest round-trips. Sum + Gauge are written as Events
// with signal_type='metric' and the metric-specific metadata stamped into
// the attributes JSON under the reserved meta.* namespace
// (meta.metric_kind, meta.metric_unit, meta.metric_temporality,
// meta.metric_monotonic).

import (
	"context"
	"database/sql"
	"fmt"
	"testing"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetrichttp"
	"go.opentelemetry.io/otel/metric"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
	"go.opentelemetry.io/otel/sdk/resource"
	semattr "go.opentelemetry.io/otel/attribute"
)

// meterProvider builds a MeterProvider that exports to the fixture's
// ingest endpoint on the default cumulative temporality.
func (f *e2eFixture) meterProvider(service string) *sdkmetric.MeterProvider {
	f.t.Helper()
	exp, err := otlpmetrichttp.New(f.ctx,
		otlpmetrichttp.WithEndpoint(f.endpointHost()),
		otlpmetrichttp.WithInsecure(),
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

func (f *e2eFixture) shutdownMeter(mp *sdkmetric.MeterProvider) {
	f.t.Helper()
	sctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := mp.Shutdown(sctx); err != nil {
		f.t.Fatalf("meter shutdown: %v", err)
	}
}

// waitForMetricEvents polls the events table until at least n metric events
// exist for the given service.
func (f *e2eFixture) waitForMetricEvents(service string, n int) {
	f.t.Helper()
	waitFor(f.t, 3*time.Second, fmt.Sprintf("metric events for %q >= %d", service, n), func() bool {
		var count int
		err := f.st.ReaderDB().QueryRowContext(f.ctx,
			`SELECT COUNT(*) FROM events WHERE signal_type = 'metric' AND service_name = ?`,
			service,
		).Scan(&count)
		return err == nil && count >= n
	})
}

// waitForDistinctMetricSeries waits for at least n distinct (name, attributes)
// tuples — the "series count" analogue under the wide-event model.
func (f *e2eFixture) waitForDistinctMetricSeries(service string, n int) {
	f.t.Helper()
	waitFor(f.t, 3*time.Second, fmt.Sprintf("distinct metric series for %q >= %d", service, n), func() bool {
		var count int
		err := f.st.ReaderDB().QueryRowContext(f.ctx,
			`SELECT COUNT(*) FROM (
				SELECT DISTINCT name, attributes
				FROM events WHERE signal_type = 'metric' AND service_name = ?
			)`,
			service,
		).Scan(&count)
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
	for range 5 {
		counter.Add(context.Background(), 1,
			metric.WithAttributes(attribute.String("http.method", "GET")))
	}
	for range 3 {
		counter.Add(context.Background(), 1,
			metric.WithAttributes(attribute.String("http.method", "POST")))
	}

	f.shutdownMeter(mp)
	f.waitForDistinctMetricSeries("counter-svc", 2)
	f.waitForMetricEvents("counter-svc", 2)

	// meta.* stamping is present: metric_kind=sum, temporality=cumulative, monotonic=1.
	rows, err := f.st.ReaderDB().QueryContext(f.ctx, `
		SELECT DISTINCT metric_kind, metric_temporality, metric_monotonic, attributes
		  FROM events
		 WHERE signal_type = 'metric' AND service_name = ? AND name = ?
	`, "counter-svc", "requests.total")
	if err != nil {
		t.Fatalf("events query: %v", err)
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
			t.Errorf("metric_kind: want sum, got %q", kind)
		}
		if temp != "cumulative" {
			t.Errorf("metric_temporality: want cumulative, got %q", temp)
		}
		if mono != 1 {
			t.Errorf("metric_monotonic: want 1, got %d", mono)
		}
		seen++
	}
	if seen != 2 {
		t.Errorf("distinct (kind,temp,mono,attrs) count: want 2, got %d", seen)
	}

	// Each (method) series' last-seen cumulative value matches.
	perMethod := map[string]float64{}
	pointRows, err := f.st.ReaderDB().QueryContext(f.ctx, `
		SELECT attributes, value
		  FROM events
		 WHERE signal_type = 'metric' AND service_name = ? AND name = ?
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

	time.Sleep(250 * time.Millisecond)
	f.shutdownMeter(mp)
	f.waitForMetricEvents("gauge-svc", 1)

	// metric_kind=gauge; monotonic NULL; temporality NULL (gauge has no
	// temporality, so buildAttrs didn't stamp the key).
	var kind string
	var temp sql.NullString
	var mono sql.NullInt64
	if err := f.st.ReaderDB().QueryRowContext(f.ctx, `
		SELECT metric_kind, metric_temporality, metric_monotonic
		  FROM events
		 WHERE signal_type = 'metric' AND service_name = ? AND name = ?
		 ORDER BY time_ns DESC LIMIT 1
	`, "gauge-svc", "memory.used").Scan(&kind, &temp, &mono); err != nil {
		t.Fatalf("event row: %v", err)
	}
	if kind != "gauge" {
		t.Errorf("metric_kind: want gauge, got %q", kind)
	}
	if temp.Valid && temp.String != "" {
		t.Errorf("metric_temporality for gauge: want NULL/empty, got %q", temp.String)
	}
	if mono.Valid {
		t.Errorf("metric_monotonic for gauge: want NULL, got %d", mono.Int64)
	}

	var value float64
	if err := f.st.ReaderDB().QueryRowContext(f.ctx, `
		SELECT value FROM events
		 WHERE signal_type = 'metric' AND service_name = ? AND name = ?
		 ORDER BY time_ns DESC LIMIT 1
	`, "gauge-svc", "memory.used").Scan(&value); err != nil {
		t.Fatalf("latest value: %v", err)
	}
	if value < 1_000_000 || value > 2_000_000 {
		t.Errorf("gauge value out of expected band: %v", value)
	}
}

// ---------------------------------------------------------------------------
// Attribute catalog: metric attribute keys land under signal_type=metric so
// /api/fields?dataset=metric returns them. meta.* keys must be filtered out.
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
	f.waitForMetricEvents("cat-svc", 1)

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

	// meta.* keys are reserved and must NOT appear in the attribute_keys
	// catalog — they're system-stamped, not user attributes.
	var metaKeyCount int
	if err := f.st.ReaderDB().QueryRowContext(f.ctx,
		`SELECT COUNT(*) FROM attribute_keys WHERE key LIKE 'meta.%'`).Scan(&metaKeyCount); err != nil {
		t.Fatal(err)
	}
	if metaKeyCount != 0 {
		t.Errorf("meta.* keys should be excluded from attribute_keys catalog; got %d", metaKeyCount)
	}
}

// containsMethod checks whether a JSON attrs column has `"http.method":"<wanted>"`.
func containsMethod(attrs, wanted string) bool {
	needle := fmt.Sprintf(`"http.method":"%s"`, wanted)
	return indexString(attrs, needle) >= 0
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
// /api/query with dataset=metrics — the shared query path works against
// events with the signal_type='metric' preset filter.
// ---------------------------------------------------------------------------

func TestE2E_MetricsQuery(t *testing.T) {
	f := newE2EFixture(t)
	defer f.close()

	mp := f.meterProvider("q-svc")
	counter, err := mp.Meter("e2e").Int64Counter("events")
	if err != nil {
		t.Fatal(err)
	}
	for range 5 {
		counter.Add(context.Background(), 1, metric.WithAttributes(attribute.String("k", "a")))
	}
	for range 3 {
		counter.Add(context.Background(), 1, metric.WithAttributes(attribute.String("k", "b")))
	}
	f.shutdownMeter(mp)
	f.waitForDistinctMetricSeries("q-svc", 2)
	f.waitForMetricEvents("q-svc", 2)

	now := time.Now()
	from := now.Add(-5 * time.Minute)

	// Aggregated MAX(value) group-by k — cumulative sum instruments
	// report their total-to-date, so MAX over a window reflects the
	// last-seen cumulative level per series.
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
	// service_name, name, kind, value, and the event attributes.
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
	for _, c := range []string{"time_ns", "service_name", "name", "kind", "value", "attributes"} {
		if rawRes.columnIdx(c) < 0 {
			t.Errorf("raw metric columns missing %q (got %+v)", c, rawRes.Columns)
		}
	}
}
