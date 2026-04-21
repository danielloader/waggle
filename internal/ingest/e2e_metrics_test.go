package ingest_test

// SDK-driven metric ingest round-trips against the Honeycomb-style folded
// wide-event model. Every OTel metric DataPoint lands in the metric_events
// table, with the metric's name as an attribute key
// (attributes["requests.total"] = 1423). Multiple metrics with the same
// label set at the same moment fold into one row — one MetricEvent
// captures whatever the SDK observed at that instant for that label set.

import (
	"context"
	"fmt"
	"testing"
	"time"

	"go.opentelemetry.io/otel/attribute"
	semattr "go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetrichttp"
	"go.opentelemetry.io/otel/metric"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
	"go.opentelemetry.io/otel/sdk/resource"
)

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

// waitForMetricEvents polls metric_events for at least n rows matching the
// service.
func (f *e2eFixture) waitForMetricEvents(service string, n int) {
	f.t.Helper()
	waitFor(f.t, 3*time.Second, fmt.Sprintf("metric events for %q >= %d", service, n), func() bool {
		var count int
		err := f.st.ReaderDB().QueryRowContext(f.ctx,
			`SELECT COUNT(*) FROM metric_events WHERE service_name = ?`,
			service,
		).Scan(&count)
		return err == nil && count >= n
	})
}

// waitForMetricAttribute polls until at least one metric_events row for the
// service has the named metric attribute populated — i.e. the fold wrote
// it to the row's attributes JSON.
func (f *e2eFixture) waitForMetricAttribute(service, metricName string) {
	f.t.Helper()
	waitFor(f.t, 3*time.Second,
		fmt.Sprintf("metric %q attribute visible for %q", metricName, service),
		func() bool {
			var count int
			err := f.st.ReaderDB().QueryRowContext(f.ctx, `
				SELECT COUNT(*) FROM metric_events
				WHERE service_name = ?
				  AND json_extract(attributes, '$.' || ?) IS NOT NULL`,
				service, `"`+metricName+`"`,
			).Scan(&count)
			return err == nil && count > 0
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
	f.waitForMetricAttribute("counter-svc", "requests.total")

	// Two label sets (GET vs POST) → at least 2 distinct rows. Each row's
	// attributes JSON carries the counter value under key "requests.total"
	// alongside the http.method label.
	rows, err := f.st.ReaderDB().QueryContext(f.ctx, `
		SELECT json_extract(attributes, '$."http.method"'),
		       MAX(json_extract(attributes, '$."requests.total"'))
		FROM metric_events
		WHERE service_name = ? AND json_extract(attributes, '$."requests.total"') IS NOT NULL
		GROUP BY json_extract(attributes, '$."http.method"')
	`, "counter-svc")
	if err != nil {
		t.Fatalf("metric_events query: %v", err)
	}
	defer rows.Close()
	got := map[string]float64{}
	for rows.Next() {
		var method string
		var value float64
		if err := rows.Scan(&method, &value); err != nil {
			t.Fatal(err)
		}
		got[method] = value
	}
	if got["GET"] != 5 {
		t.Errorf("GET cumulative: want 5, got %v", got["GET"])
	}
	if got["POST"] != 3 {
		t.Errorf("POST cumulative: want 3, got %v", got["POST"])
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
	f.waitForMetricAttribute("gauge-svc", "memory.used")

	// Latest gauge sample lives under attributes["memory.used"]. Since the
	// fold merges all metrics at the same (time, labels) tuple, the row's
	// memory.used value matches the callback's last returned number for
	// that time_ns.
	var value float64
	if err := f.st.ReaderDB().QueryRowContext(f.ctx, `
		SELECT json_extract(attributes, '$."memory.used"')
		FROM metric_events
		WHERE service_name = ? AND json_extract(attributes, '$."memory.used"') IS NOT NULL
		ORDER BY time_ns DESC LIMIT 1
	`, "gauge-svc").Scan(&value); err != nil {
		t.Fatalf("latest value: %v", err)
	}
	if value < 1_000_000 || value > 2_000_000 {
		t.Errorf("gauge value out of expected band: %v", value)
	}
}

// ---------------------------------------------------------------------------
// Attribute catalog: metric-name keys get recorded as fields so autocomplete
// surfaces them alongside user attributes. Dimension labels (route,
// status_code) also land under signal_type='metric'. meta.* keys stay out.
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

	// User-attribute dimensions land in the catalog under signal_type=metric.
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

	// The metric name itself ("hits") is recorded as a field — so field
	// autocomplete surfaces it alongside dimensions.
	var hitsCount int
	if err := f.st.ReaderDB().QueryRowContext(f.ctx,
		"SELECT COUNT(*) FROM attribute_keys WHERE signal_type = 'metric' AND service_name = ? AND key = 'hits'",
		"cat-svc").Scan(&hitsCount); err != nil {
		t.Fatal(err)
	}
	if hitsCount == 0 {
		t.Errorf("metric name 'hits' missing from attribute_keys catalog")
	}

	// meta.* keys are reserved, not user-queryable via autocomplete.
	var metaKeyCount int
	if err := f.st.ReaderDB().QueryRowContext(f.ctx,
		`SELECT COUNT(*) FROM attribute_keys WHERE key LIKE 'meta.%'`).Scan(&metaKeyCount); err != nil {
		t.Fatal(err)
	}
	if metaKeyCount != 0 {
		t.Errorf("meta.* keys should be excluded from attribute_keys catalog; got %d", metaKeyCount)
	}
}

// ---------------------------------------------------------------------------
// /api/query with dataset=metrics — the shared query path resolves metric
// names as attribute fields (the Honeycomb-style story): MAX(requests.total)
// is just a MAX on a column (via json_extract fallthrough).
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
	f.waitForMetricAttribute("q-svc", "events")

	now := time.Now()
	from := now.Add(-5 * time.Minute)

	// MAX of the `events` metric grouped by the `k` label. Cumulative sum
	// counters report their level-to-date, so MAX over a window for each
	// series equals the total adds we made.
	res, err := f.runQueryAPI(map[string]any{
		"dataset":    "metrics",
		"time_range": map[string]any{"from": rfc3339(from), "to": rfc3339(now.Add(5 * time.Second))},
		"select":     []map[string]any{{"op": "max", "field": "events"}},
		"where":      []map[string]any{{"field": "service.name", "op": "=", "value": "q-svc"}},
		"group_by":   []string{"k"},
	})
	if err != nil {
		t.Fatalf("metrics query: %v", err)
	}
	got := map[string]float64{}
	kIdx := res.columnIdx("k")
	maxIdx := res.columnIdx("max_events")
	for _, row := range res.Rows {
		key, _ := row[kIdx].(string)
		v, _ := toFloat(row[maxIdx])
		got[key] = v
	}
	if got["a"] != 5 {
		t.Errorf("max(events) where k=a: want 5, got %v", got["a"])
	}
	if got["b"] != 3 {
		t.Errorf("max(events) where k=b: want 3, got %v", got["b"])
	}

	// Raw-rows mode — one row per fold bucket, surfaces the attribute blob.
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
	for _, c := range []string{"time_ns", "service_name", "dataset", "attributes"} {
		if rawRes.columnIdx(c) < 0 {
			t.Errorf("raw metric columns missing %q (got %+v)", c, rawRes.Columns)
		}
	}
}
