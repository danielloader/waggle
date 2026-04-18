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
