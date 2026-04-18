package ingest_test

// End-to-end coverage of the /api/query surface. Every test here drives
// real data through the SDK -> ingest -> sqlite pipeline, then issues
// POST /api/query with varied aggregations + group-bys + windowing and
// asserts the response shape.

import (
	"context"
	"fmt"
	"testing"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
)

// ---------------------------------------------------------------------------
// Aggregations over duration: AVG/MIN/MAX/SUM all compute against
// duration_ns, with values sanity-checked against what we emitted.
// ---------------------------------------------------------------------------

func TestE2E_DurationAggregations(t *testing.T) {
	f := newE2EFixture(t)
	defer f.close()

	tp := f.tracerProvider("agg-svc")
	tr := tp.Tracer("e2e")

	// Emit spans with durations 10ms, 20ms, …, 100ms (10 spans).
	// We sleep the real wall-clock so start/end_time_ns reflect the gap.
	for i := 1; i <= 10; i++ {
		_, span := tr.Start(context.Background(), fmt.Sprintf("op-%d", i))
		time.Sleep(time.Duration(i) * time.Millisecond)
		span.End()
	}
	f.shutdownTracer(tp)
	f.waitForSpanCount("agg-svc", 10)

	now := time.Now()
	from := now.Add(-5 * time.Minute)
	res, err := f.runQueryAPI(map[string]any{
		"dataset": "spans",
		"time_range": map[string]any{
			"from": rfc3339(from), "to": rfc3339(now.Add(5 * time.Second)),
		},
		"select": []map[string]any{
			{"op": "avg", "field": "duration_ms"},
			{"op": "min", "field": "duration_ms"},
			{"op": "max", "field": "duration_ms"},
			{"op": "sum", "field": "duration_ms"},
			{"op": "count"},
		},
		"where": []map[string]any{{"field": "service.name", "op": "=", "value": "agg-svc"}},
	})
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	row := res.Rows[0]
	// Sleep jitter is substantial on CI, but the relationships between
	// aggregates should always hold regardless of scheduler noise:
	//   min <= avg <= max, sum >= max * 1, count = 10.
	avg, _ := toFloat(row[res.columnIdx("avg_duration_ms")])
	minv, _ := toFloat(row[res.columnIdx("min_duration_ms")])
	maxv, _ := toFloat(row[res.columnIdx("max_duration_ms")])
	sum, _ := toFloat(row[res.columnIdx("sum_duration_ms")])
	count, _ := toInt64(row[res.columnIdx("count")])
	if count != 10 {
		t.Errorf("count: want 10, got %d", count)
	}
	if minv > avg || avg > maxv {
		t.Errorf("expected min <= avg <= max, got min=%v avg=%v max=%v", minv, avg, maxv)
	}
	if sum < maxv {
		t.Errorf("sum (%v) < max (%v)?", sum, maxv)
	}
	if minv < 0 || maxv > 500 {
		t.Errorf("duration_ms out of sane range: min=%v max=%v", minv, maxv)
	}
}

// ---------------------------------------------------------------------------
// Percentiles run against a wide duration spread.
// ---------------------------------------------------------------------------

func TestE2E_Percentiles(t *testing.T) {
	f := newE2EFixture(t)
	defer f.close()

	tp := f.tracerProvider("p-svc")
	tr := tp.Tracer("e2e")

	// 20 fast spans around ~1ms, 5 slow ones around ~50ms. p50 should be
	// fast, p95 should jump into the slow tier.
	for i := 0; i < 20; i++ {
		_, span := tr.Start(context.Background(), "fast")
		time.Sleep(1 * time.Millisecond)
		span.End()
	}
	for i := 0; i < 5; i++ {
		_, span := tr.Start(context.Background(), "slow")
		time.Sleep(50 * time.Millisecond)
		span.End()
	}
	f.shutdownTracer(tp)
	f.waitForSpanCount("p-svc", 25)

	now := time.Now()
	from := now.Add(-5 * time.Minute)
	res, err := f.runQueryAPI(map[string]any{
		"dataset": "spans",
		"time_range": map[string]any{
			"from": rfc3339(from), "to": rfc3339(now.Add(5 * time.Second)),
		},
		"select": []map[string]any{
			{"op": "p50", "field": "duration_ms"},
			{"op": "p95", "field": "duration_ms"},
			{"op": "p99", "field": "duration_ms"},
		},
		"where": []map[string]any{{"field": "service.name", "op": "=", "value": "p-svc"}},
	})
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	p50, _ := toFloat(res.Rows[0][res.columnIdx("p50_duration_ms")])
	p95, _ := toFloat(res.Rows[0][res.columnIdx("p95_duration_ms")])
	p99, _ := toFloat(res.Rows[0][res.columnIdx("p99_duration_ms")])
	if p50 >= p95 || p95 > p99+0.01 {
		t.Errorf("expected p50 < p95 <= p99, got p50=%v p95=%v p99=%v", p50, p95, p99)
	}
	if p50 > 10 {
		t.Errorf("p50 should be in the fast tier (~1ms), got %v", p50)
	}
	if p95 < 10 {
		t.Errorf("p95 should fall into the slow tier (~50ms), got %v", p95)
	}
}

// ---------------------------------------------------------------------------
// count_distinct is backed by a SQL COUNT(DISTINCT …) over the JSON
// attribute key. Emit spans with 3 distinct values for `customer.tier`;
// verify the aggregate returns 3.
// ---------------------------------------------------------------------------

func TestE2E_CountDistinct(t *testing.T) {
	f := newE2EFixture(t)
	defer f.close()

	tp := f.tracerProvider("cd-svc")
	tr := tp.Tracer("e2e")

	tiers := []string{"free", "silver", "gold", "gold", "free", "free"}
	for _, tier := range tiers {
		_, span := tr.Start(context.Background(), "op")
		span.SetAttributes(attribute.String("customer.tier", tier))
		span.End()
	}
	f.shutdownTracer(tp)
	f.waitForSpanCount("cd-svc", len(tiers))

	now := time.Now()
	from := now.Add(-5 * time.Minute)
	res, err := f.runQueryAPI(map[string]any{
		"dataset": "spans",
		"time_range": map[string]any{
			"from": rfc3339(from), "to": rfc3339(now.Add(5 * time.Second)),
		},
		"select": []map[string]any{
			{"op": "count_distinct", "field": "customer.tier"},
		},
		"where": []map[string]any{{"field": "service.name", "op": "=", "value": "cd-svc"}},
	})
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	distinct, _ := toInt64(res.Rows[0][res.columnIdx("count_distinct_customer_tier")])
	if distinct != 3 {
		t.Errorf("count_distinct: want 3, got %d", distinct)
	}
}

// ---------------------------------------------------------------------------
// HAVING: filter grouped rows on the aggregated value. Only buckets
// with count >= N survive.
// ---------------------------------------------------------------------------

func TestE2E_HavingFilter(t *testing.T) {
	f := newE2EFixture(t)
	defer f.close()

	tp := f.tracerProvider("having-svc")
	tr := tp.Tracer("e2e")

	// 5 × GET, 3 × POST, 1 × DELETE.
	mix := []struct {
		method string
		count  int
	}{{"GET", 5}, {"POST", 3}, {"DELETE", 1}}
	for _, m := range mix {
		for i := 0; i < m.count; i++ {
			_, span := tr.Start(context.Background(), "http.request")
			span.SetAttributes(attribute.String("http.method", m.method))
			span.End()
		}
	}
	f.shutdownTracer(tp)
	f.waitForSpanCount("having-svc", 9)

	now := time.Now()
	from := now.Add(-5 * time.Minute)
	res, err := f.runQueryAPI(map[string]any{
		"dataset": "spans",
		"time_range": map[string]any{
			"from": rfc3339(from), "to": rfc3339(now.Add(5 * time.Second)),
		},
		"select":   []map[string]any{{"op": "count"}},
		"where":    []map[string]any{{"field": "service.name", "op": "=", "value": "having-svc"}},
		"group_by": []string{"http.method"},
		"having": []map[string]any{
			{"field": "count", "op": ">=", "value": 3},
		},
	})
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	// Only GET (5) and POST (3) should survive HAVING count >= 3.
	got := map[string]int64{}
	for _, row := range res.Rows {
		m, _ := row[res.columnIdx("http_method")].(string)
		c, _ := toInt64(row[res.columnIdx("count")])
		got[m] = c
	}
	if got["GET"] != 5 || got["POST"] != 3 {
		t.Errorf("want {GET:5, POST:3}, got %v", got)
	}
	if _, ok := got["DELETE"]; ok {
		t.Errorf("DELETE should have been filtered out by HAVING: %v", got)
	}
}

// ---------------------------------------------------------------------------
// ORDER BY + LIMIT: top-N rows by count, descending.
// ---------------------------------------------------------------------------

func TestE2E_OrderByTopN(t *testing.T) {
	f := newE2EFixture(t)
	defer f.close()

	tp := f.tracerProvider("top-svc")
	tr := tp.Tracer("e2e")

	// Emit four distinct span names at different counts.
	counts := map[string]int{"alpha": 8, "beta": 4, "gamma": 2, "delta": 1}
	for name, n := range counts {
		for i := 0; i < n; i++ {
			_, span := tr.Start(context.Background(), name)
			span.End()
		}
	}
	f.shutdownTracer(tp)
	f.waitForSpanCount("top-svc", 15)

	now := time.Now()
	from := now.Add(-5 * time.Minute)
	res, err := f.runQueryAPI(map[string]any{
		"dataset": "spans",
		"time_range": map[string]any{
			"from": rfc3339(from), "to": rfc3339(now.Add(5 * time.Second)),
		},
		"select":   []map[string]any{{"op": "count"}},
		"where":    []map[string]any{{"field": "service.name", "op": "=", "value": "top-svc"}},
		"group_by": []string{"name"},
		"order_by": []map[string]any{{"field": "count", "dir": "desc"}},
		"limit":    2,
	})
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	if len(res.Rows) != 2 {
		t.Fatalf("limit=2, got %d rows", len(res.Rows))
	}
	first := res.Rows[0][res.columnIdx("name")]
	second := res.Rows[1][res.columnIdx("name")]
	if first != "alpha" || second != "beta" {
		t.Errorf("top-2 by count desc: want [alpha, beta], got [%v, %v]", first, second)
	}
}

// ---------------------------------------------------------------------------
// Time-window filter: ListTraces with from/to should only return spans
// inside the window.
// ---------------------------------------------------------------------------

func TestE2E_TimeWindowFilter(t *testing.T) {
	f := newE2EFixture(t)
	defer f.close()

	tp := f.tracerProvider("tw-svc")
	tr := tp.Tracer("e2e")

	// Emit one span now; hold the timestamp.
	_, span := tr.Start(context.Background(), "op")
	span.End()
	pivot := time.Now()
	f.shutdownTracer(tp)
	f.waitForSpanCount("tw-svc", 1)

	// A window that ends BEFORE pivot should return zero traces.
	before := pivot.Add(-60 * time.Second)
	url := fmt.Sprintf("/api/traces?service=tw-svc&from=%d&to=%d",
		before.UnixNano(), pivot.Add(-30*time.Second).UnixNano())
	var empty struct {
		Traces []map[string]any `json:"traces"`
	}
	if err := f.getJSON(url, &empty); err != nil {
		t.Fatalf("before-pivot query: %v", err)
	}
	if len(empty.Traces) != 0 {
		t.Errorf("pre-pivot window should be empty, got %d", len(empty.Traces))
	}

	// A window around pivot should return exactly 1.
	around := fmt.Sprintf("/api/traces?service=tw-svc&from=%d&to=%d",
		pivot.Add(-60*time.Second).UnixNano(),
		pivot.Add(60*time.Second).UnixNano())
	var hit struct {
		Traces []map[string]any `json:"traces"`
	}
	if err := f.getJSON(around, &hit); err != nil {
		t.Fatalf("around-pivot query: %v", err)
	}
	if len(hit.Traces) != 1 {
		t.Errorf("around-pivot window want 1 trace, got %d", len(hit.Traces))
	}
}

// ---------------------------------------------------------------------------
// Pagination: ListTraces returns a cursor; fetching the next page yields
// the remaining traces with no overlap.
// ---------------------------------------------------------------------------

func TestE2E_ListTracesPagination(t *testing.T) {
	f := newE2EFixture(t)
	defer f.close()

	tp := f.tracerProvider("page-svc")
	tr := tp.Tracer("e2e")

	// 30 independent traces emitted in a tight loop — deliberately no
	// sleep between them, so several will share a start_time_ns at
	// microsecond resolution. The cursor must compose a tie-breaker
	// (trace_id) so none of those tied rows are skipped at a page
	// boundary.
	const total = 30
	for i := 0; i < total; i++ {
		_, span := tr.Start(context.Background(), "op", trace.WithNewRoot())
		span.End()
	}
	f.shutdownTracer(tp)
	f.waitForSpanCount("page-svc", total)

	seen := map[string]struct{}{}
	cursor := ""
	pages := 0
	for {
		pages++
		if pages > 10 {
			t.Fatal("paged more than 10 times, suspected infinite loop")
		}
		url := "/api/traces?service=page-svc&limit=10"
		if cursor != "" {
			url += "&cursor=" + cursor
		}
		var page struct {
			Traces []struct {
				TraceID string `json:"trace_id"`
			} `json:"traces"`
			NextCursor string `json:"next_cursor"`
		}
		if err := f.getJSON(url, &page); err != nil {
			t.Fatalf("page %d: %v", pages, err)
		}
		for _, tr := range page.Traces {
			if _, dup := seen[tr.TraceID]; dup {
				t.Fatalf("trace %s appeared on two pages", tr.TraceID)
			}
			seen[tr.TraceID] = struct{}{}
		}
		if page.NextCursor == "" {
			break
		}
		cursor = page.NextCursor
	}
	if len(seen) != total {
		t.Errorf("paginated total: want %d, got %d", total, len(seen))
	}
}

// ---------------------------------------------------------------------------
// Bucketed count: time-series rollup with group_by (none), bucket_ms set.
// ---------------------------------------------------------------------------

func TestE2E_BucketedCount(t *testing.T) {
	f := newE2EFixture(t)
	defer f.close()

	tp := f.tracerProvider("bucket-svc")
	tr := tp.Tracer("e2e")

	// 5 spans, clustered in time.
	for i := 0; i < 5; i++ {
		_, span := tr.Start(context.Background(), "op")
		span.End()
	}
	f.shutdownTracer(tp)
	f.waitForSpanCount("bucket-svc", 5)

	now := time.Now()
	from := now.Add(-5 * time.Minute)
	res, err := f.runQueryAPI(map[string]any{
		"dataset": "spans",
		"time_range": map[string]any{
			"from": rfc3339(from), "to": rfc3339(now.Add(5 * time.Second)),
		},
		"select":    []map[string]any{{"op": "count"}},
		"where":     []map[string]any{{"field": "service.name", "op": "=", "value": "bucket-svc"}},
		"bucket_ms": 60_000,
	})
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	// The bucket column should be present; summing counts across buckets
	// should equal the total span count.
	if res.columnIdx("bucket_ns") < 0 && res.columnIdx("bucket") < 0 {
		t.Errorf("expected a bucket_ns / bucket column, got %+v", res.Columns)
	}
	var sum int64
	countIdx := res.columnIdx("count")
	for _, row := range res.Rows {
		c, _ := toInt64(row[countIdx])
		sum += c
	}
	if sum != 5 {
		t.Errorf("bucketed counts should sum to 5, got %d", sum)
	}
}

// ---------------------------------------------------------------------------
// helpers
// ---------------------------------------------------------------------------

func toFloat(v any) (float64, bool) {
	switch n := v.(type) {
	case float64:
		return n, true
	case int64:
		return float64(n), true
	case int:
		return float64(n), true
	}
	return 0, false
}
