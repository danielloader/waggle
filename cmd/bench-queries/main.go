// bench-queries runs a labelled battery of SQL queries against a waggle SQLite
// database and prints a timing table. Queries mirror the exact SQL shapes the
// API layer produces so timings reflect real user-facing latency.
//
// Usage:
//
//	go run ./cmd/bench-queries --db ./waggle-test.db
package main

import (
	"context"
	"database/sql"
	"flag"
	"fmt"
	"log"
	"strings"
	"text/tabwriter"
	"time"

	sqstore "github.com/danielloader/waggle/internal/store/sqlite"
)

type queryCase struct {
	label string
	sql   string
	args  []any
}

func main() {
	dbPath := flag.String("db", "./waggle-test.db", "SQLite database path")
	runs := flag.Int("runs", 3, "Number of runs per query (median reported)")
	flag.Parse()

	ctx := context.Background()
	st, err := sqstore.Open(ctx, *dbPath)
	if err != nil {
		log.Fatalf("open store: %v", err)
	}
	defer st.Close()

	db := st.ReaderDB()

	// Discover data bounds so time-range queries cover real rows.
	var minNS, maxNS int64
	if err := db.QueryRowContext(ctx,
		`SELECT MIN(time_ns), MAX(time_ns) FROM events`).Scan(&minNS, &maxNS); err != nil {
		log.Fatalf("scan time bounds: %v", err)
	}
	spanNS := maxNS - minNS
	midNS := minNS + spanNS/2
	qtrNS := spanNS / 4

	// Bucket size: 1-hour buckets (in nanoseconds) for time-series queries.
	const hourNS = int64(time.Hour)

	// A representative service name present in the data.
	const svc = "api-gateway"

	// Pre-build a trace_id BLOB sample for the single-trace lookup.
	var sampleTraceID []byte
	_ = db.QueryRowContext(ctx,
		`SELECT trace_id FROM events WHERE signal_type='span' AND parent_span_id IS NULL LIMIT 1`,
	).Scan(&sampleTraceID)

	cases := []queryCase{
		// ------------------------------------------------------------------
		// 1. Service summary (ListServices) — spans by service with error rate
		// ------------------------------------------------------------------
		{
			label: "LIST services (span count + error rate per service)",
			sql: `SELECT service_name,
				COUNT(*) AS total,
				SUM(CASE WHEN status_code = 2 THEN 1 ELSE 0 END) AS errs
				FROM events WHERE signal_type = 'span'
				GROUP BY service_name ORDER BY total DESC`,
		},

		// ------------------------------------------------------------------
		// 2. Trace listing — root spans + correlated span-count + has_error
		// ------------------------------------------------------------------
		{
			label: "LIST traces (root spans + span_count correlated subquery, LIMIT 50)",
			sql: `WITH roots AS (
				SELECT trace_id, span_id, service_name, name, time_ns, end_time_ns
				FROM events
				WHERE signal_type = 'span' AND parent_span_id IS NULL
				ORDER BY time_ns DESC, trace_id DESC LIMIT 50)
			SELECT r.trace_id, r.service_name, r.name, r.time_ns,
				COALESCE(r.end_time_ns - r.time_ns, 0) AS duration_ns,
				(SELECT COUNT(*) FROM events e WHERE e.signal_type='span' AND e.trace_id=r.trace_id) AS span_count,
				COALESCE((SELECT 1 FROM events e WHERE e.signal_type='span' AND e.trace_id=r.trace_id AND e.status_code=2 LIMIT 1), 0) AS has_error
			FROM roots r ORDER BY r.time_ns DESC, r.trace_id DESC`,
		},

		// ------------------------------------------------------------------
		// 3. COUNT all spans (full filtered scan)
		// ------------------------------------------------------------------
		{
			label: "COUNT(*) all spans",
			sql:   `SELECT COUNT(*) FROM events WHERE signal_type = 'span'`,
		},

		// ------------------------------------------------------------------
		// 4. COUNT all logs
		// ------------------------------------------------------------------
		{
			label: "COUNT(*) all logs",
			sql:   `SELECT COUNT(*) FROM events WHERE signal_type = 'log'`,
		},

		// ------------------------------------------------------------------
		// 5. COUNT all metric_events
		// ------------------------------------------------------------------
		{
			label: "COUNT(*) all metric events",
			sql:   `SELECT COUNT(*) FROM metric_events`,
		},

		// ------------------------------------------------------------------
		// 6. GROUP BY service_name — span count per service
		// ------------------------------------------------------------------
		{
			label: "COUNT(*) spans GROUP BY service_name",
			sql: `SELECT service_name, COUNT(*) AS cnt
				FROM events WHERE signal_type = 'span'
				GROUP BY service_name ORDER BY cnt DESC`,
		},

		// ------------------------------------------------------------------
		// 7. GROUP BY http_route — uses idx_events_http_route
		// ------------------------------------------------------------------
		{
			label: "COUNT(*) spans GROUP BY http_route",
			sql: `SELECT http_route, COUNT(*) AS cnt
				FROM events WHERE signal_type = 'span' AND http_route IS NOT NULL
				GROUP BY http_route ORDER BY cnt DESC`,
		},

		// ------------------------------------------------------------------
		// 8. GROUP BY span_kind
		// ------------------------------------------------------------------
		{
			label: "COUNT(*) spans GROUP BY span_kind",
			sql: `SELECT span_kind, COUNT(*) AS cnt
				FROM events WHERE signal_type = 'span'
				GROUP BY span_kind ORDER BY cnt DESC`,
		},

		// ------------------------------------------------------------------
		// 9. Error count and rate per service
		// ------------------------------------------------------------------
		{
			label: "Error rate per service (status_code=2)",
			sql: `SELECT service_name,
				COUNT(*) AS total,
				SUM(CASE WHEN status_code = 2 THEN 1 ELSE 0 END) AS errors,
				CAST(SUM(CASE WHEN status_code = 2 THEN 1 ELSE 0 END) AS REAL) / COUNT(*) AS error_rate
				FROM events WHERE signal_type = 'span'
				GROUP BY service_name ORDER BY error_rate DESC`,
		},

		// ------------------------------------------------------------------
		// 10. P95 latency by service — uses UDF percentile()
		// ------------------------------------------------------------------
		{
			label: "P50/P95/P99 duration_ns by service (percentile UDF)",
			sql: `SELECT service_name,
				percentile(duration_ns, 0.50) AS p50_ns,
				percentile(duration_ns, 0.95) AS p95_ns,
				percentile(duration_ns, 0.99) AS p99_ns
				FROM events WHERE signal_type = 'span'
				GROUP BY service_name ORDER BY p95_ns DESC`,
		},

		// ------------------------------------------------------------------
		// 11. Time-bucketed span count (1-hour buckets, full window)
		// ------------------------------------------------------------------
		{
			label: "COUNT(*) spans bucketed by 1h over full window",
			sql: fmt.Sprintf(`SELECT (time_ns / %d) * %d AS bucket_ns, COUNT(*) AS cnt
				FROM events WHERE signal_type='span' AND time_ns >= ? AND time_ns < ?
				GROUP BY bucket_ns ORDER BY bucket_ns ASC`, hourNS, hourNS),
			args: []any{minNS, maxNS},
		},

		// ------------------------------------------------------------------
		// 12. Time-bucketed + GROUP BY service (multi-series chart)
		// ------------------------------------------------------------------
		{
			label: "COUNT(*) spans bucketed 1h + GROUP BY service (multi-series)",
			sql: fmt.Sprintf(`SELECT (time_ns / %d) * %d AS bucket_ns, service_name, COUNT(*) AS cnt
				FROM events WHERE signal_type='span' AND time_ns >= ? AND time_ns < ?
				GROUP BY bucket_ns, service_name ORDER BY bucket_ns ASC`, hourNS, hourNS),
			args: []any{minNS, maxNS},
		},

		// ------------------------------------------------------------------
		// 13. P95 latency bucketed by 1h (time-series percentile)
		// ------------------------------------------------------------------
		{
			label: "P95 duration_ns bucketed 1h (percentile time-series)",
			sql: fmt.Sprintf(`SELECT (time_ns / %d) * %d AS bucket_ns,
				percentile(duration_ns, 0.95) AS p95_ns
				FROM events WHERE signal_type='span' AND time_ns >= ? AND time_ns < ?
				GROUP BY bucket_ns ORDER BY bucket_ns ASC`, hourNS, hourNS),
			args: []any{minNS, maxNS},
		},

		// ------------------------------------------------------------------
		// 14. Raw rows: latest 500 spans (typical events-list page load)
		// ------------------------------------------------------------------
		{
			label: "Raw span rows LIMIT 500 ORDER BY time DESC",
			sql: `SELECT hex(trace_id), hex(span_id), hex(parent_span_id),
				service_name, name, span_kind, time_ns, duration_ns,
				COALESCE(status_code, 0), attributes
				FROM events WHERE signal_type = 'span'
				ORDER BY time_ns DESC LIMIT 500`,
		},

		// ------------------------------------------------------------------
		// 15. Filtered by service + time range (quarter of the window)
		// ------------------------------------------------------------------
		{
			label: "COUNT(*) spans filtered by service + quarter-window time range",
			sql: `SELECT COUNT(*) FROM events
				WHERE signal_type = 'span' AND service_name = ? AND time_ns >= ? AND time_ns < ?`,
			args: []any{svc, midNS - qtrNS, midNS + qtrNS},
		},

		// ------------------------------------------------------------------
		// 16. Filtered by http_route exact match
		// ------------------------------------------------------------------
		{
			label: "COUNT(*) spans WHERE http_route = '/users'",
			sql: `SELECT COUNT(*) FROM events
				WHERE signal_type = 'span' AND http_route = '/users'`,
		},

		// ------------------------------------------------------------------
		// 17. Single trace fetch (span waterfall) — uses idx_events_trace
		// ------------------------------------------------------------------
		{
			label: "GET single trace (all spans for one trace_id)",
			sql: `SELECT event_id, time_ns, end_time_ns, service_name, name,
				span_kind, COALESCE(status_code, 0), attributes, hex(trace_id), hex(span_id), hex(parent_span_id)
				FROM events WHERE signal_type = 'span' AND trace_id = ?`,
			args: []any{sampleTraceID},
		},

		// ------------------------------------------------------------------
		// 18. Log events: COUNT by severity
		// ------------------------------------------------------------------
		{
			label: "COUNT(*) logs GROUP BY severity_text",
			sql: `SELECT severity_text, COUNT(*) AS cnt
				FROM events WHERE signal_type = 'log'
				GROUP BY severity_text ORDER BY cnt DESC`,
		},

		// ------------------------------------------------------------------
		// 19. Log raw rows LIMIT 500
		// ------------------------------------------------------------------
		{
			label: "Raw log rows LIMIT 500 ORDER BY time DESC",
			sql: `SELECT time_ns, service_name, severity_number, severity_text, body, attributes
				FROM events WHERE signal_type = 'log'
				ORDER BY time_ns DESC LIMIT 500`,
		},

		// ------------------------------------------------------------------
		// 20. FTS log search
		// ------------------------------------------------------------------
		{
			label: "FTS log search: body MATCH 'payment'",
			sql: `SELECT e.time_ns, e.service_name, e.severity_text, e.body
				FROM events e
				JOIN events_fts ON events_fts.rowid = e.event_id
				WHERE events_fts MATCH 'payment' AND e.signal_type = 'log'
				ORDER BY e.time_ns DESC LIMIT 100`,
		},

		// ------------------------------------------------------------------
		// 21. Metric COUNT by service
		// ------------------------------------------------------------------
		{
			label: "COUNT(*) metric_events GROUP BY service_name",
			sql: `SELECT service_name, COUNT(*) AS cnt
				FROM metric_events GROUP BY service_name ORDER BY cnt DESC`,
		},

		// ------------------------------------------------------------------
		// 22. Metric aggregation: MAX(requests.total) by service
		// ------------------------------------------------------------------
		{
			label: "MAX(requests.total) metric GROUP BY service_name (json_extract)",
			sql: `SELECT service_name,
				MAX(CAST(json_extract(attributes, '$."requests.total"') AS INTEGER)) AS max_requests,
				AVG(CAST(json_extract(attributes, '$."cpu.utilization"') AS REAL)) AS avg_cpu
				FROM metric_events GROUP BY service_name ORDER BY max_requests DESC`,
		},

		// ------------------------------------------------------------------
		// 23. Metric time-series: AVG(cpu.utilization) bucketed 1h
		// ------------------------------------------------------------------
		{
			label: "AVG(cpu.utilization) metric bucketed 1h (json_extract)",
			sql: fmt.Sprintf(`SELECT (time_ns / %d) * %d AS bucket_ns,
				AVG(CAST(json_extract(attributes, '$."cpu.utilization"') AS REAL)) AS avg_cpu
				FROM metric_events WHERE time_ns >= ? AND time_ns < ?
				GROUP BY bucket_ns ORDER BY bucket_ns ASC`, hourNS, hourNS),
			args: []any{minNS, maxNS},
		},

		// ------------------------------------------------------------------
		// 24. json_extract filter: WHERE component = 'middleware'
		// ------------------------------------------------------------------
		{
			label: "COUNT(*) spans WHERE json_extract(attributes, '$.component') = 'middleware'",
			sql: `SELECT COUNT(*) FROM events
				WHERE signal_type = 'span'
				AND json_extract(attributes, '$."component"') = 'middleware'`,
		},

		// ------------------------------------------------------------------
		// 25. COUNT DISTINCT trace_id (cardinality)
		// ------------------------------------------------------------------
		{
			label: "COUNT(DISTINCT trace_id) — trace cardinality",
			sql:   `SELECT COUNT(DISTINCT trace_id) FROM events WHERE signal_type = 'span'`,
		},

		// ------------------------------------------------------------------
		// 26. Root-span only (idx_events_roots partial index)
		// ------------------------------------------------------------------
		{
			label: "COUNT(*) root spans only (partial index on parent_span_id IS NULL)",
			sql: `SELECT COUNT(*) FROM events
				WHERE signal_type = 'span' AND parent_span_id IS NULL`,
		},

		// ------------------------------------------------------------------
		// 27. Slowest spans (ORDER BY duration_ns DESC LIMIT 100)
		// ------------------------------------------------------------------
		{
			label: "Top-100 slowest spans (ORDER BY duration_ns DESC)",
			sql: `SELECT service_name, name, duration_ns, time_ns, attributes
				FROM events WHERE signal_type = 'span'
				ORDER BY duration_ns DESC LIMIT 100`,
		},

		// ------------------------------------------------------------------
		// 28. Error spans across all services
		// ------------------------------------------------------------------
		{
			label: "COUNT(*) + AVG duration of ERROR spans (status_code=2) by service",
			sql: `SELECT service_name, COUNT(*) AS err_count,
				AVG(duration_ns) AS avg_duration_ns
				FROM events WHERE signal_type = 'span' AND status_code = 2
				GROUP BY service_name ORDER BY err_count DESC`,
		},

		// ------------------------------------------------------------------
		// 29. Field catalog (attribute_keys)
		// ------------------------------------------------------------------
		{
			label: "LIST attribute_keys for spans (field catalog)",
			sql: `SELECT key, value_type, count FROM attribute_keys
				WHERE signal_type = 'span' ORDER BY count DESC LIMIT 100`,
		},

		// ------------------------------------------------------------------
		// 30. Heavy: COUNT(*) all events regardless of signal type
		// ------------------------------------------------------------------
		{
			label: "COUNT(*) entire events table (all signals)",
			sql:   `SELECT COUNT(*) FROM events`,
		},
	}

	type result struct {
		label   string
		timings []time.Duration
		rows    int64
	}

	results := make([]result, len(cases))

	for i, c := range cases {
		r := result{label: c.label}
		for run := range *runs {
			t0 := time.Now()
			n, err := runQuery(ctx, db, c.sql, c.args)
			elapsed := time.Since(t0)
			if err != nil {
				log.Printf("[%d] %s: ERROR: %v", i+1, c.label, err)
				break
			}
			if run == 0 {
				r.rows = n
			}
			r.timings = append(r.timings, elapsed)
		}
		results[i] = r
		med := median(r.timings)
		fmt.Printf("[%2d/%d] %-70s  rows=%-8d  %s\n",
			i+1, len(cases), truncate(c.label, 70), r.rows, med.Round(time.Millisecond))
	}

	// Summary table
	fmt.Println()
	fmt.Println(strings.Repeat("=", 110))
	fmt.Println("QUERY BENCHMARK SUMMARY")
	fmt.Println(strings.Repeat("=", 110))
	w := tabwriter.NewWriter(nil, 0, 0, 2, ' ', 0)
	w = tabwriter.NewWriter(
		// Wrap to stdout
		writerFn(func(p []byte) (int, error) { fmt.Print(string(p)); return len(p), nil }),
		0, 0, 2, ' ', 0,
	)
	fmt.Fprintln(w, "#\tLabel\tRows\tMin\tMedian\tMax")
	fmt.Fprintln(w, "-\t-----\t----\t---\t------\t---")
	for i, r := range results {
		if len(r.timings) == 0 {
			fmt.Fprintf(w, "%d\t%s\t%d\tERROR\tERROR\tERROR\n",
				i+1, truncate(r.label, 72), r.rows)
			continue
		}
		mn, med, mx := stats(r.timings)
		fmt.Fprintf(w, "%d\t%s\t%d\t%s\t%s\t%s\n",
			i+1, truncate(r.label, 72), r.rows,
			mn.Round(time.Millisecond),
			med.Round(time.Millisecond),
			mx.Round(time.Millisecond))
	}
	w.Flush()
}

// runQuery executes a SELECT, drains the rows, and returns the row count.
func runQuery(ctx context.Context, db *sql.DB, query string, args []any) (int64, error) {
	rows, err := db.QueryContext(ctx, query, args...)
	if err != nil {
		return 0, err
	}
	defer rows.Close()
	var n int64
	for rows.Next() {
		n++
	}
	return n, rows.Err()
}

func median(d []time.Duration) time.Duration {
	if len(d) == 0 {
		return 0
	}
	sorted := append([]time.Duration(nil), d...)
	sortDurations(sorted)
	return sorted[len(sorted)/2]
}

func stats(d []time.Duration) (min, med, max time.Duration) {
	sorted := append([]time.Duration(nil), d...)
	sortDurations(sorted)
	return sorted[0], sorted[len(sorted)/2], sorted[len(sorted)-1]
}

func sortDurations(d []time.Duration) {
	for i := 1; i < len(d); i++ {
		for j := i; j > 0 && d[j] < d[j-1]; j-- {
			d[j], d[j-1] = d[j-1], d[j]
		}
	}
}

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n-1] + "…"
}

type writerFn func([]byte) (int, error)

func (f writerFn) Write(p []byte) (int, error) { return f(p) }
