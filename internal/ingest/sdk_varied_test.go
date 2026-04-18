package ingest_test

// TestSDK_TracesVariedAttributes exercises the schema's load-bearing pieces
// (generated-column indexes, attribute_keys catalog counts, attribute_values
// sampler, percentile UDF) with a realistic mix of spans across services.

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"testing"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"

	"github.com/danielloader/waggle/internal/store"
)

func TestSDK_TracesVariedAttributes(t *testing.T) {
	f := newFixture(t)
	defer f.close()

	// --- emit spans across three services ------------------------------------

	tpAPI := newTracerProvider(t, f, "api-gateway")
	tpPay := newTracerProvider(t, f, "payments")
	tpDB := newTracerProvider(t, f, "db-worker")
	defer shutdown(tpAPI)
	defer shutdown(tpPay)
	defer shutdown(tpDB)

	trAPI := tpAPI.Tracer("int-test")
	trPay := tpPay.Tracer("int-test")
	trDB := tpDB.Tracer("int-test")

	// api-gateway: 20 HTTP spans.
	// 4 routes × 3 methods = 12 unique (route, method) combos, cycled to 20.
	routes := []string{"/users", "/orders", "/products", "/health"}
	methods := []string{"GET", "POST", "DELETE"}
	statusCodes := []int{200, 201, 400, 404, 500}
	tiers := []string{"bronze", "silver", "gold"}
	for i := range 20 {
		_, span := trAPI.Start(f.ctx, fmt.Sprintf("%s %s", methods[i%3], routes[i%4]))
		code := statusCodes[i%len(statusCodes)]
		span.SetAttributes(
			attribute.String("http.request.method", methods[i%3]),
			attribute.String("http.route", routes[i%4]),
			attribute.String("url.path", routes[i%4]),
			attribute.Int("http.response.status_code", code),
			attribute.String("customer.tier", tiers[i%3]),
		)
		if code >= 500 {
			span.SetStatus(codes.Error, "internal")
		}
		// Vary duration so percentile has a real distribution.
		time.Sleep(time.Duration(i%5+1) * time.Millisecond)
		span.End()
	}

	// payments: 15 RPC spans.
	rpcSvcs := []string{"PaymentService", "RefundService", "QuoteService"}
	rpcMethods := []string{"Create", "Get", "List", "Cancel"}
	for i := range 15 {
		_, span := trPay.Start(f.ctx, fmt.Sprintf("%s/%s", rpcSvcs[i%3], rpcMethods[i%4]))
		span.SetAttributes(
			attribute.String("rpc.service", rpcSvcs[i%3]),
			attribute.String("rpc.method", rpcMethods[i%4]),
			attribute.String("customer.tier", tiers[i%3]),
		)
		if i%7 == 0 {
			span.SetStatus(codes.Error, "payment declined")
		}
		span.End()
	}

	// db-worker: 15 DB spans across 3 systems.
	dbSystems := []string{"postgresql", "redis", "mysql"}
	for i := range 15 {
		sys := dbSystems[i%3]
		_, span := trDB.Start(f.ctx, fmt.Sprintf("%s query", sys))
		span.SetAttributes(
			attribute.String("db.system", sys),
			attribute.String("db.statement", fmt.Sprintf("SELECT * FROM t_%d", i)),
		)
		if i%5 == 0 {
			span.SetStatus(codes.Error, "connection refused")
		}
		span.End()
	}

	// Flush all three providers.
	shutdown(tpAPI)
	shutdown(tpPay)
	shutdown(tpDB)

	// --- wait for ingest to settle ------------------------------------------

	waitFor(t, 5*time.Second, "50 spans across 3 services", func() bool {
		svcs, err := f.st.ListServices(f.ctx)
		if err != nil {
			return false
		}
		byName := map[string]int64{}
		for _, s := range svcs {
			byName[s.ServiceName] = s.SpanCount
		}
		return byName["api-gateway"] == 20 && byName["payments"] == 15 && byName["db-worker"] == 15
	})

	// --- services + error rates ---------------------------------------------

	svcs, err := f.st.ListServices(f.ctx)
	if err != nil {
		t.Fatalf("ListServices: %v", err)
	}
	errRate := map[string]float64{}
	errCount := map[string]int64{}
	for _, s := range svcs {
		errRate[s.ServiceName] = s.ErrorRate
		errCount[s.ServiceName] = s.ErrorCount
	}
	// api-gateway: status_codes cycle through [200,201,400,404,500] for 20 rows,
	// so indices 4,9,14,19 (i%5==4) hit 500 → 4 errors.
	if errCount["api-gateway"] != 4 {
		t.Errorf("api-gateway error count: want 4, got %d", errCount["api-gateway"])
	}
	// payments: i%7==0 for i in {0,7,14} → 3 errors.
	if errCount["payments"] != 3 {
		t.Errorf("payments error count: want 3, got %d", errCount["payments"])
	}
	// db-worker: i%5==0 for i in {0,5,10} → 3 errors.
	if errCount["db-worker"] != 3 {
		t.Errorf("db-worker error count: want 3, got %d", errCount["db-worker"])
	}

	// --- attribute_keys catalog --------------------------------------------

	apiFields, err := f.st.ListFields(f.ctx, store.FieldFilter{SignalType: "span", Service: "api-gateway"})
	if err != nil {
		t.Fatalf("ListFields api-gateway: %v", err)
	}
	apiCount := map[string]int64{}
	for _, fi := range apiFields {
		apiCount[fi.Key] = fi.Count
	}
	// Each of the 20 api-gateway spans carries http.route, http.request.method,
	// http.response.status_code, url.path, customer.tier — each should show up
	// 20 times in the catalog.
	for _, key := range []string{"http.route", "http.request.method", "http.response.status_code", "url.path", "customer.tier"} {
		if apiCount[key] != 20 {
			t.Errorf("attribute_keys count for %q on api-gateway: want 20, got %d", key, apiCount[key])
		}
	}
	// db.system must not leak into api-gateway.
	if _, leaked := apiCount["db.system"]; leaked {
		t.Errorf("api-gateway catalog leaked db.system key: %+v", apiCount)
	}

	dbFields, err := f.st.ListFields(f.ctx, store.FieldFilter{SignalType: "span", Service: "db-worker"})
	if err != nil {
		t.Fatalf("ListFields db-worker: %v", err)
	}
	dbCount := map[string]int64{}
	for _, fi := range dbFields {
		dbCount[fi.Key] = fi.Count
	}
	if dbCount["db.system"] != 15 {
		t.Errorf("db-worker catalog db.system count: want 15, got %d", dbCount["db.system"])
	}
	if dbCount["db.statement"] != 15 {
		t.Errorf("db-worker catalog db.statement count: want 15, got %d", dbCount["db.statement"])
	}

	// --- attribute_values sampler ------------------------------------------

	routeValues, err := f.st.ListFieldValues(f.ctx, store.ValueFilter{
		SignalType: "span", Service: "api-gateway", Key: "http.route",
	})
	if err != nil {
		t.Fatalf("ListFieldValues http.route: %v", err)
	}
	if len(routeValues) != 4 {
		t.Errorf("http.route distinct values: want 4, got %d (%v)", len(routeValues), routeValues)
	}
	assertSameSet(t, "http.route values", routeValues, []string{"/users", "/orders", "/products", "/health"})

	tierValues, err := f.st.ListFieldValues(f.ctx, store.ValueFilter{
		SignalType: "span", Service: "api-gateway", Key: "customer.tier",
	})
	if err != nil {
		t.Fatalf("ListFieldValues customer.tier: %v", err)
	}
	assertSameSet(t, "customer.tier values", tierValues, []string{"bronze", "silver", "gold"})

	// Status codes stored as integers but stringified in the sampler.
	statusValues, err := f.st.ListFieldValues(f.ctx, store.ValueFilter{
		SignalType: "span", Service: "api-gateway", Key: "http.response.status_code",
	})
	if err != nil {
		t.Fatalf("ListFieldValues http.response.status_code: %v", err)
	}
	assertSameSet(t, "http.response.status_code values", statusValues,
		[]string{"200", "201", "400", "404", "500"})

	dbSysValues, err := f.st.ListFieldValues(f.ctx, store.ValueFilter{
		SignalType: "span", Service: "db-worker", Key: "db.system",
	})
	if err != nil {
		t.Fatalf("ListFieldValues db.system: %v", err)
	}
	assertSameSet(t, "db.system values", dbSysValues, []string{"postgresql", "redis", "mysql"})

	// --- generated-column indexes are populated and queryable ---------------

	db := f.st.ReaderDB()
	// http_route generated column should reflect the JSON attribute.
	{
		rows, err := db.QueryContext(f.ctx, `
			SELECT http_route, COUNT(*)
			FROM spans
			WHERE service_name = 'api-gateway' AND http_route IS NOT NULL
			GROUP BY http_route ORDER BY http_route`)
		if err != nil {
			t.Fatalf("http_route query: %v", err)
		}
		defer rows.Close()
		got := map[string]int64{}
		for rows.Next() {
			var route string
			var n int64
			if err := rows.Scan(&route, &n); err != nil {
				t.Fatal(err)
			}
			got[route] = n
		}
		// 20 spans cycling through 4 routes → 5 each.
		for _, r := range []string{"/users", "/orders", "/products", "/health"} {
			if got[r] != 5 {
				t.Errorf("http_route=%q count: want 5, got %d", r, got[r])
			}
		}
	}

	// http_status_code generated column: integer-typed, indexed.
	{
		var n200, n500 int64
		if err := db.QueryRowContext(f.ctx, `
			SELECT
			  SUM(CASE WHEN http_status_code = 200 THEN 1 ELSE 0 END),
			  SUM(CASE WHEN http_status_code = 500 THEN 1 ELSE 0 END)
			FROM spans WHERE service_name = 'api-gateway'`).Scan(&n200, &n500); err != nil {
			t.Fatal(err)
		}
		if n200 != 4 || n500 != 4 {
			t.Errorf("http_status_code counts: want 200→4, 500→4; got %d / %d", n200, n500)
		}
	}

	// db_system generated column.
	{
		rows, err := db.QueryContext(f.ctx, `
			SELECT db_system, COUNT(*) FROM spans
			WHERE service_name = 'db-worker' AND db_system IS NOT NULL
			GROUP BY db_system ORDER BY db_system`)
		if err != nil {
			t.Fatal(err)
		}
		defer rows.Close()
		got := map[string]int64{}
		for rows.Next() {
			var sys string
			var n int64
			if err := rows.Scan(&sys, &n); err != nil {
				t.Fatal(err)
			}
			got[sys] = n
		}
		for _, s := range []string{"postgresql", "redis", "mysql"} {
			if got[s] != 5 {
				t.Errorf("db_system=%q count: want 5, got %d", s, got[s])
			}
		}
	}

	// rpc_service generated column.
	{
		rows, err := db.QueryContext(f.ctx, `
			SELECT rpc_service, COUNT(*) FROM spans
			WHERE service_name = 'payments' AND rpc_service IS NOT NULL
			GROUP BY rpc_service`)
		if err != nil {
			t.Fatal(err)
		}
		defer rows.Close()
		got := map[string]int64{}
		for rows.Next() {
			var svc string
			var n int64
			if err := rows.Scan(&svc, &n); err != nil {
				t.Fatal(err)
			}
			got[svc] = n
		}
		// 15 payments spans cycling through 3 rpc_svcs → 5 each.
		for _, s := range []string{"PaymentService", "RefundService", "QuoteService"} {
			if got[s] != 5 {
				t.Errorf("rpc_service=%q count: want 5, got %d", s, got[s])
			}
		}
	}

	// --- percentile UDF over duration_ns ------------------------------------
	{
		var p50, p95, p99 float64
		if err := db.QueryRowContext(f.ctx, `
			SELECT percentile(duration_ns, 0.50),
			       percentile(duration_ns, 0.95),
			       percentile(duration_ns, 0.99)
			FROM spans WHERE service_name = 'api-gateway'`).Scan(&p50, &p95, &p99); err != nil {
			t.Fatal(err)
		}
		if !(p50 > 0 && p95 >= p50 && p99 >= p95) {
			t.Errorf("percentile sanity failed: p50=%v p95=%v p99=%v", p50, p95, p99)
		}
	}

	// --- index usage smoke test via EXPLAIN QUERY PLAN ----------------------
	// These confirm that the partial/generated-column indexes we declared are
	// in fact picked up by the planner for the kinds of queries the UI will run.
	assertUsesIndex(t, db, f.ctx,
		`SELECT COUNT(*) FROM spans
		 WHERE service_name = 'api-gateway' AND http_route = '/users'`,
		"idx_spans_http_route")

	assertUsesIndex(t, db, f.ctx,
		`SELECT * FROM spans
		 WHERE service_name = 'api-gateway' AND status_code = 2
		 ORDER BY start_time_ns DESC LIMIT 10`,
		"idx_spans_errors")

	// Single-trace fetch must use the (trace_id, span_id) primary key. For a
	// WITHOUT ROWID table the planner reports "USING PRIMARY KEY" rather than
	// a named autoindex.
	assertUsesIndex(t, db, f.ctx,
		`SELECT trace_id FROM spans
		 WHERE trace_id = x'00112233445566778899aabbccddeeff'`,
		"PRIMARY KEY")
}

// -----------------------------------------------------------------------------
// helpers
// -----------------------------------------------------------------------------

func newTracerProvider(t *testing.T, f *fixture, serviceName string) *sdktrace.TracerProvider {
	t.Helper()
	exp, err := otlptracehttp.New(f.ctx,
		otlptracehttp.WithEndpoint(f.endpointHost()),
		otlptracehttp.WithInsecure(),
	)
	if err != nil {
		t.Fatalf("otlptracehttp.New: %v", err)
	}
	res, err := resource.New(f.ctx, resource.WithAttributes(
		attribute.String("service.name", serviceName),
	))
	if err != nil {
		t.Fatalf("resource.New: %v", err)
	}
	return sdktrace.NewTracerProvider(
		sdktrace.WithBatcher(exp, sdktrace.WithBatchTimeout(50*time.Millisecond)),
		sdktrace.WithResource(res),
	)
}

func shutdown(tp *sdktrace.TracerProvider) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	_ = tp.Shutdown(ctx)
}

func assertSameSet(t *testing.T, name string, got, want []string) {
	t.Helper()
	if len(got) != len(want) {
		t.Errorf("%s: size mismatch: want %d (%v), got %d (%v)", name, len(want), want, len(got), got)
		return
	}
	wantSet := map[string]bool{}
	for _, v := range want {
		wantSet[v] = true
	}
	for _, v := range got {
		if !wantSet[v] {
			t.Errorf("%s: unexpected value %q; want one of %v", name, v, want)
		}
	}
}

func assertUsesIndex(t *testing.T, db *sql.DB, ctx context.Context, query, wantIdx string) {
	t.Helper()
	rows, err := db.QueryContext(ctx, "EXPLAIN QUERY PLAN "+query)
	if err != nil {
		t.Fatalf("EXPLAIN QUERY PLAN: %v", err)
	}
	defer rows.Close()
	var plan string
	for rows.Next() {
		var id, parent, notused int
		var detail string
		if err := rows.Scan(&id, &parent, &notused, &detail); err != nil {
			t.Fatal(err)
		}
		plan += detail + "\n"
	}
	if !strings.Contains(plan, wantIdx) {
		t.Errorf("query did not use %q; plan was:\n%s\n  query: %s", wantIdx, plan, query)
	}
}
