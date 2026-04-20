// loadgen-bulk fills a fresh SQLite database with synthetic OTLP-shaped events
// by writing directly to the store, bypassing the HTTP ingest path. Intended
// for query-performance benchmarking on realistic data volumes.
//
// Usage:
//
//	go run ./cmd/loadgen-bulk --db ./waggle-test.db --total 10000000
package main

import (
	"context"
	"encoding/binary"
	"flag"
	"fmt"
	"log"
	"math/rand/v2"
	"os"
	"time"

	"github.com/danielloader/waggle/internal/store"
	sqstore "github.com/danielloader/waggle/internal/store/sqlite"
)

var serviceNames = [8]string{
	"api-gateway", "order-service", "payment-service", "user-service",
	"inventory-service", "notification-service", "analytics-service", "auth-service",
}

const numInstances = 2 // resource instances per service → 16 resources total

var (
	httpMethods = [5]string{"GET", "POST", "PUT", "DELETE", "PATCH"}

	httpRoutes = [24]string{
		"/users", "/users/:id", "/users/:id/profile",
		"/orders", "/orders/:id", "/orders/:id/items", "/orders/:id/cancel",
		"/products", "/products/:id", "/products/search",
		"/checkout", "/checkout/confirm", "/checkout/summary",
		"/payments", "/payments/:id", "/payments/refund",
		"/auth/login", "/auth/logout", "/auth/refresh",
		"/inventory/:id", "/inventory/reserve",
		"/notifications/send", "/health", "/ready",
	}

	dbSystems = [4]string{"postgresql", "mysql", "redis", "elasticsearch"}

	dbStatements = [6]string{
		"SELECT * FROM users WHERE id = $1",
		"INSERT INTO orders (user_id, total) VALUES ($1, $2)",
		"UPDATE inventory SET quantity = quantity - $1 WHERE product_id = $2",
		"SELECT COUNT(*) FROM events WHERE created_at > $1",
		"DELETE FROM sessions WHERE expires_at < NOW()",
		"SELECT * FROM products WHERE category = $1 LIMIT $2",
	}

	rpcServices = [6]string{
		"OrderService", "PaymentService", "UserService",
		"InventoryService", "NotificationService", "AuthService",
	}

	rpcMethods = [8]string{
		"Create", "Get", "List", "Update",
		"Authorize", "Process", "Validate", "Send",
	}

	logBodies = [15]string{
		"request received",
		"order processing completed",
		"payment authorized",
		"cache miss: fetching from db",
		"db query completed",
		"request succeeded",
		"validation failed: missing field",
		"rate limit exceeded",
		"background job started",
		"background job completed",
		"health check passed",
		"configuration reloaded",
		"connection pool at capacity",
		"retry: downstream unavailable",
		"circuit breaker opened",
	}

	// severity pool biased towards INFO (indices 1-3)
	severityNums  = [6]int32{5, 9, 9, 9, 13, 17}
	severityTexts = [6]string{"DEBUG", "INFO", "INFO", "INFO", "WARN", "ERROR"}
)

func i32p(v int32) *int32   { return &v }
func i64p(v int64) *int64   { return &v }
func u32p(v uint32) *uint32 { return &v }

func main() {
	dbPath   := flag.String("db", "./waggle-test.db", "Output SQLite database path")
	nTotal   := flag.Int("total", 10_000_000, "Total number of events to generate")
	batchSz  := flag.Int("batch-size", 2_000, "Events per write batch")
	seedFlag := flag.Uint64("seed", 0, "Random seed (0 = time-based)")
	win      := flag.Duration("window", 24*time.Hour, "Time window for event timestamps")
	spansPct := flag.Int("spans-pct", 60, "Percentage of events that are spans")
	logsPct  := flag.Int("logs-pct", 10, "Percentage of events that are logs")
	flag.Parse()

	if *spansPct+*logsPct > 100 {
		log.Fatalf("--spans-pct + --logs-pct must be ≤ 100")
	}

	s := *seedFlag
	if s == 0 {
		s = uint64(time.Now().UnixNano())
	}
	rng := rand.New(rand.NewPCG(s, s^0xdeadbeefcafebabe))

	numSpans   := *nTotal * *spansPct / 100
	numLogs    := *nTotal * *logsPct / 100
	numMetrics := *nTotal - numSpans - numLogs
	log.Printf("loadgen-bulk: db=%s  spans=%d  logs=%d  metrics=%d  total=%d",
		*dbPath, numSpans, numLogs, numMetrics, numSpans+numLogs+numMetrics)

	ctx := context.Background()
	st, err := sqstore.Open(ctx, *dbPath)
	if err != nil {
		log.Fatalf("open store: %v", err)
	}
	defer st.Close()

	// Tune SQLite for bulk loading.
	// Keep autocheckpoint enabled at 10k pages (~40 MB) to prevent the WAL
	// growing unboundedly, which slows writes at multi-GB scale.
	wrDB := st.WriterDB()
	for _, p := range []string{
		"PRAGMA synchronous = OFF",
		"PRAGMA wal_autocheckpoint = 10000",
		"PRAGMA cache_size = -131072", // 512 MB page cache
	} {
		if _, err := wrDB.ExecContext(ctx, p); err != nil {
			log.Fatalf("pragma %s: %v", p, err)
		}
	}

	// Drop FTS triggers; rebuild the index in one pass after all inserts.
	for _, t := range []string{"events_ai", "events_ad"} {
		if _, err := wrDB.ExecContext(ctx, "DROP TRIGGER IF EXISTS "+t); err != nil {
			log.Fatalf("drop trigger %s: %v", t, err)
		}
	}

	now := time.Now()
	windowNS := win.Nanoseconds()

	// ------------------------------------------------------------------
	// Resources (8 services × 2 instances = 16) and scopes (8)
	// ------------------------------------------------------------------
	resources := make([]store.Resource, 0, len(serviceNames)*numInstances)
	for si, svc := range serviceNames {
		for inst := range numInstances {
			id := uint64(si*numInstances + inst + 1)
			resources = append(resources, store.Resource{
				ID:                id,
				ServiceName:       svc,
				ServiceNamespace:  "production",
				ServiceVersion:    "1.0.0",
				ServiceInstanceID: fmt.Sprintf("%s-%d", svc, inst),
				SDKName:           "opentelemetry",
				SDKLanguage:       "go",
				SDKVersion:        "1.26.0",
				AttributesJSON:    `{"deployment.environment":"production"}`,
				FirstSeenNS:       now.UnixNano() - windowNS,
				LastSeenNS:        now.UnixNano(),
			})
		}
	}

	scopes := make([]store.Scope, len(serviceNames))
	for si, svc := range serviceNames {
		scopes[si] = store.Scope{
			ID:      uint64(si + 1),
			Name:    svc + "/instrumentation",
			Version: "1.0.0",
		}
	}

	// Pool for random resource → scope → service lookup.
	type rsEntry struct {
		resourceID  uint64
		scopeID     uint64
		serviceName string
	}
	pool := make([]rsEntry, len(resources))
	for i, r := range resources {
		pool[i] = rsEntry{r.ID, scopes[i/numInstances].ID, r.ServiceName}
	}
	pick := func() rsEntry { return pool[rng.IntN(len(pool))] }

	randTimeNS := func() int64 {
		return now.UnixNano() - windowNS + rng.Int64N(windowNS)
	}
	newTraceID := func() []byte {
		b := make([]byte, 16)
		binary.LittleEndian.PutUint64(b[:8], rng.Uint64())
		binary.LittleEndian.PutUint64(b[8:], rng.Uint64())
		return b
	}
	newSpanID := func() []byte {
		b := make([]byte, 8)
		binary.LittleEndian.PutUint64(b, rng.Uint64())
		return b
	}

	// ------------------------------------------------------------------
	// Attribute JSON helpers (fmt.Sprintf avoids reflection overhead).
	// ------------------------------------------------------------------
	httpSpanAttrs := func(svc, kind, method, route string, code int) string {
		return fmt.Sprintf(
			`{"meta.signal_type":"span","meta.span_kind":%q,"meta.dataset":%q,"http.request.method":%q,"http.route":%q,"http.response.status_code":%d}`,
			kind, svc, method, route, code)
	}
	rpcSpanAttrs := func(svc, kind, rpcSvc, rpcMethod string) string {
		return fmt.Sprintf(
			`{"meta.signal_type":"span","meta.span_kind":%q,"meta.dataset":%q,"rpc.service":%q,"rpc.method":%q}`,
			kind, svc, rpcSvc, rpcMethod)
	}
	dbSpanAttrs := func(svc, dbSys, stmt string) string {
		return fmt.Sprintf(
			`{"meta.signal_type":"span","meta.span_kind":"CLIENT","meta.dataset":%q,"db.system":%q,"db.statement":%q}`,
			svc, dbSys, stmt)
	}
	internalSpanAttrs := func(svc, component string) string {
		return fmt.Sprintf(
			`{"meta.signal_type":"span","meta.span_kind":"INTERNAL","meta.dataset":%q,"component":%q}`,
			svc, component)
	}
	logEventAttrs := func(svc, component string) string {
		return fmt.Sprintf(
			`{"meta.signal_type":"log","meta.dataset":%q,"component":%q}`,
			svc, component)
	}
	metricEventAttrs := func(svc string, reqs, memUsed int64, cpu float64) string {
		return fmt.Sprintf(
			`{"meta.dataset":%q,"requests.total":%d,"memory.used_bytes":%d,"memory.free_bytes":%d,"cpu.utilization":%.4f,"network.bytes_sent":%d}`,
			svc, reqs, memUsed, 4_000_000_000-memUsed, cpu, reqs*512)
	}

	// ------------------------------------------------------------------
	// Attribute key deltas — one set per service/signal, included once.
	// ------------------------------------------------------------------
	nowNS := now.UnixNano()

	spanAttrKeys := func(svc string) []store.AttrKeyDelta {
		return []store.AttrKeyDelta{
			{SignalType: "span", ServiceName: svc, Key: "meta.signal_type", ValueType: "str", Count: 1, LastSeenNS: nowNS},
			{SignalType: "span", ServiceName: svc, Key: "meta.span_kind", ValueType: "str", Count: 1, LastSeenNS: nowNS},
			{SignalType: "span", ServiceName: svc, Key: "meta.dataset", ValueType: "str", Count: 1, LastSeenNS: nowNS},
			{SignalType: "span", ServiceName: svc, Key: "http.request.method", ValueType: "str", Count: 1, LastSeenNS: nowNS},
			{SignalType: "span", ServiceName: svc, Key: "http.route", ValueType: "str", Count: 1, LastSeenNS: nowNS},
			{SignalType: "span", ServiceName: svc, Key: "http.response.status_code", ValueType: "int", Count: 1, LastSeenNS: nowNS},
			{SignalType: "span", ServiceName: svc, Key: "rpc.service", ValueType: "str", Count: 1, LastSeenNS: nowNS},
			{SignalType: "span", ServiceName: svc, Key: "rpc.method", ValueType: "str", Count: 1, LastSeenNS: nowNS},
			{SignalType: "span", ServiceName: svc, Key: "db.system", ValueType: "str", Count: 1, LastSeenNS: nowNS},
			{SignalType: "span", ServiceName: svc, Key: "db.statement", ValueType: "str", Count: 1, LastSeenNS: nowNS},
			{SignalType: "span", ServiceName: svc, Key: "component", ValueType: "str", Count: 1, LastSeenNS: nowNS},
		}
	}
	logAttrKeys := func(svc string) []store.AttrKeyDelta {
		return []store.AttrKeyDelta{
			{SignalType: "log", ServiceName: svc, Key: "meta.signal_type", ValueType: "str", Count: 1, LastSeenNS: nowNS},
			{SignalType: "log", ServiceName: svc, Key: "meta.dataset", ValueType: "str", Count: 1, LastSeenNS: nowNS},
			{SignalType: "log", ServiceName: svc, Key: "component", ValueType: "str", Count: 1, LastSeenNS: nowNS},
		}
	}
	metricAttrKeys := func(svc string) []store.AttrKeyDelta {
		return []store.AttrKeyDelta{
			{SignalType: "metric", ServiceName: svc, Key: "meta.dataset", ValueType: "str", Count: 1, LastSeenNS: nowNS},
			{SignalType: "metric", ServiceName: svc, Key: "requests.total", ValueType: "int", Count: 1, LastSeenNS: nowNS},
			{SignalType: "metric", ServiceName: svc, Key: "memory.used_bytes", ValueType: "int", Count: 1, LastSeenNS: nowNS},
			{SignalType: "metric", ServiceName: svc, Key: "memory.free_bytes", ValueType: "int", Count: 1, LastSeenNS: nowNS},
			{SignalType: "metric", ServiceName: svc, Key: "cpu.utilization", ValueType: "flt", Count: 1, LastSeenNS: nowNS},
			{SignalType: "metric", ServiceName: svc, Key: "network.bytes_sent", ValueType: "int", Count: 1, LastSeenNS: nowNS},
		}
	}

	// Emit resources, scopes, and attr key catalog in one batch.
	// WriteBatch skips attr-only batches, so we piggyback on resources.
	var seedAttrKeys []store.AttrKeyDelta
	for _, svc := range serviceNames {
		seedAttrKeys = append(seedAttrKeys, spanAttrKeys(svc)...)
		seedAttrKeys = append(seedAttrKeys, logAttrKeys(svc)...)
		seedAttrKeys = append(seedAttrKeys, metricAttrKeys(svc)...)
	}
	if err := st.WriteBatch(ctx, store.Batch{
		Resources: resources,
		Scopes:    scopes,
		AttrKeys:  seedAttrKeys,
	}); err != nil {
		log.Fatalf("seed resources/scopes/attrs: %v", err)
	}

	// ------------------------------------------------------------------
	// Flush helper
	// ------------------------------------------------------------------
	batch := store.Batch{}
	batch.Events = make([]store.Event, 0, *batchSz)
	batch.MetricEvents = make([]store.MetricEvent, 0, *batchSz)

	flushEvents := func() {
		if len(batch.Events) == 0 {
			return
		}
		if err := st.WriteBatch(ctx, batch); err != nil {
			log.Fatalf("WriteBatch (events): %v", err)
		}
		batch.Events = batch.Events[:0]
	}
	flushMetrics := func() {
		if len(batch.MetricEvents) == 0 {
			return
		}
		if err := st.WriteBatch(ctx, batch); err != nil {
			log.Fatalf("WriteBatch (metrics): %v", err)
		}
		batch.MetricEvents = batch.MetricEvents[:0]
	}

	progress := func(done, total int, phase string, start time.Time) {
		pct := 100 * done / total
		elapsed := time.Since(start).Round(time.Second)
		rate := float64(done) / time.Since(start).Seconds()
		eta := time.Duration(float64(total-done)/rate) * time.Second
		log.Printf("%s: %d/%d (%d%%)  elapsed=%s  rate=%.0f/s  eta=%s",
			phase, done, total, pct, elapsed, rate, eta.Round(time.Second))
	}

	wallStart := time.Now()

	// ==================================================================
	// Phase 1: Spans
	// ==================================================================
	log.Printf("phase 1/3: generating %d spans...", numSpans)
	phaseStart := time.Now()
	spansWritten := 0
	const progressEvery = 250_000

	for spansWritten < numSpans {
		// Trace size: 1–20 spans per trace (geometric-ish)
		traceSize := 1 + rng.IntN(4) + rng.IntN(4) + rng.IntN(4) + rng.IntN(4) + rng.IntN(4)
		if spansWritten+traceSize > numSpans {
			traceSize = numSpans - spansWritten
		}

		traceID := newTraceID()
		rootTimeNS := randTimeNS()

		// Root span
		root := pick()
		rootSpanID := newSpanID()
		rootDur := int64(1+rng.IntN(5000)) * int64(time.Millisecond)
		rootEnd := rootTimeNS + rootDur

		var rootStatus int32
		switch rng.IntN(20) {
		case 0, 1: // 10% error
			rootStatus = 2
		default: // 90% OK/UNSET
			rootStatus = 1
		}

		method := httpMethods[rng.IntN(len(httpMethods))]
		route := httpRoutes[rng.IntN(len(httpRoutes))]
		httpCode := 200
		if rootStatus == 2 {
			httpCode = 500
		} else if rng.IntN(10) == 0 {
			httpCode = 400
		}

		batch.Events = append(batch.Events, store.Event{
			TimeNS:        rootTimeNS,
			EndTimeNS:     i64p(rootEnd),
			ResourceID:    root.resourceID,
			ScopeID:       root.scopeID,
			ServiceName:   root.serviceName,
			Name:          method + " " + route,
			TraceID:       traceID,
			SpanID:        rootSpanID,
			StatusCode:    i32p(rootStatus),
			Flags:         u32p(1),
			AttributesJSON: httpSpanAttrs(root.serviceName, "SERVER", method, route, httpCode),
		})

		// Child spans
		for range traceSize - 1 {
			child := pick()
			childSpanID := newSpanID()
			childOffset := rng.Int64N(rootDur / 2)
			childTimeNS := rootTimeNS + childOffset
			childDur := int64(1+rng.IntN(500)) * int64(time.Millisecond)
			if childTimeNS+childDur > rootEnd {
				childDur = rootEnd - childTimeNS
				if childDur < int64(time.Millisecond) {
					childDur = int64(time.Millisecond)
				}
			}
			childEnd := childTimeNS + childDur

			var childAttrs string
			var childName string
			switch rng.IntN(3) {
			case 0: // RPC call
				rpcSvc := rpcServices[rng.IntN(len(rpcServices))]
				rpcMethod := rpcMethods[rng.IntN(len(rpcMethods))]
				childName = rpcSvc + "/" + rpcMethod
				childAttrs = rpcSpanAttrs(child.serviceName, "CLIENT", rpcSvc, rpcMethod)
			case 1: // DB call
				dbSys := dbSystems[rng.IntN(len(dbSystems))]
				stmt := dbStatements[rng.IntN(len(dbStatements))]
				childName = dbSys + ".query"
				childAttrs = dbSpanAttrs(child.serviceName, dbSys, stmt)
			default: // Internal
				component := "middleware"
				if rng.IntN(3) == 0 {
					component = "cache"
				}
				childName = component + ".process"
				childAttrs = internalSpanAttrs(child.serviceName, component)
			}

			var childStatus int32
			if rootStatus == 2 && rng.IntN(3) == 0 {
				childStatus = 2
			} else {
				childStatus = 1
			}

			batch.Events = append(batch.Events, store.Event{
				TimeNS:         childTimeNS,
				EndTimeNS:      i64p(childEnd),
				ResourceID:     child.resourceID,
				ScopeID:        child.scopeID,
				ServiceName:    child.serviceName,
				Name:           childName,
				TraceID:        traceID,
				SpanID:         childSpanID,
				ParentSpanID:   rootSpanID,
				StatusCode:     i32p(childStatus),
				Flags:          u32p(1),
				AttributesJSON: childAttrs,
			})
		}

		prev := spansWritten
		spansWritten += traceSize

		if len(batch.Events) >= *batchSz {
			flushEvents()
		}
		if spansWritten/progressEvery > prev/progressEvery || spansWritten >= numSpans {
			progress(spansWritten, numSpans, "spans", phaseStart)
		}
	}
	flushEvents()
	log.Printf("phase 1/3 done: %d spans in %s", spansWritten, time.Since(phaseStart).Round(time.Second))

	// ==================================================================
	// Phase 2: Logs
	// ==================================================================
	log.Printf("phase 2/3: generating %d logs...", numLogs)
	phaseStart = time.Now()
	logsWritten := 0

	for logsWritten < numLogs {
		rs := pick()
		sevIdx := rng.IntN(len(severityNums))
		sevNum := severityNums[sevIdx]
		sevText := severityTexts[sevIdx]
		body := logBodies[rng.IntN(len(logBodies))]
		component := "handler"
		switch rng.IntN(4) {
		case 1:
			component = "middleware"
		case 2:
			component = "worker"
		case 3:
			component = "scheduler"
		}

		tNS := randTimeNS()
		obsTNS := tNS + int64(rng.IntN(1000))*int64(time.Microsecond)

		batch.Events = append(batch.Events, store.Event{
			TimeNS:         tNS,
			ResourceID:     rs.resourceID,
			ScopeID:        rs.scopeID,
			ServiceName:    rs.serviceName,
			Name:           sevText + ": " + body,
			SeverityNumber: i32p(sevNum),
			SeverityText:   sevText,
			Body:           body,
			ObservedTimeNS: i64p(obsTNS),
			AttributesJSON: logEventAttrs(rs.serviceName, component),
		})

		logsWritten++
		if len(batch.Events) >= *batchSz {
			flushEvents()
		}
		if logsWritten%progressEvery == 0 || logsWritten >= numLogs {
			progress(logsWritten, numLogs, "logs", phaseStart)
		}
	}
	flushEvents()
	log.Printf("phase 2/3 done: %d logs in %s", logsWritten, time.Since(phaseStart).Round(time.Second))

	// ==================================================================
	// Phase 3: Metrics
	// ==================================================================
	log.Printf("phase 3/3: generating %d metric events...", numMetrics)
	phaseStart = time.Now()
	metricsWritten := 0

	for metricsWritten < numMetrics {
		rs := pick()
		tNS := randTimeNS()
		reqs := int64(rng.IntN(10_000))
		memUsed := int64(256_000_000 + rng.IntN(3_744_000_000))
		cpu := float64(rng.IntN(100)) / 100.0

		batch.MetricEvents = append(batch.MetricEvents, store.MetricEvent{
			TimeNS:         tNS,
			ResourceID:     rs.resourceID,
			ScopeID:        rs.scopeID,
			ServiceName:    rs.serviceName,
			AttributesJSON: metricEventAttrs(rs.serviceName, reqs, memUsed, cpu),
		})

		metricsWritten++
		if len(batch.MetricEvents) >= *batchSz {
			flushMetrics()
		}
		if metricsWritten%progressEvery == 0 || metricsWritten >= numMetrics {
			progress(metricsWritten, numMetrics, "metrics", phaseStart)
		}
	}
	flushMetrics()
	log.Printf("phase 3/3 done: %d metrics in %s", metricsWritten, time.Since(phaseStart).Round(time.Second))

	// ==================================================================
	// Rebuild FTS index in one pass (much faster than per-row triggers).
	// ==================================================================
	log.Println("rebuilding FTS index...")
	ftsStart := time.Now()
	if _, err := wrDB.ExecContext(ctx, "INSERT INTO events_fts(events_fts) VALUES ('rebuild')"); err != nil {
		log.Fatalf("rebuild FTS: %v", err)
	}
	log.Printf("FTS rebuild done: %s", time.Since(ftsStart).Round(time.Second))

	// Restore FTS triggers so the DB works normally after load.
	for _, trigSQL := range []string{
		`CREATE TRIGGER IF NOT EXISTS events_ai AFTER INSERT ON events BEGIN
			INSERT INTO events_fts(rowid, body, name, service_name)
			VALUES (NEW.event_id, NEW.body, NEW.name, NEW.service_name); END`,
		`CREATE TRIGGER IF NOT EXISTS events_ad AFTER DELETE ON events BEGIN
			INSERT INTO events_fts(events_fts, rowid, body, name, service_name)
			VALUES ('delete', OLD.event_id, OLD.body, OLD.name, OLD.service_name); END`,
	} {
		if _, err := wrDB.ExecContext(ctx, trigSQL); err != nil {
			log.Fatalf("restore FTS trigger: %v", err)
		}
	}

	// Checkpoint and truncate WAL so the .db file reflects all committed
	// pages and the reported size is accurate.
	if _, err := wrDB.ExecContext(ctx, "PRAGMA wal_checkpoint(TRUNCATE)"); err != nil {
		log.Printf("WAL checkpoint: %v", err)
	}

	// Restore normal sync mode so the DB is safe for subsequent use.
	if _, err := wrDB.ExecContext(ctx, "PRAGMA synchronous = NORMAL"); err != nil {
		log.Printf("restore synchronous: %v", err)
	}

	// ==================================================================
	// Final stats
	// ==================================================================
	fi, _ := os.Stat(*dbPath)
	totalSize := int64(0)
	if fi != nil {
		totalSize = fi.Size()
	}

	log.Printf("loadgen-bulk complete!")
	log.Printf("  elapsed:    %s", time.Since(wallStart).Round(time.Second))
	log.Printf("  spans:      %d", spansWritten)
	log.Printf("  logs:       %d", logsWritten)
	log.Printf("  metrics:    %d", metricsWritten)
	log.Printf("  total:      %d", spansWritten+logsWritten+metricsWritten)
	log.Printf("  db path:    %s", *dbPath)
	log.Printf("  db size:    %.1f MB (%.2f GB)", float64(totalSize)/(1<<20), float64(totalSize)/(1<<30))
}
