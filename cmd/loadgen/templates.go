package main

import (
	"context"
	"fmt"
	"math/rand/v2"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/log"
	"go.opentelemetry.io/otel/trace"
)

// Tracers maps service names to the tracer for that service. Passed into
// every template so cross-service emissions share the same trace_id while
// each span carries the right service_name via its provider's resource.
type Tracers map[string]trace.Tracer

// TraceTemplate produces a single trace. Service is the primary owner (for
// discovery/listing in --list); Emit may touch additional services via the
// map for cross-service propagation.
type TraceTemplate struct {
	Service     string
	Description string
	Emit        func(ctx context.Context, tracers Tracers, loggers Loggers) error
}

var allTemplates = []TraceTemplate{
	{
		Service:     "api-gateway",
		Description: "HTTP GET /users — success (200) with a DB read child",
		Emit:        emitHTTPGetSuccess,
	},
	{
		Service:     "api-gateway",
		Description: "HTTP POST /orders — client error (400 validation)",
		Emit:        emitHTTPPostValidationError,
	},
	{
		Service:     "api-gateway",
		Description: "HTTP GET /reports — server error (500, marked ERROR)",
		Emit:        emitHTTPServerError,
	},
	{
		Service:     "api-gateway",
		Description: "HTTP POST /checkout — slow outlier (~p99)",
		Emit:        emitSlowCheckout,
	},
	{
		Service:     "payments",
		Description: "gRPC PaymentService/Authorize with 2 downstream calls",
		Emit:        emitPaymentsAuthorize,
	},
	{
		Service:     "payments",
		Description: "gRPC RefundService/Create — occasionally declined",
		Emit:        emitPaymentsRefund,
	},
	{
		Service:     "db-worker",
		Description: "postgres/redis/mysql query with variable latency",
		Emit:        emitDBQuery,
	},
	{
		Service:     "notifications",
		Description: "Email send with enqueue + provider call",
		Emit:        emitNotificationSend,
	},
	{
		Service:     "api-gateway",
		Description: "Timeout that records an exception event (status stays UNSET)",
		Emit:        emitTimeoutException,
	},
	{
		Service:     "payments",
		Description: "Panic-style exception + ERROR status",
		Emit:        emitPaymentsPanic,
	},
	{
		Service:     "notifications",
		Description: "Async job: producer trace + linked consumer trace",
		Emit:        emitAsyncLinkedJob,
	},
	{
		Service:     "db-worker",
		Description: "Batch processor — N items, one event per item processed",
		Emit:        emitBatchProcess,
	},
	{
		Service:     "api-gateway",
		Description: "Retrying HTTP client with exponential backoff events",
		Emit:        emitRetryingRequest,
	},
	{
		Service:     "api-gateway",
		Description: "Big checkout flow — ~23 spans across all four services (demo)",
		Emit:        emitBigCheckoutFlow,
	},
}

// -----------------------------------------------------------------------------
// Template implementations
// -----------------------------------------------------------------------------

func emitHTTPGetSuccess(ctx context.Context, tracers Tracers, loggers Loggers) error {
	tracer := tracers["api-gateway"]
	route := pickRoute()
	ctx, root := tracer.Start(ctx, "GET "+route, trace.WithSpanKind(trace.SpanKindServer))
	root.SetAttributes(
		attribute.String("http.request.method", "GET"),
		attribute.String("http.route", route),
		attribute.String("url.path", route),
		attribute.String("url.scheme", "https"),
		attribute.Int("http.response.status_code", 200),
		attribute.String("user_agent.original", pickUserAgent()),
		attribute.String("customer.tier", pickTier()),
		attribute.String("deployment.region", pickRegion()),
	)
	defer root.End()

	// Auth / authz phase — observable as an event so you can see how long
	// it took vs the downstream work, without needing a child span.
	root.AddEvent("auth.authorized", trace.WithAttributes(
		attribute.String("auth.method", "jwt"),
		attribute.Bool("auth.cached", rand.IntN(2) == 0),
	))

	sleepLike(8*time.Millisecond, 6*time.Millisecond)

	_, db := tracer.Start(ctx, "SELECT users", trace.WithSpanKind(trace.SpanKindClient))
	db.SetAttributes(
		attribute.String("db.system", "postgresql"),
		attribute.String("db.statement", "SELECT id, email FROM users WHERE org_id = $1 LIMIT 100"),
		attribute.Int("db.rows_returned", rand.IntN(100)+1),
	)
	cacheHit := rand.IntN(3) == 0
	db.AddEvent("cache.lookup", trace.WithAttributes(
		attribute.Bool("cache.hit", cacheHit),
		attribute.String("cache.key", fmt.Sprintf("users:%d", rand.IntN(1000))),
	))
	sleepLike(4*time.Millisecond, 3*time.Millisecond)
	if !cacheHit {
		db.AddEvent("cache.populated", trace.WithAttributes(
			attribute.Int64("cache.ttl_ms", 30000),
		))
	}
	db.End()

	sleepLike(1*time.Millisecond, 500*time.Microsecond)
	root.AddEvent("response.serialized", trace.WithAttributes(
		attribute.Int("response.body_size_bytes", 1200+rand.IntN(4000)),
	))
	return nil
}

func emitHTTPPostValidationError(ctx context.Context, tracers Tracers, loggers Loggers) error {
	tracer := tracers["api-gateway"]
	ctx, root := tracer.Start(ctx, "POST /orders", trace.WithSpanKind(trace.SpanKindServer))
	root.SetAttributes(
		attribute.String("http.request.method", "POST"),
		attribute.String("http.route", "/orders"),
		attribute.String("url.path", "/orders"),
		attribute.Int("http.response.status_code", 400),
		attribute.String("error.type", "validation"),
		attribute.String("customer.tier", pickTier()),
	)
	defer root.End()

	sleepLike(3*time.Millisecond, 2*time.Millisecond)

	_, val := tracer.Start(ctx, "validate_request")
	val.SetAttributes(attribute.String("validation.field", "items[0].sku"))
	val.AddEvent("validation_failed", trace.WithAttributes(
		attribute.String("message", "sku cannot be empty"),
	))
	sleepLike(1*time.Millisecond, 500*time.Microsecond)
	val.End()
	return nil
}

func emitHTTPServerError(ctx context.Context, tracers Tracers, loggers Loggers) error {
	tracer := tracers["api-gateway"]
	ctx, root := tracer.Start(ctx, "GET /reports", trace.WithSpanKind(trace.SpanKindServer))
	root.SetAttributes(
		attribute.String("http.request.method", "GET"),
		attribute.String("http.route", "/reports"),
		attribute.String("url.path", "/reports"),
		attribute.Int("http.response.status_code", 500),
		attribute.String("customer.tier", pickTier()),
	)
	root.SetStatus(codes.Error, "downstream unavailable")
	defer root.End()

	sleepLike(10*time.Millisecond, 5*time.Millisecond)

	depCtx, dep := tracer.Start(ctx, "call reporting-service")
	dep.SetAttributes(
		attribute.String("rpc.service", "ReportingService"),
		attribute.String("rpc.method", "GenerateMonthly"),
	)
	dep.SetStatus(codes.Error, "connection refused")
	sleepLike(50*time.Millisecond, 20*time.Millisecond)
	// Emit a correlated ERROR log inside the dep span's context — the SDK
	// stamps trace_id + span_id from ctx so the log row links back to this
	// span.
	if logger := loggers["api-gateway"]; logger != nil {
		emit(depCtx, logger, log.SeverityError, "ERROR",
			"reporting-service unreachable: connection refused",
			log.String("rpc.service", "ReportingService"),
			log.Int("http.response.status_code", 500),
		)
	}
	dep.End()
	return nil
}

func emitSlowCheckout(ctx context.Context, tracers Tracers, loggers Loggers) error {
	// Cross-service slow trace. api-gateway receives, then fans out to
	// the owning services — inventory on api-gateway (simulating a
	// monolith module), payments on the payments service, and
	// notifications on the notifications service. Each child inherits
	// the trace_id via ctx so they collapse into one waterfall.
	api := tracers["api-gateway"]
	pay := tracers["payments"]
	notif := tracers["notifications"]

	ctx, root := api.Start(ctx, "POST /checkout", trace.WithSpanKind(trace.SpanKindServer))
	root.SetAttributes(
		attribute.String("http.request.method", "POST"),
		attribute.String("http.route", "/checkout"),
		attribute.String("url.path", "/checkout"),
		attribute.Int("http.response.status_code", 200),
		attribute.String("customer.tier", pickTier()),
		attribute.Int("cart.item_count", rand.IntN(10)+1),
	)
	defer root.End()

	emitPhase := func(t trace.Tracer, name string, idx int) {
		_, child := t.Start(ctx, name, trace.WithSpanKind(trace.SpanKindInternal))
		child.SetAttributes(attribute.Int("step.index", idx))
		child.AddEvent("lock.acquired", trace.WithAttributes(
			attribute.String("lock.key", name),
			attribute.Int64("lock.wait_ms", int64(rand.IntN(10))),
		))
		sleepLike(30*time.Millisecond, 10*time.Millisecond)
		child.AddEvent("validation.passed")
		sleepLike(30*time.Millisecond, 15*time.Millisecond)
		child.AddEvent("state.persisted", trace.WithAttributes(
			attribute.String("state.key", fmt.Sprintf("%s.v2", name)),
		))
		sleepLike(20*time.Millisecond, 15*time.Millisecond)
		child.End()
	}

	emitPhase(api, "inventory.reserve", 0)
	emitPhase(pay, "payments.charge", 1)
	emitPhase(notif, "notifications.enqueue", 2)
	return nil
}

func emitPaymentsAuthorize(ctx context.Context, tracers Tracers, loggers Loggers) error {
	tracer := tracers["payments"]
	ctx, root := tracer.Start(ctx, "PaymentService/Authorize", trace.WithSpanKind(trace.SpanKindServer))
	root.SetAttributes(
		attribute.String("rpc.system", "grpc"),
		attribute.String("rpc.service", "PaymentService"),
		attribute.String("rpc.method", "Authorize"),
		attribute.String("customer.tier", pickTier()),
		attribute.String("payments.provider", pickPaymentProvider()),
	)
	defer root.End()

	_, fraud := tracer.Start(ctx, "FraudService/Check")
	score := rand.Float64()
	fraud.SetAttributes(
		attribute.String("rpc.service", "FraudService"),
		attribute.String("rpc.method", "Check"),
		attribute.Float64("fraud.score", score),
	)
	fraud.AddEvent("rules.evaluated", trace.WithAttributes(
		attribute.Int("rules.count", 12+rand.IntN(6)),
	))
	if score > 0.8 {
		fraud.AddEvent("fraud.high_score", trace.WithAttributes(
			attribute.Float64("fraud.score", score),
			attribute.String("fraud.reason", "velocity_exceeded"),
		))
	}
	sleepLike(6*time.Millisecond, 3*time.Millisecond)
	fraud.End()

	_, provider := tracer.Start(ctx, "stripe.authorize")
	provider.SetAttributes(
		attribute.String("peer.service", "stripe"),
		attribute.String("http.request.method", "POST"),
		attribute.String("http.route", "/v1/charges"),
		attribute.Int("http.response.status_code", 200),
	)
	provider.AddEvent("request.sent", trace.WithAttributes(
		attribute.Int("request.body_size_bytes", 420),
	))
	sleepLike(40*time.Millisecond, 20*time.Millisecond)
	provider.AddEvent("response.received", trace.WithAttributes(
		attribute.String("provider.txn_id", fmt.Sprintf("ch_%d", rand.IntN(1_000_000))),
		attribute.Bool("provider.three_ds_required", rand.IntN(5) == 0),
	))
	provider.End()
	return nil
}

func emitPaymentsRefund(ctx context.Context, tracers Tracers, loggers Loggers) error {
	// Refund spans the payments service plus a db-worker DB hop — gives a
	// two-service trace that's easy to scan in the summary swim-lane.
	pay := tracers["payments"]
	db := tracers["db-worker"]
	ctx, root := pay.Start(ctx, "RefundService/Create", trace.WithSpanKind(trace.SpanKindServer))
	root.SetAttributes(
		attribute.String("rpc.service", "RefundService"),
		attribute.String("rpc.method", "Create"),
		attribute.String("customer.tier", pickTier()),
	)
	declined := rand.IntN(5) == 0
	if declined {
		root.SetStatus(codes.Error, "refund declined")
	}
	defer root.End()

	_, dbSpan := db.Start(ctx, "SELECT payment", trace.WithSpanKind(trace.SpanKindClient))
	dbSpan.SetAttributes(
		attribute.String("db.system", "postgresql"),
		attribute.String("db.statement", "SELECT * FROM payments WHERE id = $1"),
	)
	sleepLike(3*time.Millisecond, 1*time.Millisecond)
	dbSpan.End()
	_ = pay // already used
	return nil
}

func emitDBQuery(ctx context.Context, tracers Tracers, loggers Loggers) error {
	tracer := tracers["db-worker"]
	systems := []string{"postgresql", "redis", "mysql"}
	sys := systems[rand.IntN(len(systems))]
	_, span := tracer.Start(ctx, fmt.Sprintf("%s query", sys), trace.WithSpanKind(trace.SpanKindClient))
	span.SetAttributes(
		attribute.String("db.system", sys),
		attribute.String("db.statement", pickStatement(sys)),
		attribute.Int("db.rows_returned", rand.IntN(1000)),
	)
	if rand.IntN(20) == 0 {
		span.SetStatus(codes.Error, "connection refused")
	}
	sleepLike(5*time.Millisecond, 4*time.Millisecond)
	span.End()
	return nil
}

// emitTimeoutException models a downstream timeout that the instrumentation
// records as an exception event without upgrading the span status. This is
// the interesting case: status is UNSET but `error = true` still fires via
// the exception-event branch of the synthetic field.
func emitTimeoutException(ctx context.Context, tracers Tracers, loggers Loggers) error {
	tracer := tracers["api-gateway"]
	ctx, root := tracer.Start(ctx, "GET /slow-api", trace.WithSpanKind(trace.SpanKindServer))
	root.SetAttributes(
		attribute.String("http.request.method", "GET"),
		attribute.String("http.route", "/slow-api"),
		attribute.String("url.path", "/slow-api"),
		attribute.Int("http.response.status_code", 200),
		attribute.String("customer.tier", pickTier()),
	)
	defer root.End()

	_, dep := tracer.Start(ctx, "call downstream-service")
	dep.SetAttributes(
		attribute.String("peer.service", "downstream-service"),
		attribute.Int64("timeout_ms", 500),
	)
	dep.AddEvent("exception", trace.WithAttributes(
		attribute.String("exception.type", "TimeoutError"),
		attribute.String("exception.message", "deadline exceeded waiting for downstream-service"),
		attribute.String("exception.stacktrace",
			"TimeoutError: deadline exceeded\n"+
				"  at http.Client.Do (client.go:210)\n"+
				"  at apigw.handler (handler.go:87)"),
		attribute.Bool("exception.escaped", false),
	))
	sleepLike(80*time.Millisecond, 20*time.Millisecond)
	dep.End()
	return nil
}

// emitPaymentsPanic models an unhandled error with both an exception event
// and ERROR status — belt and braces, matches what well-instrumented code
// emits via OTel's RecordError helper.
func emitPaymentsPanic(ctx context.Context, tracers Tracers, loggers Loggers) error {
	tracer := tracers["payments"]
	spanCtx, span := tracer.Start(ctx, "PaymentService/Charge", trace.WithSpanKind(trace.SpanKindServer))
	span.SetAttributes(
		attribute.String("rpc.service", "PaymentService"),
		attribute.String("rpc.method", "Charge"),
		attribute.String("customer.tier", pickTier()),
	)
	span.AddEvent("exception", trace.WithAttributes(
		attribute.String("exception.type", "NullPointerException"),
		attribute.String("exception.message", "payment.card was nil"),
		attribute.String("exception.stacktrace",
			"NullPointerException: payment.card was nil\n"+
				"  at PaymentService.charge (PaymentService.java:142)\n"+
				"  at grpc.Dispatch (Dispatch.java:55)"),
	))
	span.SetStatus(codes.Error, "nil pointer dereference")
	// Correlated ERROR log — this is what the structured logger emits right
	// before/after an exception in real instrumented code.
	if logger := loggers["payments"]; logger != nil {
		emit(spanCtx, logger, log.SeverityError, "ERROR",
			"nil pointer dereference: payment.card was nil",
			log.String("exception.type", "NullPointerException"),
			log.String("rpc.service", "PaymentService"),
			log.String("rpc.method", "Charge"),
		)
	}
	sleepLike(5*time.Millisecond, 2*time.Millisecond)
	span.End()
	return nil
}

// emitAsyncLinkedJob models the canonical async / queue-based pattern:
// one trace enqueues a job and returns immediately; the worker picks it up
// later and processes it on a separate trace with an OTel Link pointing
// back at the producer's span. This is what propagation-over-a-queue
// boundary looks like in OTel — you can't use parent/child because the
// consumer runs async with a different wall-clock start, so Links are the
// right mechanism.
func emitAsyncLinkedJob(ctx context.Context, tracers Tracers, loggers Loggers) error {
	tracer := tracers["notifications"]
	// 1) Producer trace: enqueue.
	producerCtx, producer := tracer.Start(ctx, "queue.publish",
		trace.WithSpanKind(trace.SpanKindProducer))
	producer.SetAttributes(
		attribute.String("messaging.system", "rabbitmq"),
		attribute.String("messaging.operation", "publish"),
		attribute.String("messaging.destination.name", "email.outbound"),
		attribute.String("customer.tier", pickTier()),
	)
	sleepLike(2*time.Millisecond, 1*time.Millisecond)
	producer.End()

	// Capture the producer's SpanContext — this is what the consumer links
	// back to. In real code this rides on the message payload (e.g. a
	// traceparent header on a Kafka record).
	producerSC := producer.SpanContext()

	// 2) Consumer trace: fresh trace_id, but carries a Link back.
	_, consumer := tracer.Start(ctx, "email.send",
		trace.WithSpanKind(trace.SpanKindConsumer),
		trace.WithNewRoot(), // force a new trace_id
		trace.WithLinks(trace.Link{
			SpanContext: producerSC,
			Attributes: []attribute.KeyValue{
				attribute.String("messaging.operation", "receive"),
				attribute.String("messaging.destination.name", "email.outbound"),
			},
		}),
	)
	consumer.SetAttributes(
		attribute.String("messaging.system", "rabbitmq"),
		attribute.String("messaging.operation", "process"),
		attribute.String("messaging.destination.name", "email.outbound"),
		attribute.Int("messaging.message.delivery_attempt", 1),
	)
	sleepLike(40*time.Millisecond, 20*time.Millisecond)
	consumer.End()

	// Suppress unused-ctx lint; producerCtx isn't used because the consumer
	// runs against a new root rather than a child context.
	_ = producerCtx
	return nil
}

// emitBatchProcess is the event-density template — a single long-running
// span that emits one event per item. Exercises the waterfall's ability to
// render a dense ladder of tick marks on a single bar, and gives queries
// something interesting to count (`SELECT count() WHERE event.name =
// 'item.processed'` once we have event-level datasets).
func emitBatchProcess(ctx context.Context, tracers Tracers, loggers Loggers) error {
	tracer := tracers["db-worker"]
	_, span := tracer.Start(ctx, "batch.process",
		trace.WithSpanKind(trace.SpanKindInternal))
	itemCount := 8 + rand.IntN(12) // 8–19 items
	span.SetAttributes(
		attribute.String("batch.source", "orders.pending"),
		attribute.Int("batch.size", itemCount),
	)
	defer span.End()

	span.AddEvent("batch.started", trace.WithAttributes(
		attribute.Int("batch.size", itemCount),
	))
	sleepLike(2*time.Millisecond, 1*time.Millisecond)

	failed := 0
	for i := range itemCount {
		sleepLike(3*time.Millisecond, 2*time.Millisecond)
		success := rand.IntN(10) != 0 // 10% failure per item
		if success {
			span.AddEvent("item.processed", trace.WithAttributes(
				attribute.Int("item.index", i),
				attribute.String("item.id", fmt.Sprintf("order-%d", rand.IntN(10_000))),
			))
		} else {
			failed++
			span.AddEvent("item.failed", trace.WithAttributes(
				attribute.Int("item.index", i),
				attribute.String("error.reason", "schema_mismatch"),
			))
		}
	}

	span.AddEvent("batch.completed", trace.WithAttributes(
		attribute.Int("batch.processed", itemCount-failed),
		attribute.Int("batch.failed", failed),
	))
	return nil
}

// emitRetryingRequest models an HTTP client retrying a flaky downstream.
// Each retry records a retry event carrying the attempt number and
// backoff delay. The final attempt can either succeed or fail, with the
// latter recording an exception event too (but keeping the client span
// itself OK — retry exhaustion is a business outcome, not a span-level
// bug). Great for seeing event ticks spaced out along a single bar.
func emitRetryingRequest(ctx context.Context, tracers Tracers, loggers Loggers) error {
	tracer := tracers["api-gateway"]
	spanCtx, span := tracer.Start(ctx, "http.client.request",
		trace.WithSpanKind(trace.SpanKindClient))
	span.SetAttributes(
		attribute.String("http.request.method", "POST"),
		attribute.String("http.route", "/v1/webhook"),
		attribute.String("peer.service", "partner-webhook"),
	)
	defer span.End()

	maxAttempts := 2 + rand.IntN(3)         // 2–4 attempts
	succeedOn := rand.IntN(maxAttempts + 1) // might succeed on any attempt, or never

	for attempt := 1; attempt <= maxAttempts; attempt++ {
		sleepLike(10*time.Millisecond, 5*time.Millisecond)
		if attempt == succeedOn {
			span.AddEvent("http.request.sent", trace.WithAttributes(
				attribute.Int("retry.attempt", attempt),
			))
			span.AddEvent("http.response.received", trace.WithAttributes(
				attribute.Int("http.response.status_code", 200),
				attribute.Int("retry.attempt", attempt),
			))
			span.SetAttributes(attribute.Int("http.response.status_code", 200))
			return nil
		}

		backoff := time.Duration((1<<attempt)*5) * time.Millisecond
		span.AddEvent("http.request.failed", trace.WithAttributes(
			attribute.Int("retry.attempt", attempt),
			attribute.Int("http.response.status_code", 503),
			attribute.String("error.kind", "service_unavailable"),
		))
		if attempt < maxAttempts {
			span.AddEvent("retry.scheduled", trace.WithAttributes(
				attribute.Int("retry.attempt", attempt),
				attribute.Int64("retry.backoff_ms", backoff.Milliseconds()),
			))
			time.Sleep(backoff)
		}
	}

	// Exhausted all retries — this is the "recorded exception, span status
	// stays unset" pattern: we did exhaust retries but the caller decides
	// what that means for the overall operation.
	span.AddEvent("retry.exhausted", trace.WithAttributes(
		attribute.Int("retry.attempts", maxAttempts),
	))
	span.AddEvent("exception", trace.WithAttributes(
		attribute.String("exception.type", "RetryExhaustedError"),
		attribute.String("exception.message", "partner-webhook rejected after max retries"),
	))
	span.SetAttributes(attribute.Int("http.response.status_code", 503))
	// Correlated WARN — retry exhaustion is worth surfacing in logs so the
	// user can find the trace from the log stream. Trace/span IDs flow from
	// spanCtx automatically.
	if logger := loggers["api-gateway"]; logger != nil {
		emit(spanCtx, logger, log.SeverityWarn, "WARN",
			fmt.Sprintf("partner-webhook giving up after %d attempts", maxAttempts),
			log.String("peer.service", "partner-webhook"),
			log.Int("retry.attempts", maxAttempts),
			log.String("error.kind", "retry_exhausted"),
		)
	}
	return nil
}

func emitNotificationSend(ctx context.Context, tracers Tracers, loggers Loggers) error {
	tracer := tracers["notifications"]
	ctx, root := tracer.Start(ctx, "email.send", trace.WithSpanKind(trace.SpanKindServer))
	root.SetAttributes(
		attribute.String("messaging.system", "sendgrid"),
		attribute.String("messaging.operation", "send"),
		attribute.String("customer.tier", pickTier()),
	)
	defer root.End()

	_, q := tracer.Start(ctx, "queue.enqueue")
	q.SetAttributes(attribute.String("messaging.destination.name", "email.outbound"))
	sleepLike(2*time.Millisecond, 1*time.Millisecond)
	q.End()

	_, call := tracer.Start(ctx, "sendgrid.v3.mail.send")
	call.SetAttributes(
		attribute.String("peer.service", "sendgrid"),
		attribute.Int("http.response.status_code", 202),
	)
	sleepLike(30*time.Millisecond, 10*time.Millisecond)
	call.End()
	return nil
}

// emitBigCheckoutFlow produces a deep, fan-out trace that spans every
// service and goes 3 levels deep. ~23 spans per trace — useful as the
// demo trace for the waterfall view and for testing pagination / scroll
// in span lists. Call structure (indent = depth):
//
//	POST /checkout/complete        [api-gateway]
//	  auth.validate                [api-gateway]
//	  cart.fetch                   [api-gateway]
//	    db.SELECT cart             [db-worker]
//	  inventory.check              [api-gateway]
//	    db.SELECT inventory (×3)   [db-worker]
//	  payments.authorize           [payments]
//	    fraud.score                [payments]
//	    stripe.charge              [payments]
//	    db.INSERT payment_log      [db-worker]
//	  order.create                 [api-gateway]
//	    db.INSERT orders           [db-worker]
//	    db.INSERT order_items      [db-worker]
//	    db.UPDATE inventory        [db-worker]
//	  notifications.dispatch       [notifications]
//	    email.send                 [notifications]
//	    sms.send                   [notifications]
//	    webhook.send               [notifications]
//	  analytics.emit               [api-gateway]
//	    db.INSERT analytics        [db-worker]
//	  response.serialize           [api-gateway]
func emitBigCheckoutFlow(ctx context.Context, tracers Tracers, loggers Loggers) error {
	api := tracers["api-gateway"]
	pay := tracers["payments"]
	notif := tracers["notifications"]
	db := tracers["db-worker"]

	// Deliberate clock-skew injection: the api-gateway's clock runs 35ms
	// behind everyone else, so the root span's reported [start, end]
	// sits entirely 35ms in the past relative to its children. Same
	// reported duration as before — just shifted. Children keep their
	// real wall-clock timestamps, so they all appear to start a bit
	// before root's reported start *and* the last child's actual end
	// overshoots root's reported end by ~35ms, exercising the
	// extendedToNS propagation and the right-hand whisker.
	const skewOffset = 35 * time.Millisecond
	ctx, root := api.Start(ctx, "POST /checkout/complete",
		trace.WithSpanKind(trace.SpanKindServer),
		trace.WithTimestamp(time.Now().Add(-skewOffset)))
	root.SetAttributes(
		attribute.String("http.request.method", "POST"),
		attribute.String("http.route", "/checkout/complete"),
		attribute.String("url.path", "/checkout/complete"),
		attribute.Int("http.response.status_code", 200),
		attribute.String("customer.tier", pickTier()),
		attribute.String("deployment.region", pickRegion()),
		attribute.Int("cart.item_count", 3+rand.IntN(8)),
		attribute.String("checkout.flow", "one_click"),
	)
	defer func() {
		root.End(trace.WithTimestamp(time.Now().Add(-skewOffset)))
	}()

	// simpleChild starts a sibling under ctx, sets a few attributes, and
	// sleeps a short realistic duration before ending. Keeps the body below
	// readable.
	simpleChild := func(parentCtx context.Context, t trace.Tracer, name string, attrs []attribute.KeyValue, base, jitter time.Duration) {
		_, s := t.Start(parentCtx, name, trace.WithSpanKind(trace.SpanKindInternal))
		if len(attrs) > 0 {
			s.SetAttributes(attrs...)
		}
		sleepLike(base, jitter)
		s.End()
	}

	// auth.validate — flat child.
	simpleChild(ctx, api, "auth.validate",
		[]attribute.KeyValue{
			attribute.String("auth.method", "jwt"),
			attribute.Bool("auth.cached", rand.IntN(2) == 0),
		},
		3*time.Millisecond, 2*time.Millisecond)

	// cart.fetch — one DB child under it.
	cartCtx, cart := api.Start(ctx, "cart.fetch")
	cart.SetAttributes(attribute.String("cart.id", fmt.Sprintf("cart_%d", rand.IntN(100_000))))
	simpleChild(cartCtx, db, "SELECT cart_items",
		[]attribute.KeyValue{
			attribute.String("db.system", "postgresql"),
			attribute.String("db.statement", "SELECT * FROM cart_items WHERE cart_id = $1"),
			attribute.Int("db.rows_returned", 3+rand.IntN(5)),
		},
		4*time.Millisecond, 2*time.Millisecond)
	cart.End()

	// inventory.check — fan-out of 3 parallel-looking DB lookups.
	invCtx, inv := api.Start(ctx, "inventory.check")
	inv.SetAttributes(attribute.Int("inventory.line_count", 3))
	for i := range 3 {
		simpleChild(invCtx, db, "SELECT inventory",
			[]attribute.KeyValue{
				attribute.String("db.system", "postgresql"),
				attribute.String("db.statement", "SELECT qty FROM inventory WHERE sku = $1 FOR UPDATE"),
				attribute.Int("inventory.line.index", i),
				attribute.Int("db.rows_returned", 1),
			},
			3*time.Millisecond, 1*time.Millisecond)
	}
	inv.End()

	// payments.authorize — crosses into the payments service, then into db.
	payCtx, payAuth := pay.Start(ctx, "PaymentService/Authorize",
		trace.WithSpanKind(trace.SpanKindServer))
	payAuth.SetAttributes(
		attribute.String("rpc.system", "grpc"),
		attribute.String("rpc.service", "PaymentService"),
		attribute.String("rpc.method", "Authorize"),
		attribute.String("payments.provider", pickPaymentProvider()),
	)
	simpleChild(payCtx, pay, "FraudService/Check",
		[]attribute.KeyValue{
			attribute.String("rpc.service", "FraudService"),
			attribute.Float64("fraud.score", rand.Float64()),
		},
		6*time.Millisecond, 3*time.Millisecond)
	simpleChild(payCtx, pay, "stripe.authorize",
		[]attribute.KeyValue{
			attribute.String("peer.service", "stripe"),
			attribute.String("http.request.method", "POST"),
			attribute.String("http.route", "/v1/charges"),
			attribute.Int("http.response.status_code", 200),
		},
		35*time.Millisecond, 15*time.Millisecond)
	simpleChild(payCtx, db, "INSERT payment_log",
		[]attribute.KeyValue{
			attribute.String("db.system", "postgresql"),
			attribute.String("db.statement", "INSERT INTO payment_log(...) VALUES(...)"),
		},
		4*time.Millisecond, 2*time.Millisecond)
	payAuth.End()

	// order.create — three DB writes under a single order-create span.
	orderCtx, order := api.Start(ctx, "order.create")
	order.SetAttributes(attribute.String("order.id", fmt.Sprintf("o_%d", rand.IntN(1_000_000))))
	simpleChild(orderCtx, db, "INSERT orders",
		[]attribute.KeyValue{
			attribute.String("db.system", "postgresql"),
			attribute.String("db.statement", "INSERT INTO orders(...) VALUES(...)"),
		},
		4*time.Millisecond, 2*time.Millisecond)
	simpleChild(orderCtx, db, "INSERT order_items",
		[]attribute.KeyValue{
			attribute.String("db.system", "postgresql"),
			attribute.String("db.statement", "INSERT INTO order_items(...) VALUES(...)"),
		},
		5*time.Millisecond, 3*time.Millisecond)
	simpleChild(orderCtx, db, "UPDATE inventory",
		[]attribute.KeyValue{
			attribute.String("db.system", "postgresql"),
			attribute.String("db.statement", "UPDATE inventory SET qty = qty - $1 WHERE sku = $2"),
		},
		4*time.Millisecond, 2*time.Millisecond)
	order.End()

	// notifications.dispatch — fan out to three channels.
	notifCtx, dispatch := notif.Start(ctx, "notifications.dispatch",
		trace.WithSpanKind(trace.SpanKindProducer))
	dispatch.SetAttributes(attribute.Int("notifications.channels", 3))
	simpleChild(notifCtx, notif, "email.send",
		[]attribute.KeyValue{
			attribute.String("messaging.system", "sendgrid"),
			attribute.String("messaging.operation", "send"),
		},
		25*time.Millisecond, 10*time.Millisecond)
	simpleChild(notifCtx, notif, "sms.send",
		[]attribute.KeyValue{
			attribute.String("messaging.system", "twilio"),
			attribute.String("messaging.operation", "send"),
		},
		35*time.Millisecond, 10*time.Millisecond)
	simpleChild(notifCtx, notif, "webhook.send",
		[]attribute.KeyValue{
			attribute.String("peer.service", "partner-webhook"),
			attribute.Int("http.response.status_code", 202),
		},
		20*time.Millisecond, 8*time.Millisecond)
	dispatch.End()

	// analytics.emit — one more db hop.
	anCtx, analytics := api.Start(ctx, "analytics.emit")
	analytics.SetAttributes(attribute.String("analytics.event", "checkout_completed"))
	simpleChild(anCtx, db, "INSERT analytics_events",
		[]attribute.KeyValue{
			attribute.String("db.system", "postgresql"),
			attribute.String("db.statement", "INSERT INTO analytics_events(...) VALUES(...)"),
		},
		3*time.Millisecond, 1*time.Millisecond)
	analytics.End()

	// response.serialize — flat child, ends the request.
	simpleChild(ctx, api, "response.serialize",
		[]attribute.KeyValue{
			attribute.Int("response.body_size_bytes", 800+rand.IntN(2000)),
		},
		2*time.Millisecond, 1*time.Millisecond)

	return nil
}

// -----------------------------------------------------------------------------
// Pick helpers — keep template code readable and data varied.
// -----------------------------------------------------------------------------

func pickRoute() string {
	routes := []string{"/users", "/orders", "/products", "/invoices", "/health"}
	return routes[rand.IntN(len(routes))]
}

func pickUserAgent() string {
	uas := []string{
		"Mozilla/5.0 (Macintosh; Intel Mac OS X 14_5)",
		"curl/8.1.0",
		"waggle-loadgen/0.1",
		"Go-http-client/1.1",
	}
	return uas[rand.IntN(len(uas))]
}

func pickTier() string {
	tiers := []string{"free", "bronze", "silver", "gold", "enterprise"}
	return tiers[rand.IntN(len(tiers))]
}

func pickRegion() string {
	regions := []string{"eu-west-1", "eu-west-2", "us-east-1", "ap-southeast-2"}
	return regions[rand.IntN(len(regions))]
}

func pickPaymentProvider() string {
	ps := []string{"stripe", "adyen", "braintree"}
	return ps[rand.IntN(len(ps))]
}

func pickStatement(system string) string {
	switch system {
	case "postgresql":
		s := []string{
			"SELECT * FROM users WHERE id = $1",
			"INSERT INTO events (id, payload) VALUES ($1, $2)",
			"UPDATE orders SET status = $1 WHERE id = $2",
			"DELETE FROM sessions WHERE expires_at < NOW()",
		}
		return s[rand.IntN(len(s))]
	case "redis":
		s := []string{"GET session:*", "SET rate:user", "INCR counter:requests", "HGET user meta"}
		return s[rand.IntN(len(s))]
	case "mysql":
		s := []string{
			"SELECT * FROM orders WHERE customer_id = ?",
			"UPDATE products SET stock = stock - ? WHERE sku = ?",
		}
		return s[rand.IntN(len(s))]
	}
	return "SELECT 1"
}

// sleepLike sleeps for base±jitter, minimum 100µs. Duration becomes the visible
// span duration via the SDK's time.Now() bracketing.
func sleepLike(base, jitter time.Duration) {
	j := time.Duration(rand.Int64N(int64(2*jitter))) - jitter
	d := base + j
	if d < 100*time.Microsecond {
		d = 100 * time.Microsecond
	}
	time.Sleep(d)
}
