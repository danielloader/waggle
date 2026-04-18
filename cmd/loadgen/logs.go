package main

import (
	"context"
	"fmt"
	"math/rand/v2"
	"strings"
	"time"

	"go.opentelemetry.io/otel/log"
)

// Loggers maps service names to the logger for that service — mirrors
// Tracers. Templates pick their own bucket via tpl.Service.
type Loggers map[string]log.Logger

// LogTemplate produces a single log record. Service must match one of the
// service.name values used by the trace templates so the UI groups them
// together and the FTS index has consistent per-service buckets.
type LogTemplate struct {
	Service string
	Emit    func(ctx context.Context, logger log.Logger)
}

// Service names match exactly the ones in templates.go (allTemplates) so
// log and trace data share the same service.name buckets in waggle.
var allLogs = []LogTemplate{
	// api-gateway — the chatty request-handling side.
	{Service: "api-gateway", Emit: logAPIRequestReceived},
	{Service: "api-gateway", Emit: logAPIResponseSent},
	{Service: "api-gateway", Emit: logAPIAuthWarn},
	{Service: "api-gateway", Emit: logAPIRetryWarn},
	{Service: "api-gateway", Emit: logAPISamplerError},

	// payments — transactional, mostly success but with a decline stream.
	{Service: "payments", Emit: logPaymentAuthorized},
	{Service: "payments", Emit: logPaymentDeclined},
	{Service: "payments", Emit: logPaymentFraudAlert},

	// notifications — messaging activity, bounces and retries.
	{Service: "notifications", Emit: logNotificationEnqueued},
	{Service: "notifications", Emit: logNotificationSent},
	{Service: "notifications", Emit: logNotificationBounce},

	// db-worker — operational chatter.
	{Service: "db-worker", Emit: logDBSlowQuery},
	{Service: "db-worker", Emit: logDBCacheMiss},
	{Service: "db-worker", Emit: logDBConnectionError},
}

// emit sets the common record fields then ships it. Each template picks the
// severity + body; attributes flow through as-is.
func emit(ctx context.Context, logger log.Logger, severity log.Severity, severityText, body string, attrs ...log.KeyValue) {
	now := time.Now()
	var rec log.Record
	rec.SetTimestamp(now)
	rec.SetObservedTimestamp(now)
	rec.SetSeverity(severity)
	rec.SetSeverityText(severityText)
	rec.SetBody(log.StringValue(body))
	if len(attrs) > 0 {
		rec.AddAttributes(attrs...)
	}
	logger.Emit(ctx, rec)
}

// ---------------- api-gateway ----------------

func logAPIRequestReceived(ctx context.Context, logger log.Logger) {
	route := pickRoute()
	method := []string{"GET", "GET", "GET", "POST", "PUT"}[rand.IntN(5)]
	emit(ctx, logger, log.SeverityInfo, "INFO",
		fmt.Sprintf("received request %s %s id=%d", method, route, rand.IntN(100_000)),
		log.String("http.request.method", method),
		log.String("http.route", route),
		log.String("user_agent.original", pickUserAgent()),
	)
}

func logAPIResponseSent(ctx context.Context, logger log.Logger) {
	route := pickRoute()
	status := []int{200, 200, 200, 200, 201, 204, 304, 400, 404, 500}[rand.IntN(10)]
	dur := 5 + rand.IntN(250)
	emit(ctx, logger, log.SeverityInfo, "INFO",
		fmt.Sprintf("sent response %d for %s duration_ms=%d", status, route, dur),
		log.String("http.route", route),
		log.Int("http.response.status_code", status),
		log.Int("duration_ms", dur),
	)
}

func logAPIAuthWarn(ctx context.Context, logger log.Logger) {
	emit(ctx, logger, log.SeverityWarn, "WARN",
		fmt.Sprintf("jwt expired for user=u_%d refreshing via oauth", rand.IntN(10_000)),
		log.String("auth.method", "jwt"),
		log.Bool("auth.refresh_attempted", true),
	)
}

func logAPIRetryWarn(ctx context.Context, logger log.Logger) {
	attempt := 2 + rand.IntN(3)
	emit(ctx, logger, log.SeverityWarn, "WARN",
		fmt.Sprintf("retrying upstream call after 502 attempt=%d peer=partner-webhook", attempt),
		log.Int("retry.attempt", attempt),
		log.Int("http.response.status_code", 502),
		log.String("peer.service", "partner-webhook"),
	)
}

func logAPISamplerError(ctx context.Context, logger log.Logger) {
	batch := 128 + rand.IntN(256)
	emit(ctx, logger, log.SeverityError, "ERROR",
		fmt.Sprintf("trace sampling overload: dropping span batch size=%d", batch),
		log.String("error.type", "sampler_overflow"),
		log.Int("batch.size", batch),
	)
}

// ---------------- payments ----------------

func logPaymentAuthorized(ctx context.Context, logger log.Logger) {
	amount := 500 + rand.IntN(50_000)
	emit(ctx, logger, log.SeverityInfo, "INFO",
		fmt.Sprintf("payment authorized amount_usd=%0.2f txn=ch_%d", float64(amount)/100, rand.IntN(1_000_000)),
		log.String("payments.provider", pickPaymentProvider()),
		log.String("customer.tier", pickTier()),
		log.Int("amount_cents", amount),
	)
}

func logPaymentDeclined(ctx context.Context, logger log.Logger) {
	reasons := []string{"card_declined", "insufficient_funds", "expired_card", "fraud_suspected"}
	reason := reasons[rand.IntN(len(reasons))]
	emit(ctx, logger, log.SeverityError, "ERROR",
		fmt.Sprintf("payment authorization failed: %s customer=c_%d", reason, rand.IntN(100_000)),
		log.String("payments.provider", pickPaymentProvider()),
		log.String("decline.reason", reason),
	)
}

func logPaymentFraudAlert(ctx context.Context, logger log.Logger) {
	score := 0.8 + rand.Float64()*0.2
	emit(ctx, logger, log.SeverityWarn, "WARN",
		fmt.Sprintf("fraud score above threshold score=%.2f rule=velocity_exceeded", score),
		log.Float64("fraud.score", score),
		log.String("fraud.rule", "velocity_exceeded"),
	)
}

// ---------------- notifications ----------------

func logNotificationEnqueued(ctx context.Context, logger log.Logger) {
	templates := []string{"order_receipt", "password_reset", "shipment_update", "welcome"}
	tpl := templates[rand.IntN(len(templates))]
	emit(ctx, logger, log.SeverityInfo, "INFO",
		fmt.Sprintf("notifications.enqueue email to=user%d@example.com template=%s", rand.IntN(10_000), tpl),
		log.String("messaging.operation", "publish"),
		log.String("messaging.destination.name", "email.outbound"),
		log.String("notifications.template", tpl),
	)
}

func logNotificationSent(ctx context.Context, logger log.Logger) {
	provider := []string{"sendgrid", "twilio", "ses"}[rand.IntN(3)]
	emit(ctx, logger, log.SeverityInfo, "INFO",
		fmt.Sprintf("notification delivered via=%s provider_id=n_%d", provider, rand.IntN(1_000_000)),
		log.String("messaging.system", provider),
		log.Int("http.response.status_code", 202),
	)
}

func logNotificationBounce(ctx context.Context, logger log.Logger) {
	emit(ctx, logger, log.SeverityError, "ERROR",
		fmt.Sprintf("email bounced address=invalid%d@example.com reason=mailbox_unavailable", rand.IntN(10_000)),
		log.String("bounce.type", "permanent"),
		log.String("bounce.reason", "mailbox_unavailable"),
	)
}

// ---------------- db-worker ----------------

func logDBSlowQuery(ctx context.Context, logger log.Logger) {
	dur := 500 + rand.IntN(3500)
	emit(ctx, logger, log.SeverityWarn, "WARN",
		fmt.Sprintf("slow query detected duration_ms=%d statement=\"SELECT * FROM orders WHERE user_id=$1\"", dur),
		log.String("db.system", "postgresql"),
		log.Int("duration_ms", dur),
	)
}

func logDBCacheMiss(ctx context.Context, logger log.Logger) {
	key := fmt.Sprintf("session:%x", rand.Int64N(1<<32))
	emit(ctx, logger, log.SeverityDebug, "DEBUG",
		fmt.Sprintf("cache miss key=%s backend=redis", key),
		log.String("cache.backend", "redis"),
		log.String("cache.key", key),
	)
}

func logDBConnectionError(ctx context.Context, logger log.Logger) {
	emit(ctx, logger, log.SeverityError, "ERROR",
		"connection pool exhausted: 20/20 in use waiting_ms=1000",
		log.String("db.system", "postgresql"),
		log.String("error.type", "pool_exhausted"),
	)
}

func filterLogTemplates(all []LogTemplate, services string) []LogTemplate {
	if services == "" {
		return all
	}
	want := map[string]bool{}
	for _, s := range strings.Split(services, ",") {
		if s = strings.TrimSpace(s); s != "" {
			want[s] = true
		}
	}
	out := make([]LogTemplate, 0, len(all))
	for _, t := range all {
		if want[t.Service] {
			out = append(out, t)
		}
	}
	return out
}
