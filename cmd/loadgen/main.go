// loadgen emits streams of realistic traces to an OTLP/HTTP endpoint using
// the real OpenTelemetry Go SDK. It's intended for exercising the workbench
// UI and stress-testing the ingest/storage layers.
//
// Example:
//
//	go run ./cmd/loadgen --endpoint 127.0.0.1:4318 --rate 20 --jitter 0.4
//
// Trace templates live in templates.go and each renders with fresh timestamps
// (time.Now()) at emit time, so durations are realistic. Attribute keys match
// the semconv names the workbench schema's generated columns look for
// (http.route, http.response.status_code, db.system, rpc.service), so the
// generated data lights up the corresponding indexes and UI panels.
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"math/rand/v2"
	"os"
	"os/signal"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/otlp/otlplog/otlploghttp"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	"go.opentelemetry.io/otel/sdk/resource"
	sdklog "go.opentelemetry.io/otel/sdk/log"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
)

func main() {
	var (
		endpoint       = flag.String("endpoint", "127.0.0.1:4318", "OTLP/HTTP host:port to send to")
		insecure       = flag.Bool("insecure", true, "Use http:// instead of https://")
		rate           = flag.Float64("rate", 5.0, "Target traces emitted per second")
		logsRate       = flag.Float64("logs-rate", 0, "Target log records emitted per second (0 disables)")
		jitter         = flag.Float64("jitter", 0.3, "Fraction of the inter-trace period to randomize [0.0–1.0]")
		duration       = flag.Duration("duration", 0, "How long to run (0 = until Ctrl-C)")
		servicesFlag   = flag.String("services", "", "Comma-separated service names to emit (default: all templates)")
		listTemplates  = flag.Bool("list", false, "List the built-in trace templates and exit")
		parallelism    = flag.Int("parallelism", 4, "Number of concurrent trace emitters")
		logInterval    = flag.Duration("log-every", 2*time.Second, "How often to log throughput stats")
	)
	flag.Parse()

	if *listTemplates {
		fmt.Println("Built-in trace templates:")
		for _, tpl := range allTemplates {
			fmt.Printf("  %-20s  %s\n", tpl.Service, tpl.Description)
		}
		return
	}

	if *rate <= 0 {
		log.Fatalf("--rate must be > 0")
	}
	if *logsRate < 0 {
		log.Fatalf("--logs-rate must be >= 0")
	}
	if *jitter < 0 || *jitter > 1 {
		log.Fatalf("--jitter must be in [0.0, 1.0]")
	}
	if *parallelism < 1 {
		log.Fatalf("--parallelism must be >= 1")
	}

	templates := filterTemplates(allTemplates, *servicesFlag)
	if len(templates) == 0 {
		log.Fatalf("no templates matched --services=%q", *servicesFlag)
	}

	// One tracer provider per unique service — so each template's spans get
	// the right resource attached. Real SDK semantics.
	providers, err := buildProviders(templates, *endpoint, *insecure)
	if err != nil {
		log.Fatalf("build providers: %v", err)
	}
	defer func() {
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		for _, tp := range providers {
			_ = tp.Shutdown(shutdownCtx)
		}
	}()

	// Log providers, one per unique service — mirrors the tracer setup so
	// log records carry the right service.name resource. Always built
	// (even when --logs-rate is 0) because trace templates can emit
	// correlation logs inside their own span context, independent of the
	// background log-only emission loop.
	logTemplates := filterLogTemplates(allLogs, *servicesFlag)
	if *logsRate > 0 && len(logTemplates) == 0 {
		log.Fatalf("no log templates matched --services=%q", *servicesFlag)
	}
	logProviders, err := buildLogProvidersForServices(templates, *endpoint, *insecure)
	if err != nil {
		log.Fatalf("build log providers: %v", err)
	}
	defer func() {
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		for _, lp := range logProviders {
			_ = lp.Shutdown(shutdownCtx)
		}
	}()

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	if *duration > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, *duration)
		defer cancel()
	}

	period := time.Duration(float64(time.Second) / *rate)
	log.Printf("loadgen: endpoint=%s rate=%.2f/s logs_rate=%.2f/s period=%s jitter=%.0f%% parallelism=%d trace_templates=%d log_templates=%d",
		*endpoint, *rate, *logsRate, period, *jitter*100, *parallelism, len(templates), len(logTemplates))

	emitted := &atomic.Int64{}
	failed := &atomic.Int64{}
	logsEmitted := &atomic.Int64{}

	go reportThroughput(ctx, emitted, failed, logsEmitted, *logInterval)

	// The rate limiter: one ticker yields "tokens" at the target rate; workers
	// consume tokens. This decouples rate from per-trace cost (a slow template
	// won't drag the rate down as long as parallelism > 1).
	// Build a lookup map of per-service tracers so cross-service templates
	// can start spans on another provider inside the same trace context.
	// Single-service templates just grab their own entry.
	tracers := Tracers{}
	for svc, p := range providers {
		tracers[svc] = p.Tracer("loadgen")
	}
	loggers := Loggers{}
	for svc, p := range logProviders {
		loggers[svc] = p.Logger("loadgen")
	}

	tokens := make(chan struct{}, *parallelism)
	var wg sync.WaitGroup
	for range *parallelism {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for range tokens {
				tpl := templates[rand.IntN(len(templates))]
				if err := tpl.Emit(ctx, tracers, loggers); err != nil {
					failed.Add(1)
					continue
				}
				emitted.Add(1)
			}
		}()
	}

	go func() {
		defer close(tokens)
		for {
			sleep := jitterDuration(period, *jitter)
			select {
			case <-ctx.Done():
				return
			case <-time.After(sleep):
			}
			select {
			case tokens <- struct{}{}:
			case <-ctx.Done():
				return
			default:
				// Workers saturated; drop the tick to back off, matching what
				// a real producer does under downstream pressure.
			}
		}
	}()

	// Log emitter. Independent cadence from traces — one goroutine loop is
	// plenty because each emit is just a record append to the batch
	// processor.
	if *logsRate > 0 {
		logPeriod := time.Duration(float64(time.Second) / *logsRate)
		go func() {
			for {
				sleep := jitterDuration(logPeriod, *jitter)
				select {
				case <-ctx.Done():
					return
				case <-time.After(sleep):
				}
				tpl := logTemplates[rand.IntN(len(logTemplates))]
				logger, ok := loggers[tpl.Service]
				if !ok {
					continue
				}
				tpl.Emit(ctx, logger)
				logsEmitted.Add(1)
			}
		}()
	}

	<-ctx.Done()
	wg.Wait()

	log.Printf("loadgen: stopped. emitted=%d failed=%d logs_emitted=%d",
		emitted.Load(), failed.Load(), logsEmitted.Load())
}

// jitterDuration returns base scaled by a random factor in [1-jitter, 1+jitter].
func jitterDuration(base time.Duration, jitter float64) time.Duration {
	if jitter <= 0 {
		return base
	}
	// 2*jitter*rand in [0, 2*jitter); shift to [-jitter, +jitter).
	factor := 1.0 + (rand.Float64()*2-1)*jitter
	if factor < 0.05 {
		factor = 0.05 // clamp so we don't spin
	}
	return time.Duration(float64(base) * factor)
}

// buildLogProvidersForServices builds one LoggerProvider per trace-template
// service so trace templates can emit correlation logs under the right
// service.name resource. Using trace templates (not log templates) as the
// service set ensures every service that can emit a span can also emit a
// log — independent of whether the log-only background loop is active.
func buildLogProvidersForServices(tpls []TraceTemplate, endpoint string, insecure bool) (map[string]*sdklog.LoggerProvider, error) {
	byService := map[string]*sdklog.LoggerProvider{}
	services := map[string]struct{}{}
	for _, t := range tpls {
		services[t.Service] = struct{}{}
	}

	for service := range services {
		opts := []otlploghttp.Option{otlploghttp.WithEndpoint(endpoint)}
		if insecure {
			opts = append(opts, otlploghttp.WithInsecure())
		}
		exp, err := otlploghttp.New(context.Background(), opts...)
		if err != nil {
			return nil, fmt.Errorf("log exporter for %s: %w", service, err)
		}
		res, err := resource.New(context.Background(), resource.WithAttributes(
			attribute.String("service.name", service),
			attribute.String("service.version", "loadgen-0.1.0"),
			attribute.String("deployment.environment", "dev"),
			attribute.String("telemetry.sdk.name", "waggle-loadgen"),
		))
		if err != nil {
			return nil, fmt.Errorf("resource for %s: %w", service, err)
		}
		byService[service] = sdklog.NewLoggerProvider(
			sdklog.WithProcessor(sdklog.NewBatchProcessor(exp,
				sdklog.WithExportInterval(250*time.Millisecond),
				sdklog.WithExportMaxBatchSize(256),
			)),
			sdklog.WithResource(res),
		)
	}
	return byService, nil
}

func buildProviders(tpls []TraceTemplate, endpoint string, insecure bool) (map[string]*sdktrace.TracerProvider, error) {
	byService := map[string]*sdktrace.TracerProvider{}
	services := map[string]struct{}{}
	for _, t := range tpls {
		services[t.Service] = struct{}{}
	}

	for service := range services {
		opts := []otlptracehttp.Option{otlptracehttp.WithEndpoint(endpoint)}
		if insecure {
			opts = append(opts, otlptracehttp.WithInsecure())
		}
		exp, err := otlptracehttp.New(context.Background(), opts...)
		if err != nil {
			return nil, fmt.Errorf("exporter for %s: %w", service, err)
		}
		res, err := resource.New(context.Background(), resource.WithAttributes(
			attribute.String("service.name", service),
			attribute.String("service.version", "loadgen-0.1.0"),
			attribute.String("deployment.environment", "dev"),
			attribute.String("telemetry.sdk.name", "waggle-loadgen"),
		))
		if err != nil {
			return nil, fmt.Errorf("resource for %s: %w", service, err)
		}
		byService[service] = sdktrace.NewTracerProvider(
			sdktrace.WithBatcher(exp,
				sdktrace.WithBatchTimeout(250*time.Millisecond),
				sdktrace.WithMaxExportBatchSize(256),
			),
			sdktrace.WithResource(res),
		)
	}
	return byService, nil
}

func filterTemplates(all []TraceTemplate, services string) []TraceTemplate {
	if services == "" {
		return all
	}
	want := map[string]bool{}
	for _, s := range strings.Split(services, ",") {
		if s = strings.TrimSpace(s); s != "" {
			want[s] = true
		}
	}
	out := make([]TraceTemplate, 0, len(all))
	for _, t := range all {
		if want[t.Service] {
			out = append(out, t)
		}
	}
	return out
}

func reportThroughput(ctx context.Context, emitted, failed, logsEmitted *atomic.Int64, every time.Duration) {
	if every <= 0 {
		return
	}
	t := time.NewTicker(every)
	defer t.Stop()
	var lastEmitted, lastLogs int64
	var lastTime = time.Now()
	for {
		select {
		case <-ctx.Done():
			return
		case now := <-t.C:
			cur := emitted.Load()
			curLogs := logsEmitted.Load()
			delta := cur - lastEmitted
			deltaLogs := curLogs - lastLogs
			dt := now.Sub(lastTime).Seconds()
			if dt > 0 {
				log.Printf("loadgen: emitted=%d failed=%d rate=%.1f/s logs_emitted=%d logs_rate=%.1f/s",
					cur, failed.Load(), float64(delta)/dt,
					curLogs, float64(deltaLogs)/dt)
			}
			lastEmitted = cur
			lastLogs = curLogs
			lastTime = now
		}
	}
}

// Accept an "env-var mode" for the endpoint, matching the OTel convention, so
// loadgen can be wired into existing OTEL_EXPORTER_OTLP_ENDPOINT workflows.
func init() {
	if v := os.Getenv("OTEL_EXPORTER_OTLP_ENDPOINT"); v != "" {
		// Strip http:// prefix if present; otlptracehttp expects host:port.
		v = strings.TrimPrefix(v, "http://")
		v = strings.TrimPrefix(v, "https://")
		v = strings.TrimSuffix(v, "/")
		// Only overwrite flag.Lookup default; CLI still wins.
		if f := flag.Lookup("endpoint"); f != nil {
			f.DefValue = v
			_ = f.Value.Set(v)
		}
	}
}

