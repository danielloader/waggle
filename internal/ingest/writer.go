package ingest

import (
	"context"
	"log/slog"
	"sync"
	"time"

	"github.com/danielloader/waggle/internal/store"
)

// Writer drains Batches from a buffered channel and writes them to the Store
// inside a single transaction each. One Writer should exist per process.
type Writer struct {
	store store.Store
	log   *slog.Logger

	ch   chan store.Batch
	wg   sync.WaitGroup
	quit chan struct{}

	flushSpans   int
	flushLogs    int
	flushMetrics int
	flushEvery   time.Duration

	droppedCount int64
	droppedMu    sync.Mutex
}

type WriterConfig struct {
	BufferSize   int           // how many batches we buffer before the HTTP handler blocks
	FlushSpans   int           // commit when accumulated spans reach this
	FlushLogs    int           // commit when accumulated logs reach this
	FlushMetrics int           // commit when accumulated metric points reach this
	FlushEvery   time.Duration // or when this time has elapsed since first item
}

func NewWriter(s store.Store, log *slog.Logger, cfg WriterConfig) *Writer {
	if cfg.BufferSize <= 0 {
		cfg.BufferSize = 1024
	}
	if cfg.FlushSpans <= 0 {
		cfg.FlushSpans = 1000
	}
	if cfg.FlushLogs <= 0 {
		cfg.FlushLogs = 2000
	}
	if cfg.FlushMetrics <= 0 {
		cfg.FlushMetrics = 2000
	}
	if cfg.FlushEvery <= 0 {
		cfg.FlushEvery = 50 * time.Millisecond
	}
	return &Writer{
		store:        s,
		log:          log,
		ch:           make(chan store.Batch, cfg.BufferSize),
		quit:         make(chan struct{}),
		flushSpans:   cfg.FlushSpans,
		flushLogs:    cfg.FlushLogs,
		flushMetrics: cfg.FlushMetrics,
		flushEvery:   cfg.FlushEvery,
	}
}

// Start launches the drain goroutine. Call Stop to cleanly shut it down.
func (w *Writer) Start(ctx context.Context) {
	w.wg.Add(1)
	go w.loop(ctx)
}

// Enqueue attempts to send a batch. Returns false if the buffer is full; the
// caller should respond with 503 + Retry-After so the OTLP sender retries.
func (w *Writer) Enqueue(b store.Batch) bool {
	select {
	case w.ch <- b:
		return true
	default:
		w.droppedMu.Lock()
		w.droppedCount++
		w.droppedMu.Unlock()
		return false
	}
}

// DroppedCount returns the number of batches rejected due to a full buffer.
func (w *Writer) DroppedCount() int64 {
	w.droppedMu.Lock()
	defer w.droppedMu.Unlock()
	return w.droppedCount
}

// Stop closes the channel, drains outstanding batches, and waits.
func (w *Writer) Stop(ctx context.Context) error {
	close(w.quit)
	done := make(chan struct{})
	go func() { w.wg.Wait(); close(done) }()
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (w *Writer) loop(ctx context.Context) {
	defer w.wg.Done()

	var (
		pending     store.Batch
		spanCount   int
		logCount    int
		metricCount int
		timer       *time.Timer
		timerC      <-chan time.Time
	)

	flush := func() {
		if spanCount == 0 && logCount == 0 && metricCount == 0 &&
			len(pending.Resources) == 0 && len(pending.Scopes) == 0 {
			return
		}
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if err := w.store.WriteBatch(ctx, pending); err != nil {
			w.log.Error("writeBatch failed", "err", err,
				"spans", spanCount, "logs", logCount, "metric_points", metricCount)
		}
		pending = store.Batch{}
		spanCount = 0
		logCount = 0
		metricCount = 0
		if timer != nil {
			timer.Stop()
			timer = nil
			timerC = nil
		}
	}

	merge := func(b store.Batch) {
		if len(pending.Spans) == 0 && len(pending.Logs) == 0 && len(pending.MetricPoints) == 0 {
			pending = b
		} else {
			pending.Resources = append(pending.Resources, b.Resources...)
			pending.Scopes = append(pending.Scopes, b.Scopes...)
			pending.Spans = append(pending.Spans, b.Spans...)
			pending.Logs = append(pending.Logs, b.Logs...)
			pending.MetricSeries = append(pending.MetricSeries, b.MetricSeries...)
			pending.MetricPoints = append(pending.MetricPoints, b.MetricPoints...)
			pending.AttrKeys = append(pending.AttrKeys, b.AttrKeys...)
			pending.AttrValues = append(pending.AttrValues, b.AttrValues...)
		}
		spanCount += len(b.Spans)
		logCount += len(b.Logs)
		metricCount += len(b.MetricPoints)
		if timer == nil {
			timer = time.NewTimer(w.flushEvery)
			timerC = timer.C
		}
	}

	for {
		select {
		case <-ctx.Done():
			flush()
			return
		case <-w.quit:
			// Drain anything remaining.
			for {
				select {
				case b := <-w.ch:
					merge(b)
				default:
					flush()
					return
				}
			}
		case b := <-w.ch:
			merge(b)
			if spanCount >= w.flushSpans || logCount >= w.flushLogs ||
				metricCount >= w.flushMetrics {
				flush()
			}
		case <-timerC:
			flush()
		}
	}
}
