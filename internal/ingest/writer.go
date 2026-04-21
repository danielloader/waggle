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

	flushEvents int
	flushEvery  time.Duration

	droppedCount int64
	droppedMu    sync.Mutex
}

type WriterConfig struct {
	BufferSize  int           // how many batches we buffer before the HTTP handler blocks
	FlushEvents int           // commit when accumulated events reach this
	FlushEvery  time.Duration // or when this time has elapsed since first item
}

func NewWriter(s store.Store, log *slog.Logger, cfg WriterConfig) *Writer {
	if cfg.BufferSize <= 0 {
		cfg.BufferSize = 1024
	}
	if cfg.FlushEvents <= 0 {
		cfg.FlushEvents = 2000
	}
	if cfg.FlushEvery <= 0 {
		cfg.FlushEvery = 50 * time.Millisecond
	}
	return &Writer{
		store:       s,
		log:         log,
		ch:          make(chan store.Batch, cfg.BufferSize),
		quit:        make(chan struct{}),
		flushEvents: cfg.FlushEvents,
		flushEvery:  cfg.FlushEvery,
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
		pending        store.Batch
		eventCount     int
		metaOverwrites int64
		timer          *time.Timer
		timerC         <-chan time.Time
	)

	flush := func() {
		if eventCount == 0 && len(pending.Resources) == 0 && len(pending.Scopes) == 0 {
			return
		}
		// 30s covers the worst-case commit: a WAL autocheckpoint firing
		// mid-batch under read pressure, plus a larger-than-usual insert.
		// 10s was too tight — we saw ctx cancellations cascade whenever a
		// single batch hit that blip.
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		if err := w.store.WriteBatch(ctx, pending); err != nil {
			w.log.Error("writeBatch failed", "err", err, "events", eventCount)
		}
		if metaOverwrites > 0 {
			w.log.Warn("ingest overwrote reserved meta.* attrs",
				"count", metaOverwrites,
				"note", "an OTel SDK set a key in the reserved whitelist (meta.signal_type, meta.span_kind, …)")
		}
		pending = store.Batch{}
		eventCount = 0
		metaOverwrites = 0
		if timer != nil {
			timer.Stop()
			timer = nil
			timerC = nil
		}
	}

	merge := func(b store.Batch) {
		if len(pending.Events) == 0 && len(pending.Resources) == 0 && len(pending.Scopes) == 0 {
			pending = b
		} else {
			pending.Resources = append(pending.Resources, b.Resources...)
			pending.Scopes = append(pending.Scopes, b.Scopes...)
			pending.Events = append(pending.Events, b.Events...)
			pending.AttrKeys = append(pending.AttrKeys, b.AttrKeys...)
			pending.AttrValues = append(pending.AttrValues, b.AttrValues...)
			pending.MetaOverwrites += b.MetaOverwrites
		}
		eventCount += len(b.Events)
		metaOverwrites += b.MetaOverwrites
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
			w.drainAndFlush(merge, flush)
			return
		case b := <-w.ch:
			merge(b)
			if eventCount >= w.flushEvents {
				flush()
			}
		case <-timerC:
			flush()
		}
	}
}

func (w *Writer) drainAndFlush(merge func(store.Batch), flush func()) {
	for {
		select {
		case b := <-w.ch:
			merge(b)
		default:
			flush()
			return
		}
	}
}
