package ingest_test

// The writer coalesces enqueued batches into one transaction per flush
// window. Every slice on store.Batch has to survive that merge; a slice the
// merge forgets is silently dropped telemetry, with nothing in the logs.

import (
	"context"
	"log/slog"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/danielloader/waggle/internal/ingest"
	"github.com/danielloader/waggle/internal/store"
	"github.com/danielloader/waggle/internal/store/sqlite"
)

// Two batches enqueued inside one flush window must both land. The merge used
// to carry Events but not MetricEvents, so the second batch's metric rows were
// discarded — and because every metric export carries its resource, the first
// batch always left `pending` non-empty and put the next one on that path.
func TestWriterMergePreservesMetricEvents(t *testing.T) {
	ctx := context.Background()
	st, err := sqlite.Open(ctx, filepath.Join(t.TempDir(), "test.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()

	log := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError}))
	// A long flush interval and a high event threshold guarantee both batches
	// sit in the same pending batch until Stop drains them.
	w := ingest.NewWriter(st, log, ingest.WriterConfig{
		FlushEvery:  10 * time.Second,
		FlushEvents: 10_000,
	})
	w.Start(ctx)

	now := time.Now().UnixNano()
	for i, svc := range []string{"first", "second", "third"} {
		if !w.Enqueue(store.Batch{
			Resources: []store.Resource{{
				ID: uint64(i + 1), ServiceName: svc,
				AttributesJSON: `{"service.name":"` + svc + `"}`,
				FirstSeenNS:    now, LastSeenNS: now,
			}},
			Scopes: []store.Scope{{ID: 1, Name: "go.test"}},
			MetricEvents: []store.MetricEvent{{
				TimeNS: now + int64(i), ResourceID: uint64(i + 1), ScopeID: 1, ServiceName: svc,
				AttributesJSON: `{"requests.total":1}`,
			}},
		}) {
			t.Fatalf("Enqueue(%s) dropped", svc)
		}
	}

	sctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := w.Stop(sctx); err != nil {
		t.Fatalf("Stop: %v", err)
	}

	var n int
	if err := st.ReaderDB().QueryRowContext(ctx,
		`SELECT COUNT(*) FROM metric_events`).Scan(&n); err != nil {
		t.Fatal(err)
	}
	if n != 3 {
		t.Errorf("metric_events after merged flush: want 3, got %d", n)
	}
}
