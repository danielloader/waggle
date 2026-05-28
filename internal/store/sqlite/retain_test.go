package sqlite

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/danielloader/waggle/internal/store"
)

// Reproduction battery for the periodic
//
//	"retention sweep failed" err="constraint failed: FOREIGN KEY constraint failed (787)"
//
// log line. Each test case writes a realistic shape of data and runs Retain
// with a cutoff that tries to provoke the failure.

func mkBatch(resID, scopeID uint64, traceTail byte, ts int64) store.Batch {
	tid := []byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, traceTail}
	sid := []byte{1, 2, 3, 4, 5, 6, 7, traceTail}
	end := ts + 1_000_000
	statusCode := int32(0)
	var zeroFlags uint32
	return store.Batch{
		Resources: []store.Resource{{
			ID: resID, ServiceName: "svc",
			AttributesJSON: `{"service.name":"svc"}`,
			FirstSeenNS:    ts, LastSeenNS: ts,
		}},
		Scopes: []store.Scope{{ID: scopeID, Name: "scope"}},
		Events: []store.Event{{
			TimeNS: ts, EndTimeNS: &end,
			ResourceID: resID, ScopeID: scopeID,
			ServiceName: "svc", Name: "root",
			TraceID: tid, SpanID: sid,
			StatusCode:     &statusCode,
			Flags:          &zeroFlags,
			AttributesJSON: `{"meta.signal_type":"span","meta.span_kind":"INTERNAL"}`,
		}},
	}
}

func TestRetain_OldEventsSharedResource(t *testing.T) {
	ctx := context.Background()
	s, err := Open(ctx, filepath.Join(t.TempDir(), "retain.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	now := time.Now().UnixNano()
	old := now - int64(2*time.Hour)
	recent := now - int64(5*time.Minute)

	// Both batches reference the same resource_id (1) and scope_id (1) — the
	// realistic case where one service emits over time.
	if err := s.WriteBatch(ctx, mkBatch(1, 1, 0xAA, old)); err != nil {
		t.Fatal(err)
	}
	if err := s.WriteBatch(ctx, mkBatch(1, 1, 0xBB, recent)); err != nil {
		t.Fatal(err)
	}

	cutoff := now - int64(time.Hour)
	if err := s.Retain(ctx, cutoff); err != nil {
		t.Fatalf("Retain: %v", err)
	}

	// Old event gone, recent event survives, resource still here.
	var n int
	if err := s.ReaderDB().QueryRowContext(ctx, "SELECT COUNT(*) FROM events").Scan(&n); err != nil {
		t.Fatal(err)
	}
	if n != 1 {
		t.Fatalf("expected 1 event remaining, got %d", n)
	}
	if err := s.ReaderDB().QueryRowContext(ctx, "SELECT COUNT(*) FROM resources").Scan(&n); err != nil {
		t.Fatal(err)
	}
	if n != 1 {
		t.Fatalf("expected 1 resource remaining, got %d", n)
	}
}

func TestRetain_AllOld_ResourceDeletable(t *testing.T) {
	ctx := context.Background()
	s, err := Open(ctx, filepath.Join(t.TempDir(), "retain.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	now := time.Now().UnixNano()
	old := now - int64(2*time.Hour)

	if err := s.WriteBatch(ctx, mkBatch(1, 1, 0xAA, old)); err != nil {
		t.Fatal(err)
	}

	cutoff := now - int64(time.Hour)
	if err := s.Retain(ctx, cutoff); err != nil {
		t.Fatalf("Retain: %v", err)
	}

	var n int
	if err := s.ReaderDB().QueryRowContext(ctx, "SELECT COUNT(*) FROM events").Scan(&n); err != nil {
		t.Fatal(err)
	}
	if n != 0 {
		t.Fatalf("expected 0 events, got %d", n)
	}
	if err := s.ReaderDB().QueryRowContext(ctx, "SELECT COUNT(*) FROM resources").Scan(&n); err != nil {
		t.Fatal(err)
	}
	if n != 0 {
		t.Fatalf("expected 0 resources, got %d", n)
	}
}

// Pre-existing orphan: an event's resource_id points to a resources row that
// no longer exists. PRAGMA foreign_keys=ON only checks FK on modification, so
// historical inserts made with FKs off (or via a bypass path) leave this kind
// of dangling reference. Retain shouldn't crash because of unrelated data.
func TestRetain_OrphanedEventResourceID(t *testing.T) {
	ctx := context.Background()
	s, err := Open(ctx, filepath.Join(t.TempDir(), "retain.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	now := time.Now().UnixNano()
	old := now - int64(2*time.Hour)
	recent := now - int64(5*time.Minute)

	// Set up two valid resources.
	if err := s.WriteBatch(ctx, mkBatch(1, 1, 0xAA, old)); err != nil {
		t.Fatal(err)
	}
	if err := s.WriteBatch(ctx, mkBatch(2, 1, 0xBB, recent)); err != nil {
		t.Fatal(err)
	}

	// Now inject an orphaned event referencing a non-existent resource_id 999.
	// We have to defer FK checks for this single insert to simulate the state.
	if _, err := s.WriterDB().ExecContext(ctx, "PRAGMA foreign_keys = OFF"); err != nil {
		t.Fatal(err)
	}
	if _, err := s.WriterDB().ExecContext(ctx, `INSERT INTO events
		(time_ns, end_time_ns, resource_id, scope_id, service_name, name,
		 trace_id, span_id, status_code, flags, attributes)
		VALUES (?, ?, 999, 1, 'svc', 'orphan',
		        x'0102030405060708090a0b0c0d0e0fdd',
		        x'010203040506070d', 0, 0,
		        '{"meta.signal_type":"span","meta.span_kind":"INTERNAL"}')`,
		old, old+1_000_000); err != nil {
		t.Fatal(err)
	}
	if _, err := s.WriterDB().ExecContext(ctx, "PRAGMA foreign_keys = ON"); err != nil {
		t.Fatal(err)
	}

	cutoff := now - int64(time.Hour)
	err = s.Retain(ctx, cutoff)
	t.Logf("Retain returned: %v", err)
	if err != nil {
		t.Fatalf("Retain hit FK violation — REPRODUCED: %v", err)
	}
}

// Resource last_seen_ns is OLD but a more recent event still references it
// (e.g. the resource was first seen long ago and ingest hasn't bumped its
// last_seen_ns yet). Retention must not delete the resource.
func TestRetain_OldResourceWithRecentEvent(t *testing.T) {
	ctx := context.Background()
	s, err := Open(ctx, filepath.Join(t.TempDir(), "retain.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	now := time.Now().UnixNano()
	old := now - int64(2*time.Hour)
	recent := now - int64(5*time.Minute)

	// First batch: resource with last_seen_ns = old.
	if err := s.WriteBatch(ctx, mkBatch(1, 1, 0xAA, old)); err != nil {
		t.Fatal(err)
	}
	// Force resource last_seen_ns back to old, then add a recent event
	// without bumping the resource (simulating a stale-stamp ingest path).
	if _, err := s.WriterDB().ExecContext(ctx,
		"UPDATE resources SET last_seen_ns = ? WHERE resource_id = 1", old); err != nil {
		t.Fatal(err)
	}
	if _, err := s.WriterDB().ExecContext(ctx, `INSERT INTO events
		(time_ns, end_time_ns, resource_id, scope_id, service_name, name,
		 trace_id, span_id, status_code, flags, attributes)
		VALUES (?, ?, 1, 1, 'svc', 'recent',
		        x'0102030405060708090a0b0c0d0e0fcc',
		        x'010203040506070c', 0, 0,
		        '{"meta.signal_type":"span","meta.span_kind":"INTERNAL"}')`,
		recent, recent+1_000_000); err != nil {
		t.Fatal(err)
	}

	cutoff := now - int64(time.Hour)
	if err := s.Retain(ctx, cutoff); err != nil {
		t.Fatalf("Retain (this is the prod failure mode): %v", err)
	}

	var n int
	if err := s.ReaderDB().QueryRowContext(ctx, "SELECT COUNT(*) FROM events").Scan(&n); err != nil {
		t.Fatal(err)
	}
	if n != 1 {
		t.Fatalf("expected 1 event (the recent one), got %d", n)
	}
	if err := s.ReaderDB().QueryRowContext(ctx, "SELECT COUNT(*) FROM resources").Scan(&n); err != nil {
		t.Fatal(err)
	}
	if n != 1 {
		t.Fatalf("expected 1 resource (still referenced by the recent event), got %d", n)
	}
}

// Attribute catalog rollups carry their own last_seen_ns and must age out on
// the same cutoff as events — otherwise the Fields panel keeps surfacing keys
// and values from data that was culled long ago.
func TestRetain_PrunesAttributeCatalog(t *testing.T) {
	ctx := context.Background()
	s, err := Open(ctx, filepath.Join(t.TempDir(), "retain.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	now := time.Now().UnixNano()
	old := now - int64(2*time.Hour)
	recent := now - int64(5*time.Minute)

	if _, err := s.WriterDB().ExecContext(ctx, `INSERT INTO attribute_keys
		(signal_type, service_name, key, value_type, first_seen_ns, last_seen_ns, count)
		VALUES ('span','svc','old.key','str',?,?,1), ('span','svc','new.key','str',?,?,1)`,
		old, old, recent, recent); err != nil {
		t.Fatal(err)
	}
	if _, err := s.WriterDB().ExecContext(ctx, `INSERT INTO attribute_values
		(signal_type, service_name, key, value, count, last_seen_ns)
		VALUES ('span','svc','k','old',1,?), ('span','svc','k','new',1,?)`,
		old, recent); err != nil {
		t.Fatal(err)
	}

	cutoff := now - int64(time.Hour)
	if err := s.Retain(ctx, cutoff); err != nil {
		t.Fatalf("Retain: %v", err)
	}

	var keys, vals int
	if err := s.ReaderDB().QueryRowContext(ctx, "SELECT COUNT(*) FROM attribute_keys").Scan(&keys); err != nil {
		t.Fatal(err)
	}
	if keys != 1 {
		t.Fatalf("expected 1 attribute_key (new.key), got %d", keys)
	}
	if err := s.ReaderDB().QueryRowContext(ctx, "SELECT COUNT(*) FROM attribute_values").Scan(&vals); err != nil {
		t.Fatal(err)
	}
	if vals != 1 {
		t.Fatalf("expected 1 attribute_value (new), got %d", vals)
	}
}

// A scope referenced only by culled events becomes an orphan and should be
// dropped; a scope still referenced by a surviving event must be kept.
func TestRetain_DeletesOrphanedScope_KeepsReferenced(t *testing.T) {
	ctx := context.Background()
	s, err := Open(ctx, filepath.Join(t.TempDir(), "retain.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	now := time.Now().UnixNano()
	old := now - int64(2*time.Hour)
	recent := now - int64(5*time.Minute)

	// Old batch: resource 1, scope 10 (becomes orphaned once its event is culled).
	if err := s.WriteBatch(ctx, mkBatch(1, 10, 0xAA, old)); err != nil {
		t.Fatal(err)
	}
	// Recent batch: resource 2, scope 20 (stays referenced).
	if err := s.WriteBatch(ctx, mkBatch(2, 20, 0xBB, recent)); err != nil {
		t.Fatal(err)
	}

	cutoff := now - int64(time.Hour)
	if err := s.Retain(ctx, cutoff); err != nil {
		t.Fatalf("Retain: %v", err)
	}

	var n int
	if err := s.ReaderDB().QueryRowContext(ctx, "SELECT COUNT(*) FROM scopes WHERE scope_id = 10").Scan(&n); err != nil {
		t.Fatal(err)
	}
	if n != 0 {
		t.Fatalf("expected orphaned scope 10 deleted, got %d", n)
	}
	if err := s.ReaderDB().QueryRowContext(ctx, "SELECT COUNT(*) FROM scopes WHERE scope_id = 20").Scan(&n); err != nil {
		t.Fatal(err)
	}
	if n != 1 {
		t.Fatalf("expected referenced scope 20 kept, got %d", n)
	}
}
