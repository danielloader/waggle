package sqlite

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/danielloader/waggle/internal/query"
	"github.com/danielloader/waggle/internal/store"
)

// spanEvent is a compact constructor for a span-flavoured Event row used in
// tests. Real ingestion wraps this in the OTLP transform; here we build it
// by hand so each test can control every field.
func spanEvent(tid, sid, parentSID []byte, service, name string, statusCode int32, startNS, endNS int64, attrs string) store.Event {
	end := endNS
	sc := statusCode
	var flags uint32
	if attrs == "" {
		attrs = `{"meta.signal_type":"span","meta.span_kind":"INTERNAL"}`
	}
	return store.Event{
		TimeNS: startNS, EndTimeNS: &end,
		ResourceID: 1, ScopeID: 1,
		ServiceName: service, Name: name,
		TraceID:        tid,
		SpanID:         sid,
		ParentSpanID:   parentSID,
		StatusCode:     &sc,
		Flags:          &flags,
		AttributesJSON: attrs,
	}
}

// TestRate_PostProcessDiffsPerBucket exercises the rate post-processor
// end-to-end: emit spans with a cumulative `bytes_out` attribute across
// buckets, query with rate_sum(bytes_out), and assert the rate matches
// the expected per-second delta.
func TestRate_PostProcessDiffsPerBucket(t *testing.T) {
	ctx := context.Background()
	s, err := Open(ctx, filepath.Join(t.TempDir(), "rate.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	base := time.Date(2026, 4, 17, 10, 0, 0, 0, time.UTC).UnixNano()
	bucketNS := int64(time.Second)
	values := []int64{100, 300, 600, 900, 1400}

	batch := store.Batch{
		Resources: []store.Resource{{
			ID: 1, ServiceName: "svc",
			AttributesJSON: `{"service.name":"svc"}`,
			FirstSeenNS:    base, LastSeenNS: base,
		}},
		Scopes: []store.Scope{{ID: 1, Name: "lib"}},
	}
	for i, v := range values {
		tid := make([]byte, 16)
		tid[0] = byte(i + 1)
		attrs := `{"meta.signal_type":"span","meta.span_kind":"INTERNAL","bytes_out":` + itoa(v) + `}`
		batch.Events = append(batch.Events, spanEvent(
			tid, []byte{byte(i + 1), 0, 0, 0, 0, 0, 0, 1}, nil,
			"svc", "op", 0,
			base+int64(i)*bucketNS, base+int64(i)*bucketNS+1_000_000,
			attrs,
		))
	}
	if err := s.WriteBatch(ctx, batch); err != nil {
		t.Fatalf("WriteBatch: %v", err)
	}

	q := &query.Query{
		Dataset: query.DatasetSpans,
		TimeRange: query.TimeRange{
			From: query.JSONTime{Time: time.Unix(0, base-time.Hour.Nanoseconds())},
			To:   query.JSONTime{Time: time.Unix(0, base+time.Hour.Nanoseconds())},
		},
		Select:   []query.Aggregation{{Op: query.OpRateSum, Field: "bytes_out"}},
		BucketMS: 1000,
	}
	if err := q.Validate(); err != nil {
		t.Fatalf("validate: %v", err)
	}
	compiled, err := query.Build(q)
	if err != nil {
		t.Fatalf("build: %v", err)
	}
	cols := make([]store.QueryColumn, len(compiled.Columns))
	for i, c := range compiled.Columns {
		cols[i] = store.QueryColumn{Name: c.Name, Type: c.Type}
	}
	rates := make([]store.QueryRateSpec, len(compiled.Rates))
	for i, r := range compiled.Rates {
		rates[i] = store.QueryRateSpec{ColumnIndex: r.ColumnIndex, BucketSecs: r.BucketSecs}
	}

	res, err := s.RunQuery(ctx, compiled.SQL, compiled.Args, cols, compiled.HasBucket, compiled.GroupKeys, rates)
	if err != nil {
		t.Fatalf("RunQuery: %v", err)
	}
	if !res.HasBucket {
		t.Fatalf("expected HasBucket")
	}
	if len(res.Rows) != len(values) {
		t.Fatalf("want %d buckets, got %d", len(values), len(res.Rows))
	}

	expected := []any{nil, float64(200), float64(300), float64(300), float64(500)}
	rateCol := compiled.Rates[0].ColumnIndex
	for i, row := range res.Rows {
		got := row[rateCol]
		if got == nil && expected[i] == nil {
			continue
		}
		gf, ok := got.(float64)
		if !ok {
			t.Errorf("row %d: want float rate, got %T (%v)", i, got, got)
			continue
		}
		want, _ := expected[i].(float64)
		if gf != want {
			t.Errorf("row %d rate: want %v, got %v (row=%+v)", i, want, gf, row)
		}
	}
}

// TestIsRoot_SelectsRootSpansEndToEnd seeds a trace with a root plus two
// children and asserts that a query filtering `is_root = true` returns only
// the root row, and that using is_root as a GROUP BY splits the counts.
func TestIsRoot_SelectsRootSpansEndToEnd(t *testing.T) {
	ctx := context.Background()
	s, err := Open(ctx, filepath.Join(t.TempDir(), "isroot.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	now := time.Now().UnixNano()
	tid := []byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16}
	rootSID := []byte{0x10, 0, 0, 0, 0, 0, 0, 1}
	childA := []byte{0x10, 0, 0, 0, 0, 0, 0, 2}
	childB := []byte{0x10, 0, 0, 0, 0, 0, 0, 3}

	batch := store.Batch{
		Resources: []store.Resource{{
			ID: 1, ServiceName: "svc",
			AttributesJSON: `{"service.name":"svc"}`,
			FirstSeenNS:    now, LastSeenNS: now,
		}},
		Scopes: []store.Scope{{ID: 1, Name: "lib"}},
		Events: []store.Event{
			spanEvent(tid, rootSID, nil, "svc", "root", 0, now, now+10_000_000, ""),
			spanEvent(tid, childA, rootSID, "svc", "childA", 0, now+1, now+5_000_000, ""),
			spanEvent(tid, childB, rootSID, "svc", "childB", 0, now+2, now+3_000_000, ""),
		},
	}
	if err := s.WriteBatch(ctx, batch); err != nil {
		t.Fatal(err)
	}

	q := &query.Query{
		Dataset: query.DatasetSpans,
		TimeRange: query.TimeRange{
			From: query.JSONTime{Time: time.Unix(0, now-time.Hour.Nanoseconds())},
			To:   query.JSONTime{Time: time.Unix(0, now+time.Hour.Nanoseconds())},
		},
		Where:   []query.Filter{{Field: "is_root", Op: query.FilterEq, Value: true}},
		OrderBy: []query.Order{{Field: "duration_ns", Dir: "desc"}},
		Limit:   10,
	}
	if err := q.Validate(); err != nil {
		t.Fatalf("validate: %v", err)
	}
	compiled, err := query.Build(q)
	if err != nil {
		t.Fatalf("build: %v", err)
	}
	cols := make([]store.QueryColumn, len(compiled.Columns))
	for i, c := range compiled.Columns {
		cols[i] = store.QueryColumn{Name: c.Name, Type: c.Type}
	}
	res, err := s.RunQuery(ctx, compiled.SQL, compiled.Args, cols, compiled.HasBucket, compiled.GroupKeys, nil)
	if err != nil {
		t.Fatalf("RunQuery: %v", err)
	}
	if len(res.Rows) != 1 {
		t.Fatalf("is_root=true: want 1 row, got %d (%v)", len(res.Rows), res.Rows)
	}
	nameIdx := -1
	for i, c := range res.Columns {
		if c.Name == "name" {
			nameIdx = i
			break
		}
	}
	if nameIdx < 0 {
		t.Fatal("no name column in result")
	}
	if res.Rows[0][nameIdx] != "root" {
		t.Errorf("expected root span; got %v", res.Rows[0][nameIdx])
	}

	q2 := &query.Query{
		Dataset:   query.DatasetSpans,
		TimeRange: q.TimeRange,
		Select:    []query.Aggregation{{Op: query.OpCount}},
		GroupBy:   []string{"is_root"},
	}
	if err := q2.Validate(); err != nil {
		t.Fatalf("validate: %v", err)
	}
	c2, err := query.Build(q2)
	if err != nil {
		t.Fatal(err)
	}
	cols2 := make([]store.QueryColumn, len(c2.Columns))
	for i, c := range c2.Columns {
		cols2[i] = store.QueryColumn{Name: c.Name, Type: c.Type}
	}
	res2, err := s.RunQuery(ctx, c2.SQL, c2.Args, cols2, c2.HasBucket, c2.GroupKeys, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(res2.Rows) != 2 {
		t.Fatalf("group by is_root: want 2 rows, got %d (%v)", len(res2.Rows), res2.Rows)
	}
	byRoot := map[int64]int64{}
	for _, row := range res2.Rows {
		rv, _ := row[0].(int64)
		cv, _ := row[1].(int64)
		byRoot[rv] = cv
	}
	if byRoot[1] != 1 {
		t.Errorf("root count: want 1, got %d", byRoot[1])
	}
	if byRoot[0] != 2 {
		t.Errorf("child count: want 2, got %d", byRoot[0])
	}
}

// TestError_SyntheticFieldCombinesStatusAndExceptionEvents seeds three
// spans — plain OK, ERROR status, and OK-status-with-exception-event — and
// confirms `error = true` picks up the latter two.
func TestError_SyntheticFieldCombinesStatusAndExceptionEvents(t *testing.T) {
	ctx := context.Background()
	s, err := Open(ctx, filepath.Join(t.TempDir(), "error.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	now := time.Now().UnixNano()
	tid := []byte{0xAA, 0xBB, 0xCC, 0xDD, 0xEE, 0xFF, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9}
	okSID := []byte{0x10, 0, 0, 0, 0, 0, 0, 1}
	errSID := []byte{0x10, 0, 0, 0, 0, 0, 0, 2}
	excSID := []byte{0x10, 0, 0, 0, 0, 0, 0, 3}

	excEvent := spanEvent(tid, excSID, nil, "svc", "exc", 0, now, now+1_000_000, "")
	excEvent.SpanEvents = []store.SpanEvent{{
		TraceID: tid, SpanID: excSID, Seq: 0,
		TimeNS: now, Name: "exception",
		AttributesJSON: `{"exception.type":"TimeoutError","exception.message":"deadline exceeded"}`,
	}}

	batch := store.Batch{
		Resources: []store.Resource{{
			ID: 1, ServiceName: "svc",
			AttributesJSON: `{"service.name":"svc"}`,
			FirstSeenNS:    now, LastSeenNS: now,
		}},
		Scopes: []store.Scope{{ID: 1, Name: "lib"}},
		Events: []store.Event{
			spanEvent(tid, okSID, nil, "svc", "ok", 0, now, now+1_000_000, ""),
			spanEvent(tid, errSID, nil, "svc", "err", 2, now, now+1_000_000, ""),
			excEvent,
		},
	}
	if err := s.WriteBatch(ctx, batch); err != nil {
		t.Fatalf("WriteBatch: %v", err)
	}

	run := func(where []query.Filter) int {
		t.Helper()
		q := &query.Query{
			Dataset: query.DatasetSpans,
			TimeRange: query.TimeRange{
				From: query.JSONTime{Time: time.Unix(0, now-time.Hour.Nanoseconds())},
				To:   query.JSONTime{Time: time.Unix(0, now+time.Hour.Nanoseconds())},
			},
			Select: []query.Aggregation{{Op: query.OpCount}},
			Where:  where,
		}
		if err := q.Validate(); err != nil {
			t.Fatalf("validate: %v", err)
		}
		c, err := query.Build(q)
		if err != nil {
			t.Fatal(err)
		}
		cols := make([]store.QueryColumn, len(c.Columns))
		for i, col := range c.Columns {
			cols[i] = store.QueryColumn{Name: col.Name, Type: col.Type}
		}
		res, err := s.RunQuery(ctx, c.SQL, c.Args, cols, c.HasBucket, c.GroupKeys, nil)
		if err != nil {
			t.Fatalf("RunQuery: %v", err)
		}
		if len(res.Rows) != 1 {
			t.Fatalf("want 1 row, got %d", len(res.Rows))
		}
		n, _ := res.Rows[0][0].(int64)
		return int(n)
	}

	if n := run([]query.Filter{{Field: "status_code", Op: query.FilterEq, Value: 2}}); n != 1 {
		t.Errorf("status_code=2 count: want 1, got %d", n)
	}
	if n := run([]query.Filter{{Field: "error", Op: query.FilterEq, Value: true}}); n != 2 {
		t.Errorf("error=true count: want 2, got %d", n)
	}
	if n := run([]query.Filter{{Field: "error", Op: query.FilterEq, Value: false}}); n != 1 {
		t.Errorf("error=false count: want 1, got %d", n)
	}
}

func itoa(v int64) string {
	if v == 0 {
		return "0"
	}
	neg := v < 0
	if neg {
		v = -v
	}
	var b [20]byte
	i := len(b)
	for v > 0 {
		i--
		b[i] = byte('0' + v%10)
		v /= 10
	}
	if neg {
		i--
		b[i] = '-'
	}
	return string(b[i:])
}
