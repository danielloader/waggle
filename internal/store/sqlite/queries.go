package sqlite

import (
	"context"
	"database/sql"
	"encoding/hex"
	"errors"
	"fmt"
	"strconv"
	"strings"

	"github.com/danielloader/waggle/internal/store"
)

// WriteBatch persists one ingest batch inside a single transaction.
//
// Order: resources → scopes → events → span_events → span_links →
// metric_events → attribute_keys → attribute_values.
func (s *Store) WriteBatch(ctx context.Context, b store.Batch) error {
	if len(b.Events) == 0 && len(b.MetricEvents) == 0 &&
		len(b.Resources) == 0 && len(b.Scopes) == 0 {
		return nil
	}

	tx, err := s.writer.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin tx: %w", err)
	}
	defer tx.Rollback()

	if err := writeResources(ctx, tx, b.Resources); err != nil {
		return err
	}
	if err := writeScopes(ctx, tx, b.Scopes); err != nil {
		return err
	}
	if err := writeEvents(ctx, tx, b.Events); err != nil {
		return err
	}
	if err := writeMetricEvents(ctx, tx, b.MetricEvents); err != nil {
		return err
	}
	if err := writeAttrKeys(ctx, tx, b.AttrKeys); err != nil {
		return err
	}
	if err := writeAttrValues(ctx, tx, b.AttrValues); err != nil {
		return err
	}
	return tx.Commit()
}

func writeResources(ctx context.Context, tx *sql.Tx, rs []store.Resource) error {
	if len(rs) == 0 {
		return nil
	}
	const q = `INSERT INTO resources
		(resource_id, service_name, service_namespace, service_version,
		 service_instance_id, sdk_name, sdk_language, sdk_version,
		 attributes, first_seen_ns, last_seen_ns)
		VALUES (?,?,?,?,?,?,?,?,?,?,?)
		ON CONFLICT(resource_id) DO UPDATE SET
		  last_seen_ns  = max(last_seen_ns, excluded.last_seen_ns),
		  first_seen_ns = min(first_seen_ns, excluded.first_seen_ns)`
	stmt, err := tx.PrepareContext(ctx, q)
	if err != nil {
		return fmt.Errorf("prepare resources: %w", err)
	}
	defer stmt.Close()
	for _, r := range rs {
		if _, err := stmt.ExecContext(ctx,
			int64(r.ID), r.ServiceName, nullStr(r.ServiceNamespace), nullStr(r.ServiceVersion),
			nullStr(r.ServiceInstanceID), nullStr(r.SDKName), nullStr(r.SDKLanguage),
			nullStr(r.SDKVersion), r.AttributesJSON, r.FirstSeenNS, r.LastSeenNS,
		); err != nil {
			return fmt.Errorf("insert resource %d: %w", r.ID, err)
		}
	}
	return nil
}

func writeScopes(ctx context.Context, tx *sql.Tx, ss []store.Scope) error {
	if len(ss) == 0 {
		return nil
	}
	const q = `INSERT INTO scopes (scope_id, name, version, attributes)
		VALUES (?,?,?,?) ON CONFLICT(scope_id) DO NOTHING`
	stmt, err := tx.PrepareContext(ctx, q)
	if err != nil {
		return fmt.Errorf("prepare scopes: %w", err)
	}
	defer stmt.Close()
	for _, sc := range ss {
		if _, err := stmt.ExecContext(ctx,
			int64(sc.ID), sc.Name, nullStr(sc.Version), nullStr(sc.AttributesJSON),
		); err != nil {
			return fmt.Errorf("insert scope %d: %w", sc.ID, err)
		}
	}
	return nil
}

func writeEvents(ctx context.Context, tx *sql.Tx, events []store.Event) error {
	if len(events) == 0 {
		return nil
	}
	const q = `INSERT INTO events
		(time_ns, end_time_ns, resource_id, scope_id, service_name, name,
		 trace_id, span_id, parent_span_id, status_code, status_message,
		 trace_state, flags, severity_number, severity_text, body,
		 observed_time_ns, attributes)
		VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)`
	stmt, err := tx.PrepareContext(ctx, q)
	if err != nil {
		return fmt.Errorf("prepare events: %w", err)
	}
	defer stmt.Close()

	const eventQ = `INSERT INTO span_events
		(trace_id, span_id, seq, time_ns, name, attributes, dropped_attrs_count)
		VALUES (?,?,?,?,?,?,?) ON CONFLICT DO NOTHING`
	evStmt, err := tx.PrepareContext(ctx, eventQ)
	if err != nil {
		return fmt.Errorf("prepare span_events: %w", err)
	}
	defer evStmt.Close()

	const linkQ = `INSERT INTO span_links
		(trace_id, span_id, seq, linked_trace_id, linked_span_id, trace_state,
		 flags, attributes, dropped_attrs_count)
		VALUES (?,?,?,?,?,?,?,?,?) ON CONFLICT DO NOTHING`
	lnStmt, err := tx.PrepareContext(ctx, linkQ)
	if err != nil {
		return fmt.Errorf("prepare span_links: %w", err)
	}
	defer lnStmt.Close()

	for _, e := range events {
		if _, err := stmt.ExecContext(ctx,
			e.TimeNS, nullInt64Ptr(e.EndTimeNS), int64(e.ResourceID), int64(e.ScopeID),
			e.ServiceName, e.Name,
			nullBytes(e.TraceID), nullBytes(e.SpanID), nullBytes(e.ParentSpanID),
			nullInt32Ptr(e.StatusCode), nullStr(e.StatusMessage),
			nullStr(e.TraceState), nullUint32Ptr(e.Flags),
			nullInt32Ptr(e.SeverityNumber), nullStr(e.SeverityText),
			nullStr(e.Body), nullInt64Ptr(e.ObservedTimeNS),
			e.AttributesJSON,
		); err != nil {
			return fmt.Errorf("insert event: %w", err)
		}
		for _, ev := range e.SpanEvents {
			if _, err := evStmt.ExecContext(ctx,
				ev.TraceID, ev.SpanID, ev.Seq, ev.TimeNS, ev.Name,
				ev.AttributesJSON, ev.DroppedAttrsCount,
			); err != nil {
				return fmt.Errorf("insert span_event: %w", err)
			}
		}
		for _, ln := range e.SpanLinks {
			if _, err := lnStmt.ExecContext(ctx,
				ln.TraceID, ln.SpanID, ln.Seq, ln.LinkedTraceID, ln.LinkedSpanID,
				nullStr(ln.TraceState), ln.Flags, ln.AttributesJSON, ln.DroppedAttrsCount,
			); err != nil {
				return fmt.Errorf("insert span_link: %w", err)
			}
		}
	}
	return nil
}

func writeMetricEvents(ctx context.Context, tx *sql.Tx, rows []store.MetricEvent) error {
	if len(rows) == 0 {
		return nil
	}
	const q = `INSERT INTO metric_events
		(time_ns, resource_id, scope_id, service_name, attributes)
		VALUES (?,?,?,?,?)`
	stmt, err := tx.PrepareContext(ctx, q)
	if err != nil {
		return fmt.Errorf("prepare metric_events: %w", err)
	}
	defer stmt.Close()
	for _, r := range rows {
		if _, err := stmt.ExecContext(ctx,
			r.TimeNS, int64(r.ResourceID), int64(r.ScopeID),
			r.ServiceName, r.AttributesJSON,
		); err != nil {
			return fmt.Errorf("insert metric_event: %w", err)
		}
	}
	return nil
}

func writeAttrKeys(ctx context.Context, tx *sql.Tx, keys []store.AttrKeyDelta) error {
	if len(keys) == 0 {
		return nil
	}
	const q = `INSERT INTO attribute_keys
		(signal_type, service_name, key, value_type, first_seen_ns, last_seen_ns, count)
		VALUES (?,?,?,?,?,?,?)
		ON CONFLICT(signal_type, service_name, key, value_type) DO UPDATE SET
		  last_seen_ns  = max(last_seen_ns, excluded.last_seen_ns),
		  first_seen_ns = min(first_seen_ns, excluded.first_seen_ns),
		  count         = count + excluded.count`
	stmt, err := tx.PrepareContext(ctx, q)
	if err != nil {
		return fmt.Errorf("prepare attribute_keys: %w", err)
	}
	defer stmt.Close()
	for _, k := range keys {
		if _, err := stmt.ExecContext(ctx,
			k.SignalType, k.ServiceName, k.Key, k.ValueType,
			k.LastSeenNS, k.LastSeenNS, k.Count,
		); err != nil {
			return fmt.Errorf("upsert attribute_key: %w", err)
		}
	}
	return nil
}

func writeAttrValues(ctx context.Context, tx *sql.Tx, vs []store.AttrValueDelta) error {
	if len(vs) == 0 {
		return nil
	}
	const q = `INSERT INTO attribute_values
		(signal_type, service_name, key, value, count, last_seen_ns)
		VALUES (?,?,?,?,?,?)
		ON CONFLICT(signal_type, service_name, key, value) DO UPDATE SET
		  count        = count + excluded.count,
		  last_seen_ns = max(last_seen_ns, excluded.last_seen_ns)`
	stmt, err := tx.PrepareContext(ctx, q)
	if err != nil {
		return fmt.Errorf("prepare attribute_values: %w", err)
	}
	defer stmt.Close()
	for _, v := range vs {
		if _, err := stmt.ExecContext(ctx,
			v.SignalType, v.ServiceName, v.Key, v.Value, v.Count, v.LastSeenNS,
		); err != nil {
			return fmt.Errorf("upsert attribute_value: %w", err)
		}
	}
	return nil
}

// =========================================================================
// Read paths
// =========================================================================

func (s *Store) ListServices(ctx context.Context) ([]store.ServiceSummary, error) {
	const q = `SELECT service_name,
		COUNT(*) AS total,
		SUM(CASE WHEN status_code = 2 THEN 1 ELSE 0 END) AS errs
		FROM events WHERE signal_type = 'span'
		GROUP BY service_name ORDER BY total DESC`
	rows, err := s.reader.QueryContext(ctx, q)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []store.ServiceSummary
	for rows.Next() {
		var r store.ServiceSummary
		if err := rows.Scan(&r.ServiceName, &r.SpanCount, &r.ErrorCount); err != nil {
			return nil, err
		}
		if r.SpanCount > 0 {
			r.ErrorRate = float64(r.ErrorCount) / float64(r.SpanCount)
		}
		out = append(out, r)
	}
	return out, rows.Err()
}

func (s *Store) ListTraces(ctx context.Context, f store.TraceFilter) ([]store.TraceSummary, string, error) {
	limit := clampLimit(f.Limit, 50, 500)

	var b strings.Builder
	b.WriteString(`WITH roots AS (
		SELECT trace_id, span_id, service_name, name, time_ns, end_time_ns
		FROM events WHERE signal_type = 'span' AND parent_span_id IS NULL`)
	args := []any{}
	if f.Service != "" {
		b.WriteString(" AND service_name = ?")
		args = append(args, f.Service)
	}
	if f.FromNS > 0 {
		b.WriteString(" AND time_ns >= ?")
		args = append(args, f.FromNS)
	}
	if f.ToNS > 0 {
		b.WriteString(" AND time_ns < ?")
		args = append(args, f.ToNS)
	}
	if cursorNS, tidHex, ok := parseCompositeCursor(f.Cursor); ok {
		if tid, err := hex.DecodeString(tidHex); err == nil {
			b.WriteString(" AND (time_ns < ? OR (time_ns = ? AND trace_id < ?))")
			args = append(args, cursorNS, cursorNS, tid)
		}
	}
	b.WriteString(` ORDER BY time_ns DESC, trace_id DESC LIMIT ?)
		SELECT r.trace_id, r.service_name, r.name, r.time_ns,
		  COALESCE(r.end_time_ns - r.time_ns, 0) AS duration_ns,
		  (SELECT COUNT(*) FROM events e WHERE e.signal_type = 'span' AND e.trace_id = r.trace_id) AS span_count,
		  COALESCE((SELECT 1 FROM events e WHERE e.signal_type = 'span' AND e.trace_id = r.trace_id AND e.status_code = 2 LIMIT 1), 0) AS has_error
		FROM roots r ORDER BY r.time_ns DESC, r.trace_id DESC`)
	args = append(args, limit)

	if f.HasError != nil {
		outer := "SELECT * FROM (" + b.String() + ") WHERE has_error = ?"
		b.Reset()
		b.WriteString(outer)
		if *f.HasError {
			args = append(args, 1)
		} else {
			args = append(args, 0)
		}
	}

	rows, err := s.reader.QueryContext(ctx, b.String(), args...)
	if err != nil {
		return nil, "", err
	}
	defer rows.Close()

	var out []store.TraceSummary
	for rows.Next() {
		var (
			tid      []byte
			ts       store.TraceSummary
			hasError int
		)
		if err := rows.Scan(&tid, &ts.RootService, &ts.RootName, &ts.StartTimeNS, &ts.DurationNS, &ts.SpanCount, &hasError); err != nil {
			return nil, "", err
		}
		ts.TraceID = hex.EncodeToString(tid)
		ts.HasError = hasError == 1
		out = append(out, ts)
	}
	if err := rows.Err(); err != nil {
		return nil, "", err
	}

	cursor := ""
	if len(out) == limit {
		last := out[len(out)-1]
		cursor = encodeCompositeCursor(last.StartTimeNS, last.TraceID)
	}
	return out, cursor, nil
}

// parseCompositeCursor decodes a "primary:secondary" cursor. Primary is
// always an int64 timestamp; secondary is a free-form string tag (hex
// trace_id for traces, event_id for logs). Malformed cursors are ignored
// (ok=false) so the caller treats it as no cursor and returns the first
// page — same as an empty string.
func parseCompositeCursor(s string) (primary int64, secondary string, ok bool) {
	if s == "" {
		return 0, "", false
	}
	primStr, sec, found := strings.Cut(s, ":")
	if !found || sec == "" {
		return 0, "", false
	}
	n, err := strconv.ParseInt(primStr, 10, 64)
	if err != nil {
		return 0, "", false
	}
	return n, sec, true
}

func encodeCompositeCursor(primary int64, secondary string) string {
	return strconv.FormatInt(primary, 10) + ":" + secondary
}

func (s *Store) GetTrace(ctx context.Context, traceIDHex string) (store.TraceDetail, error) {
	tid, err := hex.DecodeString(traceIDHex)
	if err != nil {
		return store.TraceDetail{}, fmt.Errorf("invalid trace id: %w", err)
	}
	const spanQ = `SELECT span_id, parent_span_id, resource_id, service_name, name,
		  COALESCE(span_kind, ''), time_ns, COALESCE(end_time_ns, time_ns),
		  COALESCE(duration_ns, 0), COALESCE(status_code, 0), status_message, attributes
		FROM events WHERE signal_type = 'span' AND trace_id = ?
		ORDER BY time_ns, span_id`
	rows, err := s.reader.QueryContext(ctx, spanQ, tid)
	if err != nil {
		return store.TraceDetail{}, err
	}
	defer rows.Close()

	var spans []store.SpanOut
	resourceIDs := map[uint64]struct{}{}
	for rows.Next() {
		var (
			spanID, parentID []byte
			resID            int64
			sp               store.SpanOut
			statusMsg        sql.NullString
		)
		if err := rows.Scan(&spanID, &parentID, &resID, &sp.ServiceName, &sp.Name,
			&sp.Kind, &sp.StartTimeNS, &sp.EndTimeNS, &sp.DurationNS,
			&sp.StatusCode, &statusMsg, &sp.AttributesJSON); err != nil {
			return store.TraceDetail{}, err
		}
		sp.TraceID = traceIDHex
		sp.SpanID = hex.EncodeToString(spanID)
		if parentID != nil {
			sp.ParentSpanID = hex.EncodeToString(parentID)
		}
		sp.ResourceID = uint64(resID)
		if statusMsg.Valid {
			sp.StatusMessage = statusMsg.String
		}
		spans = append(spans, sp)
		resourceIDs[uint64(resID)] = struct{}{}
	}
	if err := rows.Err(); err != nil {
		return store.TraceDetail{}, err
	}

	if err := s.attachEvents(ctx, tid, spans); err != nil {
		return store.TraceDetail{}, err
	}
	if err := s.attachLinks(ctx, tid, spans); err != nil {
		return store.TraceDetail{}, err
	}

	resources, err := s.loadResources(ctx, resourceIDs)
	if err != nil {
		return store.TraceDetail{}, err
	}

	return store.TraceDetail{TraceID: traceIDHex, Spans: spans, Resources: resources}, nil
}

func (s *Store) attachEvents(ctx context.Context, tid []byte, spans []store.SpanOut) error {
	const q = `SELECT span_id, time_ns, name, attributes FROM span_events
		WHERE trace_id = ? ORDER BY span_id, seq`
	rows, err := s.reader.QueryContext(ctx, q, tid)
	if err != nil {
		return err
	}
	defer rows.Close()
	byID := map[string]int{}
	for i, sp := range spans {
		byID[sp.SpanID] = i
	}
	for rows.Next() {
		var spanID []byte
		var ev store.SpanEventOut
		if err := rows.Scan(&spanID, &ev.TimeNS, &ev.Name, &ev.AttributesJSON); err != nil {
			return err
		}
		if idx, ok := byID[hex.EncodeToString(spanID)]; ok {
			spans[idx].Events = append(spans[idx].Events, ev)
		}
	}
	return rows.Err()
}

func (s *Store) attachLinks(ctx context.Context, tid []byte, spans []store.SpanOut) error {
	const q = `SELECT span_id, linked_trace_id, linked_span_id, attributes
		FROM span_links WHERE trace_id = ? ORDER BY span_id, seq`
	rows, err := s.reader.QueryContext(ctx, q, tid)
	if err != nil {
		return err
	}
	defer rows.Close()
	byID := map[string]int{}
	for i, sp := range spans {
		byID[sp.SpanID] = i
	}
	for rows.Next() {
		var spanID, linkedTID, linkedSID []byte
		var attrs string
		if err := rows.Scan(&spanID, &linkedTID, &linkedSID, &attrs); err != nil {
			return err
		}
		if idx, ok := byID[hex.EncodeToString(spanID)]; ok {
			spans[idx].Links = append(spans[idx].Links, store.SpanLinkOut{
				LinkedTraceID:  hex.EncodeToString(linkedTID),
				LinkedSpanID:   hex.EncodeToString(linkedSID),
				AttributesJSON: attrs,
			})
		}
	}
	return rows.Err()
}

func (s *Store) loadResources(ctx context.Context, ids map[uint64]struct{}) (map[uint64]store.Resource, error) {
	if len(ids) == 0 {
		return map[uint64]store.Resource{}, nil
	}
	placeholders := make([]string, 0, len(ids))
	args := make([]any, 0, len(ids))
	for id := range ids {
		placeholders = append(placeholders, "?")
		args = append(args, int64(id))
	}
	q := `SELECT resource_id, service_name, service_namespace, service_version,
		service_instance_id, sdk_name, sdk_language, sdk_version,
		attributes, first_seen_ns, last_seen_ns
		FROM resources WHERE resource_id IN (` + strings.Join(placeholders, ",") + `)`
	rows, err := s.reader.QueryContext(ctx, q, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := make(map[uint64]store.Resource, len(ids))
	for rows.Next() {
		var r store.Resource
		var id int64
		var ns, ver, inst, sdk, lang, sdkv sql.NullString
		if err := rows.Scan(&id, &r.ServiceName, &ns, &ver, &inst, &sdk, &lang, &sdkv,
			&r.AttributesJSON, &r.FirstSeenNS, &r.LastSeenNS); err != nil {
			return nil, err
		}
		r.ID = uint64(id)
		r.ServiceNamespace = ns.String
		r.ServiceVersion = ver.String
		r.ServiceInstanceID = inst.String
		r.SDKName = sdk.String
		r.SDKLanguage = lang.String
		r.SDKVersion = sdkv.String
		out[r.ID] = r
	}
	return out, rows.Err()
}

func (s *Store) ListFields(ctx context.Context, f store.FieldFilter) ([]store.FieldInfo, error) {
	switch f.SignalType {
	case store.SignalSpan, store.SignalLog, store.SignalMetric:
	default:
		return nil, errors.New("signal_type must be 'span', 'log', or 'metric'")
	}
	limit := clampLimit(f.Limit, 100, 500)

	out := matchingSyntheticFields(f.SignalType, f.Prefix)
	byKey := make(map[string]int, len(out))
	for i, fi := range out {
		byKey[fi.Key] = i
	}

	// Service-scoped when f.Service is non-empty: include service-specific
	// rows plus shared ones (service_name = ''). When empty, the sentinel
	// clause `? = ''` is true and the whole filter passes, so we scan
	// across all services with one query text.
	const q = `SELECT key, value_type, SUM(count) AS c FROM attribute_keys
		WHERE signal_type = ?
		  AND (? = '' OR service_name = ? OR service_name = '')
		  AND key LIKE ? || '%'
		GROUP BY key, value_type
		ORDER BY c DESC, key ASC LIMIT ?`
	rows, err := s.reader.QueryContext(ctx, q, f.SignalType, f.Service, f.Service, f.Prefix, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	for rows.Next() {
		var fi store.FieldInfo
		if err := rows.Scan(&fi.Key, &fi.ValueType, &fi.Count); err != nil {
			return nil, err
		}
		if idx, ok := byKey[fi.Key]; ok {
			out[idx].Count += fi.Count
			continue
		}
		byKey[fi.Key] = len(out)
		out = append(out, fi)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	if len(out) > limit {
		out = out[:limit]
	}
	return out, nil
}

// syntheticFields per signal — promoted columns and meta.* attributes that are
// always present and that the query builder resolves natively. Keep in sync
// with realColumn() in internal/query/builder.go.
var syntheticSpanFields = []store.FieldInfo{
	{Key: "name", ValueType: "str"},
	{Key: "service.name", ValueType: "str"},
	{Key: "meta.span_kind", ValueType: "str"},
	{Key: "status_code", ValueType: "int"},
	{Key: "duration_ns", ValueType: "int"},
	{Key: "duration_ms", ValueType: "int"},
	{Key: "time_ns", ValueType: "time"},
	{Key: "parent_span_id", ValueType: "str"},
	{Key: "trace_id", ValueType: "str"},
	{Key: "is_root", ValueType: "bool"},
	{Key: "error", ValueType: "bool"},
}

var syntheticLogFields = []store.FieldInfo{
	{Key: "service.name", ValueType: "str"},
	{Key: "severity_number", ValueType: "int"},
	{Key: "severity_text", ValueType: "str"},
	{Key: "body", ValueType: "str"},
	{Key: "time_ns", ValueType: "time"},
	{Key: "trace_id", ValueType: "str"},
	{Key: "error", ValueType: "bool"},
}

// Metric events have no per-column metric identity — metric names live
// as attribute keys inside the attributes JSON (Honeycomb-style). The
// synthetic list covers the structural columns; metric-name fields
// surface through attribute_keys like any other attribute.
var syntheticMetricFields = []store.FieldInfo{
	{Key: "service.name", ValueType: "str"},
	{Key: "meta.dataset", ValueType: "str"},
	{Key: "time_ns", ValueType: "time"},
}

func matchingSyntheticFields(signalType, prefix string) []store.FieldInfo {
	var src []store.FieldInfo
	switch signalType {
	case store.SignalSpan:
		src = syntheticSpanFields
	case store.SignalLog:
		src = syntheticLogFields
	case store.SignalMetric:
		src = syntheticMetricFields
	default:
		return nil
	}
	out := make([]store.FieldInfo, 0, len(src))
	for _, fi := range src {
		if prefix == "" || strings.HasPrefix(fi.Key, prefix) {
			out = append(out, fi)
		}
	}
	return out
}

func (s *Store) ListFieldValues(ctx context.Context, f store.ValueFilter) ([]string, error) {
	limit := clampLimit(f.Limit, 50, 200)

	// Column-backed autocomplete: some fields live as real or virtual columns
	// on `events` (name, service_name, http_route, span_kind, …). Scan the
	// column directly — more accurate than attribute_values and available for
	// fields the sampler can't observe (e.g. generated columns).
	if col, ok := realColumnForValues(f.SignalType, f.Key); ok {
		return s.listEventColumnValues(ctx, col, f, limit)
	}

	// Same conditional-filter trick as ListFields: `? = ''` short-circuits
	// the service clause when no service is supplied, so one query text
	// handles both scopes. GROUP BY unifies the no-service case (values
	// can appear across multiple services and need summing); when a
	// service is pinned the primary key guarantees a single row per
	// (key, value) so SUM(count) == count.
	const q = `SELECT value FROM attribute_values
		WHERE signal_type = ?
		  AND (? = '' OR service_name = ?)
		  AND key = ? AND value LIKE ? || '%'
		GROUP BY value
		ORDER BY SUM(count) DESC, value ASC LIMIT ?`
	rows, err := s.reader.QueryContext(ctx, q, f.SignalType, f.Service, f.Service, f.Key, f.Prefix, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		var v string
		if err := rows.Scan(&v); err != nil {
			return nil, err
		}
		out = append(out, v)
	}
	return out, rows.Err()
}

// realColumnForValues maps a user-facing field name → events-table column when
// the field is backed by a real or virtual column. Metric queries hit
// metric_events which has only service_name + dataset as promoted columns;
// metric-name values flow through attribute_values. Keep in sync with
// realColumn() in internal/query/builder.go.
func realColumnForValues(signalType, key string) (col string, ok bool) {
	// Metric rows live in metric_events and don't have span/log columns.
	// Fall through to attribute_values for anything other than the two
	// columns metric_events actually has.
	if signalType == store.SignalMetric {
		switch key {
		case "service.name":
			return "service_name", true
		case "meta.dataset", "dataset":
			return "dataset", true
		}
		return "", false
	}

	// Shared columns on the events table.
	switch key {
	case "name":
		return "name", true
	case "service.name":
		return "service_name", true
	case "trace_id":
		return "trace_id", true
	case "http.request.method":
		return "http_method", true
	case "http.response.status_code":
		return "http_status_code", true
	case "http.route":
		return "http_route", true
	case "rpc.service":
		return "rpc_service", true
	case "db.system":
		return "db_system", true
	case "meta.span_kind":
		return "span_kind", true
	case "meta.annotation_type":
		return "annotation_type", true
	case "meta.dataset", "dataset":
		return "dataset", true
	}
	switch signalType {
	case store.SignalLog:
		if key == "severity_text" {
			return "severity_text", true
		}
	}
	return "", false
}

func (s *Store) listEventColumnValues(
	ctx context.Context,
	col string,
	f store.ValueFilter,
	limit int,
) ([]string, error) {
	// Metric rows live in metric_events; everything else in events with a
	// signal_type predicate.
	var q string
	args := []any{}
	if f.SignalType == store.SignalMetric {
		q = fmt.Sprintf(`SELECT CAST(%s AS TEXT) AS v, COUNT(*) AS n
			FROM metric_events
			WHERE %s IS NOT NULL
			  AND CAST(%s AS TEXT) LIKE ? || '%%'`, col, col, col)
		args = append(args, f.Prefix)
	} else {
		q = fmt.Sprintf(`SELECT CAST(%s AS TEXT) AS v, COUNT(*) AS n
			FROM events
			WHERE signal_type = ?
			  AND %s IS NOT NULL
			  AND CAST(%s AS TEXT) LIKE ? || '%%'`, col, col, col)
		args = append(args, f.SignalType, f.Prefix)
	}
	if f.Service != "" {
		q += ` AND service_name = ?`
		args = append(args, f.Service)
	}
	q += ` GROUP BY v ORDER BY n DESC, v ASC LIMIT ?`
	args = append(args, limit)

	rows, err := s.reader.QueryContext(ctx, q, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		var v string
		var n int64
		if err := rows.Scan(&v, &n); err != nil {
			return nil, err
		}
		out = append(out, v)
	}
	return out, rows.Err()
}

func (s *Store) ListSpanNames(ctx context.Context, service, prefix string, limit int) ([]string, error) {
	limit = clampLimit(limit, 50, 200)
	var q string
	var args []any
	if service != "" {
		q = `SELECT DISTINCT name FROM events WHERE signal_type = 'span' AND service_name = ? AND name LIKE ? || '%' ORDER BY name LIMIT ?`
		args = []any{service, prefix, limit}
	} else {
		q = `SELECT DISTINCT name FROM events WHERE signal_type = 'span' AND name LIKE ? || '%' ORDER BY name LIMIT ?`
		args = []any{prefix, limit}
	}
	rows, err := s.reader.QueryContext(ctx, q, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		var v string
		if err := rows.Scan(&v); err != nil {
			return nil, err
		}
		out = append(out, v)
	}
	return out, rows.Err()
}

func (s *Store) SearchLogs(ctx context.Context, f store.LogFilter) ([]store.LogOut, string, error) {
	limit := clampLimit(f.Limit, 200, 1000)

	var b strings.Builder
	args := []any{}
	if f.Query != "" {
		// FTS join — the FTS index covers both logs (via body) and spans (via
		// name) but we only return log events from this endpoint.
		b.WriteString(`SELECT e.event_id, e.time_ns, e.service_name, e.severity_text, e.severity_number,
			e.body, e.trace_id, e.span_id, e.attributes
			FROM events_fts JOIN events e ON e.event_id = events_fts.rowid
			WHERE events_fts MATCH ? AND e.signal_type = 'log'`)
		args = append(args, f.Query)
	} else {
		b.WriteString(`SELECT event_id, time_ns, service_name, severity_text, severity_number,
			body, trace_id, span_id, attributes FROM events WHERE signal_type = 'log'`)
	}
	qualified := f.Query != ""
	col := func(name string) string {
		if qualified {
			return "e." + name
		}
		return name
	}
	if f.Service != "" {
		fmt.Fprintf(&b, " AND %s = ?", col("service_name"))
		args = append(args, f.Service)
	}
	if f.FromNS > 0 {
		fmt.Fprintf(&b, " AND %s >= ?", col("time_ns"))
		args = append(args, f.FromNS)
	}
	if f.ToNS > 0 {
		fmt.Fprintf(&b, " AND %s < ?", col("time_ns"))
		args = append(args, f.ToNS)
	}
	if cursorNS, logIDStr, ok := parseCompositeCursor(f.Cursor); ok {
		if logID, err := strconv.ParseInt(logIDStr, 10, 64); err == nil {
			fmt.Fprintf(&b, " AND (%s < ? OR (%s = ? AND %s < ?))",
				col("time_ns"), col("time_ns"), col("event_id"))
			args = append(args, cursorNS, cursorNS, logID)
		}
	}
	fmt.Fprintf(&b, " ORDER BY %s DESC, %s DESC LIMIT ?", col("time_ns"), col("event_id"))
	args = append(args, limit)

	rows, err := s.reader.QueryContext(ctx, b.String(), args...)
	if err != nil {
		return nil, "", err
	}
	defer rows.Close()

	var out []store.LogOut
	for rows.Next() {
		var l store.LogOut
		var sev sql.NullString
		var body sql.NullString
		var sevNum sql.NullInt32
		var tid, sid []byte
		if err := rows.Scan(&l.LogID, &l.TimeNS, &l.ServiceName, &sev, &sevNum,
			&body, &tid, &sid, &l.AttributesJSON); err != nil {
			return nil, "", err
		}
		l.SeverityText = sev.String
		l.SeverityNumber = sevNum.Int32
		l.Body = body.String
		if tid != nil {
			l.TraceID = hex.EncodeToString(tid)
		}
		if sid != nil {
			l.SpanID = hex.EncodeToString(sid)
		}
		out = append(out, l)
	}
	if err := rows.Err(); err != nil {
		return nil, "", err
	}

	cursor := ""
	if len(out) == limit {
		last := out[len(out)-1]
		cursor = encodeCompositeCursor(last.TimeNS, strconv.FormatInt(last.LogID, 10))
	}
	return out, cursor, nil
}

func (s *Store) RunQuery(
	ctx context.Context,
	sqlStr string,
	args []any,
	columns []store.QueryColumn,
	hasBucket bool,
	groupKeys []string,
	rates []store.QueryRateSpec,
) (store.QueryResult, error) {
	rows, err := s.reader.QueryContext(ctx, sqlStr, args...)
	if err != nil {
		return store.QueryResult{}, fmt.Errorf("query: %w", err)
	}
	defer rows.Close()

	result := store.QueryResult{
		Columns:   columns,
		HasBucket: hasBucket,
		GroupKeys: groupKeys,
	}

	for rows.Next() {
		dest := make([]any, len(columns))
		ptrs := make([]any, len(columns))
		for i := range dest {
			ptrs[i] = &dest[i]
		}
		if err := rows.Scan(ptrs...); err != nil {
			return store.QueryResult{}, fmt.Errorf("scan: %w", err)
		}
		row := make([]any, len(columns))
		for i, v := range dest {
			row[i] = coerceForJSON(v)
		}
		result.Rows = append(result.Rows, row)
	}
	if err := rows.Err(); err != nil {
		return store.QueryResult{}, err
	}

	if len(rates) > 0 && hasBucket {
		applyRateTransforms(&result, rates)
	}
	return result, nil
}

// applyRateTransforms walks the result rows in-place, turning the chosen
// columns into per-second deltas between consecutive buckets in the same
// group tuple. The first bucket of each group gets a NULL (no prior point),
// and a negative delta is clamped to 0 — counter resets shouldn't produce
// misleading spikes downward.
func applyRateTransforms(r *store.QueryResult, rates []store.QueryRateSpec) {
	rateCols := map[int]float64{}
	for _, s := range rates {
		rateCols[s.ColumnIndex] = s.BucketSecs
	}
	const bucketIdx = 0

	groupIdxs := make([]int, 0, len(r.Columns))
	for i := range r.Columns {
		if i == bucketIdx {
			continue
		}
		if _, isRate := rateCols[i]; isRate {
			continue
		}
		if looksLikeAggAlias(r.Columns[i].Name) {
			continue
		}
		groupIdxs = append(groupIdxs, i)
	}

	type groupState struct {
		prev map[int]float64
		seen bool
	}
	groups := map[string]*groupState{}

	sortByGroupThenBucket(r.Rows, groupIdxs, bucketIdx)

	for _, row := range r.Rows {
		key := groupKey(row, groupIdxs)
		g, ok := groups[key]
		if !ok {
			g = &groupState{prev: map[int]float64{}}
			groups[key] = g
		}
		for col, bucketSecs := range rateCols {
			curr, ok := asFloat(row[col])
			if !ok {
				row[col] = nil
				continue
			}
			if !g.seen {
				row[col] = nil
				g.prev[col] = curr
				continue
			}
			prev := g.prev[col]
			delta := curr - prev
			if delta < 0 {
				delta = 0
			}
			if bucketSecs <= 0 {
				row[col] = nil
			} else {
				row[col] = delta / bucketSecs
			}
			g.prev[col] = curr
		}
		g.seen = true
	}
}

func sortByGroupThenBucket(rows [][]any, groupIdxs []int, bucketIdx int) {
	sortRows(rows, func(a, b []any) bool {
		for _, idx := range groupIdxs {
			av, bv := a[idx], b[idx]
			if c := compareAny(av, bv); c != 0 {
				return c < 0
			}
		}
		ai, _ := asFloat(a[bucketIdx])
		bi, _ := asFloat(b[bucketIdx])
		return ai < bi
	})
}

func sortRows(rows [][]any, less func(a, b []any) bool) {
	for i := 1; i < len(rows); i++ {
		for j := i; j > 0 && less(rows[j], rows[j-1]); j-- {
			rows[j], rows[j-1] = rows[j-1], rows[j]
		}
	}
}

func compareAny(a, b any) int {
	if a == nil && b == nil {
		return 0
	}
	if a == nil {
		return -1
	}
	if b == nil {
		return 1
	}
	if af, aok := asFloat(a); aok {
		if bf, bok := asFloat(b); bok {
			switch {
			case af < bf:
				return -1
			case af > bf:
				return 1
			default:
				return 0
			}
		}
	}
	as, _ := a.(string)
	bs, _ := b.(string)
	switch {
	case as < bs:
		return -1
	case as > bs:
		return 1
	default:
		return 0
	}
}

func groupKey(row []any, idxs []int) string {
	var out []byte
	for _, i := range idxs {
		if v, ok := row[i].(string); ok {
			out = append(out, v...)
		} else if v, ok := asFloat(row[i]); ok {
			out = strconvAppendFloat(out, v)
		} else if row[i] == nil {
			out = append(out, '\x00')
		} else {
			out = append(out, []byte(fmt.Sprint(row[i]))...)
		}
		out = append(out, '\x01')
	}
	return string(out)
}

func strconvAppendFloat(b []byte, v float64) []byte {
	return append(b, []byte(fmt.Sprintf("%v", v))...)
}

func asFloat(v any) (float64, bool) {
	switch t := v.(type) {
	case float64:
		return t, true
	case float32:
		return float64(t), true
	case int64:
		return float64(t), true
	case int:
		return float64(t), true
	case nil:
		return 0, false
	}
	return 0, false
}

func looksLikeAggAlias(name string) bool {
	if name == "count" {
		return true
	}
	prefixes := []string{
		"count_", "sum_", "avg_", "min_", "max_",
		"p001_", "p01_", "p05_", "p10_", "p25_", "p50_",
		"p75_", "p90_", "p95_", "p99_", "p999_",
		"rate_sum_", "rate_avg_", "rate_max_",
	}
	for _, p := range prefixes {
		if len(name) > len(p) && name[:len(p)] == p {
			return true
		}
	}
	return false
}

func coerceForJSON(v any) any {
	switch t := v.(type) {
	case []byte:
		return string(t)
	default:
		return v
	}
}

func (s *Store) Retain(ctx context.Context, olderThanNS int64) error {
	tx, err := s.writer.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if _, err := tx.ExecContext(ctx, `DELETE FROM events WHERE time_ns < ?`, olderThanNS); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, `DELETE FROM metric_events WHERE time_ns < ?`, olderThanNS); err != nil {
		return err
	}
	// Span events and links get cleaned up by trace_id no-longer-referenced.
	if _, err := tx.ExecContext(ctx,
		`DELETE FROM span_events WHERE (trace_id, span_id) NOT IN
			(SELECT trace_id, span_id FROM events WHERE signal_type='span')`); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx,
		`DELETE FROM span_links WHERE (trace_id, span_id) NOT IN
			(SELECT trace_id, span_id FROM events WHERE signal_type='span')`); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, `DELETE FROM resources
		WHERE last_seen_ns < ?
		  AND resource_id NOT IN (SELECT DISTINCT resource_id FROM events)
		  AND resource_id NOT IN (SELECT DISTINCT resource_id FROM metric_events)`,
		olderThanNS); err != nil {
		return err
	}
	return tx.Commit()
}

func (s *Store) Clear(ctx context.Context) error {
	tx, err := s.writer.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	stmts := []string{
		`DELETE FROM span_events`,
		`DELETE FROM span_links`,
		`DELETE FROM events`,
		`DELETE FROM metric_events`,
		`DELETE FROM scopes`,
		`DELETE FROM resources`,
		`DELETE FROM attribute_keys`,
		`DELETE FROM attribute_values`,
	}
	for _, q := range stmts {
		if _, err := tx.ExecContext(ctx, q); err != nil {
			return err
		}
	}
	if err := tx.Commit(); err != nil {
		return err
	}
	_, err = s.writer.ExecContext(ctx, "VACUUM")
	return err
}

// =========================================================================
// helpers
// =========================================================================

func clampLimit(n, def, max int) int {
	if n <= 0 {
		return def
	}
	if n > max {
		return max
	}
	return n
}

func nullStr(s string) any {
	if s == "" {
		return nil
	}
	return s
}

func nullBytes(b []byte) any {
	if len(b) == 0 {
		return nil
	}
	return b
}

func nullInt64Ptr(p *int64) any {
	if p == nil {
		return nil
	}
	return *p
}

func nullInt32Ptr(p *int32) any {
	if p == nil {
		return nil
	}
	return *p
}

func nullUint32Ptr(p *uint32) any {
	if p == nil {
		return nil
	}
	return *p
}
