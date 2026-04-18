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
// Order: resources → scopes → spans → span_events → span_links → logs →
// attribute_keys → attribute_values. Constraints fire in that order.
func (s *Store) WriteBatch(ctx context.Context, b store.Batch) error {
	if len(b.Spans) == 0 && len(b.Logs) == 0 && len(b.Resources) == 0 && len(b.Scopes) == 0 {
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
	if err := writeSpans(ctx, tx, b.Spans); err != nil {
		return err
	}
	if err := writeLogs(ctx, tx, b.Logs); err != nil {
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

func writeSpans(ctx context.Context, tx *sql.Tx, spans []store.Span) error {
	if len(spans) == 0 {
		return nil
	}
	const q = `INSERT INTO spans
		(trace_id, span_id, parent_span_id, resource_id, scope_id, service_name,
		 name, kind, start_time_ns, end_time_ns, status_code, status_message,
		 trace_state, flags, dropped_attrs_count, dropped_events_count,
		 dropped_links_count, attributes)
		VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
		ON CONFLICT(trace_id, span_id) DO NOTHING`
	stmt, err := tx.PrepareContext(ctx, q)
	if err != nil {
		return fmt.Errorf("prepare spans: %w", err)
	}
	defer stmt.Close()

	const eventQ = `INSERT INTO span_events
		(trace_id, span_id, seq, time_ns, name, attributes, dropped_attrs_count)
		VALUES (?,?,?,?,?,?,?) ON CONFLICT DO NOTHING`
	eventStmt, err := tx.PrepareContext(ctx, eventQ)
	if err != nil {
		return fmt.Errorf("prepare span_events: %w", err)
	}
	defer eventStmt.Close()

	const linkQ = `INSERT INTO span_links
		(trace_id, span_id, seq, linked_trace_id, linked_span_id, trace_state,
		 flags, attributes, dropped_attrs_count)
		VALUES (?,?,?,?,?,?,?,?,?) ON CONFLICT DO NOTHING`
	linkStmt, err := tx.PrepareContext(ctx, linkQ)
	if err != nil {
		return fmt.Errorf("prepare span_links: %w", err)
	}
	defer linkStmt.Close()

	for _, sp := range spans {
		if _, err := stmt.ExecContext(ctx,
			sp.TraceID, sp.SpanID, nullBytes(sp.ParentSpanID), int64(sp.ResourceID),
			int64(sp.ScopeID), sp.ServiceName, sp.Name, sp.Kind,
			sp.StartTimeNS, sp.EndTimeNS, sp.StatusCode, nullStr(sp.StatusMessage),
			nullStr(sp.TraceState), sp.Flags, sp.DroppedAttrsCount,
			sp.DroppedEventsCount, sp.DroppedLinksCount, sp.AttributesJSON,
		); err != nil {
			return fmt.Errorf("insert span: %w", err)
		}
		for _, ev := range sp.Events {
			if _, err := eventStmt.ExecContext(ctx,
				ev.TraceID, ev.SpanID, ev.Seq, ev.TimeNS, ev.Name,
				ev.AttributesJSON, ev.DroppedAttrsCount,
			); err != nil {
				return fmt.Errorf("insert span_event: %w", err)
			}
		}
		for _, ln := range sp.Links {
			if _, err := linkStmt.ExecContext(ctx,
				ln.TraceID, ln.SpanID, ln.Seq, ln.LinkedTraceID, ln.LinkedSpanID,
				nullStr(ln.TraceState), ln.Flags, ln.AttributesJSON, ln.DroppedAttrsCount,
			); err != nil {
				return fmt.Errorf("insert span_link: %w", err)
			}
		}
	}
	return nil
}

func writeLogs(ctx context.Context, tx *sql.Tx, logs []store.LogRecord) error {
	if len(logs) == 0 {
		return nil
	}
	const q = `INSERT INTO logs
		(resource_id, scope_id, service_name, time_ns, observed_time_ns,
		 severity_number, severity_text, body, body_json, trace_id, span_id,
		 flags, dropped_attrs_count, attributes)
		VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)`
	stmt, err := tx.PrepareContext(ctx, q)
	if err != nil {
		return fmt.Errorf("prepare logs: %w", err)
	}
	defer stmt.Close()
	for _, l := range logs {
		if _, err := stmt.ExecContext(ctx,
			int64(l.ResourceID), int64(l.ScopeID), l.ServiceName, l.TimeNS,
			nullInt64(l.ObservedTimeNS), l.SeverityNumber, nullStr(l.SeverityText),
			nullStr(l.Body), nullStr(l.BodyJSON), nullBytes(l.TraceID),
			nullBytes(l.SpanID), l.Flags, l.DroppedAttrsCount, l.AttributesJSON,
		); err != nil {
			return fmt.Errorf("insert log: %w", err)
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
		FROM spans GROUP BY service_name ORDER BY total DESC`
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
		SELECT trace_id, span_id, service_name, name, start_time_ns, end_time_ns
		FROM spans WHERE parent_span_id IS NULL`)
	args := []any{}
	if f.Service != "" {
		b.WriteString(" AND service_name = ?")
		args = append(args, f.Service)
	}
	if f.FromNS > 0 {
		b.WriteString(" AND start_time_ns >= ?")
		args = append(args, f.FromNS)
	}
	if f.ToNS > 0 {
		b.WriteString(" AND start_time_ns < ?")
		args = append(args, f.ToNS)
	}
	if f.Cursor != "" {
		cursorNS, err := strconv.ParseInt(f.Cursor, 10, 64)
		if err == nil {
			b.WriteString(" AND start_time_ns < ?")
			args = append(args, cursorNS)
		}
	}
	b.WriteString(` ORDER BY start_time_ns DESC LIMIT ?)
		SELECT r.trace_id, r.service_name, r.name, r.start_time_ns,
		  (r.end_time_ns - r.start_time_ns) AS duration_ns,
		  (SELECT COUNT(*) FROM spans s WHERE s.trace_id = r.trace_id) AS span_count,
		  COALESCE((SELECT 1 FROM spans s WHERE s.trace_id = r.trace_id AND s.status_code = 2 LIMIT 1), 0) AS has_error
		FROM roots r ORDER BY r.start_time_ns DESC`)
	args = append(args, limit)

	if f.HasError != nil {
		// Post-filter: the HAVING-style condition after the CTE.
		// Simpler to re-wrap:
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
			tid       []byte
			ts        store.TraceSummary
			hasError  int
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
		cursor = strconv.FormatInt(out[len(out)-1].StartTimeNS, 10)
	}
	return out, cursor, nil
}

func (s *Store) GetTrace(ctx context.Context, traceIDHex string) (store.TraceDetail, error) {
	tid, err := hex.DecodeString(traceIDHex)
	if err != nil {
		return store.TraceDetail{}, fmt.Errorf("invalid trace id: %w", err)
	}
	const spanQ = `SELECT span_id, parent_span_id, resource_id, service_name, name,
		  kind, start_time_ns, end_time_ns, duration_ns, status_code, status_message, attributes
		FROM spans WHERE trace_id = ? ORDER BY start_time_ns, span_id`
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

	// Attach events + links (one query per signal; simpler and spans-per-trace is small).
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
	if f.SignalType != "span" && f.SignalType != "log" {
		return nil, errors.New("signal_type must be 'span' or 'log'")
	}
	limit := clampLimit(f.Limit, 100, 500)
	const q = `SELECT key, value_type, SUM(count) AS c FROM attribute_keys
		WHERE signal_type = ? AND (service_name = ? OR service_name = '')
		  AND key LIKE ? || '%'
		GROUP BY key, value_type
		ORDER BY c DESC, key ASC LIMIT ?`
	rows, err := s.reader.QueryContext(ctx, q, f.SignalType, f.Service, f.Prefix, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []store.FieldInfo
	for rows.Next() {
		var fi store.FieldInfo
		if err := rows.Scan(&fi.Key, &fi.ValueType, &fi.Count); err != nil {
			return nil, err
		}
		out = append(out, fi)
	}
	return out, rows.Err()
}

func (s *Store) ListFieldValues(ctx context.Context, f store.ValueFilter) ([]string, error) {
	limit := clampLimit(f.Limit, 50, 200)

	// Some high-value fields live in dedicated columns (span.name,
	// service_name, http_route, etc.) rather than the attributes JSON, so
	// the attribute_values sampler has no rows for them. For those, fall
	// through to a DISTINCT scan on the source table — the column-backed
	// indexes keep it cheap even at moderate data sizes.
	if col, table, ok := realColumnForValues(f.SignalType, f.Key); ok {
		return s.listColumnValues(ctx, table, col, f, limit)
	}

	// When no service is specified, pool values across every service and
	// sum their occurrence counts — gives cross-service autocomplete when
	// the user hasn't narrowed the dataset yet.
	var (
		q    string
		args []any
	)
	if f.Service == "" {
		q = `SELECT value FROM attribute_values
			WHERE signal_type = ? AND key = ? AND value LIKE ? || '%'
			GROUP BY value
			ORDER BY SUM(count) DESC, value ASC LIMIT ?`
		args = []any{f.SignalType, f.Key, f.Prefix, limit}
	} else {
		q = `SELECT value FROM attribute_values
			WHERE signal_type = ? AND service_name = ? AND key = ? AND value LIKE ? || '%'
			ORDER BY count DESC, value ASC LIMIT ?`
		args = []any{f.SignalType, f.Service, f.Key, f.Prefix, limit}
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

// realColumnForValues maps a user-facing field name to (column, table) when
// the field is backed by a dedicated column. The column expression is the
// raw SQL column/generated-column name — safe because it comes from a
// hard-coded map, not from user input. Keep in sync with realColumn() in
// internal/query/builder.go.
func realColumnForValues(signalType, key string) (col, table string, ok bool) {
	switch signalType {
	case "span":
		switch key {
		case "name":
			return "name", "spans", true
		case "service.name":
			return "service_name", "spans", true
		case "http.request.method":
			return "http_method", "spans", true
		case "http.response.status_code":
			return "http_status_code", "spans", true
		case "http.route":
			return "http_route", "spans", true
		case "rpc.service":
			return "rpc_service", "spans", true
		case "db.system":
			return "db_system", "spans", true
		}
	case "log":
		switch key {
		case "service.name":
			return "service_name", "logs", true
		case "severity_text":
			return "severity_text", "logs", true
		}
	}
	return "", "", false
}

func (s *Store) listColumnValues(
	ctx context.Context,
	table, col string,
	f store.ValueFilter,
	limit int,
) ([]string, error) {
	// Prefix filter against the column. We cast to TEXT so integer columns
	// like http_status_code still compare against the "200" the user types.
	q := fmt.Sprintf(`SELECT CAST(%s AS TEXT) AS v, COUNT(*) AS n
		FROM %s
		WHERE %s IS NOT NULL
		  AND CAST(%s AS TEXT) LIKE ? || '%%'`, col, table, col, col)
	args := []any{f.Prefix}
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
		q = `SELECT DISTINCT name FROM spans WHERE service_name = ? AND name LIKE ? || '%' ORDER BY name LIMIT ?`
		args = []any{service, prefix, limit}
	} else {
		q = `SELECT DISTINCT name FROM spans WHERE name LIKE ? || '%' ORDER BY name LIMIT ?`
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
		b.WriteString(`SELECT l.log_id, l.time_ns, l.service_name, l.severity_text, l.severity_number,
			l.body, l.trace_id, l.span_id, l.attributes
			FROM logs_fts JOIN logs l ON l.log_id = logs_fts.rowid
			WHERE logs_fts MATCH ?`)
		args = append(args, f.Query)
	} else {
		b.WriteString(`SELECT log_id, time_ns, service_name, severity_text, severity_number,
			body, trace_id, span_id, attributes FROM logs WHERE 1=1`)
	}
	if f.Service != "" {
		if f.Query != "" {
			b.WriteString(" AND l.service_name = ?")
		} else {
			b.WriteString(" AND service_name = ?")
		}
		args = append(args, f.Service)
	}
	if f.FromNS > 0 {
		if f.Query != "" {
			b.WriteString(" AND l.time_ns >= ?")
		} else {
			b.WriteString(" AND time_ns >= ?")
		}
		args = append(args, f.FromNS)
	}
	if f.ToNS > 0 {
		if f.Query != "" {
			b.WriteString(" AND l.time_ns < ?")
		} else {
			b.WriteString(" AND time_ns < ?")
		}
		args = append(args, f.ToNS)
	}
	if f.Cursor != "" {
		if cursorNS, err := strconv.ParseInt(f.Cursor, 10, 64); err == nil {
			if f.Query != "" {
				b.WriteString(" AND l.time_ns < ?")
			} else {
				b.WriteString(" AND time_ns < ?")
			}
			args = append(args, cursorNS)
		}
	}
	if f.Query != "" {
		b.WriteString(" ORDER BY l.time_ns DESC LIMIT ?")
	} else {
		b.WriteString(" ORDER BY time_ns DESC LIMIT ?")
	}
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
		var tid, sid []byte
		if err := rows.Scan(&l.LogID, &l.TimeNS, &l.ServiceName, &sev, &l.SeverityNumber,
			&body, &tid, &sid, &l.AttributesJSON); err != nil {
			return nil, "", err
		}
		l.SeverityText = sev.String
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
		cursor = strconv.FormatInt(out[len(out)-1].TimeNS, 10)
	}
	return out, cursor, nil
}

func (s *Store) RunQuery(
	ctx context.Context,
	sql string,
	args []any,
	columns []store.QueryColumn,
	hasBucket bool,
	groupKeys []string,
	rates []store.QueryRateSpec,
) (store.QueryResult, error) {
	rows, err := s.reader.QueryContext(ctx, sql, args...)
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
	// bucket column is always index 0 when HasBucket.
	const bucketIdx = 0

	// Group tuple = every column that is neither the bucket nor a rate
	// column, and is not a non-rate aggregation. For rate purposes the
	// GROUP BY coordinates are what matters; the simplest correct definition
	// is "all columns that aren't the bucket and aren't themselves rate
	// outputs". Non-rate aggregations (e.g. count) included in the same
	// query are treated as distinct series and left untouched; they happen
	// to contribute their own column index which we skip below.
	groupIdxs := make([]int, 0, len(r.Columns))
	for i := range r.Columns {
		if i == bucketIdx {
			continue
		}
		if _, isRate := rateCols[i]; isRate {
			continue
		}
		// Only treat non-aggregation (GROUP BY) columns as grouping keys.
		// Distinguish by type — aggregations produced by the builder are
		// typed "int" or "float" and GROUP BY columns carry the resolved
		// field type. We rely on a simpler rule: if the column is numeric
		// AND its name looks like an aggregation alias (count, p95_xxx,
		// sum_xxx, etc.), skip it. Otherwise include it.
		if looksLikeAggAlias(r.Columns[i].Name) {
			continue
		}
		groupIdxs = append(groupIdxs, i)
	}

	// Group rows by their GROUP BY tuple.
	type groupState struct {
		prev map[int]float64 // columnIndex -> last value
		seen bool
	}
	groups := map[string]*groupState{}

	// Stable ordering: sort rows by (group key, bucket ASC) so diffs are
	// computed against the immediate predecessor in the series.
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
				// NULL or non-numeric: leave the rate as NULL.
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
	// Small sort; rows tend to be 50-500 items for a dev tool.
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

// looksLikeAggAlias is a heuristic — aggregation aliases produced by the
// builder follow a known prefix set. Any column not matching is treated as
// a GROUP BY key for rate-grouping purposes.
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

// coerceForJSON turns the loosely-typed values SQLite gives us into shapes
// that encode cleanly over JSON. The main issue is []byte, which encoding/json
// would base64-encode — for our result set, there should be no BLOB columns
// in query output (we only select scalars + timestamps), but the coercion is
// defensive.
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
	if _, err := tx.ExecContext(ctx, `DELETE FROM spans WHERE start_time_ns < ?`, olderThanNS); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, `DELETE FROM logs WHERE time_ns < ?`, olderThanNS); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, `DELETE FROM resources
		WHERE last_seen_ns < ?
		  AND resource_id NOT IN (SELECT DISTINCT resource_id FROM spans)
		  AND resource_id NOT IN (SELECT DISTINCT resource_id FROM logs)`, olderThanNS); err != nil {
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
		`DELETE FROM spans`,
		`DELETE FROM logs`,
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

func nullInt64(v int64) any {
	if v == 0 {
		return nil
	}
	return v
}

func nullBytes(b []byte) any {
	if len(b) == 0 {
		return nil
	}
	return b
}
