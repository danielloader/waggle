package api

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/danielloader/waggle/internal/query"
	"github.com/danielloader/waggle/internal/store"
)

// Router mounts the internal /api/* endpoints used by the React UI.
type Router struct {
	store store.Store
	log   *slog.Logger
}

func NewRouter(s store.Store, log *slog.Logger) *Router {
	return &Router{store: s, log: log}
}

func (rt *Router) Mount(mux *http.ServeMux) {
	mux.HandleFunc("GET /api/services", rt.listServices)
	mux.HandleFunc("GET /api/traces", rt.listTraces)
	mux.HandleFunc("GET /api/traces/{traceID}", rt.getTrace)
	mux.HandleFunc("GET /api/fields", rt.listFields)
	mux.HandleFunc("GET /api/fields/{key}/values", rt.listFieldValues)
	mux.HandleFunc("GET /api/span-names", rt.listSpanNames)
	mux.HandleFunc("GET /api/logs/search", rt.searchLogs)
	mux.HandleFunc("POST /api/query", rt.runQuery)
	mux.HandleFunc("GET /api/history", rt.listHistory)
	mux.HandleFunc("GET /api/history/{hash}", rt.getHistoryByHash)
	mux.HandleFunc("POST /api/clear", rt.clear)
}

func (rt *Router) runQuery(w http.ResponseWriter, r *http.Request) {
	var q query.Query
	if err := json.NewDecoder(r.Body).Decode(&q); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]any{"error": "invalid JSON: " + err.Error()})
		return
	}
	if err := q.Validate(); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]any{"error": err.Error()})
		return
	}
	compiled, err := query.Build(&q)
	if err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]any{"error": err.Error()})
		return
	}

	res, err := rt.store.RunQuery(r.Context(), compiled.SQL, compiled.Args, compiled.Columns, compiled.HasBucket, compiled.GroupKeys, compiled.Rates)
	if err != nil {
		rt.writeError(w, err)
		return
	}
	// Record the run in query_history (best-effort — don't fail the user
	// query on a history bookkeeping error). Skip raw-rows mode; a bare
	// "browse the table" isn't something the user wants back in their
	// recent-queries list. Surface the hash on the response so the UI
	// can thread it through trace links as a source-query shortcode.
	if len(q.Select) > 0 {
		if hash := rt.recordHistory(r.Context(), &q); hash != nil {
			res.HistoryHash = hex.EncodeToString(hash)
		}
	}
	writeJSON(w, http.StatusOK, res)
}

// recordHistory upserts the run into query_history and returns the
// content hash. Returns nil on any bookkeeping error — the caller
// continues regardless.
func (rt *Router) recordHistory(ctx context.Context, q *query.Query) []byte {
	hash, err := query.HashQuery(q)
	if err != nil {
		rt.log.Debug("history hash failed", "err", err)
		return nil
	}
	raw, err := json.Marshal(q)
	if err != nil {
		rt.log.Debug("history marshal failed", "err", err)
		return nil
	}
	display := query.FormatDisplay(q)
	if err := rt.store.RecordQueryRun(ctx, string(q.Dataset), string(raw), display, hash); err != nil {
		rt.log.Debug("history write failed", "err", err)
		return nil
	}
	return hash
}

// getHistoryByHash returns a single query_history entry keyed by the
// hex-encoded SHA-256 content hash. Used by the trace-view "filter by"
// flow to recover the originating query without round-tripping the
// full QuerySearch through the URL.
func (rt *Router) getHistoryByHash(w http.ResponseWriter, r *http.Request) {
	raw := strings.ToLower(r.PathValue("hash"))
	hash, err := hex.DecodeString(raw)
	if err != nil || len(hash) != 32 {
		writeJSON(w, http.StatusBadRequest, map[string]any{
			"error": "invalid hash: expected 64 lowercase hex characters",
			"hash":  raw,
		})
		return
	}
	entry, err := rt.store.GetQueryHistoryByHash(r.Context(), hash)
	if errors.Is(err, store.ErrNotFound) {
		writeJSON(w, http.StatusNotFound, map[string]any{
			"error": "query history entry not found",
			"hash":  raw,
		})
		return
	}
	if err != nil {
		rt.writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, entry)
}

func (rt *Router) listHistory(w http.ResponseWriter, r *http.Request) {
	limit := parseInt(r.URL.Query().Get("limit"), 100)
	entries, err := rt.store.ListQueryHistory(r.Context(), limit)
	if err != nil {
		rt.writeError(w, err)
		return
	}
	if entries == nil {
		entries = []store.QueryHistoryEntry{}
	}
	writeJSON(w, http.StatusOK, map[string]any{"entries": entries})
}

func (rt *Router) listServices(w http.ResponseWriter, r *http.Request) {
	svcs, err := rt.store.ListServices(r.Context(), r.URL.Query().Get("dataset"))
	if err != nil {
		rt.writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"services": svcs})
}

func (rt *Router) listTraces(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	f := store.TraceFilter{
		Service: q.Get("service"),
		FromNS:  parseTimeNS(q.Get("from")),
		ToNS:    parseTimeNS(q.Get("to")),
		Limit:   parseInt(q.Get("limit"), 50),
		Cursor:  q.Get("cursor"),
	}
	if v := q.Get("has_error"); v != "" {
		b := v == "true" || v == "1"
		f.HasError = &b
	}
	traces, cursor, err := rt.store.ListTraces(r.Context(), f)
	if err != nil {
		rt.writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"traces": traces, "next_cursor": cursor})
}

func (rt *Router) getTrace(w http.ResponseWriter, r *http.Request) {
	id := strings.ToLower(r.PathValue("traceID"))
	if len(id) != 32 || !isHexTraceID(id) {
		writeJSON(w, http.StatusBadRequest, map[string]any{
			"error":    "invalid trace id: expected 32 lowercase hex characters",
			"trace_id": id,
		})
		return
	}
	detail, err := rt.store.GetTrace(r.Context(), id)
	if err != nil {
		rt.writeError(w, err)
		return
	}
	if len(detail.Spans) == 0 {
		writeJSON(w, http.StatusNotFound, map[string]any{
			"error":    "trace not found",
			"trace_id": id,
		})
		return
	}
	writeJSON(w, http.StatusOK, detail)
}

func isHexTraceID(s string) bool {
	for i := 0; i < len(s); i++ {
		c := s[i]
		if !((c >= '0' && c <= '9') || (c >= 'a' && c <= 'f')) {
			return false
		}
	}
	return true
}

func (rt *Router) listFields(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	signal := q.Get("dataset")
	if signal == "" {
		signal = "span"
	}
	fields, err := rt.store.ListFields(r.Context(), store.FieldFilter{
		SignalType: signal,
		Service:    q.Get("service"),
		Prefix:     q.Get("prefix"),
		Limit:      parseInt(q.Get("limit"), 100),
	})
	if err != nil {
		rt.writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"fields": fields})
}

func (rt *Router) listFieldValues(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	signal := q.Get("dataset")
	if signal == "" {
		signal = "span"
	}
	values, err := rt.store.ListFieldValues(r.Context(), store.ValueFilter{
		SignalType: signal,
		Service:    q.Get("service"),
		Key:        r.PathValue("key"),
		Prefix:     q.Get("prefix"),
		Limit:      parseInt(q.Get("limit"), 50),
	})
	if err != nil {
		rt.writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"values": values})
}

func (rt *Router) listSpanNames(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	names, err := rt.store.ListSpanNames(r.Context(), q.Get("service"), q.Get("prefix"), parseInt(q.Get("limit"), 50))
	if err != nil {
		rt.writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"names": names})
}

func (rt *Router) searchLogs(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	logs, cursor, err := rt.store.SearchLogs(r.Context(), store.LogFilter{
		Query:   q.Get("q"),
		Service: q.Get("service"),
		FromNS:  parseTimeNS(q.Get("from")),
		ToNS:    parseTimeNS(q.Get("to")),
		Limit:   parseInt(q.Get("limit"), 200),
		Cursor:  q.Get("cursor"),
	})
	if err != nil {
		rt.writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"logs": logs, "next_cursor": cursor})
}

func (rt *Router) clear(w http.ResponseWriter, r *http.Request) {
	if err := rt.store.Clear(r.Context()); err != nil {
		rt.writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"ok": true})
}

func (rt *Router) writeError(w http.ResponseWriter, err error) {
	rt.log.Warn("api error", "err", err)
	writeJSON(w, http.StatusInternalServerError, map[string]any{"error": err.Error()})
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func parseTimeNS(s string) int64 {
	if s == "" {
		return 0
	}
	// Accept RFC3339Nano or integer ns.
	if n, err := strconv.ParseInt(s, 10, 64); err == nil {
		return n
	}
	if t, err := time.Parse(time.RFC3339Nano, s); err == nil {
		return t.UnixNano()
	}
	if t, err := time.Parse(time.RFC3339, s); err == nil {
		return t.UnixNano()
	}
	return 0
}

func parseInt(s string, def int) int {
	if s == "" {
		return def
	}
	n, err := strconv.Atoi(s)
	if err != nil {
		return def
	}
	return n
}
