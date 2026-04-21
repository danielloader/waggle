package ingest

import (
	"errors"
	"log/slog"
	"net/http"

	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"

	collogspb "go.opentelemetry.io/proto/otlp/collector/logs/v1"
	colmetricspb "go.opentelemetry.io/proto/otlp/collector/metrics/v1"
	coltracepb "go.opentelemetry.io/proto/otlp/collector/trace/v1"

	"github.com/danielloader/waggle/internal/otlp"
)

// Handler wires the OTLP ingest HTTP endpoints to the Writer.
type Handler struct {
	writer *Writer
	tee    *Tee // optional log-mirror sink; nil = disabled
	log    *slog.Logger
}

func NewHandler(w *Writer, log *slog.Logger) *Handler {
	return &Handler{writer: w, log: log}
}

// WithTee attaches a log-mirror sink. Pass nil to keep tee disabled.
// Chainable so the caller can write `NewHandler(...).WithTee(tee)`.
func (h *Handler) WithTee(t *Tee) *Handler {
	h.tee = t
	return h
}

// Mount registers POST /v1/{traces,logs,metrics} on the given mux.
func (h *Handler) Mount(mux *http.ServeMux) {
	mux.HandleFunc("POST /v1/traces", h.handleTraces)
	mux.HandleFunc("POST /v1/logs", h.handleLogs)
	mux.HandleFunc("POST /v1/metrics", h.handleMetrics)
}

func (h *Handler) handleTraces(w http.ResponseWriter, r *http.Request) {
	req, err := decodeTraces(r)
	if err != nil {
		h.writeDecodeError(w, r, err)
		return
	}
	batch := otlp.TransformResourceSpans(req.ResourceSpans)
	if !h.writer.Enqueue(batch) {
		h.writeBackpressure(w, r, "traces")
		return
	}
	h.writeOK(w, r, &coltracepb.ExportTraceServiceResponse{})
}

func (h *Handler) handleLogs(w http.ResponseWriter, r *http.Request) {
	req, err := decodeLogs(r)
	if err != nil {
		h.writeDecodeError(w, r, err)
		return
	}
	batch := otlp.TransformResourceLogs(req.ResourceLogs)
	// Tee *before* enqueue so the mirror reflects the same records that
	// were accepted, even if enqueue later returns backpressure-513.
	// Tee is a no-op when not configured.
	h.tee.WriteBatch(batch)
	if !h.writer.Enqueue(batch) {
		h.writeBackpressure(w, r, "logs")
		return
	}
	h.writeOK(w, r, &collogspb.ExportLogsServiceResponse{})
}

func (h *Handler) handleMetrics(w http.ResponseWriter, r *http.Request) {
	req, err := decodeMetrics(r)
	if err != nil {
		h.writeDecodeError(w, r, err)
		return
	}
	batch := otlp.TransformResourceMetrics(req.ResourceMetrics)
	if !h.writer.Enqueue(batch) {
		h.writeBackpressure(w, r, "metrics")
		return
	}
	h.writeOK(w, r, &colmetricspb.ExportMetricsServiceResponse{})
}

func (h *Handler) writeOK(w http.ResponseWriter, r *http.Request, resp proto.Message) {
	if isJSON(r) {
		w.Header().Set("Content-Type", "application/json")
		raw, err := protojson.Marshal(resp)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(raw)
		return
	}
	w.Header().Set("Content-Type", "application/x-protobuf")
	raw, err := proto.Marshal(resp)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(raw)
}

func (h *Handler) writeDecodeError(w http.ResponseWriter, r *http.Request, err error) {
	status := http.StatusBadRequest
	if errors.Is(err, errUnsupportedContentType) {
		status = http.StatusUnsupportedMediaType
	}
	h.log.Warn("ingest decode failed", "err", err, "path", r.URL.Path)
	http.Error(w, err.Error(), status)
}

func (h *Handler) writeBackpressure(w http.ResponseWriter, _ *http.Request, signal string) {
	h.log.Warn("ingest buffer full", "signal", signal)
	w.Header().Set("Retry-After", "1")
	http.Error(w, "ingest buffer full; retry", http.StatusServiceUnavailable)
}
