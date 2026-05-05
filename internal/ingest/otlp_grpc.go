package ingest

import (
	"context"
	"log/slog"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	collogspb "go.opentelemetry.io/proto/otlp/collector/logs/v1"
	colmetricspb "go.opentelemetry.io/proto/otlp/collector/metrics/v1"
	coltracepb "go.opentelemetry.io/proto/otlp/collector/trace/v1"

	"github.com/danielloader/waggle/internal/otlp"
)

// GRPCHandler wires the OTLP ingest gRPC services to the Writer. It mirrors
// Handler (otlp_http.go) but skips body decoding — gRPC unmarshals for us.
type GRPCHandler struct {
	writer *Writer
	tee    *Tee
	log    *slog.Logger
}

func NewGRPCHandler(w *Writer, log *slog.Logger) *GRPCHandler {
	return &GRPCHandler{writer: w, log: log}
}

func (h *GRPCHandler) WithTee(t *Tee) *GRPCHandler {
	h.tee = t
	return h
}

// Register binds the trace, logs, and metrics services on the given server.
func (h *GRPCHandler) Register(s *grpc.Server) {
	coltracepb.RegisterTraceServiceServer(s, &grpcTraceServer{h: h})
	collogspb.RegisterLogsServiceServer(s, &grpcLogsServer{h: h})
	colmetricspb.RegisterMetricsServiceServer(s, &grpcMetricsServer{h: h})
}

// codes.Unavailable is the OTLP-spec backpressure signal — parity with the
// HTTP 503 + Retry-After response in otlp_http.go.
var errBackpressure = status.Error(codes.Unavailable, "ingest buffer full; retry")

type grpcTraceServer struct {
	coltracepb.UnimplementedTraceServiceServer
	h *GRPCHandler
}

func (s *grpcTraceServer) Export(_ context.Context, req *coltracepb.ExportTraceServiceRequest) (*coltracepb.ExportTraceServiceResponse, error) {
	batch := otlp.TransformResourceSpans(req.ResourceSpans)
	if !s.h.writer.Enqueue(batch) {
		s.h.log.Warn("ingest buffer full", "signal", "traces", "transport", "grpc")
		return nil, errBackpressure
	}
	return &coltracepb.ExportTraceServiceResponse{}, nil
}

type grpcLogsServer struct {
	collogspb.UnimplementedLogsServiceServer
	h *GRPCHandler
}

func (s *grpcLogsServer) Export(_ context.Context, req *collogspb.ExportLogsServiceRequest) (*collogspb.ExportLogsServiceResponse, error) {
	batch := otlp.TransformResourceLogs(req.ResourceLogs)
	// Tee before enqueue so the mirror reflects offered records even when
	// enqueue rejects with backpressure (matches otlp_http.go:64-67).
	s.h.tee.WriteBatch(batch)
	if !s.h.writer.Enqueue(batch) {
		s.h.log.Warn("ingest buffer full", "signal", "logs", "transport", "grpc")
		return nil, errBackpressure
	}
	return &collogspb.ExportLogsServiceResponse{}, nil
}

type grpcMetricsServer struct {
	colmetricspb.UnimplementedMetricsServiceServer
	h *GRPCHandler
}

func (s *grpcMetricsServer) Export(_ context.Context, req *colmetricspb.ExportMetricsServiceRequest) (*colmetricspb.ExportMetricsServiceResponse, error) {
	batch := otlp.TransformResourceMetrics(req.ResourceMetrics)
	if !s.h.writer.Enqueue(batch) {
		s.h.log.Warn("ingest buffer full", "signal", "metrics", "transport", "grpc")
		return nil, errBackpressure
	}
	return &colmetricspb.ExportMetricsServiceResponse{}, nil
}
