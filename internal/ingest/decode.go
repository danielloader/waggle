package ingest

import (
	"compress/gzip"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"

	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"

	collogspb "go.opentelemetry.io/proto/otlp/collector/logs/v1"
	colmetricspb "go.opentelemetry.io/proto/otlp/collector/metrics/v1"
	coltracepb "go.opentelemetry.io/proto/otlp/collector/trace/v1"
)

const (
	maxCompressedBytes   = 10 * 1024 * 1024 // 10 MiB
	maxDecompressedBytes = 50 * 1024 * 1024 // 50 MiB
)

var errUnsupportedContentType = errors.New("unsupported content-type")

func decodeTraces(r *http.Request) (*coltracepb.ExportTraceServiceRequest, error) {
	body, err := readBody(r)
	if err != nil {
		return nil, err
	}
	req := &coltracepb.ExportTraceServiceRequest{}
	if err := unmarshal(r.Header.Get("Content-Type"), body, req); err != nil {
		return nil, err
	}
	return req, nil
}

func decodeLogs(r *http.Request) (*collogspb.ExportLogsServiceRequest, error) {
	body, err := readBody(r)
	if err != nil {
		return nil, err
	}
	req := &collogspb.ExportLogsServiceRequest{}
	if err := unmarshal(r.Header.Get("Content-Type"), body, req); err != nil {
		return nil, err
	}
	return req, nil
}

func decodeMetrics(r *http.Request) (*colmetricspb.ExportMetricsServiceRequest, error) {
	body, err := readBody(r)
	if err != nil {
		return nil, err
	}
	req := &colmetricspb.ExportMetricsServiceRequest{}
	if err := unmarshal(r.Header.Get("Content-Type"), body, req); err != nil {
		return nil, err
	}
	return req, nil
}

// readBody handles Content-Encoding: gzip with a decompressed-size cap, and
// caps the raw compressed body too. All reads fail closed.
func readBody(r *http.Request) ([]byte, error) {
	var reader io.Reader = http.MaxBytesReader(nil, r.Body, maxCompressedBytes)

	switch strings.ToLower(r.Header.Get("Content-Encoding")) {
	case "", "identity":
	case "gzip":
		gz, err := gzip.NewReader(reader)
		if err != nil {
			return nil, fmt.Errorf("gzip reader: %w", err)
		}
		defer gz.Close()
		reader = gz
	default:
		return nil, fmt.Errorf("unsupported content-encoding")
	}

	limited := io.LimitReader(reader, maxDecompressedBytes+1)
	body, err := io.ReadAll(limited)
	if err != nil {
		return nil, fmt.Errorf("read body: %w", err)
	}
	if len(body) > maxDecompressedBytes {
		return nil, fmt.Errorf("body exceeds %d byte limit", maxDecompressedBytes)
	}
	return body, nil
}

func unmarshal(contentType string, body []byte, into proto.Message) error {
	mediaType := contentType
	if i := strings.Index(mediaType, ";"); i >= 0 {
		mediaType = mediaType[:i]
	}
	mediaType = strings.TrimSpace(strings.ToLower(mediaType))

	switch mediaType {
	case "application/x-protobuf", "application/protobuf":
		return proto.Unmarshal(body, into)
	case "application/json":
		// OTLP/JSON encodes trace_id/span_id as lowercase hex; protojson
		// only understands base64 for bytes fields. Rewrite those values
		// before unmarshal so spec-compliant clients round-trip correctly.
		fixed, err := rewriteOTLPJSONIDs(body)
		if err != nil {
			// Fall back to the original body — protojson will surface the
			// real error with better context than our pre-parse would.
			fixed = body
		}
		return protojson.Unmarshal(fixed, into)
	default:
		return fmt.Errorf("%w: %q", errUnsupportedContentType, contentType)
	}
}

// isJSON reports whether the request's Content-Type is OTLP/JSON; the writer
// encodes the response in the same form per the OTLP spec.
func isJSON(r *http.Request) bool {
	mt := strings.ToLower(r.Header.Get("Content-Type"))
	if i := strings.Index(mt, ";"); i >= 0 {
		mt = mt[:i]
	}
	return strings.TrimSpace(mt) == "application/json"
}
