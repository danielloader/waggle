package ingest_test

import (
	"bytes"
	"context"
	"encoding/hex"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"

	"google.golang.org/protobuf/proto"

	commonpb "go.opentelemetry.io/proto/otlp/common/v1"
	resourcepb "go.opentelemetry.io/proto/otlp/resource/v1"
	coltracepb "go.opentelemetry.io/proto/otlp/collector/trace/v1"
	tracepb "go.opentelemetry.io/proto/otlp/trace/v1"

	"github.com/danielloader/waggle/internal/ingest"
	"github.com/danielloader/waggle/internal/store/sqlite"
)

func TestIngestProtobufRoundTrip(t *testing.T) {
	ctx := context.Background()
	st, err := sqlite.Open(ctx, filepath.Join(t.TempDir(), "test.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()

	log := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError}))
	w := ingest.NewWriter(st, log, ingest.WriterConfig{FlushEvery: 5 * time.Millisecond})
	w.Start(ctx)
	defer func() {
		sctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		_ = w.Stop(sctx)
	}()

	h := ingest.NewHandler(w, log)
	mux := http.NewServeMux()
	h.Mount(mux)
	srv := httptest.NewServer(mux)
	defer srv.Close()

	traceID := make([]byte, 16)
	for i := range traceID {
		traceID[i] = byte(i + 1)
	}
	spanID := []byte{0x10, 0x11, 0x12, 0x13, 0x14, 0x15, 0x16, 0x17}

	req := &coltracepb.ExportTraceServiceRequest{
		ResourceSpans: []*tracepb.ResourceSpans{{
			Resource: &resourcepb.Resource{
				Attributes: []*commonpb.KeyValue{
					{Key: "service.name", Value: strVal("checkout")},
				},
			},
			ScopeSpans: []*tracepb.ScopeSpans{{
				Scope: &commonpb.InstrumentationScope{Name: "lib", Version: "1.0"},
				Spans: []*tracepb.Span{{
					TraceId: traceID, SpanId: spanID, Name: "POST /checkout",
					Kind: tracepb.Span_SPAN_KIND_SERVER,
					StartTimeUnixNano: uint64(time.Now().UnixNano()),
					EndTimeUnixNano:   uint64(time.Now().Add(100 * time.Millisecond).UnixNano()),
					Status:            &tracepb.Status{Code: tracepb.Status_STATUS_CODE_OK},
					Attributes: []*commonpb.KeyValue{
						{Key: "http.route", Value: strVal("/checkout")},
						{Key: "http.response.status_code", Value: intVal(200)},
					},
				}},
			}},
		}},
	}
	raw, err := proto.Marshal(req)
	if err != nil {
		t.Fatal(err)
	}
	resp, err := http.Post(srv.URL+"/v1/traces", "application/x-protobuf", bytes.NewReader(raw))
	if err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status %d", resp.StatusCode)
	}

	// Wait for writer flush.
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		detail, err := st.GetTrace(ctx, hex.EncodeToString(traceID))
		if err == nil && len(detail.Spans) == 1 {
			if detail.Spans[0].Name != "POST /checkout" {
				t.Fatalf("unexpected name %q", detail.Spans[0].Name)
			}
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatal("span did not appear in store in time")
}

func strVal(s string) *commonpb.AnyValue {
	return &commonpb.AnyValue{Value: &commonpb.AnyValue_StringValue{StringValue: s}}
}

func intVal(n int64) *commonpb.AnyValue {
	return &commonpb.AnyValue{Value: &commonpb.AnyValue_IntValue{IntValue: n}}
}

// TestIngestOTLPJSONHexIDs asserts spec compliance: a JSON payload with
// hex-encoded trace_id / span_id lands with the correct bytes in SQLite,
// not the garbage protojson would produce by interpreting hex as base64.
func TestIngestOTLPJSONHexIDs(t *testing.T) {
	ctx := context.Background()
	st, err := sqlite.Open(ctx, filepath.Join(t.TempDir(), "test.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()

	log := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError}))
	w := ingest.NewWriter(st, log, ingest.WriterConfig{FlushEvery: 5 * time.Millisecond})
	w.Start(ctx)
	defer func() {
		sctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		_ = w.Stop(sctx)
	}()

	h := ingest.NewHandler(w, log)
	mux := http.NewServeMux()
	h.Mount(mux)
	srv := httptest.NewServer(mux)
	defer srv.Close()

	const traceHex = "4bf92f3577b34da6a3ce929d0e0e4736"
	const spanHex = "00f067aa0ba902b7"
	body := `{
		"resourceSpans": [{
			"resource": {"attributes": [
				{"key":"service.name","value":{"stringValue":"json-hex-svc"}}
			]},
			"scopeSpans": [{
				"scope": {"name":"lib","version":"1"},
				"spans": [{
					"traceId":"` + traceHex + `",
					"spanId":"` + spanHex + `",
					"name":"json-hex-root",
					"kind":2,
					"startTimeUnixNano":"1713383000000000000",
					"endTimeUnixNano":"1713383000100000000",
					"status":{"code":1}
				}]
			}]
		}]
	}`
	resp, err := http.Post(srv.URL+"/v1/traces", "application/json", bytes.NewReader([]byte(body)))
	if err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status %d", resp.StatusCode)
	}

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		detail, err := st.GetTrace(ctx, traceHex)
		if err == nil && len(detail.Spans) == 1 {
			sp := detail.Spans[0]
			if sp.Name != "json-hex-root" {
				t.Fatalf("unexpected name: %q", sp.Name)
			}
			if sp.SpanID != spanHex {
				t.Fatalf("span_id round-trip: want %q, got %q", spanHex, sp.SpanID)
			}
			if sp.TraceID != traceHex {
				t.Fatalf("trace_id round-trip: want %q, got %q", traceHex, sp.TraceID)
			}
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("span with hex trace_id %s never appeared in store", traceHex)
}
