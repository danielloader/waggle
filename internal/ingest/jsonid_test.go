package ingest

import (
	"encoding/base64"
	"encoding/json"
	"strings"
	"testing"
)

func TestRewriteOTLPJSONIDs_HexToBase64(t *testing.T) {
	// Spec-compliant OTLP/JSON payload: trace_id as 32-char lowercase hex,
	// span_id / parent_span_id as 16-char hex.
	in := []byte(`{
		"resourceSpans": [{
			"resource": {"attributes": [{"key":"service.name","value":{"stringValue":"svc"}}]},
			"scopeSpans": [{
				"spans": [{
					"traceId":"4bf92f3577b34da6a3ce929d0e0e4736",
					"spanId":"00f067aa0ba902b7",
					"parentSpanId":"00f067aa0ba902b7",
					"name":"test"
				}]
			}]
		}]
	}`)
	out, err := rewriteOTLPJSONIDs(in)
	if err != nil {
		t.Fatalf("rewriteOTLPJSONIDs: %v", err)
	}
	var got map[string]any
	if err := json.Unmarshal(out, &got); err != nil {
		t.Fatalf("output not valid JSON: %v", err)
	}

	span := got["resourceSpans"].([]any)[0].(map[string]any)["scopeSpans"].([]any)[0].(map[string]any)["spans"].([]any)[0].(map[string]any)

	// Expected base64 of the hex-decoded IDs.
	tid, _ := base64.StdEncoding.DecodeString(span["traceId"].(string))
	sid, _ := base64.StdEncoding.DecodeString(span["spanId"].(string))
	pid, _ := base64.StdEncoding.DecodeString(span["parentSpanId"].(string))

	if len(tid) != 16 {
		t.Errorf("traceId decoded length: want 16 bytes, got %d (%q)", len(tid), span["traceId"])
	}
	if len(sid) != 8 {
		t.Errorf("spanId decoded length: want 8 bytes, got %d (%q)", len(sid), span["spanId"])
	}
	if len(pid) != 8 {
		t.Errorf("parentSpanId decoded length: want 8 bytes, got %d (%q)", len(pid), span["parentSpanId"])
	}

	// Non-ID bytes fields (attributes, body_json) are untouched.
	if !strings.Contains(string(out), `"service.name"`) {
		t.Errorf("output lost service.name attribute: %s", string(out))
	}
}

func TestRewriteOTLPJSONIDs_AlreadyBase64(t *testing.T) {
	// 24-char base64 of 16 random bytes. Not valid hex (contains '+' or '/'
	// often, and length 24 isn't an 8/16-byte hex). Should pass through.
	in := []byte(`{"resourceSpans":[{"scopeSpans":[{"spans":[{"traceId":"S/kvNXezTaajzpKdDg5HNg==","spanId":"APBnqgupArc="}]}]}]}`)
	out, err := rewriteOTLPJSONIDs(in)
	if err != nil {
		t.Fatalf("rewriteOTLPJSONIDs: %v", err)
	}
	if !strings.Contains(string(out), "S/kvNXezTaajzpKdDg5HNg==") {
		t.Errorf("base64 trace_id was mutated: %s", out)
	}
}

func TestRewriteOTLPJSONIDs_SnakeCaseKeys(t *testing.T) {
	// protojson also accepts snake_case input. Our walker must handle it too.
	in := []byte(`{"resourceSpans":[{"scopeSpans":[{"spans":[{"trace_id":"4bf92f3577b34da6a3ce929d0e0e4736","span_id":"00f067aa0ba902b7"}]}]}]}`)
	out, err := rewriteOTLPJSONIDs(in)
	if err != nil {
		t.Fatal(err)
	}
	var m map[string]any
	_ = json.Unmarshal(out, &m)
	span := m["resourceSpans"].([]any)[0].(map[string]any)["scopeSpans"].([]any)[0].(map[string]any)["spans"].([]any)[0].(map[string]any)
	tidStr := span["trace_id"].(string)
	b, err := base64.StdEncoding.DecodeString(tidStr)
	if err != nil {
		t.Fatalf("trace_id not valid base64 after rewrite: %q (%v)", tidStr, err)
	}
	if len(b) != 16 {
		t.Errorf("trace_id size: want 16, got %d", len(b))
	}
}

func TestRewriteOTLPJSONIDs_IgnoresAttributeValues(t *testing.T) {
	// An attribute value named "trace_id" (under stringValue) must NOT be
	// rewritten — the walker only touches top-level traceId/spanId fields,
	// not nested AnyValue payloads. The attribute key structure is
	// {"key":"...","value":{"stringValue":"..."}} so the hex string lives
	// under "stringValue", which is not in our ID-field whitelist.
	in := []byte(`{
		"resourceSpans":[{"resource":{
			"attributes":[{
				"key":"custom.trace_reference",
				"value":{"stringValue":"4bf92f3577b34da6a3ce929d0e0e4736"}
			}]
		},"scopeSpans":[]}]
	}`)
	out, err := rewriteOTLPJSONIDs(in)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(out), "4bf92f3577b34da6a3ce929d0e0e4736") {
		t.Errorf("attribute value with hex-like content was rewritten: %s", string(out))
	}
}
