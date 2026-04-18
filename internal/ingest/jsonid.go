package ingest

import (
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
)

// rewriteOTLPJSONIDs handles the OTLP/JSON spec quirk where trace and span
// IDs are encoded as lowercase hex strings while the rest of the bytes fields
// use standard base64. google.golang.org/protobuf/encoding/protojson only
// understands base64, so we walk the payload first and convert any hex ID
// value into its base64 equivalent. All other fields pass through untouched.
//
// Conversion is conservative — we only rewrite a value when the raw string
// is a valid hex encoding of exactly 8 bytes (span ID) or 16 bytes (trace
// ID). Anything else is left alone, so a client that (against spec) sends
// a base64-encoded ID still round-trips correctly.
func rewriteOTLPJSONIDs(body []byte) ([]byte, error) {
	var v any
	if err := json.Unmarshal(body, &v); err != nil {
		return body, err
	}
	walkOTLPJSON(v)
	return json.Marshal(v)
}

func walkOTLPJSON(v any) {
	switch t := v.(type) {
	case map[string]any:
		for k, val := range t {
			if isOTLPIDField(k) {
				if s, ok := val.(string); ok {
					if enc, ok := hexToBase64ID(s); ok {
						t[k] = enc
						continue
					}
				}
			}
			walkOTLPJSON(val)
		}
	case []any:
		for _, item := range t {
			walkOTLPJSON(item)
		}
	}
}

// isOTLPIDField reports whether a JSON key carries a hex-encoded OTLP ID.
// Covers both the protojson camelCase form (default) and the snake_case
// aliases protojson also accepts on the ingest side.
func isOTLPIDField(k string) bool {
	switch k {
	case "traceId", "trace_id",
		"spanId", "span_id",
		"parentSpanId", "parent_span_id":
		return true
	}
	return false
}

// hexToBase64ID converts a hex string of an 8- or 16-byte OTel ID into
// standard base64. Returns false if the value doesn't look like a hex ID,
// in which case the caller leaves it alone.
func hexToBase64ID(s string) (string, bool) {
	if s == "" {
		return "", false
	}
	if !isHex(s) {
		return "", false
	}
	b, err := hex.DecodeString(s)
	if err != nil {
		return "", false
	}
	// Accept only known OTel ID sizes; 8-byte spanId and 16-byte traceId.
	if len(b) != 8 && len(b) != 16 {
		return "", false
	}
	return base64.StdEncoding.EncodeToString(b), true
}

func isHex(s string) bool {
	if len(s)%2 != 0 {
		return false
	}
	for i := 0; i < len(s); i++ {
		c := s[i]
		switch {
		case c >= '0' && c <= '9':
		case c >= 'a' && c <= 'f':
		case c >= 'A' && c <= 'F':
		default:
			return false
		}
	}
	return true
}
