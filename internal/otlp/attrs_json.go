package otlp

import (
	"encoding/base64"
	"encoding/json"
	"sort"
	"strconv"
	"sync"
	"unicode/utf8"

	commonpb "go.opentelemetry.io/proto/otlp/common/v1"
)

// Allocation hot path. OTLP ingest emits one buildAttrs call per span, log,
// span event, and span link. Profiling (2026-04-23) showed
// attrsToMap + buildAttrs + encoding/json.Marshal accounted for ~37% of all
// bytes allocated during sustained ingest. The direct appender below skips
// the map[string]any intermediate and the reflection walk inside json.Marshal.
//
// Output shape matches the prior map + json.Marshal path: a JSON object with
// keys in lexicographic order. Key ordering is load-bearing for resource
// dedup hashing (registerResource) and for any downstream equality checks.

var attrBufPool = sync.Pool{
	New: func() any {
		b := make([]byte, 0, 512)
		return &b
	},
}

func getAttrBuf() *[]byte {
	bp := attrBufPool.Get().(*[]byte)
	*bp = (*bp)[:0]
	return bp
}

func putAttrBuf(bp *[]byte) {
	// Don't pool buffers that grew unreasonably — a 64 KB ceiling keeps
	// the pool's average size bounded.
	if cap(*bp) > 64*1024 {
		return
	}
	attrBufPool.Put(bp)
}

// encodeUserAttrs writes userAttrs into dst as a JSON object, in sorted key
// order. Used by the resource/scope path where there are no meta stamps.
func encodeUserAttrs(dst []byte, userAttrs []*commonpb.KeyValue) []byte {
	if len(userAttrs) == 0 {
		return append(dst, '{', '}')
	}

	// Build key → last-index map, matching json.Marshal(map) "last wins".
	lastIdx := make(map[string]int, len(userAttrs))
	for i, kv := range userAttrs {
		lastIdx[kv.Key] = i
	}
	keys := make([]string, 0, len(lastIdx))
	for k := range lastIdx {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	dst = append(dst, '{')
	for i, k := range keys {
		if i > 0 {
			dst = append(dst, ',')
		}
		dst = appendJSONString(dst, k)
		dst = append(dst, ':')
		dst = appendAnyValueJSON(dst, userAttrs[lastIdx[k]].Value)
	}
	return append(dst, '}')
}

// encodeMergedAttrs writes userAttrs merged with metaStamps into dst as a
// JSON object in sorted key order. metaStamps wins on collision; reserved
// meta.* collisions are reported via onReservedOverwrite.
func encodeMergedAttrs(
	dst []byte,
	userAttrs []*commonpb.KeyValue,
	metaStamps map[string]any,
	onReservedOverwrite func(),
) []byte {
	if len(userAttrs) == 0 && len(metaStamps) == 0 {
		return append(dst, '{', '}')
	}

	userIdx := make(map[string]int, len(userAttrs))
	for i, kv := range userAttrs {
		if _, hasMeta := metaStamps[kv.Key]; hasMeta {
			if _, reserved := reservedMetaKeys[kv.Key]; reserved {
				if onReservedOverwrite != nil {
					onReservedOverwrite()
				}
			}
			continue
		}
		userIdx[kv.Key] = i
	}

	keys := make([]string, 0, len(userIdx)+len(metaStamps))
	for k := range userIdx {
		keys = append(keys, k)
	}
	for k := range metaStamps {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	dst = append(dst, '{')
	for i, k := range keys {
		if i > 0 {
			dst = append(dst, ',')
		}
		dst = appendJSONString(dst, k)
		dst = append(dst, ':')
		if v, ok := metaStamps[k]; ok {
			dst = appendGoValueJSON(dst, v)
		} else {
			dst = appendAnyValueJSON(dst, userAttrs[userIdx[k]].Value)
		}
	}
	return append(dst, '}')
}

// appendAnyValueJSON writes one OTLP AnyValue as JSON. Mirrors the
// anyValueToGo → json.Marshal path exactly.
func appendAnyValueJSON(dst []byte, v *commonpb.AnyValue) []byte {
	if v == nil {
		return append(dst, "null"...)
	}
	switch t := v.Value.(type) {
	case *commonpb.AnyValue_StringValue:
		return appendJSONString(dst, t.StringValue)
	case *commonpb.AnyValue_BoolValue:
		if t.BoolValue {
			return append(dst, "true"...)
		}
		return append(dst, "false"...)
	case *commonpb.AnyValue_IntValue:
		return strconv.AppendInt(dst, t.IntValue, 10)
	case *commonpb.AnyValue_DoubleValue:
		return appendJSONFloat(dst, t.DoubleValue)
	case *commonpb.AnyValue_ArrayValue:
		dst = append(dst, '[')
		if t.ArrayValue != nil {
			for i, av := range t.ArrayValue.Values {
				if i > 0 {
					dst = append(dst, ',')
				}
				dst = appendAnyValueJSON(dst, av)
			}
		}
		return append(dst, ']')
	case *commonpb.AnyValue_KvlistValue:
		if t.KvlistValue == nil {
			return append(dst, '{', '}')
		}
		return encodeUserAttrs(dst, t.KvlistValue.Values)
	case *commonpb.AnyValue_BytesValue:
		return appendJSONBase64(dst, t.BytesValue)
	default:
		return append(dst, "null"...)
	}
}

// appendGoValueJSON writes one Go value as JSON. metaStamps values are
// produced by this package: strings, json.RawMessage for structured log
// bodies, plus numeric/bool fallbacks for defensive parity with the
// previous json.Marshal(map[string]any) path.
func appendGoValueJSON(dst []byte, v any) []byte {
	switch t := v.(type) {
	case nil:
		return append(dst, "null"...)
	case string:
		return appendJSONString(dst, t)
	case bool:
		if t {
			return append(dst, "true"...)
		}
		return append(dst, "false"...)
	case int:
		return strconv.AppendInt(dst, int64(t), 10)
	case int32:
		return strconv.AppendInt(dst, int64(t), 10)
	case int64:
		return strconv.AppendInt(dst, t, 10)
	case uint32:
		return strconv.AppendUint(dst, uint64(t), 10)
	case uint64:
		return strconv.AppendUint(dst, t, 10)
	case float32:
		return appendJSONFloat(dst, float64(t))
	case float64:
		return appendJSONFloat(dst, t)
	case json.RawMessage:
		if len(t) == 0 {
			return append(dst, "null"...)
		}
		return append(dst, t...)
	case []byte:
		return appendJSONBase64(dst, t)
	default:
		// Fall back to reflection for anything exotic; the hot path never
		// reaches here.
		raw, err := json.Marshal(t)
		if err != nil {
			return append(dst, "null"...)
		}
		return append(dst, raw...)
	}
}

func appendJSONFloat(dst []byte, f float64) []byte {
	// encoding/json emits an error on NaN/Inf; we fall back to null to keep
	// the output valid (ingest prefers losing one field over failing a batch).
	if f != f { // NaN
		return append(dst, "null"...)
	}
	if f > 1.7976931348623157e308 || f < -1.7976931348623157e308 {
		return append(dst, "null"...)
	}
	return strconv.AppendFloat(dst, f, 'g', -1, 64)
}

func appendJSONBase64(dst []byte, b []byte) []byte {
	dst = append(dst, '"')
	n := base64.StdEncoding.EncodedLen(len(b))
	start := len(dst)
	// Grow dst so there's room for n more bytes, then slice up to that.
	for cap(dst) < start+n {
		dst = append(dst, 0)
	}
	dst = dst[:start+n]
	base64.StdEncoding.Encode(dst[start:], b)
	return append(dst, '"')
}

// appendJSONString appends a JSON-encoded string to dst. Matches
// encoding/json's default (HTML-safe) escaping so existing consumers see
// identical bytes.
func appendJSONString(dst []byte, s string) []byte {
	dst = append(dst, '"')
	start := 0
	for i := 0; i < len(s); {
		c := s[i]
		if c < 0x80 {
			if htmlSafeEscape(c) {
				dst = append(dst, s[start:i]...)
				switch c {
				case '\\', '"':
					dst = append(dst, '\\', c)
				case '\n':
					dst = append(dst, '\\', 'n')
				case '\r':
					dst = append(dst, '\\', 'r')
				case '\t':
					dst = append(dst, '\\', 't')
				case '\b':
					dst = append(dst, '\\', 'b')
				case '\f':
					dst = append(dst, '\\', 'f')
				default:
					dst = append(dst, '\\', 'u', '0', '0',
						hexDigit(c>>4), hexDigit(c&0xF))
				}
				i++
				start = i
				continue
			}
			i++
			continue
		}
		r, size := utf8.DecodeRuneInString(s[i:])
		if r == utf8.RuneError && size == 1 {
			dst = append(dst, s[start:i]...)
			dst = append(dst, '\\', 'u', 'f', 'f', 'f', 'd')
			i += size
			start = i
			continue
		}
		// U+2028 / U+2029 are legal JSON but illegal in JS — encoding/json
		// escapes them by default, so we do too.
		if r == ' ' || r == ' ' {
			dst = append(dst, s[start:i]...)
			dst = append(dst, '\\', 'u', '2', '0', '2',
				hexDigit(byte(r)&0xF))
			i += size
			start = i
			continue
		}
		i += size
	}
	dst = append(dst, s[start:]...)
	return append(dst, '"')
}

func htmlSafeEscape(c byte) bool {
	switch c {
	case '"', '\\', '<', '>', '&':
		return true
	}
	return c < 0x20
}

func hexDigit(n byte) byte {
	if n < 10 {
		return '0' + n
	}
	return 'a' + (n - 10)
}
