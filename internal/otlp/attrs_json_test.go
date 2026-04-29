package otlp

import (
	"encoding/json"
	"testing"

	commonpb "go.opentelemetry.io/proto/otlp/common/v1"
)

// The direct JSON appender replaced attrsToMap + json.Marshal on the hot
// ingest path. Its output bytes feed resource-dedup hashes (registerResource)
// so any drift from encoding/json breaks dedup on the wire. These tests
// cross-check encodeUserAttrs and encodeMergedAttrs against the prior
// json.Marshal path for a spread of value shapes.

func kvStr(k, v string) *commonpb.KeyValue {
	return &commonpb.KeyValue{Key: k, Value: &commonpb.AnyValue{
		Value: &commonpb.AnyValue_StringValue{StringValue: v}}}
}
func kvInt(k string, v int64) *commonpb.KeyValue {
	return &commonpb.KeyValue{Key: k, Value: &commonpb.AnyValue{
		Value: &commonpb.AnyValue_IntValue{IntValue: v}}}
}
func kvBool(k string, v bool) *commonpb.KeyValue {
	return &commonpb.KeyValue{Key: k, Value: &commonpb.AnyValue{
		Value: &commonpb.AnyValue_BoolValue{BoolValue: v}}}
}
func kvDouble(k string, v float64) *commonpb.KeyValue {
	return &commonpb.KeyValue{Key: k, Value: &commonpb.AnyValue{
		Value: &commonpb.AnyValue_DoubleValue{DoubleValue: v}}}
}
func kvBytes(k string, v []byte) *commonpb.KeyValue {
	return &commonpb.KeyValue{Key: k, Value: &commonpb.AnyValue{
		Value: &commonpb.AnyValue_BytesValue{BytesValue: v}}}
}
func kvArr(k string, vs ...*commonpb.AnyValue) *commonpb.KeyValue {
	return &commonpb.KeyValue{Key: k, Value: &commonpb.AnyValue{
		Value: &commonpb.AnyValue_ArrayValue{ArrayValue: &commonpb.ArrayValue{Values: vs}}}}
}
func kvKvlist(k string, kvs ...*commonpb.KeyValue) *commonpb.KeyValue {
	return &commonpb.KeyValue{Key: k, Value: &commonpb.AnyValue{
		Value: &commonpb.AnyValue_KvlistValue{KvlistValue: &commonpb.KeyValueList{Values: kvs}}}}
}
func avStr(v string) *commonpb.AnyValue {
	return &commonpb.AnyValue{Value: &commonpb.AnyValue_StringValue{StringValue: v}}
}
func avInt(v int64) *commonpb.AnyValue {
	return &commonpb.AnyValue{Value: &commonpb.AnyValue_IntValue{IntValue: v}}
}

func referenceUserAttrs(attrs []*commonpb.KeyValue) string {
	if len(attrs) == 0 {
		return "{}"
	}
	m := attrsToMap(attrs)
	raw, err := json.Marshal(m)
	if err != nil {
		return "{}"
	}
	return string(raw)
}

func referenceMerged(attrs []*commonpb.KeyValue, meta map[string]any) string {
	m := attrsToMap(attrs)
	for k, v := range meta {
		m[k] = v
	}
	raw, err := json.Marshal(m)
	if err != nil {
		return "{}"
	}
	return string(raw)
}

func TestEncodeUserAttrs_MatchesJSONMarshal(t *testing.T) {
	tests := []struct {
		name  string
		attrs []*commonpb.KeyValue
	}{
		{"empty", nil},
		{"single string", []*commonpb.KeyValue{kvStr("service.name", "checkout")}},
		{"mixed scalars", []*commonpb.KeyValue{
			kvStr("http.route", "/users/{id}"),
			kvInt("http.status_code", 200),
			kvBool("cache.hit", true),
			kvDouble("duration.seconds", 0.0123),
		}},
		{"unsorted input, sorted output", []*commonpb.KeyValue{
			kvStr("zeta", "z"),
			kvStr("alpha", "a"),
			kvStr("mike", "m"),
		}},
		{"escape chars", []*commonpb.KeyValue{
			kvStr("quote.key", `he said "hi"`),
			kvStr("slash.key", `a\b/c`),
			kvStr("ctl.key", "line1\nline2\ttab"),
			kvStr("html.key", "<b>&nbsp;</b>"),
			kvStr("tab\tin.key", "x"),
		}},
		{"unicode", []*commonpb.KeyValue{
			kvStr("emoji", "🔥"),
			kvStr("chinese", "你好"),
			kvStr("line.sep", " "),
			kvStr("para.sep", " "),
		}},
		{"array values", []*commonpb.KeyValue{
			kvArr("list", avStr("a"), avStr("b"), avInt(3)),
		}},
		{"nested kvlist", []*commonpb.KeyValue{
			kvKvlist("nested", kvStr("inner", "v"), kvInt("count", 7)),
		}},
		{"bytes", []*commonpb.KeyValue{
			kvBytes("trace.id", []byte{0xde, 0xad, 0xbe, 0xef}),
		}},
		{"duplicate keys (last wins)", []*commonpb.KeyValue{
			kvStr("dup", "first"),
			kvStr("dup", "second"),
		}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := string(encodeUserAttrs(nil, tc.attrs))
			want := referenceUserAttrs(tc.attrs)
			if got != want {
				t.Errorf("output drift\nwant: %s\n got: %s", want, got)
			}
			// And the map parses back the same way.
			var gotMap, wantMap any
			if err := json.Unmarshal([]byte(got), &gotMap); err != nil {
				t.Errorf("produced invalid JSON: %v", err)
			}
			if err := json.Unmarshal([]byte(want), &wantMap); err != nil {
				t.Errorf("reference produced invalid JSON: %v", err)
			}
		})
	}
}

func TestEncodeMergedAttrs_MatchesJSONMarshal(t *testing.T) {
	tests := []struct {
		name  string
		attrs []*commonpb.KeyValue
		meta  map[string]any
	}{
		{"meta only", nil, map[string]any{
			"meta.signal_type": "span",
			"meta.dataset":     "checkout",
		}},
		{"user only", []*commonpb.KeyValue{kvStr("x", "y")}, nil},
		{"meta wins on collision (non-reserved)",
			[]*commonpb.KeyValue{kvStr("dup", "user")},
			map[string]any{"dup": "system"}},
		{"meta wins on reserved collision",
			[]*commonpb.KeyValue{kvStr("meta.signal_type", "user-lied")},
			map[string]any{"meta.signal_type": "span"}},
		{"json.RawMessage meta (structured body)",
			[]*commonpb.KeyValue{kvStr("x", "y")},
			map[string]any{"body.structured": json.RawMessage(`{"a":1,"b":[1,2,3]}`)}},
		{"mixed with nested array",
			[]*commonpb.KeyValue{
				kvArr("tags", avStr("blue"), avStr("green")),
				kvInt("status", 200),
			},
			map[string]any{
				"meta.signal_type": "log",
				"meta.dataset":     "orders",
			}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := string(encodeMergedAttrs(nil, tc.attrs, tc.meta, nil))
			want := referenceMerged(tc.attrs, tc.meta)
			if got != want {
				t.Errorf("output drift\nwant: %s\n got: %s", want, got)
			}
		})
	}
}

func TestEncodeMergedAttrs_CountsReservedOverwrites(t *testing.T) {
	// Reserved key collision → callback fires. Non-reserved user/meta
	// collision → callback does not fire.
	var fired int
	_ = encodeMergedAttrs(nil,
		[]*commonpb.KeyValue{
			kvStr("meta.signal_type", "user-override"), // reserved, fires
			kvStr("unreserved.key", "user"),            // not reserved
			kvStr("meta.dataset", "user-override"),     // NOT in reservedMetaKeys
		},
		map[string]any{
			"meta.signal_type": "span",
			"unreserved.key":   "system",
			"meta.dataset":     "orders",
		},
		func() { fired++ })
	if fired != 1 {
		t.Errorf("expected exactly 1 reserved overwrite, got %d", fired)
	}
}

func TestBuildAttrs_StableAcrossRepeatedPooledBuffers(t *testing.T) {
	// Two back-to-back calls should return independent strings; the pooled
	// buffer reset must not leak bytes from the previous call.
	tr := newTransformer()
	a := tr.buildAttrs([]*commonpb.KeyValue{kvStr("k", "v1")},
		map[string]any{"meta.signal_type": "span"})
	b := tr.buildAttrs([]*commonpb.KeyValue{kvStr("k", "v2")},
		map[string]any{"meta.signal_type": "log"})
	if a == b {
		t.Errorf("expected distinct outputs, both = %s", a)
	}
	if a != `{"k":"v1","meta.signal_type":"span"}` {
		t.Errorf("first call: %s", a)
	}
	if b != `{"k":"v2","meta.signal_type":"log"}` {
		t.Errorf("second call: %s", b)
	}
}
