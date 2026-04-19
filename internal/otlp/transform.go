package otlp

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"hash/fnv"
	"sort"
	"strings"
	"time"

	commonpb "go.opentelemetry.io/proto/otlp/common/v1"
	logspb "go.opentelemetry.io/proto/otlp/logs/v1"
	metricspb "go.opentelemetry.io/proto/otlp/metrics/v1"
	resourcepb "go.opentelemetry.io/proto/otlp/resource/v1"
	tracepb "go.opentelemetry.io/proto/otlp/trace/v1"

	"github.com/danielloader/waggle/internal/store"
)

// Reserved meta.* keys. Any OTel attribute with one of these keys is
// overwritten at ingest with the system-computed value. Non-whitelisted
// meta.foo user keys pass through untouched.
var reservedMetaKeys = map[string]struct{}{
	"meta.signal_type":       {},
	"meta.annotation_type":   {},
	"meta.span_kind":         {},
	"meta.metric_kind":       {},
	"meta.metric_unit":       {},
	"meta.metric_temporality": {},
	"meta.metric_monotonic":  {},
}

// TransformResourceSpans converts an OTLP ExportTraceServiceRequest-level
// collection of ResourceSpans into a store.Batch of span Events.
func TransformResourceSpans(batches []*tracepb.ResourceSpans) store.Batch {
	t := newTransformer()
	for _, rs := range batches {
		t.ingestResourceSpans(rs)
	}
	return t.finish()
}

// TransformResourceLogs converts ResourceLogs into a store.Batch of log Events.
func TransformResourceLogs(batches []*logspb.ResourceLogs) store.Batch {
	t := newTransformer()
	for _, rl := range batches {
		t.ingestResourceLogs(rl)
	}
	return t.finish()
}

// TransformResourceMetrics converts ResourceMetrics into a store.Batch of
// metric Events. Scalar kinds (Sum, Gauge) produce one Event per data point;
// Histogram / ExpHistogram / Summary are not yet written (attribute catalog
// only) until distribution support lands.
func TransformResourceMetrics(batches []*metricspb.ResourceMetrics) store.Batch {
	t := newTransformer()
	for _, rm := range batches {
		t.ingestResourceMetrics(rm)
	}
	return t.finish()
}

type transformer struct {
	now            int64
	resources      map[uint64]store.Resource
	scopes         map[uint64]store.Scope
	events         []store.Event
	attrKeys       map[attrKeyID]*store.AttrKeyDelta
	attrValues     map[attrValueID]*store.AttrValueDelta
	metaOverwrites int64
}

type attrKeyID struct {
	signalType  string
	serviceName string
	key         string
	valueType   string
}

type attrValueID struct {
	signalType  string
	serviceName string
	key         string
	value       string
}

func newTransformer() *transformer {
	return &transformer{
		now:        time.Now().UnixNano(),
		resources:  map[uint64]store.Resource{},
		scopes:     map[uint64]store.Scope{},
		attrKeys:   map[attrKeyID]*store.AttrKeyDelta{},
		attrValues: map[attrValueID]*store.AttrValueDelta{},
	}
}

func (t *transformer) finish() store.Batch {
	b := store.Batch{EnqueuedAt: time.Now(), MetaOverwrites: t.metaOverwrites}
	for _, r := range t.resources {
		b.Resources = append(b.Resources, r)
	}
	for _, s := range t.scopes {
		b.Scopes = append(b.Scopes, s)
	}
	b.Events = t.events
	for _, d := range t.attrKeys {
		b.AttrKeys = append(b.AttrKeys, *d)
	}
	for _, d := range t.attrValues {
		b.AttrValues = append(b.AttrValues, *d)
	}
	return b
}

func (t *transformer) ingestResourceSpans(rs *tracepb.ResourceSpans) {
	resID, service := t.registerResource(rs.Resource)

	for _, ss := range rs.ScopeSpans {
		scopeID := t.registerScope(ss.Scope)
		for _, sp := range ss.Spans {
			t.ingestSpan(sp, resID, scopeID, service)
		}
	}
}

func (t *transformer) ingestResourceLogs(rl *logspb.ResourceLogs) {
	resID, service := t.registerResource(rl.Resource)

	for _, sl := range rl.ScopeLogs {
		scopeID := t.registerScope(sl.Scope)
		for _, lr := range sl.LogRecords {
			t.ingestLog(lr, resID, scopeID, service)
		}
	}
}

func (t *transformer) registerResource(r *resourcepb.Resource) (uint64, string) {
	attrs, service, ns, ver, inst, sdkN, sdkL, sdkV := explodeResource(r)
	id := hash64("res", canonicalJSON(attrs))
	flat := attrsToJSON(attrs)

	if _, ok := t.resources[id]; !ok {
		t.resources[id] = store.Resource{
			ID:                id,
			ServiceName:       service,
			ServiceNamespace:  ns,
			ServiceVersion:    ver,
			ServiceInstanceID: inst,
			SDKName:           sdkN,
			SDKLanguage:       sdkL,
			SDKVersion:        sdkV,
			AttributesJSON:    flat,
			FirstSeenNS:       t.now,
			LastSeenNS:        t.now,
		}
		t.noteAttrKeys("resource", "", r.GetAttributes())
	}
	return id, service
}

func (t *transformer) registerScope(sc *commonpb.InstrumentationScope) uint64 {
	if sc == nil {
		id := hash64("scope", "")
		if _, ok := t.scopes[id]; !ok {
			t.scopes[id] = store.Scope{ID: id, Name: ""}
		}
		return id
	}
	attrsJSON := ""
	if len(sc.Attributes) > 0 {
		attrsJSON = attrsToJSON(sc.Attributes)
	}
	id := hash64("scope", sc.Name+"|"+sc.Version+"|"+attrsJSON)
	if _, ok := t.scopes[id]; !ok {
		t.scopes[id] = store.Scope{ID: id, Name: sc.Name, Version: sc.Version, AttributesJSON: attrsJSON}
	}
	return id
}

func (t *transformer) ingestSpan(sp *tracepb.Span, resID, scopeID uint64, service string) {
	meta := map[string]any{
		"meta.signal_type": store.SignalSpan,
		"meta.span_kind":   spanKindString(sp.Kind),
	}
	attrsJSON := t.buildAttrs(sp.Attributes, meta)

	endNS := int64(sp.EndTimeUnixNano)
	statusCode := statusCode(sp.Status)
	flags := sp.Flags

	row := store.Event{
		TimeNS:         int64(sp.StartTimeUnixNano),
		EndTimeNS:      &endNS,
		ResourceID:     resID,
		ScopeID:        scopeID,
		ServiceName:    service,
		Name:           sp.Name,
		TraceID:        cloneBytes(sp.TraceId),
		SpanID:         cloneBytes(sp.SpanId),
		ParentSpanID:   cloneBytes(sp.ParentSpanId),
		StatusCode:     &statusCode,
		StatusMessage:  statusMessage(sp.Status),
		TraceState:     sp.TraceState,
		Flags:          &flags,
		AttributesJSON: attrsJSON,
	}

	for i, ev := range sp.Events {
		evMeta := map[string]any{
			"meta.signal_type":     store.SignalSpan,
			"meta.annotation_type": "span_event",
		}
		row.SpanEvents = append(row.SpanEvents, store.SpanEvent{
			TraceID: row.TraceID, SpanID: row.SpanID, Seq: i,
			TimeNS: int64(ev.TimeUnixNano), Name: ev.Name,
			AttributesJSON:    t.buildAttrs(ev.Attributes, evMeta),
			DroppedAttrsCount: ev.DroppedAttributesCount,
		})
		t.noteAttrKeys("event", service, ev.Attributes)
	}
	for i, ln := range sp.Links {
		lnMeta := map[string]any{
			"meta.signal_type":     store.SignalSpan,
			"meta.annotation_type": "link",
		}
		row.SpanLinks = append(row.SpanLinks, store.SpanLink{
			TraceID: row.TraceID, SpanID: row.SpanID, Seq: i,
			LinkedTraceID: cloneBytes(ln.TraceId), LinkedSpanID: cloneBytes(ln.SpanId),
			TraceState: ln.TraceState, Flags: ln.Flags,
			AttributesJSON:    t.buildAttrs(ln.Attributes, lnMeta),
			DroppedAttrsCount: ln.DroppedAttributesCount,
		})
		t.noteAttrKeys("link", service, ln.Attributes)
	}

	t.events = append(t.events, row)
	t.noteAttrKeys(store.SignalSpan, service, sp.Attributes)
	t.noteAttrValues(store.SignalSpan, service, sp.Attributes)
}

func (t *transformer) ingestLog(lr *logspb.LogRecord, resID, scopeID uint64, service string) {
	body, bodyJSON := flattenAnyValue(lr.Body)
	meta := map[string]any{
		"meta.signal_type": store.SignalLog,
	}
	// If the body was structured, stash the JSON form as an attribute so it's
	// still queryable without a dedicated body_json column.
	if bodyJSON != "" {
		meta["body.structured"] = json.RawMessage(bodyJSON)
	}
	attrsJSON := t.buildAttrs(lr.Attributes, meta)

	sevNum := int32(lr.SeverityNumber)
	flags := lr.Flags
	var obsPtr *int64
	if lr.ObservedTimeUnixNano != 0 {
		obs := int64(lr.ObservedTimeUnixNano)
		obsPtr = &obs
	}

	row := store.Event{
		TimeNS:         int64(lr.TimeUnixNano),
		ResourceID:     resID,
		ScopeID:        scopeID,
		ServiceName:    service,
		Name:           "",
		TraceID:        cloneBytes(lr.TraceId),
		SpanID:         cloneBytes(lr.SpanId),
		Flags:          &flags,
		SeverityNumber: &sevNum,
		SeverityText:   lr.SeverityText,
		Body:           body,
		ObservedTimeNS: obsPtr,
		AttributesJSON: attrsJSON,
	}
	t.events = append(t.events, row)
	t.noteAttrKeys(store.SignalLog, service, lr.Attributes)
	t.noteAttrValues(store.SignalLog, service, lr.Attributes)
}

// ingestResourceMetrics walks a ResourceMetrics envelope, registers the
// resource + scopes, and routes each Metric to the per-kind decoder.
// Histogram / ExpHistogram / Summary metrics are noted in the attribute
// catalog (so their series keys appear in /api/fields) but no Event rows
// are emitted — distribution support lands separately.
func (t *transformer) ingestResourceMetrics(rm *metricspb.ResourceMetrics) {
	resID, service := t.registerResource(rm.Resource)
	for _, sm := range rm.ScopeMetrics {
		scopeID := t.registerScope(sm.Scope)
		for _, m := range sm.Metrics {
			t.ingestMetric(m, resID, scopeID, service)
		}
	}
}

func (t *transformer) ingestMetric(
	m *metricspb.Metric, resID, scopeID uint64, service string,
) {
	switch data := m.Data.(type) {
	case *metricspb.Metric_Sum:
		temp := temporalityString(data.Sum.GetAggregationTemporality())
		mono := data.Sum.GetIsMonotonic()
		for _, p := range data.Sum.GetDataPoints() {
			t.ingestNumberPoint(m, "sum", temp, &mono, p, resID, scopeID, service)
		}
	case *metricspb.Metric_Gauge:
		for _, p := range data.Gauge.GetDataPoints() {
			t.ingestNumberPoint(m, "gauge", "", nil, p, resID, scopeID, service)
		}
	case *metricspb.Metric_Histogram:
		for _, p := range data.Histogram.GetDataPoints() {
			t.noteAttrKeys(store.SignalMetric, service, p.Attributes)
			t.noteAttrValues(store.SignalMetric, service, p.Attributes)
		}
	case *metricspb.Metric_ExponentialHistogram:
		for _, p := range data.ExponentialHistogram.GetDataPoints() {
			t.noteAttrKeys(store.SignalMetric, service, p.Attributes)
			t.noteAttrValues(store.SignalMetric, service, p.Attributes)
		}
	case *metricspb.Metric_Summary:
		for _, p := range data.Summary.GetDataPoints() {
			t.noteAttrKeys(store.SignalMetric, service, p.Attributes)
			t.noteAttrValues(store.SignalMetric, service, p.Attributes)
		}
	}
}

// ingestNumberPoint handles Sum + Gauge data points. Emits one Event per
// datapoint — no series dedup; every datapoint carries its own attrs.
func (t *transformer) ingestNumberPoint(
	m *metricspb.Metric, kind, temporality string, monotonic *bool,
	p *metricspb.NumberDataPoint,
	resID, scopeID uint64, service string,
) {
	var value float64
	switch v := p.Value.(type) {
	case *metricspb.NumberDataPoint_AsDouble:
		value = v.AsDouble
	case *metricspb.NumberDataPoint_AsInt:
		value = float64(v.AsInt)
	default:
		return
	}

	meta := map[string]any{
		"meta.signal_type": store.SignalMetric,
		"meta.metric_kind": kind,
	}
	if m.Unit != "" {
		meta["meta.metric_unit"] = m.Unit
	}
	if temporality != "" {
		meta["meta.metric_temporality"] = temporality
	}
	if monotonic != nil {
		if *monotonic {
			meta["meta.metric_monotonic"] = 1
		} else {
			meta["meta.metric_monotonic"] = 0
		}
	}
	if m.Description != "" {
		meta["meta.metric_description"] = m.Description
	}
	attrsJSON := t.buildAttrs(p.Attributes, meta)

	row := store.Event{
		TimeNS:         int64(p.TimeUnixNano),
		ResourceID:     resID,
		ScopeID:        scopeID,
		ServiceName:    service,
		Name:           m.Name,
		Value:          &value,
		AttributesJSON: attrsJSON,
	}
	t.events = append(t.events, row)
	t.noteAttrKeys(store.SignalMetric, service, p.Attributes)
	t.noteAttrValues(store.SignalMetric, service, p.Attributes)
}

func temporalityString(t metricspb.AggregationTemporality) string {
	switch t {
	case metricspb.AggregationTemporality_AGGREGATION_TEMPORALITY_DELTA:
		return "delta"
	case metricspb.AggregationTemporality_AGGREGATION_TEMPORALITY_CUMULATIVE:
		return "cumulative"
	}
	return ""
}

// spanKindString maps OTel's Span.Kind enum to its canonical string form,
// the same surface Honeycomb exposes in meta.span_kind.
func spanKindString(k tracepb.Span_SpanKind) string {
	switch k {
	case tracepb.Span_SPAN_KIND_INTERNAL:
		return "INTERNAL"
	case tracepb.Span_SPAN_KIND_SERVER:
		return "SERVER"
	case tracepb.Span_SPAN_KIND_CLIENT:
		return "CLIENT"
	case tracepb.Span_SPAN_KIND_PRODUCER:
		return "PRODUCER"
	case tracepb.Span_SPAN_KIND_CONSUMER:
		return "CONSUMER"
	}
	return "UNSPECIFIED"
}

// buildAttrs merges user attributes with system-stamped meta.* keys. If a
// user key collides with a reserved meta.* key, the system value wins and
// the overwrite counter bumps for telemetry.
func (t *transformer) buildAttrs(userAttrs []*commonpb.KeyValue, metaStamps map[string]any) string {
	m := attrsToMap(userAttrs)
	for k, v := range metaStamps {
		if _, collision := m[k]; collision {
			if _, reserved := reservedMetaKeys[k]; reserved {
				t.metaOverwrites++
			}
		}
		m[k] = v
	}
	raw, err := json.Marshal(m)
	if err != nil {
		return "{}"
	}
	return string(raw)
}

// noteAttrKeys registers each (key, valueType) observation for the catalog.
// Keys starting with "meta." are filtered out from the catalog view since
// they're structural and surface via syntheticFields instead.
func (t *transformer) noteAttrKeys(signalType, service string, attrs []*commonpb.KeyValue) {
	for _, kv := range attrs {
		if strings.HasPrefix(kv.Key, "meta.") {
			continue
		}
		vt := kvValueType(kv.Value)
		id := attrKeyID{signalType, service, kv.Key, vt}
		if d, ok := t.attrKeys[id]; ok {
			d.Count++
			d.LastSeenNS = t.now
			continue
		}
		t.attrKeys[id] = &store.AttrKeyDelta{
			SignalType: signalType, ServiceName: service,
			Key: kv.Key, ValueType: vt, Count: 1, LastSeenNS: t.now,
		}
	}
}

// noteAttrValues registers a bounded top-K sampling of values for str/int/bool
// attributes.
func (t *transformer) noteAttrValues(signalType, service string, attrs []*commonpb.KeyValue) {
	for _, kv := range attrs {
		if strings.HasPrefix(kv.Key, "meta.") {
			continue
		}
		vt := kvValueType(kv.Value)
		if vt != "str" && vt != "int" && vt != "bool" {
			continue
		}
		vs, ok := kvValueString(kv.Value)
		if !ok {
			continue
		}
		id := attrValueID{signalType, service, kv.Key, vs}
		if d, ok := t.attrValues[id]; ok {
			d.Count++
			d.LastSeenNS = t.now
			continue
		}
		t.attrValues[id] = &store.AttrValueDelta{
			SignalType: signalType, ServiceName: service,
			Key: kv.Key, Value: vs, Count: 1, LastSeenNS: t.now,
		}
	}
}

// =========================================================================
// helpers
// =========================================================================

func explodeResource(r *resourcepb.Resource) (attrs []*commonpb.KeyValue, service, ns, ver, inst, sdkN, sdkL, sdkV string) {
	if r == nil {
		return nil, "unknown", "", "", "", "", "", ""
	}
	attrs = r.Attributes
	service = "unknown"
	for _, kv := range r.Attributes {
		switch kv.Key {
		case "service.name":
			if s, ok := kvValueString(kv.Value); ok {
				service = s
			}
		case "service.namespace":
			ns, _ = kvValueString(kv.Value)
		case "service.version":
			ver, _ = kvValueString(kv.Value)
		case "service.instance.id":
			inst, _ = kvValueString(kv.Value)
		case "telemetry.sdk.name":
			sdkN, _ = kvValueString(kv.Value)
		case "telemetry.sdk.language":
			sdkL, _ = kvValueString(kv.Value)
		case "telemetry.sdk.version":
			sdkV, _ = kvValueString(kv.Value)
		}
	}
	return
}

func canonicalJSON(attrs []*commonpb.KeyValue) string {
	m := attrsToMap(attrs)
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	out := make(map[string]any, len(m))
	var pairs []string
	for _, k := range keys {
		out[k] = m[k]
		pairs = append(pairs, k)
	}
	raw, _ := json.Marshal(struct {
		Keys   []string       `json:"_keys"`
		Values map[string]any `json:"_values"`
	}{pairs, out})
	return string(raw)
}

func attrsToJSON(attrs []*commonpb.KeyValue) string {
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

func attrsToMap(attrs []*commonpb.KeyValue) map[string]any {
	m := make(map[string]any, len(attrs))
	for _, kv := range attrs {
		m[kv.Key] = anyValueToGo(kv.Value)
	}
	return m
}

func anyValueToGo(v *commonpb.AnyValue) any {
	if v == nil {
		return nil
	}
	switch t := v.Value.(type) {
	case *commonpb.AnyValue_StringValue:
		return t.StringValue
	case *commonpb.AnyValue_BoolValue:
		return t.BoolValue
	case *commonpb.AnyValue_IntValue:
		return t.IntValue
	case *commonpb.AnyValue_DoubleValue:
		return t.DoubleValue
	case *commonpb.AnyValue_ArrayValue:
		if t.ArrayValue == nil {
			return []any{}
		}
		out := make([]any, 0, len(t.ArrayValue.Values))
		for _, av := range t.ArrayValue.Values {
			out = append(out, anyValueToGo(av))
		}
		return out
	case *commonpb.AnyValue_KvlistValue:
		if t.KvlistValue == nil {
			return map[string]any{}
		}
		return attrsToMap(t.KvlistValue.Values)
	case *commonpb.AnyValue_BytesValue:
		return base64.StdEncoding.EncodeToString(t.BytesValue)
	default:
		return nil
	}
}

func kvValueType(v *commonpb.AnyValue) string {
	if v == nil {
		return "str"
	}
	switch v.Value.(type) {
	case *commonpb.AnyValue_StringValue:
		return "str"
	case *commonpb.AnyValue_BoolValue:
		return "bool"
	case *commonpb.AnyValue_IntValue:
		return "int"
	case *commonpb.AnyValue_DoubleValue:
		return "flt"
	case *commonpb.AnyValue_ArrayValue:
		return "arr"
	case *commonpb.AnyValue_KvlistValue:
		return "kv"
	case *commonpb.AnyValue_BytesValue:
		return "bytes"
	default:
		return "str"
	}
}

func kvValueString(v *commonpb.AnyValue) (string, bool) {
	if v == nil {
		return "", false
	}
	switch t := v.Value.(type) {
	case *commonpb.AnyValue_StringValue:
		return t.StringValue, true
	case *commonpb.AnyValue_BoolValue:
		if t.BoolValue {
			return "true", true
		}
		return "false", true
	case *commonpb.AnyValue_IntValue:
		return fmt.Sprintf("%d", t.IntValue), true
	case *commonpb.AnyValue_DoubleValue:
		return fmt.Sprintf("%g", t.DoubleValue), true
	default:
		return "", false
	}
}

// flattenAnyValue returns a plain-text representation suitable for FTS5 +
// the original JSON form when the body was structured.
func flattenAnyValue(v *commonpb.AnyValue) (text, jsonForm string) {
	if v == nil {
		return "", ""
	}
	switch t := v.Value.(type) {
	case *commonpb.AnyValue_StringValue:
		return t.StringValue, ""
	case *commonpb.AnyValue_BoolValue:
		if t.BoolValue {
			return "true", ""
		}
		return "false", ""
	case *commonpb.AnyValue_IntValue:
		return fmt.Sprintf("%d", t.IntValue), ""
	case *commonpb.AnyValue_DoubleValue:
		return fmt.Sprintf("%g", t.DoubleValue), ""
	case *commonpb.AnyValue_ArrayValue, *commonpb.AnyValue_KvlistValue:
		goVal := anyValueToGo(v)
		raw, _ := json.Marshal(goVal)
		return string(raw), string(raw)
	case *commonpb.AnyValue_BytesValue:
		enc := base64.StdEncoding.EncodeToString(t.BytesValue)
		return enc, ""
	default:
		return "", ""
	}
}

func statusCode(s *tracepb.Status) int32 {
	if s == nil {
		return 0
	}
	return int32(s.Code)
}

func statusMessage(s *tracepb.Status) string {
	if s == nil {
		return ""
	}
	return s.Message
}

func hash64(domain, payload string) uint64 {
	h := fnv.New64a()
	_, _ = h.Write([]byte(domain))
	_, _ = h.Write([]byte{0})
	_, _ = h.Write([]byte(payload))
	v := h.Sum64()
	return v & ((1 << 63) - 1)
}

func cloneBytes(b []byte) []byte {
	if len(b) == 0 {
		return nil
	}
	out := make([]byte, len(b))
	copy(out, b)
	return out
}
