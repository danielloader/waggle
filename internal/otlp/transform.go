package otlp

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"hash/fnv"
	"sort"
	"time"

	commonpb "go.opentelemetry.io/proto/otlp/common/v1"
	logspb "go.opentelemetry.io/proto/otlp/logs/v1"
	metricspb "go.opentelemetry.io/proto/otlp/metrics/v1"
	resourcepb "go.opentelemetry.io/proto/otlp/resource/v1"
	tracepb "go.opentelemetry.io/proto/otlp/trace/v1"

	"github.com/danielloader/waggle/internal/store"
)

// Transform converts an OTLP ExportTraceServiceRequest-level collection of
// ResourceSpans into a store.Batch.
func TransformResourceSpans(batches []*tracepb.ResourceSpans) store.Batch {
	t := newTransformer()
	for _, rs := range batches {
		t.ingestResourceSpans(rs)
	}
	return t.finish()
}

// TransformResourceLogs converts ResourceLogs into a store.Batch.
func TransformResourceLogs(batches []*logspb.ResourceLogs) store.Batch {
	t := newTransformer()
	for _, rl := range batches {
		t.ingestResourceLogs(rl)
	}
	return t.finish()
}

// TransformResourceMetrics converts ResourceMetrics into a store.Batch.
// v1 writes Sum + Gauge data points into metric_series + metric_points;
// histogram / exp-histogram / summary instruments are decoded (so the
// catalog knows about them) but their data points are currently dropped
// because the sibling tables for distributions aren't live yet.
func TransformResourceMetrics(batches []*metricspb.ResourceMetrics) store.Batch {
	t := newTransformer()
	for _, rm := range batches {
		t.ingestResourceMetrics(rm)
	}
	return t.finish()
}

type transformer struct {
	now          int64
	resources    map[uint64]store.Resource
	scopes       map[uint64]store.Scope
	spans        []store.Span
	logs         []store.LogRecord
	metricSeries map[store.MetricSeriesRef]store.MetricSeries
	metricPoints []store.MetricPoint
	attrKeys     map[attrKeyID]*store.AttrKeyDelta
	attrValues   map[attrValueID]*store.AttrValueDelta
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
		now:          time.Now().UnixNano(),
		resources:    map[uint64]store.Resource{},
		scopes:       map[uint64]store.Scope{},
		metricSeries: map[store.MetricSeriesRef]store.MetricSeries{},
		attrKeys:     map[attrKeyID]*store.AttrKeyDelta{},
		attrValues:   map[attrValueID]*store.AttrValueDelta{},
	}
}

func (t *transformer) finish() store.Batch {
	b := store.Batch{EnqueuedAt: time.Now()}
	for _, r := range t.resources {
		b.Resources = append(b.Resources, r)
	}
	for _, s := range t.scopes {
		b.Scopes = append(b.Scopes, s)
	}
	b.Spans = t.spans
	b.Logs = t.logs
	for _, s := range t.metricSeries {
		b.MetricSeries = append(b.MetricSeries, s)
	}
	b.MetricPoints = t.metricPoints
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
	// Hash on a canonical form (stable across key orderings) so identical
	// resource sets always dedupe to the same row, but store a flat JSON
	// object for display — that's what the UI's attribute parser and
	// downstream queries expect.
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
		// Surface resource-level attribute keys in the catalog too.
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
	attrsJSON := attrsToJSON(sp.Attributes)

	row := store.Span{
		TraceID:            cloneBytes(sp.TraceId),
		SpanID:             cloneBytes(sp.SpanId),
		ParentSpanID:       cloneBytes(sp.ParentSpanId),
		ResourceID:         resID,
		ScopeID:            scopeID,
		ServiceName:        service,
		Name:               sp.Name,
		Kind:               int32(sp.Kind),
		StartTimeNS:        int64(sp.StartTimeUnixNano),
		EndTimeNS:          int64(sp.EndTimeUnixNano),
		StatusCode:         statusCode(sp.Status),
		StatusMessage:      statusMessage(sp.Status),
		TraceState:         sp.TraceState,
		Flags:              sp.Flags,
		DroppedAttrsCount:  sp.DroppedAttributesCount,
		DroppedEventsCount: sp.DroppedEventsCount,
		DroppedLinksCount:  sp.DroppedLinksCount,
		AttributesJSON:     attrsJSON,
	}

	for i, ev := range sp.Events {
		row.Events = append(row.Events, store.SpanEvent{
			TraceID: row.TraceID, SpanID: row.SpanID, Seq: i,
			TimeNS: int64(ev.TimeUnixNano), Name: ev.Name,
			AttributesJSON:    attrsToJSON(ev.Attributes),
			DroppedAttrsCount: ev.DroppedAttributesCount,
		})
		t.noteAttrKeys("event", service, ev.Attributes)
	}
	for i, ln := range sp.Links {
		row.Links = append(row.Links, store.SpanLink{
			TraceID: row.TraceID, SpanID: row.SpanID, Seq: i,
			LinkedTraceID: cloneBytes(ln.TraceId), LinkedSpanID: cloneBytes(ln.SpanId),
			TraceState: ln.TraceState, Flags: ln.Flags,
			AttributesJSON:    attrsToJSON(ln.Attributes),
			DroppedAttrsCount: ln.DroppedAttributesCount,
		})
		t.noteAttrKeys("link", service, ln.Attributes)
	}

	t.spans = append(t.spans, row)
	t.noteAttrKeys("span", service, sp.Attributes)
	t.noteAttrValues("span", service, sp.Attributes)
}

func (t *transformer) ingestLog(lr *logspb.LogRecord, resID, scopeID uint64, service string) {
	body, bodyJSON := flattenAnyValue(lr.Body)

	row := store.LogRecord{
		ResourceID:        resID,
		ScopeID:           scopeID,
		ServiceName:       service,
		TimeNS:            int64(lr.TimeUnixNano),
		ObservedTimeNS:    int64(lr.ObservedTimeUnixNano),
		SeverityNumber:    int32(lr.SeverityNumber),
		SeverityText:      lr.SeverityText,
		Body:              body,
		BodyJSON:          bodyJSON,
		TraceID:           cloneBytes(lr.TraceId),
		SpanID:            cloneBytes(lr.SpanId),
		Flags:             lr.Flags,
		DroppedAttrsCount: lr.DroppedAttributesCount,
		AttributesJSON:    attrsToJSON(lr.Attributes),
	}
	t.logs = append(t.logs, row)
	t.noteAttrKeys("log", service, lr.Attributes)
	t.noteAttrValues("log", service, lr.Attributes)
}

// ingestResourceMetrics walks a ResourceMetrics envelope, registers the
// resource + scopes, and routes each Metric to the per-kind decoder.
// Histogram / ExpHistogram / Summary metrics are catalogued (so their
// series keys appear in the attribute catalog) but their data points
// are not yet written — the sibling tables for distributions land in a
// later stage; see plans/metrics.md §4.
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
		kind := "sum"
		temp := temporalityString(data.Sum.GetAggregationTemporality())
		mono := data.Sum.GetIsMonotonic()
		for _, p := range data.Sum.GetDataPoints() {
			t.ingestNumberPoint(m, kind, temp, &mono, p, resID, scopeID, service)
		}
	case *metricspb.Metric_Gauge:
		for _, p := range data.Gauge.GetDataPoints() {
			t.ingestNumberPoint(m, "gauge", "", nil, p, resID, scopeID, service)
		}
	case *metricspb.Metric_Histogram:
		// Catalogue only — see stages 4–5.
		temp := temporalityString(data.Histogram.GetAggregationTemporality())
		for _, p := range data.Histogram.GetDataPoints() {
			t.catalogueSeries(m, "histogram", temp, nil, p.Attributes,
				resID, scopeID, service)
		}
	case *metricspb.Metric_ExponentialHistogram:
		temp := temporalityString(data.ExponentialHistogram.GetAggregationTemporality())
		for _, p := range data.ExponentialHistogram.GetDataPoints() {
			t.catalogueSeries(m, "exp_histogram", temp, nil, p.Attributes,
				resID, scopeID, service)
		}
	case *metricspb.Metric_Summary:
		for _, p := range data.Summary.GetDataPoints() {
			t.catalogueSeries(m, "summary", "", nil, p.Attributes,
				resID, scopeID, service)
		}
	}
}

// ingestNumberPoint handles Sum + Gauge data points — both represented
// by metricspb.NumberDataPoint. Writes a series row + a scalar point.
func (t *transformer) ingestNumberPoint(
	m *metricspb.Metric, kind, temporality string, monotonic *bool,
	p *metricspb.NumberDataPoint,
	resID, scopeID uint64, service string,
) {
	ref := t.catalogueSeries(m, kind, temporality, monotonic, p.Attributes,
		resID, scopeID, service)

	var value float64
	switch v := p.Value.(type) {
	case *metricspb.NumberDataPoint_AsDouble:
		value = v.AsDouble
	case *metricspb.NumberDataPoint_AsInt:
		value = float64(v.AsInt)
	default:
		return
	}
	t.metricPoints = append(t.metricPoints, store.MetricPoint{
		SeriesRef:   ref,
		TimeNS:      int64(p.TimeUnixNano),
		StartTimeNS: int64(p.StartTimeUnixNano),
		Value:       value,
	})
}

// catalogueSeries upserts the (resource, scope, name, attrs) row into
// the per-batch metricSeries map and returns the ref so the caller can
// emit points. Also notes each attribute key/value under the "metric"
// signal so `/api/fields?dataset=metric` has something to serve.
func (t *transformer) catalogueSeries(
	m *metricspb.Metric, kind, temporality string, monotonic *bool,
	attrs []*commonpb.KeyValue,
	resID, scopeID uint64, service string,
) store.MetricSeriesRef {
	attrsJSON := attrsToJSON(attrs)
	ref := store.MetricSeriesRef{
		ResourceID: resID, ScopeID: scopeID,
		Name: m.Name, AttributesJSON: attrsJSON,
	}
	if existing, ok := t.metricSeries[ref]; ok {
		existing.LastSeenNS = t.now
		t.metricSeries[ref] = existing
	} else {
		t.metricSeries[ref] = store.MetricSeries{
			ResourceID:     resID,
			ScopeID:        scopeID,
			ServiceName:    service,
			Name:           m.Name,
			Description:    m.Description,
			Unit:           m.Unit,
			Kind:           kind,
			Temporality:    temporality,
			Monotonic:      monotonic,
			AttributesJSON: attrsJSON,
			FirstSeenNS:    t.now,
			LastSeenNS:     t.now,
		}
	}
	t.noteAttrKeys("metric", service, attrs)
	t.noteAttrValues("metric", service, attrs)
	return ref
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

// noteAttrKeys registers each (key, valueType) observation for the catalog.
func (t *transformer) noteAttrKeys(signalType, service string, attrs []*commonpb.KeyValue) {
	for _, kv := range attrs {
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
// attributes. Bounded per-batch to keep the map small; final bounding is done
// server-side by the periodic prune.
func (t *transformer) noteAttrValues(signalType, service string, attrs []*commonpb.KeyValue) {
	for _, kv := range attrs {
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
	// Encode sorted-by-key so the hash is stable.
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
// the original JSON form when the body was structured. Both may be empty.
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
	// Clamp to 63 bits so it fits cleanly into SQLite INTEGER (signed 64-bit).
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
