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
	"meta.signal_type":        {},
	"meta.annotation_type":    {},
	"meta.span_kind":          {},
	"meta.metric_kind":        {},
	"meta.metric_unit":        {},
	"meta.metric_temporality": {},
	"meta.metric_monotonic":   {},
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
	metricEvents   map[metricEventKey]*metricEventRow
	attrKeys       map[attrKeyID]*store.AttrKeyDelta
	attrValues     map[attrValueID]*store.AttrValueDelta
	metaOverwrites int64
}

// metricEventKey identifies one folded metric row within an export cycle:
// a (resource, scope, time, label set) tuple. Every scalar metric observed
// at that moment for that label set merges into the same row.
type metricEventKey struct {
	resourceID uint64
	scopeID    uint64
	timeNS     int64
	// labelsHash is a hash of the datapoint's label attributes — the label
	// set that uniquely identifies one time-series. Merge keyed on this
	// plus the time/scope/resource tuple.
	labelsHash uint64
}

type metricEventRow struct {
	// labels holds the datapoint's label attributes (what Prometheus calls
	// "series labels" and OTel calls DataPoint.Attributes). Kept separate
	// from metrics so we can serialise the merged attributes at finish time.
	labels      map[string]any
	metrics     map[string]any // metric name → value
	serviceName string
	dataset     string
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
		metricEvents: map[metricEventKey]*metricEventRow{},
		attrKeys:     map[attrKeyID]*store.AttrKeyDelta{},
		attrValues:   map[attrValueID]*store.AttrValueDelta{},
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
	// Serialise folded metric rows. Each row's attributes = label set +
	// metric-name keys + meta.dataset.
	for key, row := range t.metricEvents {
		merged := make(map[string]any, len(row.labels)+len(row.metrics)+1)
		for k, v := range row.labels {
			merged[k] = v
		}
		for k, v := range row.metrics {
			merged[k] = v
		}
		merged["meta.dataset"] = row.dataset
		raw, err := json.Marshal(merged)
		if err != nil {
			continue
		}
		b.MetricEvents = append(b.MetricEvents, store.MetricEvent{
			TimeNS:         key.timeNS,
			ResourceID:     key.resourceID,
			ScopeID:        key.scopeID,
			ServiceName:    row.serviceName,
			AttributesJSON: string(raw),
		})
	}
	for _, d := range t.attrKeys {
		b.AttrKeys = append(b.AttrKeys, *d)
	}
	for _, d := range t.attrValues {
		b.AttrValues = append(b.AttrValues, *d)
	}
	return b
}

func (t *transformer) ingestResourceSpans(rs *tracepb.ResourceSpans) {
	resID, service, dataset := t.registerResource(rs.Resource)

	for _, ss := range rs.ScopeSpans {
		scopeID := t.registerScope(ss.Scope)
		for _, sp := range ss.Spans {
			t.ingestSpan(sp, resID, scopeID, service, dataset)
		}
	}
}

func (t *transformer) ingestResourceLogs(rl *logspb.ResourceLogs) {
	resID, service, dataset := t.registerResource(rl.Resource)

	for _, sl := range rl.ScopeLogs {
		scopeID := t.registerScope(sl.Scope)
		for _, lr := range sl.LogRecords {
			t.ingestLog(lr, resID, scopeID, service, dataset)
		}
	}
}

func (t *transformer) registerResource(r *resourcepb.Resource) (uint64, string, string) {
	attrs, service, dataset, ns, ver, inst, sdkN, sdkL, sdkV := explodeResource(r)
	// attrsToJSON sorts keys (map iteration → json.Marshal), so the output
	// is stable for equivalent attribute sets. Reuse it for both dedup-hash
	// input and storage instead of serialising twice.
	flat := attrsToJSON(attrs)
	id := hash64("res", flat)

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
	return id, service, dataset
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

func (t *transformer) ingestSpan(sp *tracepb.Span, resID, scopeID uint64, service, dataset string) {
	meta := map[string]any{
		"meta.signal_type": store.SignalSpan,
		"meta.span_kind":   spanKindString(sp.Kind),
		"meta.dataset":     dataset,
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

func (t *transformer) ingestLog(lr *logspb.LogRecord, resID, scopeID uint64, service, dataset string) {
	body, bodyJSON := flattenAnyValue(lr.Body)
	meta := map[string]any{
		"meta.signal_type": store.SignalLog,
		"meta.dataset":     dataset,
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

// ingestResourceMetrics walks a ResourceMetrics envelope and folds every
// scalar metric DataPoint into a MetricEvent row keyed on (resource, scope,
// time, label set). Multiple metrics observed at the same moment with the
// same label set merge into one row — Honeycomb-style "one event per
// unique (time, attribute set)" with metric names as attribute keys.
//
// Histograms unpack to <name>.p50 / .p95 / .p99 / .sum / .count / .min /
// .max fields on the same folded row. ExponentialHistogram + Summary are
// catalogue-only for now (distribution support lands later).
func (t *transformer) ingestResourceMetrics(rm *metricspb.ResourceMetrics) {
	resID, service, dataset := t.registerResource(rm.Resource)
	for _, sm := range rm.ScopeMetrics {
		scopeID := t.registerScope(sm.Scope)
		for _, m := range sm.Metrics {
			t.ingestMetric(m, resID, scopeID, service, dataset)
		}
	}
}

func (t *transformer) ingestMetric(
	m *metricspb.Metric, resID, scopeID uint64, service, dataset string,
) {
	switch data := m.Data.(type) {
	case *metricspb.Metric_Sum:
		for _, p := range data.Sum.GetDataPoints() {
			if v, ok := numberPointValue(p); ok {
				t.foldMetric(m.Name, v, p.Attributes, int64(p.TimeUnixNano),
					resID, scopeID, service, dataset)
			}
		}
	case *metricspb.Metric_Gauge:
		for _, p := range data.Gauge.GetDataPoints() {
			if v, ok := numberPointValue(p); ok {
				t.foldMetric(m.Name, v, p.Attributes, int64(p.TimeUnixNano),
					resID, scopeID, service, dataset)
			}
		}
	case *metricspb.Metric_Histogram:
		for _, p := range data.Histogram.GetDataPoints() {
			t.foldHistogram(m.Name, p, resID, scopeID, service, dataset)
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

func numberPointValue(p *metricspb.NumberDataPoint) (float64, bool) {
	switch v := p.Value.(type) {
	case *metricspb.NumberDataPoint_AsDouble:
		return v.AsDouble, true
	case *metricspb.NumberDataPoint_AsInt:
		return float64(v.AsInt), true
	}
	return 0, false
}

// foldMetric adds a single scalar metric observation to the folded row
// identified by (resource, scope, time, label set). Creates the row if
// absent; merges a new metric-name key on existing rows.
func (t *transformer) foldMetric(
	metricName string, value float64, labels []*commonpb.KeyValue,
	timeNS int64, resID, scopeID uint64, service, dataset string,
) {
	row := t.foldRow(labels, timeNS, resID, scopeID, service, dataset)
	row.metrics[metricName] = value
	t.noteAttrKeys(store.SignalMetric, service, labels)
	t.noteAttrValues(store.SignalMetric, service, labels)
	// Record the metric-name key itself in the catalog so autocomplete
	// surfaces it as a queryable field.
	t.noteMetricName(service, metricName, "flt")
}

// foldHistogram expands a histogram point into <name>.count/.sum/.min/.max/
// .p50/.p95/.p99 fields on the folded row. Percentiles come from bucket
// bounds via linear interpolation — same trick Honeycomb uses to surface
// histograms as queryable fields.
func (t *transformer) foldHistogram(
	metricName string, p *metricspb.HistogramDataPoint,
	resID, scopeID uint64, service, dataset string,
) {
	row := t.foldRow(p.Attributes, int64(p.TimeUnixNano), resID, scopeID, service, dataset)
	row.metrics[metricName+".count"] = p.Count
	row.metrics[metricName+".sum"] = p.GetSum()
	if p.Min != nil {
		row.metrics[metricName+".min"] = *p.Min
	}
	if p.Max != nil {
		row.metrics[metricName+".max"] = *p.Max
	}
	for _, q := range []float64{0.5, 0.95, 0.99} {
		pct := histogramPercentile(p, q)
		row.metrics[fmt.Sprintf("%s.p%d", metricName, int(q*100))] = pct
	}
	t.noteAttrKeys(store.SignalMetric, service, p.Attributes)
	t.noteAttrValues(store.SignalMetric, service, p.Attributes)
	for _, suffix := range []string{".count", ".sum", ".min", ".max", ".p50", ".p95", ".p99"} {
		t.noteMetricName(service, metricName+suffix, "flt")
	}
}

// foldRow looks up or creates the folded row for a given datapoint tuple.
func (t *transformer) foldRow(
	labels []*commonpb.KeyValue, timeNS int64,
	resID, scopeID uint64, service, dataset string,
) *metricEventRow {
	labelsMap := attrsToMap(labels)
	key := metricEventKey{
		resourceID: resID,
		scopeID:    scopeID,
		timeNS:     timeNS,
		labelsHash: hashLabels(labelsMap),
	}
	row, ok := t.metricEvents[key]
	if !ok {
		row = &metricEventRow{
			labels:      labelsMap,
			metrics:     map[string]any{},
			serviceName: service,
			dataset:     dataset,
		}
		t.metricEvents[key] = row
	}
	return row
}

// hashLabels produces a stable hash of a label set so two datapoints with
// identical labels fold into the same row.
func hashLabels(labels map[string]any) uint64 {
	keys := make([]string, 0, len(labels))
	for k := range labels {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	h := fnv.New64a()
	for _, k := range keys {
		_, _ = h.Write([]byte(k))
		_, _ = h.Write([]byte{0})
		_, _ = h.Write([]byte(fmt.Sprint(labels[k])))
		_, _ = h.Write([]byte{0})
	}
	return h.Sum64()
}

// noteMetricName records a metric-name key in the attribute_keys catalog
// under signal_type='metric'. Autocomplete then surfaces metric names as
// regular fields alongside user attributes.
func (t *transformer) noteMetricName(service, name, valueType string) {
	id := attrKeyID{store.SignalMetric, service, name, valueType}
	if d, ok := t.attrKeys[id]; ok {
		d.Count++
		d.LastSeenNS = t.now
		return
	}
	t.attrKeys[id] = &store.AttrKeyDelta{
		SignalType: store.SignalMetric, ServiceName: service,
		Key: name, ValueType: valueType, Count: 1, LastSeenNS: t.now,
	}
}

// histogramPercentile computes the q-th percentile from a bucketed
// histogram via linear interpolation. OTel histograms ship explicit
// bucket bounds + counts; we assume uniform distribution within each
// bucket — same approximation Prometheus uses for histogram_quantile().
func histogramPercentile(p *metricspb.HistogramDataPoint, q float64) float64 {
	if p.Count == 0 {
		return 0
	}
	target := float64(p.Count) * q
	bounds := p.ExplicitBounds
	counts := p.BucketCounts
	if len(counts) == 0 {
		return 0
	}
	var cum uint64
	for i, c := range counts {
		cum += c
		if float64(cum) >= target {
			// The bucket that contains the target. Lower/upper bounds of
			// this bucket — first bucket has -inf as lower, last has +inf
			// as upper. Use sum-weighted mid-point for unbounded edges.
			var lower, upper float64
			if i == 0 {
				lower = p.GetMin()
			} else {
				lower = bounds[i-1]
			}
			if i < len(bounds) {
				upper = bounds[i]
			} else {
				upper = p.GetMax()
			}
			if upper == 0 && p.GetMax() == 0 {
				upper = lower
			}
			// Linear interp within the bucket: how far into this bucket?
			bucketStart := float64(cum - c)
			frac := 0.0
			if c > 0 {
				frac = (target - bucketStart) / float64(c)
			}
			return lower + frac*(upper-lower)
		}
	}
	return p.GetMax()
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

func explodeResource(r *resourcepb.Resource) (attrs []*commonpb.KeyValue, service, dataset, ns, ver, inst, sdkN, sdkL, sdkV string) {
	if r == nil {
		return nil, "unknown", "unknown", "", "", "", "", "", ""
	}
	attrs = r.Attributes
	service = "unknown"
	var explicitDataset string
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
		case "waggle.dataset":
			// Explicit override: a collector forwarding from many services
			// can stamp this on its resource to group their telemetry into a
			// named dataset (e.g. "infra-metrics", "ingress") regardless of
			// the individual service.name values.
			if s, ok := kvValueString(kv.Value); ok {
				explicitDataset = s
			}
		}
	}
	// Default dataset derivation: the explicit override wins; otherwise the
	// dataset equals service.name. This matches Honeycomb's default where
	// each instrumented service gets its own dataset.
	if explicitDataset != "" {
		dataset = explicitDataset
	} else {
		dataset = service
	}
	return
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
