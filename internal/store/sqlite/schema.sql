-- waggle SQLite schema v3 — split wide-event model.
--
-- Two storage surfaces, both wide-event shaped:
--
--  * `events` carries spans + logs + anything else that's an "event at a
--    point in time with rich structured context". Span events and span
--    links live as child rows keyed by (trace_id, span_id).
--
--  * `metric_events` carries OTel metrics, Honeycomb-style: the metric
--    name is an attribute field (e.g. `requests.total=1423` lands as a
--    key in the attributes JSON) and every metric observed at the same
--    time with the same label set is folded into one row. No dedicated
--    `value` or `metric_name` columns.
--
-- The split is an honest schema: span/log columns (trace_id, severity_*,
-- body, …) don't exist on metric rows; metric-only concepts (metric_kind,
-- metric_unit) don't exist on event rows.
--
-- `meta.*` is a reserved attribute prefix, surfaced as virtual columns:
--   meta.dataset          — logical namespace, defaults to service.name
--   meta.signal_type      — 'span' | 'log' (metric rows don't stamp this)
--   meta.span_kind        — OTel SpanKind enum name
--   meta.annotation_type  — 'span_event' | 'link' for span child rows
--   meta.metric_kind      — 'sum' | 'gauge' | 'histogram' | …
--   meta.metric_unit      — OTel unit string
--   meta.metric_temporality / meta.metric_monotonic — counter shape

CREATE TABLE IF NOT EXISTS schema_meta (
    key   TEXT PRIMARY KEY,
    value TEXT NOT NULL
) STRICT;

INSERT OR REPLACE INTO schema_meta(key, value) VALUES ('version', '3');

-- =========================================================================
-- Resources: one row per unique resource attribute set
-- =========================================================================
CREATE TABLE IF NOT EXISTS resources (
    resource_id         INTEGER PRIMARY KEY,
    service_name        TEXT NOT NULL,
    service_namespace   TEXT,
    service_version     TEXT,
    service_instance_id TEXT,
    sdk_name            TEXT,
    sdk_language        TEXT,
    sdk_version         TEXT,
    attributes          TEXT NOT NULL,
    first_seen_ns       INTEGER NOT NULL,
    last_seen_ns        INTEGER NOT NULL
) STRICT;

CREATE INDEX IF NOT EXISTS idx_resources_service ON resources(service_name);

-- =========================================================================
-- Scopes: instrumentation scope identities
-- =========================================================================
CREATE TABLE IF NOT EXISTS scopes (
    scope_id   INTEGER PRIMARY KEY,
    name       TEXT NOT NULL,
    version    TEXT,
    attributes TEXT
) STRICT;

CREATE INDEX IF NOT EXISTS idx_scopes_name ON scopes(name);

-- =========================================================================
-- Events — spans + logs (anything that isn't a metric)
-- =========================================================================
CREATE TABLE IF NOT EXISTS events (
    event_id         INTEGER PRIMARY KEY,
    time_ns          INTEGER NOT NULL,
    end_time_ns      INTEGER,
    duration_ns      INTEGER GENERATED ALWAYS AS (end_time_ns - time_ns) VIRTUAL,

    resource_id      INTEGER NOT NULL REFERENCES resources(resource_id),
    scope_id         INTEGER REFERENCES scopes(scope_id),
    service_name     TEXT NOT NULL,
    name             TEXT NOT NULL,

    -- Trace correlation. Spans populate all three; logs populate trace_id +
    -- span_id when emitted under a span context.
    trace_id         BLOB,
    span_id          BLOB,
    parent_span_id   BLOB,

    -- Span-only scalars
    status_code      INTEGER,
    status_message   TEXT,
    trace_state      TEXT,
    flags            INTEGER,

    -- Log-only scalars
    severity_number  INTEGER,
    severity_text    TEXT,
    body             TEXT,
    observed_time_ns INTEGER,

    attributes       TEXT NOT NULL DEFAULT '{}',

    -- meta.* virtual columns
    dataset          TEXT GENERATED ALWAYS AS (json_extract(attributes, '$."meta.dataset"')) VIRTUAL,
    signal_type      TEXT GENERATED ALWAYS AS (json_extract(attributes, '$."meta.signal_type"')) VIRTUAL,
    span_kind        TEXT GENERATED ALWAYS AS (json_extract(attributes, '$."meta.span_kind"')) VIRTUAL,
    annotation_type  TEXT GENERATED ALWAYS AS (json_extract(attributes, '$."meta.annotation_type"')) VIRTUAL,

    -- Convenience extractions (user attrs, not meta.*)
    http_method      TEXT    GENERATED ALWAYS AS (json_extract(attributes, '$."http.request.method"')) VIRTUAL,
    http_status_code INTEGER GENERATED ALWAYS AS (json_extract(attributes, '$."http.response.status_code"')) VIRTUAL,
    http_route       TEXT    GENERATED ALWAYS AS (json_extract(attributes, '$."http.route"')) VIRTUAL,
    rpc_service      TEXT    GENERATED ALWAYS AS (json_extract(attributes, '$."rpc.service"')) VIRTUAL,
    db_system        TEXT    GENERATED ALWAYS AS (json_extract(attributes, '$."db.system"')) VIRTUAL
) STRICT;

-- idx_events_time was previously defined here; every real query filters
-- on signal_type or service_name first, so the composite indexes below
-- always shadow it. Drop it on schema re-apply to reclaim the disk +
-- write-amplification cost on existing databases.
DROP INDEX IF EXISTS idx_events_time;
CREATE INDEX IF NOT EXISTS idx_events_dataset_time     ON events(dataset, time_ns DESC) WHERE dataset IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_events_signal_time      ON events(signal_type, time_ns DESC);
CREATE INDEX IF NOT EXISTS idx_events_svc_time         ON events(service_name, time_ns DESC);
CREATE INDEX IF NOT EXISTS idx_events_svc_signal_time  ON events(service_name, signal_type, time_ns DESC);
CREATE INDEX IF NOT EXISTS idx_events_trace            ON events(trace_id, time_ns) WHERE trace_id IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_events_roots            ON events(service_name, time_ns DESC, trace_id)
    WHERE signal_type = 'span' AND parent_span_id IS NULL;
CREATE INDEX IF NOT EXISTS idx_events_span_duration    ON events(service_name, duration_ns DESC)
    WHERE signal_type = 'span';
CREATE INDEX IF NOT EXISTS idx_events_errors           ON events(service_name, time_ns DESC)
    WHERE status_code = 2 OR severity_number >= 17;
CREATE INDEX IF NOT EXISTS idx_events_http_route       ON events(service_name, http_route, time_ns DESC)
    WHERE http_route IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_events_http_status      ON events(service_name, http_status_code, time_ns DESC)
    WHERE http_status_code IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_events_name_time        ON events(name, time_ns DESC);
CREATE INDEX IF NOT EXISTS idx_events_log_severity     ON events(severity_number, time_ns DESC)
    WHERE signal_type = 'log';

-- =========================================================================
-- Span events (annotations on a span — OTel's Span.Events)
-- =========================================================================
CREATE TABLE IF NOT EXISTS span_events (
    trace_id            BLOB NOT NULL,
    span_id             BLOB NOT NULL,
    seq                 INTEGER NOT NULL,
    time_ns             INTEGER NOT NULL,
    name                TEXT NOT NULL,
    attributes          TEXT NOT NULL DEFAULT '{}',
    dropped_attrs_count INTEGER NOT NULL DEFAULT 0,
    PRIMARY KEY (trace_id, span_id, seq)
) STRICT, WITHOUT ROWID;

CREATE INDEX IF NOT EXISTS idx_span_events_time ON span_events(time_ns DESC);

-- =========================================================================
-- Span links
-- =========================================================================
CREATE TABLE IF NOT EXISTS span_links (
    trace_id            BLOB NOT NULL,
    span_id             BLOB NOT NULL,
    seq                 INTEGER NOT NULL,
    linked_trace_id     BLOB NOT NULL,
    linked_span_id      BLOB NOT NULL,
    trace_state         TEXT,
    flags               INTEGER NOT NULL DEFAULT 0,
    attributes          TEXT NOT NULL DEFAULT '{}',
    dropped_attrs_count INTEGER NOT NULL DEFAULT 0,
    PRIMARY KEY (trace_id, span_id, seq)
) STRICT, WITHOUT ROWID;

CREATE INDEX IF NOT EXISTS idx_span_links_target ON span_links(linked_trace_id, linked_span_id);

-- =========================================================================
-- FTS5 mirror over events.body + events.name
-- Metric names are structured identifiers, not prose — they don't belong
-- in the FTS index and would pollute porter tokenization. Metric search
-- goes through attribute_keys.
-- =========================================================================
CREATE VIRTUAL TABLE IF NOT EXISTS events_fts USING fts5(
    body, name, service_name UNINDEXED,
    content='events', content_rowid='event_id', tokenize='porter unicode61'
);

CREATE TRIGGER IF NOT EXISTS events_ai AFTER INSERT ON events BEGIN
    INSERT INTO events_fts(rowid, body, name, service_name)
    VALUES (NEW.event_id, NEW.body, NEW.name, NEW.service_name);
END;

CREATE TRIGGER IF NOT EXISTS events_ad AFTER DELETE ON events BEGIN
    INSERT INTO events_fts(events_fts, rowid, body, name, service_name)
    VALUES ('delete', OLD.event_id, OLD.body, OLD.name, OLD.service_name);
END;

-- =========================================================================
-- Metric events — Honeycomb-style folded metrics.
--
-- One row = one unique (resource, scope, time_ns, attribute-set) tuple
-- within an OTel export cycle. Every scalar metric observed at that
-- moment for that label set lands as an attribute key/value pair in
-- `attributes`. Histograms expand to <name>.p50/.p95/.sum/.count fields.
--
-- No dedicated value/kind/unit columns — metric identity and payload
-- both live in `attributes`. Query builder's unknown-field fallthrough
-- (json_extract) handles `MAX(requests.total)` transparently.
-- =========================================================================
CREATE TABLE IF NOT EXISTS metric_events (
    event_id     INTEGER PRIMARY KEY,
    time_ns      INTEGER NOT NULL,
    resource_id  INTEGER NOT NULL REFERENCES resources(resource_id),
    scope_id     INTEGER REFERENCES scopes(scope_id),
    service_name TEXT NOT NULL,
    attributes   TEXT NOT NULL DEFAULT '{}',

    dataset      TEXT GENERATED ALWAYS AS (json_extract(attributes, '$."meta.dataset"')) VIRTUAL
) STRICT;

CREATE INDEX IF NOT EXISTS idx_metric_events_time          ON metric_events(time_ns DESC);
CREATE INDEX IF NOT EXISTS idx_metric_events_svc_time      ON metric_events(service_name, time_ns DESC);
CREATE INDEX IF NOT EXISTS idx_metric_events_dataset_time  ON metric_events(dataset, time_ns DESC) WHERE dataset IS NOT NULL;

-- =========================================================================
-- Attribute key catalog (for the Fields panel + name autocomplete)
--
-- Records each (signal_type, service, key, value_type) observation.
-- For metric_events the signal_type is 'metric'; metric-name keys like
-- `requests.total` appear here so autocomplete can surface them in the
-- field picker alongside real attributes.
-- =========================================================================
CREATE TABLE IF NOT EXISTS attribute_keys (
    signal_type   TEXT NOT NULL,
    service_name  TEXT NOT NULL,
    key           TEXT NOT NULL,
    value_type    TEXT NOT NULL,
    first_seen_ns INTEGER NOT NULL,
    last_seen_ns  INTEGER NOT NULL,
    count         INTEGER NOT NULL DEFAULT 1,
    PRIMARY KEY (signal_type, service_name, key, value_type)
) STRICT, WITHOUT ROWID;

CREATE INDEX IF NOT EXISTS idx_attrkeys_service ON attribute_keys(service_name, signal_type);
CREATE INDEX IF NOT EXISTS idx_attrkeys_key     ON attribute_keys(key);

-- =========================================================================
-- Attribute value samples (for VALUE autocomplete on str/int/bool keys)
-- =========================================================================
CREATE TABLE IF NOT EXISTS attribute_values (
    signal_type   TEXT NOT NULL,
    service_name  TEXT NOT NULL,
    key           TEXT NOT NULL,
    value         TEXT NOT NULL,
    count         INTEGER NOT NULL DEFAULT 1,
    last_seen_ns  INTEGER NOT NULL,
    PRIMARY KEY (signal_type, service_name, key, value)
) STRICT, WITHOUT ROWID;

CREATE INDEX IF NOT EXISTS idx_attrvals_lookup ON attribute_values(signal_type, service_name, key, count DESC);

-- =========================================================================
-- Query history — one row per distinct query AST the user has run, with
-- run-count + last-run-time rolled up so repeats update the existing row
-- instead of piling on new ones. The `hash` is a stable content hash of
-- `query_json` so the UNIQUE key dedupes "same query" regardless of
-- wall-clock submission time. `display_text` is a pre-rendered one-glance
-- summary ("SELECT COUNT WHERE … GROUP BY …") so the list view doesn't
-- need to rehydrate + format on every render.
-- =========================================================================
CREATE TABLE IF NOT EXISTS query_history (
    id            INTEGER PRIMARY KEY AUTOINCREMENT,
    dataset       TEXT NOT NULL,
    hash          BLOB NOT NULL UNIQUE,
    query_json    TEXT NOT NULL,
    display_text  TEXT NOT NULL,
    run_count     INTEGER NOT NULL DEFAULT 1,
    first_run_ns  INTEGER NOT NULL,
    last_run_ns   INTEGER NOT NULL
) STRICT;

CREATE INDEX IF NOT EXISTS idx_query_history_last ON query_history(last_run_ns DESC);
