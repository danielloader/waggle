-- waggle SQLite schema v2 — unified wide-events model.
--
-- All telemetry (spans, logs, metric points) lives in the single `events`
-- table. The signal is identified by a reserved `meta.signal_type` attribute
-- surfaced as a virtual column; same for `meta.span_kind`, `meta.metric_kind`,
-- `meta.metric_unit`, `meta.metric_temporality`, `meta.metric_monotonic`.
-- Only these specific keys are reserved — user attributes under the `meta.`
-- prefix outside the whitelist pass through.

CREATE TABLE IF NOT EXISTS schema_meta (
    key   TEXT PRIMARY KEY,
    value TEXT NOT NULL
) STRICT;

INSERT OR REPLACE INTO schema_meta(key, value) VALUES ('version', '2');

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
-- Events — unified wide-event table
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
    -- span_id when emitted under a span context; metric points leave NULL.
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

    -- Metric-only scalar (scalar kinds only; histograms / summaries
    -- future-tracked separately once we add them).
    value            REAL,

    attributes       TEXT NOT NULL DEFAULT '{}',

    -- meta.* virtual columns (system-stamped descriptors)
    signal_type        TEXT    GENERATED ALWAYS AS (json_extract(attributes, '$."meta.signal_type"')) VIRTUAL,
    span_kind          TEXT    GENERATED ALWAYS AS (json_extract(attributes, '$."meta.span_kind"')) VIRTUAL,
    annotation_type    TEXT    GENERATED ALWAYS AS (json_extract(attributes, '$."meta.annotation_type"')) VIRTUAL,
    metric_kind        TEXT    GENERATED ALWAYS AS (json_extract(attributes, '$."meta.metric_kind"')) VIRTUAL,
    metric_unit        TEXT    GENERATED ALWAYS AS (json_extract(attributes, '$."meta.metric_unit"')) VIRTUAL,
    metric_temporality TEXT    GENERATED ALWAYS AS (json_extract(attributes, '$."meta.metric_temporality"')) VIRTUAL,
    metric_monotonic   INTEGER GENERATED ALWAYS AS (json_extract(attributes, '$."meta.metric_monotonic"')) VIRTUAL,

    -- Convenience extractions (user attrs, not meta.*)
    http_method      TEXT    GENERATED ALWAYS AS (json_extract(attributes, '$."http.request.method"')) VIRTUAL,
    http_status_code INTEGER GENERATED ALWAYS AS (json_extract(attributes, '$."http.response.status_code"')) VIRTUAL,
    http_route       TEXT    GENERATED ALWAYS AS (json_extract(attributes, '$."http.route"')) VIRTUAL,
    rpc_service      TEXT    GENERATED ALWAYS AS (json_extract(attributes, '$."rpc.service"')) VIRTUAL,
    db_system        TEXT    GENERATED ALWAYS AS (json_extract(attributes, '$."db.system"')) VIRTUAL
) STRICT;

CREATE INDEX IF NOT EXISTS idx_events_time            ON events(time_ns DESC);
CREATE INDEX IF NOT EXISTS idx_events_signal_time     ON events(signal_type, time_ns DESC);
CREATE INDEX IF NOT EXISTS idx_events_svc_time        ON events(service_name, time_ns DESC);
CREATE INDEX IF NOT EXISTS idx_events_svc_signal_time ON events(service_name, signal_type, time_ns DESC);
CREATE INDEX IF NOT EXISTS idx_events_trace           ON events(trace_id, time_ns) WHERE trace_id IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_events_roots           ON events(service_name, time_ns DESC, trace_id)
    WHERE signal_type = 'span' AND parent_span_id IS NULL;
CREATE INDEX IF NOT EXISTS idx_events_span_duration   ON events(service_name, duration_ns DESC)
    WHERE signal_type = 'span';
CREATE INDEX IF NOT EXISTS idx_events_errors          ON events(service_name, time_ns DESC)
    WHERE status_code = 2 OR severity_number >= 17;
CREATE INDEX IF NOT EXISTS idx_events_http_route      ON events(service_name, http_route, time_ns DESC)
    WHERE http_route IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_events_http_status     ON events(service_name, http_status_code, time_ns DESC)
    WHERE http_status_code IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_events_name_time       ON events(name, time_ns DESC);
CREATE INDEX IF NOT EXISTS idx_events_metric_name     ON events(name, time_ns DESC)
    WHERE signal_type = 'metric';
CREATE INDEX IF NOT EXISTS idx_events_log_severity    ON events(severity_number, time_ns DESC)
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
-- Attribute key catalog (for the Fields panel + name autocomplete)
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
