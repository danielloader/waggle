-- otel-workbench SQLite schema v1
-- See plan.md for design rationale.

CREATE TABLE IF NOT EXISTS schema_meta (
    key   TEXT PRIMARY KEY,
    value TEXT NOT NULL
) STRICT;

INSERT OR IGNORE INTO schema_meta(key, value) VALUES ('version', '1');

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
-- Spans
-- =========================================================================
CREATE TABLE IF NOT EXISTS spans (
    trace_id             BLOB NOT NULL,
    span_id              BLOB NOT NULL,
    parent_span_id       BLOB,
    resource_id          INTEGER NOT NULL REFERENCES resources(resource_id),
    scope_id             INTEGER NOT NULL REFERENCES scopes(scope_id),
    service_name         TEXT NOT NULL,
    name                 TEXT NOT NULL,
    kind                 INTEGER NOT NULL,
    start_time_ns        INTEGER NOT NULL,
    end_time_ns          INTEGER NOT NULL,
    duration_ns          INTEGER NOT NULL GENERATED ALWAYS AS (end_time_ns - start_time_ns) STORED,
    status_code          INTEGER NOT NULL DEFAULT 0,
    status_message       TEXT,
    trace_state          TEXT,
    flags                INTEGER NOT NULL DEFAULT 0,
    dropped_attrs_count  INTEGER NOT NULL DEFAULT 0,
    dropped_events_count INTEGER NOT NULL DEFAULT 0,
    dropped_links_count  INTEGER NOT NULL DEFAULT 0,
    attributes           TEXT NOT NULL DEFAULT '{}',
    http_method          TEXT    GENERATED ALWAYS AS (json_extract(attributes, '$."http.request.method"')) VIRTUAL,
    http_status_code     INTEGER GENERATED ALWAYS AS (json_extract(attributes, '$."http.response.status_code"')) VIRTUAL,
    http_route           TEXT    GENERATED ALWAYS AS (json_extract(attributes, '$."http.route"')) VIRTUAL,
    rpc_service          TEXT    GENERATED ALWAYS AS (json_extract(attributes, '$."rpc.service"')) VIRTUAL,
    db_system            TEXT    GENERATED ALWAYS AS (json_extract(attributes, '$."db.system"')) VIRTUAL,
    PRIMARY KEY (trace_id, span_id)
) STRICT, WITHOUT ROWID;

CREATE INDEX IF NOT EXISTS idx_spans_service_time     ON spans(service_name, start_time_ns DESC);
CREATE INDEX IF NOT EXISTS idx_spans_time             ON spans(start_time_ns DESC);
CREATE INDEX IF NOT EXISTS idx_spans_roots            ON spans(service_name, start_time_ns DESC) WHERE parent_span_id IS NULL;
CREATE INDEX IF NOT EXISTS idx_spans_service_duration ON spans(service_name, duration_ns DESC);
CREATE INDEX IF NOT EXISTS idx_spans_errors           ON spans(service_name, start_time_ns DESC) WHERE status_code = 2;
CREATE INDEX IF NOT EXISTS idx_spans_http_route       ON spans(service_name, http_route, start_time_ns DESC) WHERE http_route IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_spans_http_status      ON spans(service_name, http_status_code, start_time_ns DESC) WHERE http_status_code IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_spans_name_time        ON spans(name, start_time_ns DESC);

-- =========================================================================
-- Span events
-- =========================================================================
CREATE TABLE IF NOT EXISTS span_events (
    trace_id            BLOB NOT NULL,
    span_id             BLOB NOT NULL,
    seq                 INTEGER NOT NULL,
    time_ns             INTEGER NOT NULL,
    name                TEXT NOT NULL,
    attributes          TEXT NOT NULL DEFAULT '{}',
    dropped_attrs_count INTEGER NOT NULL DEFAULT 0,
    PRIMARY KEY (trace_id, span_id, seq),
    FOREIGN KEY (trace_id, span_id) REFERENCES spans(trace_id, span_id) ON DELETE CASCADE
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
    PRIMARY KEY (trace_id, span_id, seq),
    FOREIGN KEY (trace_id, span_id) REFERENCES spans(trace_id, span_id) ON DELETE CASCADE
) STRICT, WITHOUT ROWID;

CREATE INDEX IF NOT EXISTS idx_span_links_target ON span_links(linked_trace_id, linked_span_id);

-- =========================================================================
-- Logs
-- =========================================================================
CREATE TABLE IF NOT EXISTS logs (
    log_id              INTEGER PRIMARY KEY,
    resource_id         INTEGER NOT NULL REFERENCES resources(resource_id),
    scope_id            INTEGER NOT NULL REFERENCES scopes(scope_id),
    service_name        TEXT NOT NULL,
    time_ns             INTEGER NOT NULL,
    observed_time_ns    INTEGER,
    severity_number     INTEGER NOT NULL DEFAULT 0,
    severity_text       TEXT,
    body                TEXT,
    body_json           TEXT,
    trace_id            BLOB,
    span_id             BLOB,
    flags               INTEGER NOT NULL DEFAULT 0,
    dropped_attrs_count INTEGER NOT NULL DEFAULT 0,
    attributes          TEXT NOT NULL DEFAULT '{}'
) STRICT;

CREATE INDEX IF NOT EXISTS idx_logs_service_time ON logs(service_name, time_ns DESC);
CREATE INDEX IF NOT EXISTS idx_logs_time         ON logs(time_ns DESC);
CREATE INDEX IF NOT EXISTS idx_logs_severity     ON logs(severity_number, time_ns DESC);
CREATE INDEX IF NOT EXISTS idx_logs_trace        ON logs(trace_id) WHERE trace_id IS NOT NULL;

-- =========================================================================
-- Logs FTS5 mirror
-- =========================================================================
CREATE VIRTUAL TABLE IF NOT EXISTS logs_fts USING fts5(
    body, severity_text, service_name UNINDEXED,
    content='logs', content_rowid='log_id', tokenize='porter unicode61'
);

CREATE TRIGGER IF NOT EXISTS logs_ai AFTER INSERT ON logs BEGIN
    INSERT INTO logs_fts(rowid, body, severity_text, service_name)
    VALUES (NEW.log_id, NEW.body, NEW.severity_text, NEW.service_name);
END;

CREATE TRIGGER IF NOT EXISTS logs_ad AFTER DELETE ON logs BEGIN
    INSERT INTO logs_fts(logs_fts, rowid, body, severity_text, service_name)
    VALUES ('delete', OLD.log_id, OLD.body, OLD.severity_text, OLD.service_name);
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
