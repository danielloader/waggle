# OpenTelemetry metrics

Status: **In progress** — v1 (Sum + Gauge) in flight; the schema is
extensible to histograms so stages 4–5 can land without a migration
break.

## 1. Context

waggle ingests traces and logs today. Metrics complete the triplet but
have a materially different data model — not events to filter but
timeseries to aggregate — so they need their own storage shape, query
primitives, and UI surface. This file is the design-of-record so future
stages can pick up without re-deriving semantics.

## 2. Data model

Instrument kinds from the OTLP spec:

| Kind | Scalar / distribution | v1? |
| --- | --- | --- |
| Sum (counter) | scalar — single `value` per point, monotonic or not | ✓ |
| Gauge | scalar — sampled current value | ✓ |
| Histogram | distribution — `count`, `sum`, `min`, `max`, `bucket_bounds[]`, `bucket_counts[]` | stage 4 |
| ExpHistogram | distribution — exponential-scale bucket arrays | stage 5 |
| Summary | quantile[] snapshot | stage 5 |

Every point has:

- A `series` key: (resource, scope, metric name, kind, attributes).
  Stable across reports — many points per series.
- Attributes: dimensional tags that identify *this* timeseries.
- `time_ns` (end of window) + `start_time_ns` (begin of window for
  cumulative sums and all histograms).
- Temporality: `cumulative` (running total since start_time) or `delta`
  (this interval only).

## 3. Decisions (v1)

Answered upfront so stages don't wobble:

1. **Scope**: Sum + Gauge for v1. Histograms, ExpHistograms, and
   Summary are stages 4–5. Storage must not need migrating when they
   land.
2. **Rate math**: **at query time**, via `LAG(value)` window functions.
   Preserves the cumulative data faithfully; catches counter resets
   explicitly (negative diff → treat as 0, series started over).
3. **Cardinality**: no hard cap in v1. Benchmark under realistic load
   before deciding (SQLite + JSON-attr tuple → each series is cheap,
   but we don't yet know the degradation curve under millions). Add
   a limit if the benchmarks show a breakdown.
4. **Heatmap rendering** (for histograms in stage 4): pull in a small
   library (`@visx/heatmap` is the leading candidate). Not needed in
   v1.
5. **Exemplars**: skipped. Would let a histogram bucket link to the
   trace_id of a span that contributed to it; adds one column and one
   UI affordance. Revisit after stage 4 lands if there's demand.

## 4. Schema

```sql
-- One row per unique (resource, scope, name, attrs). New points
-- attach to the existing series_id; static facets don't duplicate.
CREATE TABLE metric_series (
  series_id     INTEGER PRIMARY KEY,
  resource_id   INTEGER NOT NULL REFERENCES resources(resource_id),
  scope_id      INTEGER NOT NULL REFERENCES scopes(scope_id),
  service_name  TEXT NOT NULL,
  name          TEXT NOT NULL,
  description   TEXT,
  unit          TEXT,
  kind          TEXT NOT NULL,         -- sum | gauge | histogram | exp_histogram | summary
  temporality   TEXT,                   -- cumulative | delta (sum + histogram)
  monotonic     INTEGER,                -- 0/1 (sum only; nullable otherwise)
  attributes    TEXT NOT NULL,          -- JSON object of attribute k/v
  first_seen_ns INTEGER NOT NULL,
  last_seen_ns  INTEGER NOT NULL,
  UNIQUE(resource_id, scope_id, name, attributes)
);

CREATE INDEX idx_metric_series_svc_name ON metric_series(service_name, name);
CREATE INDEX idx_metric_series_name ON metric_series(name);

-- Scalar points: Sum + Gauge both live here. Series.kind picks them
-- apart. This is the only table stage 1 writes / reads.
CREATE TABLE metric_points (
  series_id     INTEGER NOT NULL REFERENCES metric_series(series_id),
  time_ns       INTEGER NOT NULL,
  start_time_ns INTEGER,                -- required for cumulative sum
  value         REAL NOT NULL,
  PRIMARY KEY (series_id, time_ns)
);

CREATE INDEX idx_metric_points_time ON metric_points(time_ns);
```

Histogram + ExpHistogram points live in their own tables landing in
stages 4–5. They reference the same `metric_series` row, so the series
catalog is universal and the query builder's grouping/filtering path is
reusable:

```sql
-- Stage 4 (histograms): NOT CREATED YET, documenting the shape.
CREATE TABLE metric_histogram_points (
  series_id     INTEGER NOT NULL REFERENCES metric_series(series_id),
  time_ns       INTEGER NOT NULL,
  start_time_ns INTEGER,
  count         INTEGER NOT NULL,
  sum           REAL,
  min_val       REAL,
  max_val       REAL,
  bucket_bounds TEXT NOT NULL,          -- JSON array of explicit boundaries
  bucket_counts TEXT NOT NULL,          -- JSON, len(bucket_counts) = len(bucket_bounds)+1
  PRIMARY KEY (series_id, time_ns)
);
```

Attribute catalog gets a new `signal_type = 'metric'` flavour on
`attribute_keys` and `attribute_values` so `/api/fields?dataset=metric`
works by the same code path as spans and logs.

## 5. Query math

### Rate / increase / delta (scalar)

For cumulative counters (default for Sum), raw values aren't useful —
users want per-second rates. Query-time, per bucket:

```sql
-- Rate over a bucket, computed from two points spanning the bucket
-- boundary. NULLIF guards against two reports at the same ns; GREATEST
-- masks counter resets (value went down → series restarted).
WITH lagged AS (
  SELECT
    series_id,
    time_ns / ? AS bucket,                       -- bucket_ns
    value,
    LAG(value) OVER (PARTITION BY series_id ORDER BY time_ns) AS prev_value,
    time_ns - LAG(time_ns) OVER (...) AS elapsed_ns
  FROM metric_points
  WHERE series_id IN (...) AND time_ns BETWEEN ? AND ?
)
SELECT
  bucket,
  SUM(GREATEST(value - prev_value, 0) * 1e9 / NULLIF(elapsed_ns, 0))
    AS rate_per_sec
FROM lagged
GROUP BY bucket;
```

Gauges skip the LAG — we just roll them up with the user-chosen
aggregation (AVG / MAX / MIN / LAST per bucket).

### Histogram percentile (stage 4)

UDF `histogram_quantile(bounds_json, counts_json, q REAL) → REAL` does
linear interpolation between the two bucket boundaries that bracket
the `q`-th percentile. Cheap — the same kind of UDF the SQLite store
already registers for the existing `percentile` aggregate.

## 6. API

New dataset value flowing through the existing query-builder shape:

- `POST /api/query` with `dataset: "metrics"` — the query DSL gains a
  small set of metric-specific ops, otherwise identical to spans/logs:
  - `rate`, `increase`, `delta` (scalar, cumulative temporality)
  - `sum_over_time`, `avg_over_time`, `max_over_time`, `min_over_time`,
    `last_over_time` (scalar, any temporality — roll-up semantics)
  - `histogram_quantile(q)` (histograms, stage 4)
- `GET /api/metrics` — distinct `(name, kind, unit, description)` across
  all series. Powers the UI's metric-name picker.
- `GET /api/metrics/{name}/series?service=…` — list series
  (attribute-set + series_id) for one metric. Powers the "one line per
  series" view in the UI.

Ingest: `POST /v1/metrics` mounted in `ingest.Handler.Mount()`. Same
protobuf + JSON treatment as the trace/log endpoints.

## 7. UI

Metrics don't fit the traces-and-logs explorer shape — they're lines,
not rows. Separate nav entry + separate page.

- Sidebar gets a **Metrics** item (third entry after Traces / Logs).
- `/metrics` — name picker (typical user knows what metric they want),
  defaulting to nothing; once a metric is picked, the rest of the
  page shows:
  - Define panel — same shape as trace/logs, with metric-specific
    aggregation ops in the Select cell.
  - Chart — `QueryChart` for scalars (one line per series when a
    `group_by` is set, otherwise one line per unique attribute set
    capped at ~20). Stage 4 replaces this with a heatmap for
    histograms.
  - Overview tab — `topk` series by the selected aggregation.
  - Series tab — one row per series with its attribute set, matching
    the Traces tab pattern on the spans page.

## 8. Staging (execution order)

Each stage is a separate PR. CI gates (`test` job) apply from #9's
branch protection. Before a stage lands, its own e2e tests must pass
end-to-end through the OTel SDK, matching the pattern established in
`internal/ingest/e2e_*_test.go`.

1. **Schema + this plan file.** `internal/store/sqlite/schema.sql`
   adds `metric_series` + `metric_points`. `attribute_keys` +
   `attribute_values` don't need new columns — `signal_type = 'metric'`
   slots into the existing `TEXT` column.
2. **Ingest.** OTLP metrics protobuf decode
   (`go.opentelemetry.io/proto/otlp/collector/metrics/v1`) → an
   internal `store.MetricSeries` + `store.MetricPoint` types → writer
   batch → SQLite. New models in `internal/store/models.go`, transform
   in `internal/otlp/transform.go`, handler in
   `internal/ingest/otlp_http.go`, writer in
   `internal/ingest/writer.go`.
3. **API + query engine.** `/api/metrics` and `/api/metrics/{name}/series`
   handlers. `dataset: "metrics"` in `internal/query/builder.go` with
   the new aggregation ops, and the `LAG`-based rate plumbing in
   `internal/store/sqlite/queries.go`.
4. **UI.** `/metrics` route, name picker, chart. Reuses `DefinePanel`.
5. **Histograms.** New table, ingest path, `histogram_quantile` UDF,
   heatmap view with `@visx/heatmap`.
6. **ExpHistograms + Summary.** Last — rare in the wild.

Stages 1–4 together deliver the "functional scalar metrics"
milestone users expect. 5 + 6 add coverage for the long tail.

## 9. Out of scope for now

- **Exemplars** (trace_id/span_id on histogram buckets). Revisit after
  stage 4 if there's demand.
- **Recording rules / alerting.** Separate feature, not in the model
  today.
- **Cross-signal correlation UI.** Beyond a basic `service.name` join
  it's a larger design; metrics view stays standalone in v1.
- **Cardinality limits.** Benchmark first. Add only if necessary.
