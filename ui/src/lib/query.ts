/**
 * Client-side types mirroring the Go query package wire shape.
 * Plus a small helper library for URL <-> Query conversion and for driving
 * the /api/query endpoint.
 */
import { z } from "zod";

// "events" is the mixed-signal view (no signal_type prefix filter). The
// other three pin queries to one signal. The UI defaults to "events" and
// lets the user narrow via a pill in the Define panel.
export const DATASETS = ["events", "spans", "logs", "metrics"] as const;
export type Dataset = (typeof DATASETS)[number];

export function datasetLabel(d: Dataset): string {
  switch (d) {
    case "events":
      return "events";
    case "spans":
      return "spans";
    case "logs":
      return "logs";
    case "metrics":
      return "metrics";
  }
}

export const AGG_OPS = [
  "count",
  "count_field",
  "count_distinct",
  "sum",
  "avg",
  "min",
  "max",
  "p001",
  "p01",
  "p05",
  "p10",
  "p25",
  "p50",
  "p75",
  "p90",
  "p95",
  "p99",
  "p999",
  "rate_sum",
  "rate_avg",
  "rate_max",
] as const;
export type AggOp = (typeof AGG_OPS)[number];

/**
 * Synthetic / meta fields that are always boolean. User attributes carrying
 * bool type are discovered via /api/fields. This set is for built-ins the
 * catalog doesn't know about.
 */
export const BOOLEAN_META_FIELDS = new Set<string>(["is_root", "error"]);

export const FILTER_OPS = [
  "=",
  "!=",
  ">",
  ">=",
  "<",
  "<=",
  "in",
  "!in",
  "exists",
  "!exists",
  "contains",
  "!contains",
  "starts-with",
  "!starts-with",
  "ends-with",
  "!ends-with",
] as const;
export type FilterOp = (typeof FILTER_OPS)[number];

export interface Aggregation {
  op: AggOp;
  field?: string;
  alias?: string;
}

export interface Filter {
  field: string;
  op: FilterOp;
  // "in" uses an array; other ops use a scalar. Booleans are valid scalars
  // (for flag-shaped attributes) but not array elements — the SQL layer
  // doesn't accept `IN (true, false, ...)` semantics.
  value?: string | number | boolean | (string | number)[];
}

export interface Order {
  field: string;
  dir?: "asc" | "desc";
}

/** The full wire shape sent to POST /api/query. */
export interface Query {
  dataset: Dataset;
  time_range: { from: string; to: string };
  select: Aggregation[];
  where?: Filter[];
  group_by?: string[];
  order_by?: Order[];
  having?: Filter[];
  limit?: number;
  bucket_ms?: number;
}

export interface QueryColumn {
  name: string;
  type: string;
}

export interface QueryResult {
  columns: QueryColumn[];
  rows: unknown[][];
  has_bucket: boolean;
  group_keys?: string[];
}

// ---------------------------------------------------------------------------
// Time ranges
// ---------------------------------------------------------------------------

/**
 * A URL-friendly relative range identifier. Resolved to absolute RFC3339 at
 * query time so the server never sees relative inputs.
 */
export const TIME_RANGES = ["15m", "1h", "6h", "24h", "7d"] as const;
export type TimeRangeKey = (typeof TIME_RANGES)[number];

// Auto-refresh cadences exposed in the time-range picker. "off" disables
// the interval entirely; the numeric ones drive the chart + events query
// refetchInterval.
export const REFRESH_RATES = ["off", "5s", "10s", "30s", "1m", "5m"] as const;
export type RefreshRate = (typeof REFRESH_RATES)[number];

export function refreshIntervalMs(r: RefreshRate): number | false {
  switch (r) {
    case "off":
      return false;
    case "5s":
      return 5_000;
    case "10s":
      return 10_000;
    case "30s":
      return 30_000;
    case "1m":
      return 60_000;
    case "5m":
      return 5 * 60_000;
  }
}

export function refreshRateLabel(r: RefreshRate): string {
  return r === "off" ? "Off" : r;
}

export function timeRangeLabel(r: TimeRangeKey): string {
  switch (r) {
    case "15m":
      return "Last 15 minutes";
    case "1h":
      return "Last 1 hour";
    case "6h":
      return "Last 6 hours";
    case "24h":
      return "Last 24 hours";
    case "7d":
      return "Last 7 days";
  }
}

export function resolveTimeRange(r: TimeRangeKey, now = new Date()): { from: string; to: string } {
  const to = now;
  const from = new Date(to.getTime() - rangeMs(r));
  return { from: from.toISOString(), to: to.toISOString() };
}

/**
 * Resolve the search state to a concrete [fromMs, toMs] window. Explicit
 * `from` / `to` win over the preset; otherwise the preset is anchored
 * against `now`. Every downstream consumer (chart axis, bucket sizing,
 * /api/query time_range) derives from this single function.
 */
export interface ResolvedRange {
  fromMs: number;
  toMs: number;
  durationMs: number;
  /** True when the user has zoomed into a custom window rather than a preset. */
  isCustom: boolean;
}

export function resolveSearchRange(
  search: Pick<QuerySearch, "range" | "from" | "to">,
  now = new Date(),
): ResolvedRange {
  if (search.from != null && search.to != null && search.to > search.from) {
    return {
      fromMs: search.from,
      toMs: search.to,
      durationMs: search.to - search.from,
      isCustom: true,
    };
  }
  const tr = resolveTimeRange(search.range, now);
  const fromMs = new Date(tr.from).getTime();
  const toMs = new Date(tr.to).getTime();
  return {
    fromMs,
    toMs,
    durationMs: toMs - fromMs,
    isCustom: false,
  };
}

export function rangeMs(r: TimeRangeKey): number {
  switch (r) {
    case "15m":
      return 15 * 60 * 1000;
    case "1h":
      return 60 * 60 * 1000;
    case "6h":
      return 6 * 60 * 60 * 1000;
    case "24h":
      return 24 * 60 * 60 * 1000;
    case "7d":
      return 7 * 24 * 60 * 60 * 1000;
  }
}

/**
 * Manual granularities a user can pick from the time-range popover. "auto"
 * is the default — the chart picks a sensible bucket size from the range.
 * Everything else is a fixed ms value that overrides auto-selection.
 */
export const GRANULARITIES = [
  "auto",
  "1s",
  "2s",
  "5s",
  "10s",
  "30s",
  "1m",
  "2m",
  "5m",
  "10m",
  "30m",
  "1h",
] as const;
export type Granularity = (typeof GRANULARITIES)[number];

export function granularityLabel(g: Granularity): string {
  switch (g) {
    case "auto":
      return "Auto";
    case "1s":
    case "2s":
    case "5s":
    case "10s":
    case "30s":
      return g.replace("s", " second" + (g === "1s" ? "" : "s"));
    case "1m":
    case "2m":
    case "5m":
    case "10m":
    case "30m":
      return g.replace("m", " minute" + (g === "1m" ? "" : "s"));
    case "1h":
      return "1 hour";
  }
}

export function granularityMs(g: Granularity): number | null {
  switch (g) {
    case "auto":
      return null;
    case "1s":
      return 1_000;
    case "2s":
      return 2_000;
    case "5s":
      return 5_000;
    case "10s":
      return 10_000;
    case "30s":
      return 30_000;
    case "1m":
      return 60_000;
    case "2m":
      return 120_000;
    case "5m":
      return 300_000;
    case "10m":
      return 600_000;
    case "30m":
      return 1_800_000;
    case "1h":
      return 3_600_000;
  }
}

/**
 * Minimum allowed bucket size for a given window duration. Targets ≤ 3600
 * buckets per series so Recharts stays responsive — Honeycomb applies the
 * same kind of floor for the same reason.
 */
export function minBucketMsForDuration(durationMs: number): number {
  const MINUTE = 60_000;
  const HOUR = 60 * MINUTE;
  const DAY = 24 * HOUR;
  if (durationMs <= 15 * MINUTE) return 1_000; // 1s × 900 buckets
  if (durationMs <= HOUR) return 1_000; // 1s × 3600 buckets
  if (durationMs <= 6 * HOUR) return 10_000; // 10s × 2160 buckets
  if (durationMs <= DAY) return 60_000; // 1m × 1440 buckets
  if (durationMs <= 7 * DAY) return 300_000; // 5m × 2016 buckets
  return Math.max(300_000, Math.ceil(durationMs / 3600));
}

/**
 * The subset of GRANULARITIES valid for the given window duration. "auto"
 * is always included; everything else must be at least
 * `minBucketMsForDuration`. Drives the TimeRangePicker dropdown.
 */
export function allowedGranularities(durationMs: number): Granularity[] {
  const floor = minBucketMsForDuration(durationMs);
  return GRANULARITIES.filter((g) => {
    const ms = granularityMs(g);
    return ms === null || ms >= floor;
  });
}

/**
 * If the given granularity is too fine for the given duration, snap it to
 * the smallest valid granularity instead. "auto" is always valid.
 */
export function clampGranularity(durationMs: number, g: Granularity): Granularity {
  const ms = granularityMs(g);
  if (ms === null) return g;
  const floor = minBucketMsForDuration(durationMs);
  if (ms >= floor) return g;
  const allowed = allowedGranularities(durationMs);
  for (const a of allowed) {
    if (a !== "auto") return a;
  }
  return "auto";
}

/**
 * Choose a bucket size for the window. Honors the user's explicit choice
 * when ≥ the floor; otherwise clamps to the floor so a deep-linked URL
 * can't produce an unbounded point set.
 */
export function bucketMsFor(durationMs: number, g: Granularity = "auto"): number {
  const floor = minBucketMsForDuration(durationMs);
  const explicit = granularityMs(g);
  if (explicit !== null) return Math.max(explicit, floor);
  return Math.max(floor, Math.floor(durationMs / 60));
}

// ---------------------------------------------------------------------------
// URL schema (consumed by TanStack Router validateSearch)
// ---------------------------------------------------------------------------

const filterSchema = z.object({
  field: z.string(),
  op: z.enum(FILTER_OPS),
  value: z.union([z.string(), z.number(), z.boolean(), z.array(z.union([z.string(), z.number()]))]).optional(),
});

const aggregationSchema = z.object({
  op: z.enum(AGG_OPS),
  field: z.string().optional(),
  alias: z.string().optional(),
});

const orderSchema = z.object({
  field: z.string(),
  dir: z.enum(["asc", "desc"]).optional(),
});

export const querySearchSchema = z.object({
  dataset: z.enum(DATASETS).default("events"),
  range: z.enum(TIME_RANGES).default("1h"),
  // Absolute start/end in milliseconds. When both are set, they override
  // `range` — this is what click-to-zoom and the custom picker write. The
  // preset remains in the URL so "back to Last 1h" is one click away.
  from: z.number().int().positive().optional(),
  to: z.number().int().positive().optional(),
  granularity: z.enum(GRANULARITIES).default("auto"),
  // Default is a COUNT of events. An empty `select` array is normalized to
  // [{op:"count"}] by callers — we keep the schema permissive so a shared
  // URL that elides `select` still renders the default view.
  select: z.array(aggregationSchema).default([]),
  where: z.array(filterSchema).default([]),
  group_by: z.array(z.string()).default([]),
  order_by: z.array(orderSchema).default([]),
  having: z.array(filterSchema).default([]),
  limit: z.number().int().min(1).max(10000).optional(),
  // Active tab under the chart. "overview" is the cheap default — it rolls
  // the aggregation up across the time range. "traces" shows top-N slowest
  // root spans (spans dataset only). "explore" loads raw events, which is
  // deliberately lazy because it's the heavier query.
  tab: z.enum(["overview", "traces", "explore"]).default("overview"),
  // Auto-refresh cadence for chart + events queries. Off by default so
  // loaded URLs don't re-hit the backend unexpectedly — users opt in via
  // the time-range picker when they want live behaviour. For a preset
  // range like "1h" this effectively slides the window on each refetch.
  refresh: z.enum(REFRESH_RATES).default("off"),
});

export type QuerySearch = z.infer<typeof querySearchSchema>;

/**
 * Normalize an empty SELECT to a sensible default. For every dataset we
 * fall back to COUNT — it's meaningful on all of them. Metric events are
 * folded one-row-per-(time, label-set), so COUNT over a window = "how
 * many label combinations were observed"; a counter-rate or gauge
 * aggregation requires the user to pick the specific metric name (a
 * field like requests.total) via the Select cell.
 */
export function selectOrDefault(
  sel: Aggregation[],
  _dataset?: Dataset,
): Aggregation[] {
  if (sel.length > 0) return sel;
  return [{ op: "count" }];
}

// ---------------------------------------------------------------------------
// HTTP client
// ---------------------------------------------------------------------------

export async function runQuery(q: Query, signal?: AbortSignal): Promise<QueryResult> {
  const res = await fetch("/api/query", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(q),
    signal,
  });
  if (!res.ok) {
    throw new Error(`${res.status} ${res.statusText}: ${await res.text()}`);
  }
  return (await res.json()) as QueryResult;
}

// ---------------------------------------------------------------------------
// Compose a Query from the URL search state + dataset
// ---------------------------------------------------------------------------

export function buildCountQuery(
  dataset: Dataset,
  search: QuerySearch,
  now = new Date(),
): Query {
  const resolved = resolveSearchRange(search, now);
  const bucket = bucketMsFor(resolved.durationMs, search.granularity);
  const expectedBuckets = Math.ceil(resolved.durationMs / bucket);
  const defaultLimit = Math.max(1000, Math.min(10_000, expectedBuckets * 8));
  return {
    dataset,
    time_range: {
      from: new Date(resolved.fromMs).toISOString(),
      to: new Date(resolved.toMs).toISOString(),
    },
    select: selectOrDefault(search.select, dataset),
    where: search.where,
    group_by: search.group_by,
    order_by: search.order_by,
    having: search.having,
    bucket_ms: bucket,
    limit: search.limit ?? defaultLimit,
  };
}

/**
 * Un-bucketed version of the chart query. Returns one row per GROUP BY
 * tuple (or a single row when no GROUP BY) — the shape the Overview tab
 * renders. Cheap in SQLite even on large datasets.
 *
 * Rate aggregations (rate_sum / rate_avg / rate_max) require bucket_ms
 * to be meaningful, so in the un-bucketed Overview view we fall back to
 * the underlying aggregation — rate_sum → sum, rate_avg → avg, etc.
 * That way a chart built around "RATE_SUM(bytes)" still produces a
 * sensible "SUM(bytes) over the window" number on the Overview tab.
 */
export function buildOverviewQuery(
  dataset: Dataset,
  search: QuerySearch,
  now = new Date(),
): Query {
  const resolved = resolveSearchRange(search, now);
  const rewritten = selectOrDefault(search.select, dataset).map(downgradeRateOp);
  return {
    dataset,
    time_range: {
      from: new Date(resolved.fromMs).toISOString(),
      to: new Date(resolved.toMs).toISOString(),
    },
    select: rewritten,
    where: search.where,
    group_by: search.group_by,
    order_by: search.order_by,
    having: search.having,
    limit: search.limit ?? 1000,
  };
}

function downgradeRateOp(a: Aggregation): Aggregation {
  switch (a.op) {
    case "rate_sum":
      return { ...a, op: "sum" };
    case "rate_avg":
      return { ...a, op: "avg" };
    case "rate_max":
      return { ...a, op: "max" };
    default:
      return a;
  }
}

/** Short human summary of the SELECT list for the Define panel. */
export function summarizeSelect(
  sel: Aggregation[],
  dataset?: Dataset,
): string {
  const items = selectOrDefault(sel, dataset);
  return items
    .map((a) => {
      if (a.op === "count") return "COUNT";
      const base = a.op.toUpperCase();
      return a.field ? `${base}(${a.field})` : base;
    })
    .join(", ");
}

/** Short human summary of a list of filters. */
export function summarizeFilters(fs: Filter[], emptyLabel: string): string {
  if (fs.length === 0) return emptyLabel;
  return fs
    .map((f) => {
      if (f.op === "exists" || f.op === "!exists") return `${f.field} ${f.op}`;
      const v = Array.isArray(f.value) ? `[${f.value.join(", ")}]` : String(f.value ?? "");
      return `${f.field} ${f.op} ${v}`;
    })
    .join(", ");
}

export function summarizeGroupBy(g: string[]): string {
  return g.length === 0 ? "None; don't segment" : g.join(", ");
}

export function summarizeOrderBy(o: Order[]): string {
  if (o.length === 0) return "None";
  return o.map((x) => `${x.field} ${x.dir ?? "desc"}`).join(", ");
}
