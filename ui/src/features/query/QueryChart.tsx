import { useMemo } from "react";
import {
  Area,
  AreaChart,
  CartesianGrid,
  Line,
  LineChart,
  ResponsiveContainer,
  Tooltip,
  XAxis,
  YAxis,
} from "recharts";

// Recharts 3.x doesn't expose a clean public type for tooltip-content props,
// so model the shape we actually touch at runtime.
interface ChartTooltipPayloadItem {
  dataKey?: string | number;
  name?: string | number;
  value?: number | string;
  color?: string;
}
interface ChartTooltipContentProps {
  active?: boolean;
  label?: number | string;
  payload?: ChartTooltipPayloadItem[];
  bucketMs: number;
}
import type { QueryResult } from "../../lib/query";
import { serviceColor } from "../../lib/colors";

interface Props {
  result: QueryResult | undefined;
  loading?: boolean;
  error?: unknown;
  height?: number;
  /** Bucket size in ms. Drives the tooltip's per-second rate calculation. */
  bucketMs: number;
  /** Query window. Drives the X-axis domain and tick spacing. */
  fromMs: number;
  toMs: number;
  /**
   * Click handler for a data point. Fires with the bucket's start time in
   * ms — caller can narrow the window (zoom) and jump to Explore Data.
   */
  onBucketClick?: (tMs: number) => void;
}

/**
 * Time-series chart with Honeycomb-style X axis: regularly spaced labels at
 * clock-round minute/hour boundaries regardless of bucket resolution. Hover
 * reveals a tooltip with the exact bucket timestamp, bucket size, and a
 * per-second rate derived from bucketMs.
 */
export function QueryChart({
  result,
  loading,
  error,
  height = 220,
  bucketMs,
  fromMs,
  toMs,
  onBucketClick,
}: Props) {
  // Zero-fill any missing buckets so the chart doesn't interpolate a
  // smooth curve across quiet periods. Each "no data" bucket drops to 0
  // for every series, producing an obvious visual gap between bursts.
  const { data, series } = useMemo(
    () => buildSeries(result, { fromMs, toMs, bucketMs }),
    [result, fromMs, toMs, bucketMs],
  );

  const durationMs = Math.max(1, toMs - fromMs);
  const majorTicks = useMemo(
    () => computeMajorTicks(fromMs, toMs, durationMs),
    [fromMs, toMs, durationMs],
  );
  const tickStep = majorTickStep(durationMs);

  if (error) {
    return (
      <Centered height={height}>
        <span style={{ color: "var(--color-error)" }}>
          Query error: {(error as Error).message}
        </span>
      </Centered>
    );
  }
  if (loading && !result) return <Centered height={height}>Running query…</Centered>;
  if (!result || !result.has_bucket) {
    return (
      <Centered height={height}>
        <span style={{ color: "var(--color-ink-muted)" }}>No time-series results</span>
      </Centered>
    );
  }
  if (data.length === 0) {
    return <Centered height={height}>No data in selected range.</Centered>;
  }

  const tooltip = (
    <Tooltip
      cursor={{ stroke: "var(--color-ink-muted)", strokeDasharray: "2 2" }}
      content={<ChartTooltip bucketMs={bucketMs} />}
    />
  );
  const xAxis = (
    <XAxis
      dataKey="t"
      type="number"
      // Full query range, not data range — a quiet tail now renders as
      // visible empty space on the right instead of squeezing the last
      // bucket to the edge.
      domain={[fromMs, toMs]}
      allowDataOverflow
      ticks={majorTicks}
      tickFormatter={(t) => formatMajorTick(t as number, tickStep)}
      stroke="var(--color-ink-muted)"
      fontSize={11}
      minTickGap={8}
    />
  );

  // Recharts' state.activeLabel is the hovered x value (bucket start in ms)
  // when the user clicks anywhere on the plot area. This is the reliable
  // way to get a per-bucket click without wiring dot-level handlers (which
  // don't fire when dots are hidden, as they are on our dense charts).
  const chartClick = onBucketClick
    ? (state: { activeLabel?: number | string } | undefined) => {
        const label = state?.activeLabel;
        const t = typeof label === "number" ? label : Number(label);
        if (Number.isFinite(t)) onBucketClick(t);
      }
    : undefined;
  const clickable = onBucketClick != null;

  if (series.length <= 1) {
    return (
      <div
        style={{ height, width: "100%", cursor: clickable ? "pointer" : undefined }}
      >
        <ResponsiveContainer>
          <AreaChart
            data={data}
            margin={{ top: 10, right: 16, left: 0, bottom: 0 }}
            onClick={chartClick}
          >
            <CartesianGrid stroke="var(--color-border)" vertical={false} />
            {xAxis}
            <YAxis stroke="var(--color-ink-muted)" fontSize={11} width={40} />
            {tooltip}
            <Area
              type="monotone"
              dataKey={series[0]?.key ?? "value"}
              name={series[0]?.label ?? "count"}
              stroke="var(--color-accent)"
              fill="var(--color-accent)"
              fillOpacity={0.15}
              isAnimationActive={false}
              activeDot={clickable ? { r: 5, style: { cursor: "pointer" } } : undefined}
            />
          </AreaChart>
        </ResponsiveContainer>
      </div>
    );
  }

  return (
    <div
      style={{ height, width: "100%", cursor: clickable ? "pointer" : undefined }}
    >
      <ResponsiveContainer>
        <LineChart
          data={data}
          margin={{ top: 10, right: 16, left: 0, bottom: 0 }}
          onClick={chartClick}
        >
          <CartesianGrid stroke="var(--color-border)" vertical={false} />
          {xAxis}
          <YAxis stroke="var(--color-ink-muted)" fontSize={11} width={40} />
          {tooltip}
          {series.map((s) => (
            <Line
              key={s.key}
              type="monotone"
              dataKey={s.key}
              stroke={colorForSeries(s.label)}
              dot={false}
              strokeWidth={1.5}
              name={s.label}
              isAnimationActive={false}
              activeDot={clickable ? { r: 5, style: { cursor: "pointer" } } : undefined}
            />
          ))}
        </LineChart>
      </ResponsiveContainer>
    </div>
  );
}

// ---------------------------------------------------------------------------
// Tooltip
// ---------------------------------------------------------------------------

function ChartTooltip({ active, payload, label, bucketMs }: ChartTooltipContentProps) {
  if (!active || !payload || payload.length === 0) return null;

  const t = typeof label === "number" ? label : Number(label);
  const d = new Date(t);
  const bucketSec = Math.max(bucketMs / 1000, 1 / 1000);

  return (
    <div
      className="rounded-md border shadow-sm text-xs"
      style={{
        background: "var(--color-surface)",
        borderColor: "var(--color-border)",
        padding: 8,
        minWidth: 220,
      }}
    >
      <div style={{ fontWeight: 500 }}>
        {formatTooltipDate(d)}{" "}
        <span style={{ color: "var(--color-ink-muted)", fontWeight: 400 }}>
          ({humanBucket(bucketMs)})
        </span>
      </div>
      <table style={{ marginTop: 4, width: "100%", borderCollapse: "collapse" }}>
        <thead>
          <tr style={{ color: "var(--color-ink-muted)", fontSize: 10, textAlign: "left" }}>
            <th style={{ fontWeight: 500 }}>Series</th>
            <th style={{ fontWeight: 500, textAlign: "right" }}>Value</th>
            <th style={{ fontWeight: 500, textAlign: "right" }}>/sec</th>
          </tr>
        </thead>
        <tbody>
          {payload.map((p) => {
            const v = typeof p.value === "number" ? p.value : Number(p.value ?? 0);
            return (
              <tr key={String(p.dataKey)}>
                <td style={{ paddingRight: 8 }}>
                  <span style={{ display: "inline-flex", alignItems: "center", gap: 6 }}>
                    <span
                      style={{
                        display: "inline-block",
                        width: 8,
                        height: 8,
                        borderRadius: 2,
                        background: p.color,
                      }}
                    />
                    {p.name ?? String(p.dataKey)}
                  </span>
                </td>
                <td style={{ textAlign: "right", fontVariantNumeric: "tabular-nums" }}>
                  {v.toLocaleString()}
                </td>
                <td
                  style={{
                    textAlign: "right",
                    fontVariantNumeric: "tabular-nums",
                    color: "var(--color-ink-muted)",
                  }}
                >
                  {(v / bucketSec).toFixed(2)}
                </td>
              </tr>
            );
          })}
        </tbody>
      </table>
    </div>
  );
}

// ---------------------------------------------------------------------------
// Axis tick generation
// ---------------------------------------------------------------------------

/**
 * Major-tick step chosen from the span of the query range. Matches the
 * Honeycomb convention of round minute/15-min/hour/6-hour boundaries at
 * longer scales.
 */
function majorTickStep(totalRangeMs: number): number {
  const MINUTE = 60_000;
  const HOUR = 60 * MINUTE;
  if (totalRangeMs <= 15 * MINUTE) return MINUTE;
  if (totalRangeMs <= 60 * MINUTE) return 5 * MINUTE;
  if (totalRangeMs <= 6 * HOUR) return 15 * MINUTE;
  if (totalRangeMs <= 24 * HOUR) return HOUR;
  return 6 * HOUR;
}

/**
 * Compute major-tick positions aligned to clock boundaries over the full
 * query range. Takes explicit bounds (not data-derived) so ticks reflect
 * the selected window faithfully even when ingestion is quiet.
 */
function computeMajorTicks(fromMs: number, toMs: number, durationMs: number): number[] {
  if (!Number.isFinite(fromMs) || !Number.isFinite(toMs) || toMs <= fromMs) return [];
  const step = majorTickStep(durationMs);
  // Align to local clock boundaries so ticks land on pleasant timestamps
  // (22:00 not 22:03:41) by computing the local-offset rounded-up epoch.
  const offset = new Date().getTimezoneOffset() * 60_000;
  const first = Math.ceil((fromMs - offset) / step) * step + offset;

  const ticks: number[] = [];
  for (let t = first; t <= toMs; t += step) {
    ticks.push(t);
  }
  return ticks;
}

function formatMajorTick(t: number, step: number): string {
  const d = new Date(t);
  if (step >= 6 * 60 * 60_000) {
    // Multi-hour steps: show date + hour.
    return `${d.toLocaleDateString([], { month: "short", day: "numeric" })} ${d.getHours().toString().padStart(2, "0")}:00`;
  }
  return d.toLocaleTimeString([], { hour: "2-digit", minute: "2-digit" });
}

function formatTooltipDate(d: Date): string {
  const date = d.toLocaleDateString([], { month: "short", day: "numeric", year: "numeric" });
  const time = d.toLocaleTimeString([], { hour12: false });
  return `${date} ${time}`;
}

function humanBucket(ms: number): string {
  if (ms >= 3_600_000) return `${Math.round(ms / 3_600_000)}h bucket`;
  if (ms >= 60_000) return `${Math.round(ms / 60_000)}m bucket`;
  if (ms >= 1000) return `${Math.round(ms / 1000)}s bucket`;
  return `${ms}ms bucket`;
}

// ---------------------------------------------------------------------------
// Series pivot (unchanged from before except uses t as number for X type)
// ---------------------------------------------------------------------------

function buildSeries(
  result: QueryResult | undefined,
  { fromMs, toMs, bucketMs }: { fromMs: number; toMs: number; bucketMs: number },
): {
  data: { t: number; [k: string]: number }[];
  series: { key: string; label: string }[];
} {
  if (!result || !result.has_bucket || !result.rows) return { data: [], series: [] };

  const bucketIdx = 0;
  const groupIdxs: number[] = [];
  let aggIdx = -1;
  for (let i = 1; i < result.columns.length; i++) {
    if (i === result.columns.length - 1) {
      aggIdx = i;
    } else {
      groupIdxs.push(i);
    }
  }
  if (aggIdx === -1) return { data: [], series: [] };

  const seen = new Set<string>();
  const seriesOrder: { key: string; label: string }[] = [];
  const byBucket = new Map<number, { t: number; [k: string]: number }>();

  for (const row of result.rows) {
    const bucketNS = Number(row[bucketIdx]);
    const t = Math.floor(bucketNS / 1_000_000);
    const label = groupIdxs.length === 0 ? "count" : groupIdxs.map((g) => String(row[g] ?? "·")).join(" / ");
    const key = sanitizeKey(label);
    const value = Number(row[aggIdx] ?? 0);
    if (!seen.has(key)) {
      seen.add(key);
      seriesOrder.push({ key, label });
    }
    const bucket = byBucket.get(t) ?? { t };
    bucket[key] = value;
    byBucket.set(t, bucket);
  }

  // Zero-fill missing buckets across the query's full range. Matches the
  // backend's alignment — buckets are emitted at `(ns / bucketNs) *
  // bucketNs`, so the corresponding ms boundary is `floor(t / bucketMs) *
  // bucketMs`. For every expected bucket position, if we have a row we
  // keep it; otherwise we inject a zero for each discovered series.
  if (bucketMs > 0 && toMs > fromMs) {
    const filled: typeof byBucket = new Map();
    const startMs = Math.floor(fromMs / bucketMs) * bucketMs;
    for (let t = startMs; t <= toMs; t += bucketMs) {
      const existing = byBucket.get(t);
      if (existing) {
        filled.set(t, existing);
      } else {
        const row: { t: number; [k: string]: number } = { t };
        for (const s of seriesOrder) row[s.key] = 0;
        filled.set(t, row);
      }
    }
    // Any buckets the server returned with timestamps outside [start, to)
    // (shouldn't happen, but defensive) are preserved verbatim so the user
    // doesn't silently lose a peak to a timing boundary issue.
    for (const [t, row] of byBucket) {
      if (!filled.has(t)) filled.set(t, row);
    }
    const data = Array.from(filled.values()).sort((a, b) => a.t - b.t);
    return { data, series: seriesOrder };
  }

  const data = Array.from(byBucket.values()).sort((a, b) => a.t - b.t);
  return { data, series: seriesOrder };
}

function sanitizeKey(s: string): string {
  return s.replace(/[^a-zA-Z0-9_]/g, "_") || "series";
}

function colorForSeries(label: string): string {
  return serviceColor(label);
}

function Centered({ children, height }: { children: React.ReactNode; height: number }) {
  return (
    <div
      className="flex items-center justify-center text-sm"
      style={{ height, color: "var(--color-ink-muted)" }}
    >
      {children}
    </div>
  );
}
