import { useMemo, useState } from "react";
import clsx from "clsx";
import { X } from "lucide-react";
import { serviceColor } from "../../lib/colors";
import { formatDuration } from "../../lib/format";
import { isSpanError } from "./tree";
import type { TraceModel, WaterfallRow } from "./tree";

interface Props {
  model: TraceModel;
  selectedSpanID: string | null;
  onSelect: (spanID: string) => void;
}

const LANE_HEIGHT = 10;
const LANE_GAP = 4;
/** Horizontal hairline between neighbouring bars in a lane. */
const SEGMENT_GAP_PX = 2;
/**
 * Honeycomb caps the summary at 6 depth levels — deeper spans still live
 * in the waterfall below, but the summary's job is a fast visual scan.
 * https://docs.honeycomb.io/reference/honeycomb-ui/query/trace-waterfall#trace-summary
 */
const MAX_LANES = 6;

/**
 * Honeycomb-style trace summary band. Spans are grouped by tree depth —
 * one lane per depth level, root at the top — and coloured by service.
 * Gives the reader a compressed picture of parallelism, depth and hot
 * zones before the waterfall's table repeats the same data with fields.
 */
export function TraceSummary({ model, selectedSpanID, onSelect }: Props) {
  const lanes = useMemo(() => groupByDepth(model.rows), [model.rows]);
  const traceDurNS = Math.max(1, model.traceEndNS - model.traceStartNS);
  const [highlightErrors, setHighlightErrors] = useState(false);
  const hasErrors = useMemo(
    () => model.rows.some((r) => isSpanError(r.span)),
    [model.rows],
  );

  return (
    <div
      className="px-4 py-3 border-b flex flex-col gap-3"
      style={{
        background: "var(--color-surface-muted)",
        borderColor: "var(--color-border)",
      }}
    >
      <div
        className="text-xs flex items-center gap-2"
        style={{ color: "var(--color-ink-muted)" }}
      >
        <span className="font-semibold" style={{ color: "var(--color-ink)" }}>
          Trace summary
        </span>
        <span>·</span>
        <span>
          {model.spanCount} span{model.spanCount === 1 ? "" : "s"} at{" "}
          {formatTraceStart(model.traceStartNS)} ({formatDuration(traceDurNS)})
        </span>
        {hasErrors && (
          <button
            type="button"
            onClick={() => setHighlightErrors((v) => !v)}
            className={clsx(
              "ml-auto inline-flex items-center gap-1 px-2 py-0.5 rounded-full text-[11px] border",
              highlightErrors
                ? "font-medium"
                : "hover:bg-[var(--color-card-hover)]",
            )}
            style={{
              borderColor: highlightErrors
                ? "var(--color-error)"
                : "var(--color-border)",
              background: highlightErrors
                ? "rgba(217, 48, 37, 0.08)"
                : undefined,
              color: highlightErrors ? "var(--color-error)" : undefined,
            }}
            title={
              highlightErrors
                ? "Showing only error spans — click to restore"
                : "Dim non-error spans to make errors stand out"
            }
          >
            Highlight errors
            {highlightErrors && <X className="w-3 h-3" />}
          </button>
        )}
      </div>
      <div className="flex flex-col" style={{ gap: LANE_GAP }}>
        {lanes.map((rows, i) => (
          <Lane
            key={i}
            rows={rows}
            traceStartNS={model.traceStartNS}
            traceDurNS={traceDurNS}
            selectedSpanID={selectedSpanID}
            highlightErrors={highlightErrors}
            onSelect={onSelect}
          />
        ))}
      </div>
    </div>
  );
}

function Lane({
  rows,
  traceStartNS,
  traceDurNS,
  selectedSpanID,
  highlightErrors,
  onSelect,
}: {
  rows: WaterfallRow[];
  traceStartNS: number;
  traceDurNS: number;
  selectedSpanID: string | null;
  highlightErrors: boolean;
  onSelect: (spanID: string) => void;
}) {
  return (
    <div className="relative" style={{ height: LANE_HEIGHT }}>
      {rows.map((row) => (
        <Segment
          key={row.span.span_id}
          row={row}
          traceStartNS={traceStartNS}
          traceDurNS={traceDurNS}
          isSelected={selectedSpanID === row.span.span_id}
          dim={highlightErrors && !isSpanError(row.span)}
          onSelect={onSelect}
        />
      ))}
    </div>
  );
}

function Segment({
  row,
  traceStartNS,
  traceDurNS,
  isSelected,
  dim,
  onSelect,
}: {
  row: WaterfallRow;
  traceStartNS: number;
  traceDurNS: number;
  isSelected: boolean;
  dim: boolean;
  onSelect: (spanID: string) => void;
}) {
  // Summary bars are always coloured by service — the detail waterfall
  // below re-renders them in red when they carry an error. Keeping the
  // summary purely service-coloured means the reader can read it as a
  // parallelism/shape view without overloading it with status.
  const color = serviceColor(row.span.service_name);
  const leftPct = ((row.span.start_ns - traceStartNS) / traceDurNS) * 100;
  const widthPct = Math.max((row.durationNS / traceDurNS) * 100, 0.15);
  const isError = isSpanError(row.span);

  return (
    <button
      type="button"
      onClick={() => onSelect(row.span.span_id)}
      title={`${row.span.name} · ${formatDuration(row.durationNS)}${isError ? " · error" : ""}`}
      className="absolute top-0 h-full rounded-[2px] p-0 border-0 hover:z-10"
      style={{
        left: `${leftPct}%`,
        width: `calc(${widthPct}% - ${SEGMENT_GAP_PX}px)`,
        background: color,
        opacity: dim ? 0.2 : isSelected ? 1 : 0.85,
      }}
    />
  );
}

/**
 * Bucket rows into lanes keyed by tree depth, capped at MAX_LANES to match
 * Honeycomb's "up to 6 levels" rule. Spans deeper than the cap still
 * appear in the detail waterfall below — the summary is a glance view.
 */
function groupByDepth(rows: WaterfallRow[]): WaterfallRow[][] {
  const lanes: WaterfallRow[][] = [];
  for (const row of rows) {
    if (row.depth >= MAX_LANES) continue;
    const lane = lanes[row.depth] ?? (lanes[row.depth] = []);
    lane.push(row);
  }
  return lanes.filter((l): l is WaterfallRow[] => Boolean(l));
}

/**
 * Short, locale-aware trace start timestamp — Honeycomb's trace summary
 * header shows something like "Apr 17 2026 21:45:49 UTC+01:00"; we match
 * the shape without going to the extra wire round-trip of Intl.DateTimeFormat
 * for the offset string.
 */
function formatTraceStart(ns: number): string {
  const d = new Date(ns / 1_000_000);
  const date = d.toLocaleDateString(undefined, {
    month: "short",
    day: "2-digit",
    year: "numeric",
  });
  const time = d.toLocaleTimeString(undefined, {
    hour: "2-digit",
    minute: "2-digit",
    second: "2-digit",
    hour12: false,
  });
  return `${date} ${time}`;
}
