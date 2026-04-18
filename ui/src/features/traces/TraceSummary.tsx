import { useMemo } from "react";
import clsx from "clsx";
import { serviceColor } from "../../lib/colors";
import { formatDuration } from "../../lib/format";
import { NAME_COL, DURATION_COL } from "./Waterfall";
import { isSpanError } from "./tree";
import type { TraceModel, WaterfallRow } from "./tree";

interface Props {
  model: TraceModel;
  selectedSpanID: string | null;
  onSelect: (spanID: string) => void;
}

const LANE_HEIGHT = 10;
const LANE_GAP = 2;

/**
 * Honeycomb-style trace summary band. Every span in the trace is drawn as
 * a coloured segment in a service-scoped lane, at its time-proportional
 * position. Gives the reader the shape of the trace at a glance —
 * parallelism, density, error clusters — before they scroll the detail
 * waterfall below.
 *
 * Lane layout: one lane per unique service.name, ordered by first
 * appearance in the trace (so root's service tops the stack). Segments
 * are clickable and sync with the detail waterfall's selection.
 */
export function TraceSummary({ model, selectedSpanID, onSelect }: Props) {
  const lanes = useMemo(() => groupByService(model.rows), [model.rows]);
  const traceDurNS = Math.max(1, model.traceEndNS - model.traceStartNS);

  return (
    <div
      className="px-0 py-2 border-b"
      style={{
        background: "var(--color-surface-muted)",
        borderColor: "var(--color-border)",
      }}
    >
      <div className="flex flex-col" style={{ gap: LANE_GAP }}>
        {lanes.map(([service, rows]) => (
          <Lane
            key={service}
            service={service}
            rows={rows}
            traceStartNS={model.traceStartNS}
            traceDurNS={traceDurNS}
            selectedSpanID={selectedSpanID}
            onSelect={onSelect}
          />
        ))}
      </div>
    </div>
  );
}

function Lane({
  service,
  rows,
  traceStartNS,
  traceDurNS,
  selectedSpanID,
  onSelect,
}: {
  service: string;
  rows: WaterfallRow[];
  traceStartNS: number;
  traceDurNS: number;
  selectedSpanID: string | null;
  onSelect: (spanID: string) => void;
}) {
  const color = serviceColor(service);
  return (
    <div className="flex items-center">
      <div
        className="shrink-0 flex items-center gap-1.5 text-[11px] truncate"
        style={{
          width: NAME_COL + DURATION_COL,
          paddingLeft: 12,
          paddingRight: 12,
          color: "var(--color-ink-muted)",
        }}
      >
        <span
          className="w-2 h-2 rounded-full shrink-0"
          style={{ background: color }}
        />
        <span className="truncate font-medium">{service}</span>
        <span
          className="ml-auto tabular-nums"
          style={{ color: "var(--color-ink-muted)", opacity: 0.8 }}
        >
          {rows.length}
        </span>
      </div>
      <div
        className="flex-1 relative"
        style={{ height: LANE_HEIGHT, marginRight: 16 }}
      >
        {rows.map((row) => (
          <Segment
            key={row.span.span_id}
            row={row}
            color={color}
            traceStartNS={traceStartNS}
            traceDurNS={traceDurNS}
            isSelected={selectedSpanID === row.span.span_id}
            onSelect={onSelect}
          />
        ))}
      </div>
    </div>
  );
}

function Segment({
  row,
  color,
  traceStartNS,
  traceDurNS,
  isSelected,
  onSelect,
}: {
  row: WaterfallRow;
  color: string;
  traceStartNS: number;
  traceDurNS: number;
  isSelected: boolean;
  onSelect: (spanID: string) => void;
}) {
  const leftPct = ((row.span.start_ns - traceStartNS) / traceDurNS) * 100;
  // Floor at 0.15% so a 1ns span in a 10s trace still has something
  // clickable. The floor is imperceptible at typical zooms.
  const widthPct = Math.max((row.durationNS / traceDurNS) * 100, 0.15);
  const isError = isSpanError(row.span);

  return (
    <button
      type="button"
      onClick={() => onSelect(row.span.span_id)}
      title={`${row.span.name} · ${formatDuration(row.durationNS)}${isError ? " · error" : ""}`}
      className={clsx(
        "absolute top-0 h-full rounded-[2px] p-0 border-0",
        "hover:z-10 hover:shadow-sm",
      )}
      style={{
        left: `${leftPct}%`,
        width: `${widthPct}%`,
        // Errors read as red across both summary and waterfall; selection
        // still wins with an accent outline on top.
        background: isError ? "var(--color-error)" : color,
        outline: isSelected ? `2px solid var(--color-accent)` : undefined,
        outlineOffset: isSelected ? 0 : -1,
      }}
    />
  );
}

function groupByService(rows: WaterfallRow[]): [string, WaterfallRow[]][] {
  const map = new Map<string, WaterfallRow[]>();
  for (const row of rows) {
    const svc = row.span.service_name || "unknown";
    const arr = map.get(svc);
    if (arr) {
      arr.push(row);
    } else {
      map.set(svc, [row]);
    }
  }
  return Array.from(map.entries());
}
