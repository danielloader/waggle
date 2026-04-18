import { useEffect, useRef } from "react";
import { useVirtualizer } from "@tanstack/react-virtual";
import { ChevronDown, ChevronRight, AlertTriangle } from "lucide-react";
import clsx from "clsx";
import { serviceColor } from "../../lib/colors";
import { formatDuration } from "../../lib/format";
import type { TraceModel, WaterfallRow } from "./tree";
import { isSpanError } from "./tree";

const ROW_HEIGHT = 28;
export const NAME_COL = 320;
export const SERVICE_COL = 160;

interface Props {
  model: TraceModel;
  rows: WaterfallRow[];
  selectedSpanID: string | null;
  collapsed: Set<string>;
  onSelect: (spanID: string) => void;
  onToggleCollapse: (spanID: string) => void;
  /** When set, span_ids matching get a subtle highlight (search results). */
  highlightSpanIDs?: Set<string>;
  /**
   * Target for imperative scroll. When this changes, the waterfall scrolls
   * the matching row into view (if visible — caller is responsible for
   * uncollapsing ancestors first).
   */
  scrollToSpanID?: string | null;
  /**
   * Called when a span-event tick is clicked. Caller is responsible for
   * making the span the active selection and surfacing the event detail
   * in the right-hand panel.
   */
  onSelectEvent?: (spanID: string, eventIdx: number) => void;
}

/**
 * Honeycomb-style trace waterfall. Virtualized — handles multi-thousand-span
 * traces without shipping every row into the DOM. Each row is:
 *
 *   [▸/▾]  ●  name   service.name   3.66ms ▬▬▬▬▬  ─ ─ ─
 *
 * The solid bar is the span's own [start_ns, end_ns] range. When any
 * descendant falls outside that range (apparent clock skew), a dashed
 * extension draws the bar out to the full covered span and a ⚠ icon nudges
 * the reader to notice. Duration label floats at the bar's leading edge
 * Honeycomb-style — no separate duration column.
 */
export function Waterfall({
  model,
  rows,
  selectedSpanID,
  collapsed,
  onSelect,
  onToggleCollapse,
  highlightSpanIDs,
  scrollToSpanID,
  onSelectEvent,
}: Props) {
  const parentRef = useRef<HTMLDivElement | null>(null);
  const rowVirtualizer = useVirtualizer({
    count: rows.length,
    getScrollElement: () => parentRef.current,
    estimateSize: () => ROW_HEIGHT,
    overscan: 12,
  });

  useEffect(() => {
    if (!scrollToSpanID) return;
    const idx = rows.findIndex((r) => r.span.span_id === scrollToSpanID);
    if (idx < 0) return;
    rowVirtualizer.scrollToIndex(idx, { align: "center" });
  }, [scrollToSpanID, rows, rowVirtualizer]);

  const traceDurNS = Math.max(1, model.traceEndNS - model.traceStartNS);

  return (
    <div className="h-full flex flex-col">
      <TimelineRuler traceDurNS={traceDurNS} />
      <div
        ref={parentRef}
        className="flex-1 overflow-auto"
        style={{ contain: "strict" }}
      >
        <div
          style={{
            height: rowVirtualizer.getTotalSize(),
            position: "relative",
            width: "100%",
          }}
        >
          {rowVirtualizer.getVirtualItems().map((vr) => {
            const row = rows[vr.index];
            return (
              <Row
                key={row.span.span_id}
                row={row}
                top={vr.start}
                traceStartNS={model.traceStartNS}
                traceDurNS={traceDurNS}
                isSelected={selectedSpanID === row.span.span_id}
                isCollapsed={collapsed.has(row.span.span_id)}
                isHighlighted={
                  highlightSpanIDs?.has(row.span.span_id) ?? false
                }
                isAltRow={vr.index % 2 === 1}
                onSelect={onSelect}
                onToggleCollapse={onToggleCollapse}
                onSelectEvent={onSelectEvent}
              />
            );
          })}
        </div>
      </div>
    </div>
  );
}

// ---------------------------------------------------------------------------

function TimelineRuler({ traceDurNS }: { traceDurNS: number }) {
  const ticks = computeTicks(traceDurNS);
  return (
    <div
      className="flex h-7 text-[10px] sticky top-0 z-10 border-b"
      style={{
        background: "var(--color-surface)",
        borderColor: "var(--color-border)",
        color: "var(--color-ink-muted)",
      }}
    >
      <div
        className="shrink-0 px-3 py-1 border-r"
        style={{ width: NAME_COL, borderColor: "var(--color-border)" }}
      >
        name
      </div>
      <div
        className="shrink-0 px-3 py-1 border-r"
        style={{ width: SERVICE_COL, borderColor: "var(--color-border)" }}
      >
        Service Name
      </div>
      <div className="flex-1 relative px-2 py-1">
        {ticks.map((t, i) => (
          <span
            key={i}
            className="absolute"
            style={{
              left: `${(t / traceDurNS) * 100}%`,
              top: 6,
              transform: i === ticks.length - 1 ? "translateX(-100%)" : undefined,
            }}
          >
            {formatDuration(t)}
          </span>
        ))}
      </div>
    </div>
  );
}

function computeTicks(traceDurNS: number): number[] {
  const steps = 5;
  const out: number[] = [];
  for (let i = 0; i <= steps; i++) {
    out.push(Math.round((traceDurNS * i) / steps));
  }
  return out;
}

// ---------------------------------------------------------------------------

interface RowProps {
  row: WaterfallRow;
  top: number;
  traceStartNS: number;
  traceDurNS: number;
  isSelected: boolean;
  isCollapsed: boolean;
  isHighlighted: boolean;
  isAltRow: boolean;
  onSelect: (spanID: string) => void;
  onToggleCollapse: (spanID: string) => void;
  onSelectEvent?: (spanID: string, eventIdx: number) => void;
}

function Row({
  row,
  top,
  traceStartNS,
  traceDurNS,
  isSelected,
  isCollapsed,
  isHighlighted,
  isAltRow,
  onSelect,
  onToggleCollapse,
  onSelectEvent,
}: RowProps) {
  const color = serviceColor(row.span.service_name);
  const isError = isSpanError(row.span);
  const leftPct = (row.offsetNS / traceDurNS) * 100;
  const widthPct = Math.max((row.durationNS / traceDurNS) * 100, 0.2);

  const extFromPct = ((row.extendedFromNS - traceStartNS) / traceDurNS) * 100;
  const extToPct = ((row.extendedToNS - traceStartNS) / traceDurNS) * 100;

  const barColor = isError ? "var(--color-error)" : color;
  // Label sits on a lighter shade of the bar's own colour — creates a
  // subtle contrast patch inside the bar so the numbers are readable
  // without a hard white backing that would look pasted-on.
  const labelBg = isError
    ? "color-mix(in srgb, var(--color-error) 45%, white)"
    : `color-mix(in srgb, ${color} 55%, white)`;

  return (
    <div
      className={clsx(
        "absolute left-0 right-0 flex items-center cursor-pointer select-none",
        "hover:bg-[var(--color-surface-muted)]",
      )}
      style={{
        top,
        height: ROW_HEIGHT,
        // Precedence: selection → search highlight → zebra stripe. The
        // stripe is a subtle tick so the eye can track a long row across
        // the full-width timeline without losing its place.
        background: isSelected
          ? "color-mix(in srgb, var(--color-accent) 14%, transparent)"
          : isHighlighted
            ? "rgba(246, 178, 107, 0.18)"
            : isAltRow
              ? "var(--color-surface-muted)"
              : undefined,
      }}
      onClick={() => onSelect(row.span.span_id)}
    >
      <div
        className="shrink-0 flex items-center gap-1 pr-2"
        style={{ width: NAME_COL, paddingLeft: row.depth * 16 + 8 }}
      >
        {row.childCount > 0 ? (
          <button
            type="button"
            className="p-0.5 rounded hover:bg-[var(--color-border)]/50"
            onClick={(e) => {
              e.stopPropagation();
              onToggleCollapse(row.span.span_id);
            }}
          >
            {isCollapsed ? (
              <ChevronRight className="w-3 h-3" />
            ) : (
              <ChevronDown className="w-3 h-3" />
            )}
          </button>
        ) : (
          <span className="w-4" />
        )}
        <span
          className="w-2 h-2 rounded-full shrink-0"
          style={{ background: color }}
          title={row.span.service_name}
        />
        <span
          className="text-xs truncate font-medium"
          style={isError ? { color: "var(--color-error)" } : undefined}
        >
          {row.span.name}
        </span>
        {row.hasSkew && (
          <AlertTriangle
            className="w-3 h-3 shrink-0"
            style={{ color: "var(--color-ink-muted)" }}
            aria-label="Descendant timing extends past this span — likely clock skew"
          >
            <title>Descendant extends past span (clock skew)</title>
          </AlertTriangle>
        )}
      </div>
      <div
        className="shrink-0 px-3 text-xs truncate"
        style={{ width: SERVICE_COL, color: "var(--color-ink-muted)" }}
        title={row.span.service_name}
      >
        {row.span.service_name}
      </div>
      <div className="flex-1 relative h-full">
        {/* Dashed skew extension — drawn under the solid bar. */}
        {row.hasSkew && (
          <div
            className="absolute border-t border-dashed"
            style={{
              left: `${extFromPct}%`,
              width: `${Math.max(extToPct - extFromPct, 0.2)}%`,
              top: "50%",
              borderColor: color,
              opacity: 0.5,
            }}
          />
        )}
        {/* Solid own-duration bar. Errors flip to red outright for
            glanceability — outline variants disappear into the other bars
            on a busy waterfall. */}
        <div
          className="absolute rounded-sm"
          style={{
            left: `${leftPct}%`,
            width: `${widthPct}%`,
            top: 8,
            height: 10,
            background: barColor,
          }}
        />
        {/* Duration label at the bar's leading edge. Backed by a lighter
            shade of the same bar colour so it reads as a native extension
            of the bar rather than a pasted-on pill. */}
        <span
          className="absolute text-[11px] tabular-nums pointer-events-none whitespace-nowrap"
          style={{
            left: `${leftPct}%`,
            top: 5,
            marginLeft: 2,
            padding: "0 4px",
            color: "var(--color-ink)",
            background: labelBg,
            borderRadius: 2,
          }}
        >
          {formatDuration(row.durationNS)}
        </span>
        {/* Span event ticks — positioned on the shared trace timeline, so
            they may fall outside the bar's pixel extent if the SDK
            recorded an event past the reported end_time. Clickable to
            drill into the event's detail view. */}
        {(row.span.events ?? []).map((ev, i) => {
          const evPct = ((ev.time_ns - traceStartNS) / traceDurNS) * 100;
          const isException = ev.name === "exception";
          return (
            <EventMarker
              key={i}
              leftPct={evPct}
              name={ev.name}
              offsetLabel={formatDuration(ev.time_ns - row.span.start_ns)}
              isException={isException}
              onClick={
                onSelectEvent
                  ? (e) => {
                      e.stopPropagation();
                      onSelectEvent(row.span.span_id, i);
                    }
                  : undefined
              }
            />
          );
        })}
      </div>
    </div>
  );
}

function EventMarker({
  leftPct,
  name,
  offsetLabel,
  isException,
  onClick,
}: {
  leftPct: number;
  name: string;
  offsetLabel: string;
  isException: boolean;
  onClick?: (e: React.MouseEvent) => void;
}) {
  const tooltip = `${name} @ +${offsetLabel}`;
  const color = isException ? "var(--color-error)" : "#1d1d1b";
  return (
    <button
      type="button"
      onClick={onClick}
      title={tooltip}
      aria-label={tooltip}
      className="absolute p-0 m-0 border-0 bg-transparent cursor-pointer"
      style={{
        left: `${leftPct}%`,
        top: 6,
        width: 16,
        height: 16,
        transform: "translateX(-50%)",
      }}
    >
      <span
        style={{
          display: "block",
          width: 8,
          height: 8,
          margin: "4px auto",
          background: color,
          borderRadius: "50%",
          boxShadow: "0 0 0 1.5px rgba(255,255,255,0.9)",
        }}
      />
    </button>
  );
}
