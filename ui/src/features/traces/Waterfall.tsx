import { useEffect, useMemo, useRef, useState, type CSSProperties } from "react";
import { useVirtualizer } from "@tanstack/react-virtual";
import { AlertTriangle, Link as LinkIcon } from "lucide-react";
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
  /**
   * Called when the row's span-links chain-icon is clicked. Caller should
   * select the span and flip the right-hand panel to its Links tab.
   */
  onSelectLinks?: (spanID: string) => void;
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
  onSelectLinks,
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

  const [hoveredSpanId, setHoveredSpanId] = useState<string | null>(null);
  // Path of the hovered span — used to detect subtree membership below.
  const hoveredPath = hoveredSpanId
    ? (rows.find((r) => r.span.span_id === hoveredSpanId)?.path ?? null)
    : null;

  // Bounding box (in virtual-scroll coordinates) for the hovered span and all
  // its visible descendants. Rendered as a single overlay rect behind the rows.
  const subtreeBox = useMemo(() => {
    if (!hoveredPath) return null;
    let first = -1, last = -1;
    for (let i = 0; i < rows.length; i++) {
      const p = rows[i].path;
      if (p === hoveredPath || p.startsWith(hoveredPath + ".")) {
        if (first === -1) first = i;
        last = i;
      }
    }
    if (first === -1) return null;
    return { top: first * ROW_HEIGHT, height: (last - first + 1) * ROW_HEIGHT };
  }, [hoveredPath, rows]);

  // Measured timeline pixel width — drives the label-placement heuristic in
  // each Row. We need real pixels (not percentages) to decide whether a
  // duration label will fit inside its bar.
  const [timelineWidthPx, setTimelineWidthPx] = useState(0);
  useEffect(() => {
    const el = parentRef.current;
    if (!el) return;
    const update = () => {
      // Subtract NAME_COL + SERVICE_COL for the two fixed columns and the
      // 16px total horizontal padding on each row's timeline (px-2). What
      // remains is the content box where bar percentages are anchored.
      setTimelineWidthPx(
        Math.max(0, el.clientWidth - NAME_COL - SERVICE_COL - 16),
      );
    };
    update();
    const ro = new ResizeObserver(update);
    ro.observe(el);
    return () => ro.disconnect();
  }, []);

  const traceDurNS = Math.max(1, model.traceEndNS - model.traceStartNS);

  return (
    <div className="h-full flex flex-col">
      <TimelineRuler traceDurNS={traceDurNS} />
      <div
        ref={parentRef}
        className="flex-1 overflow-y-auto overflow-x-hidden"
        style={{ contain: "strict" }}
      >
        <div
          style={{
            height: rowVirtualizer.getTotalSize(),
            position: "relative",
            width: "100%",
          }}
        >
          {/* Subtree bounding box — rendered before rows so it sits behind them */}
          {subtreeBox && (
            <div
              className="absolute left-0 right-0 pointer-events-none"
              style={{
                top: subtreeBox.top,
                height: subtreeBox.height,
                background: "color-mix(in srgb, var(--color-accent) 7%, transparent)",
                boxShadow: "inset 0 0 0 1px color-mix(in srgb, var(--color-accent) 22%, transparent)",
              }}
            />
          )}
          {rowVirtualizer.getVirtualItems().map((vr) => {
            const row = rows[vr.index];
            return (
              <Row
                key={row.span.span_id}
                row={row}
                top={vr.start}
                traceStartNS={model.traceStartNS}
                traceDurNS={traceDurNS}
                timelineWidthPx={timelineWidthPx}
                isSelected={selectedSpanID === row.span.span_id}
                isCollapsed={collapsed.has(row.span.span_id)}
                isHighlighted={
                  highlightSpanIDs?.has(row.span.span_id) ?? false
                }
                isAltRow={vr.index % 2 === 1}
                onSelect={onSelect}
                onToggleCollapse={onToggleCollapse}
                onHover={setHoveredSpanId}
                onSelectEvent={onSelectEvent}
                onSelectLinks={onSelectLinks}
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
      <div className="flex-1 px-2 py-1">
        <div className="relative w-full h-full">
          {ticks.map((t, i) => (
            <span
              key={i}
              className="absolute"
              style={{
                left: `${(t / traceDurNS) * 100}%`,
                top: 0,
                transform: i === ticks.length - 1 ? "translateX(-100%)" : undefined,
              }}
            >
              {formatDuration(t)}
            </span>
          ))}
        </div>
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
  timelineWidthPx: number;
  isSelected: boolean;
  isCollapsed: boolean;
  isHighlighted: boolean;
  isAltRow: boolean;
  onSelect: (spanID: string) => void;
  onToggleCollapse: (spanID: string) => void;
  onHover: (spanID: string | null) => void;
  onSelectEvent?: (spanID: string, eventIdx: number) => void;
  onSelectLinks?: (spanID: string) => void;
}

function Row({
  row,
  top,
  traceStartNS,
  traceDurNS,
  timelineWidthPx,
  isSelected,
  isCollapsed,
  isHighlighted,
  isAltRow,
  onSelect,
  onToggleCollapse,
  onHover,
  onSelectEvent,
  onSelectLinks,
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

  // Honeycomb-style label placement. If the bar is wide enough, the label
  // sits inside at the leading edge (pill-style, background-tinted). If
  // not, it floats to the side with more room — right of the bar by
  // default, flipping to the left when the bar is butted against the
  // timeline's right edge (e.g. late-starting spans near trace end).
  const labelText = formatDuration(row.durationNS);
  // 11px tabular-nums is ≈6.5px per char; +8 for the padding/margin budget.
  const labelWidthPx = labelText.length * 6.5 + 8;
  const barWidthPx = (widthPct / 100) * timelineWidthPx;
  const leadingSpacePx = (leftPct / 100) * timelineWidthPx;
  const trailingSpacePx = Math.max(
    0,
    ((100 - leftPct - widthPct) / 100) * timelineWidthPx,
  );
  // Before the parent has been measured (first paint), keep the label
  // inside to avoid a visible jump when the width arrives.
  const labelSide: "inside" | "right" | "left" =
    timelineWidthPx === 0 || barWidthPx >= labelWidthPx + 4
      ? "inside"
      : trailingSpacePx >= labelWidthPx + 4
        ? "right"
        : leadingSpacePx >= labelWidthPx + 4
          ? "left"
          : "inside";
  const labelStyle: CSSProperties =
    labelSide === "inside"
      ? {
          left: `${leftPct}%`,
          marginLeft: 2,
          padding: "0 4px",
          color: "var(--color-ink)",
          background: labelBg,
          borderRadius: 2,
        }
      : labelSide === "right"
        ? {
            left: `${leftPct + widthPct}%`,
            marginLeft: 4,
            color: "var(--color-ink-muted)",
          }
        : {
            right: `${100 - leftPct}%`,
            marginRight: 4,
            color: "var(--color-ink-muted)",
          };

  return (
    <div
      className={clsx(
        "absolute left-0 right-0 flex items-center cursor-pointer select-none",
        "hover:bg-surface-muted",
      )}
      style={{
        top,
        height: ROW_HEIGHT,
        // Precedence: selection → search highlight → zebra stripe.
        background: isSelected
          ? "color-mix(in srgb, var(--color-accent) 14%, transparent)"
          : isHighlighted
            ? "rgba(246, 178, 107, 0.18)"
            : isAltRow
              ? "var(--color-surface-muted)"
              : undefined,
      }}
      onClick={() => onSelect(row.span.span_id)}
      onMouseEnter={() => onHover(row.span.span_id)}
      onMouseLeave={() => onHover(null)}
    >
      <div
        className="shrink-0 flex items-center gap-1 pr-2"
        style={{ width: NAME_COL, paddingLeft: row.depth * 16 + 8 }}
      >
        {row.childCount > 0 ? (
          <button
            type="button"
            className="text-[10px] tabular-nums leading-none px-1 py-0.5 rounded shrink-0 hover:bg-border/50"
            style={{
              minWidth: 18,
              textAlign: "center",
              background: isCollapsed
                ? "var(--color-accent)"
                : "var(--color-surface-muted)",
              color: isCollapsed
                ? "white"
                : "var(--color-ink-muted)",
              border: "1px solid var(--color-border)",
            }}
            onClick={(e) => {
              e.stopPropagation();
              onToggleCollapse(row.span.span_id);
            }}
            aria-expanded={!isCollapsed}
            aria-label={`${isCollapsed ? "Expand" : "Collapse"} ${row.childCount} child span${row.childCount === 1 ? "" : "s"}`}
          >
            {row.childCount}
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
      <div className="flex-1 h-full px-2">
        {/* Percentages on the absolute children below are anchored to this
            inner div's width. Keeping the `px-2` on the outer wrapper
            means percentages land inside the padded area instead of
            spilling to the timeline column's border edges. */}
        <div className="relative w-full h-full">
        {/* Skew whiskers — rendered as a thin wire just below the bar
            with a vertical cap at the extreme descendant bound, and a
            short drop from the bar's own edge down to the wire. Only the
            side(s) with actual overrun draw. Mirrors Honeycomb's box-plot
            convention for "span covered more time than it claimed". */}
        {row.extendedFromNS < row.span.start_ns && (
          <SkewWhisker
            color={color}
            side="left"
            wireFromPct={extFromPct}
            wireToPct={leftPct}
          />
        )}
        {row.extendedToNS > row.span.end_ns && (
          <SkewWhisker
            color={color}
            side="right"
            wireFromPct={leftPct + widthPct}
            wireToPct={extToPct}
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
        {/* Duration label. Position chosen per-row: inside when the bar is
            wide enough (pill-styled), otherwise floated to whichever side
            has more room so short spans don't end up with labels clipped
            past the timeline's right edge. */}
        <span
          className="absolute text-[11px] tabular-nums pointer-events-none whitespace-nowrap"
          style={{ top: 5, ...labelStyle }}
        >
          {labelText}
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
        {(row.span.links?.length ?? 0) > 0 && onSelectLinks && (
          <button
            type="button"
            onClick={(e) => {
              e.stopPropagation();
              onSelectLinks(row.span.span_id);
            }}
            title={`View ${row.span.links!.length} span link${row.span.links!.length === 1 ? "" : "s"}`}
            aria-label={`View ${row.span.links!.length} span link${row.span.links!.length === 1 ? "" : "s"}`}
            className="absolute p-0.5 rounded hover:bg-[var(--color-card-hover)]"
            style={{
              right: 2,
              top: 6,
              color: "var(--color-ink-muted)",
            }}
          >
            <LinkIcon className="w-3.5 h-3.5" />
          </button>
        )}
        </div>
      </div>
    </div>
  );
}

/**
 * Whisker indicating clock skew on one side of the bar: a thin horizontal
 * wire emerging from the bar's midline out to the extreme descendant
 * bound, terminated by a vertical cap. Drawn with the service colour at
 * reduced opacity so it reads as "related to the bar but not part of its
 * reported duration".
 */
function SkewWhisker({
  color,
  side,
  wireFromPct,
  wireToPct,
}: {
  color: string;
  side: "left" | "right";
  /** Left edge of the horizontal wire, as % of the timeline. */
  wireFromPct: number;
  /** Right edge of the horizontal wire, as % of the timeline. */
  wireToPct: number;
}) {
  // Bar is drawn at top:8 height:10 (midline y=13). Centre the wire and
  // cap on that midline so the whisker reads as continuing straight out
  // of the bar rather than branching below it.
  const wireTop = 13;
  const capTop = 10;
  const capHeight = 7;
  const capPct = side === "left" ? wireFromPct : wireToPct;
  const wireWidthPct = Math.max(wireToPct - wireFromPct, 0);
  return (
    <>
      <div
        className="absolute"
        style={{
          left: `${wireFromPct}%`,
          width: `${wireWidthPct}%`,
          top: wireTop,
          height: 1,
          background: color,
          opacity: 0.7,
        }}
      />
      <div
        className="absolute"
        style={{
          left: `${capPct}%`,
          top: capTop,
          height: capHeight,
          width: 1,
          background: color,
          opacity: 0.7,
          transform: "translateX(-50%)",
        }}
      />
    </>
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
