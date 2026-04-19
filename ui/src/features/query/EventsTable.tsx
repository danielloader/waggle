import { useState } from "react";
import { useQuery } from "@tanstack/react-query";
import { Link } from "@tanstack/react-router";
import clsx from "clsx";
import { ChevronDown, ChevronRight } from "lucide-react";
import type {
  Dataset,
  QueryResult,
  QuerySearch,
} from "../../lib/query";
import { refreshIntervalMs, resolveSearchRange, runQuery } from "../../lib/query";
import { formatDuration } from "../../lib/format";
import { CopyButton } from "../../components/ui/CopyButton";
import { AttributesPanel } from "../../components/ui/AttributesPanel";

interface Props {
  dataset: Dataset;
  search: QuerySearch;
  /** Reports the scroll container's vertical offset on every scroll event so
   *  the page can collapse the query/chart header once the user drills in. */
  onScrollY?: (y: number) => void;
}

/**
 * Events table. Sits below the chart and shows matching rows for the current
 * dataset + WHERE + time range. Rows render polymorphically based on each
 * row's `signal_type`: span rows show duration + status + trace link; log
 * rows show severity + body; metric rows show metric kind + value.
 *
 * When the current dataset is "events" (all signals) rows from different
 * signals mix naturally. When it's pinned to spans/logs/metrics the render
 * path is the same — every row simply carries the same signal_type.
 */
export function EventsTable({ dataset, search, onScrollY }: Props) {
  const result = useQuery({
    queryKey: [
      "events",
      dataset,
      search.where,
      search.range,
      search.from,
      search.to,
    ],
    queryFn: ({ signal }) => {
      const resolved = resolveSearchRange(search);
      return runQuery(
        {
          dataset,
          time_range: {
            from: new Date(resolved.fromMs).toISOString(),
            to: new Date(resolved.toMs).toISOString(),
          },
          select: [], // raw-rows mode
          where: search.where,
          limit: 500,
        },
        signal,
      );
    },
    refetchInterval: refreshIntervalMs(search.refresh),
  });

  if (result.isPending) return <Centered>Loading…</Centered>;
  if (result.isError)
    return <Centered>Error: {(result.error as Error).message}</Centered>;
  const rows = result.data?.rows ?? [];
  if (rows.length === 0) {
    return (
      <Centered>
        <div className="max-w-md text-center">
          <div className="font-medium mb-2">No matching events</div>
          <div className="text-sm" style={{ color: "var(--color-ink-muted)" }}>
            Try widening the time range or removing a filter.
          </div>
        </div>
      </Centered>
    );
  }

  return <UnifiedTable dataset={dataset} result={result.data!} onScrollY={onScrollY} />;
}

// ---------------------------------------------------------------------------
// Unified renderer
// ---------------------------------------------------------------------------

function UnifiedTable({
  dataset,
  result,
  onScrollY,
}: {
  dataset: Dataset;
  result: QueryResult;
  onScrollY?: (y: number) => void;
}) {
  const idx = columnIndex(result);
  const [expanded, setExpanded] = useState<Set<number>>(() => new Set());
  const toggle = (i: number) =>
    setExpanded((prev) => {
      const next = new Set(prev);
      next.has(i) ? next.delete(i) : next.add(i);
      return next;
    });

  return (
    <div
      className="h-full overflow-auto font-mono text-xs leading-5"
      onScroll={onScrollY ? (e) => onScrollY(e.currentTarget.scrollTop) : undefined}
    >
      <table className="w-full border-separate border-spacing-0">
        <thead
          className="sticky top-0 z-10 text-[10px] uppercase tracking-wide"
          style={{ background: "var(--color-surface)", color: "var(--color-ink-muted)" }}
        >
          <tr>
            <Th>{/* chevron column */}</Th>
            <Th>Time</Th>
            <Th>Signal</Th>
            <Th>Service</Th>
            <Th>Name</Th>
            <Th>Detail</Th>
            <Th>Trace</Th>
          </tr>
        </thead>
        <tbody>
          {result.rows.map((row, i) => {
            const r = normalizeRow(row, idx, dataset);
            const isOpen = expanded.has(i);
            return (
              <EventRow
                key={i}
                row={r}
                isOpen={isOpen}
                onToggle={() => toggle(i)}
              />
            );
          })}
        </tbody>
      </table>
    </div>
  );
}

// ---------------------------------------------------------------------------
// Row + helpers
// ---------------------------------------------------------------------------

interface NormalizedRow {
  signalType: string; // "span" | "log" | "metric"
  timeNS: number;
  service: string;
  name: string;
  // Signal-specific detail values
  durationNS?: number;
  statusCode?: number;
  severityNumber?: number;
  severityText?: string;
  body?: string;
  metricKind?: string;
  value?: number;
  // Trace/span linkage
  traceID?: string;
  spanID?: string;
  // Raw attributes JSON for the expand panel
  attributes: string;
  // For the filter-target in AttributesPanel
  filterTarget: "/events";
}

function normalizeRow(
  row: unknown[],
  idx: Record<string, number>,
  dataset: Dataset,
): NormalizedRow {
  // Resolve signal_type: present as a column on dataset=events, implied by
  // the preset otherwise.
  const signalFromRow =
    idx.signal_type !== undefined ? String(row[idx.signal_type] ?? "") : "";
  const signal = signalFromRow || impliedSignal(dataset);
  const num = (k: string): number | undefined => {
    if (idx[k] === undefined) return undefined;
    const v = row[idx[k]];
    if (v === null || v === undefined) return undefined;
    return typeof v === "number" ? v : Number(v);
  };
  const str = (k: string): string | undefined => {
    if (idx[k] === undefined) return undefined;
    const v = row[idx[k]];
    return v == null ? undefined : String(v);
  };
  return {
    signalType: signal,
    timeNS:
      num("time_ns") ??
      num("start_time_ns") ??
      0,
    service: str("service_name") ?? "",
    name: str("name") ?? "",
    durationNS: num("duration_ns"),
    statusCode: num("status_code"),
    severityNumber: num("severity_number"),
    severityText: str("severity_text"),
    body: str("body"),
    metricKind: str("metric_kind") ?? str("kind"),
    value: num("value"),
    traceID: (str("trace_id") ?? "").toLowerCase() || undefined,
    spanID: (str("span_id") ?? "").toLowerCase() || undefined,
    attributes: str("attributes") ?? "{}",
    filterTarget: "/events",
  };
}

function impliedSignal(dataset: Dataset): string {
  switch (dataset) {
    case "spans":
      return "span";
    case "logs":
      return "log";
    case "metrics":
      return "metric";
    default:
      return "";
  }
}

function EventRow({
  row,
  isOpen,
  onToggle,
}: {
  row: NormalizedRow;
  isOpen: boolean;
  onToggle: () => void;
}) {
  return (
    <>
      <tr
        className="align-top cursor-pointer hover:bg-[var(--color-card-hover)]"
        onClick={onToggle}
      >
        <Td className="w-6">
          {isOpen ? (
            <ChevronDown className="w-3.5 h-3.5" />
          ) : (
            <ChevronRight className="w-3.5 h-3.5" />
          )}
        </Td>
        <Td muted className="whitespace-nowrap">
          {formatWall(row.timeNS)}
        </Td>
        <Td>
          <SignalPill signal={row.signalType} />
        </Td>
        <Td>{row.service}</Td>
        <Td className="break-all">
          <NameCell row={row} />
        </Td>
        <Td className="whitespace-nowrap">
          <DetailCell row={row} />
        </Td>
        <Td onClick={(e) => e.stopPropagation()}>
          {row.traceID ? (
            <span className="inline-flex items-center gap-1">
              <Link
                to="/traces/$traceId"
                params={{ traceId: row.traceID }}
                className="underline"
                style={{ color: "var(--color-accent)" }}
              >
                {row.traceID.slice(0, 8)}
              </Link>
              <CopyButton value={row.traceID} label="Copy trace ID" />
            </span>
          ) : (
            <span style={{ color: "var(--color-ink-muted)" }}>·</span>
          )}
        </Td>
      </tr>
      {isOpen && (
        <tr>
          <td
            colSpan={7}
            className="border-b"
            style={{
              background: "var(--color-surface-muted)",
              borderColor: "var(--color-border)",
            }}
          >
            <AttributesPanel
              attributesJson={row.attributes}
              filterTarget={row.filterTarget}
            />
          </td>
        </tr>
      )}
    </>
  );
}

function NameCell({ row }: { row: NormalizedRow }) {
  if (row.signalType === "log") {
    return <span className="whitespace-pre-wrap">{row.body || row.name || "(no body)"}</span>;
  }
  return <span>{row.name}</span>;
}

function DetailCell({ row }: { row: NormalizedRow }) {
  if (row.signalType === "span") {
    const err = row.statusCode === 2;
    return (
      <span className="inline-flex items-center gap-2">
        {row.durationNS !== undefined && (
          <span className="tabular-nums">{formatDuration(row.durationNS)}</span>
        )}
        <span style={{ color: err ? "var(--color-error)" : "var(--color-ok)" }}>
          {err ? "error" : "ok"}
        </span>
      </span>
    );
  }
  if (row.signalType === "log") {
    const n = row.severityNumber ?? 0;
    return (
      <span style={{ color: severityColor(n) }}>
        {row.severityText || severityName(n)}
      </span>
    );
  }
  if (row.signalType === "metric") {
    const parts: string[] = [];
    if (row.metricKind) parts.push(row.metricKind);
    if (row.value !== undefined) parts.push(formatMetricValue(row.value));
    return (
      <span className="tabular-nums" style={{ color: "var(--color-ink-muted)" }}>
        {parts.join(" · ")}
      </span>
    );
  }
  return <span style={{ color: "var(--color-ink-muted)" }}>·</span>;
}

function SignalPill({ signal }: { signal: string }) {
  const color =
    signal === "span"
      ? "var(--color-accent)"
      : signal === "log"
        ? "var(--color-warn, var(--color-ink-muted))"
        : signal === "metric"
          ? "var(--color-ok)"
          : "var(--color-ink-muted)";
  return (
    <span
      className="inline-block text-[10px] px-1.5 py-0.5 rounded uppercase tracking-wide"
      style={{
        background: "var(--color-surface-muted)",
        color,
        border: "1px solid " + color,
      }}
    >
      {signal || "—"}
    </span>
  );
}

// ---------------------------------------------------------------------------
// helpers
// ---------------------------------------------------------------------------

function columnIndex(result: QueryResult): Record<string, number> {
  const map: Record<string, number> = {};
  result.columns.forEach((c, i) => {
    map[c.name] = i;
  });
  return map;
}

function formatWall(ns: number): string {
  if (!ns) return "";
  const d = new Date(Math.floor(ns / 1_000_000));
  return (
    d.toLocaleTimeString([], { hour12: false }) +
    "." +
    String(d.getMilliseconds()).padStart(3, "0")
  );
}

function severityName(n: number): string {
  if (n >= 21) return "FATAL";
  if (n >= 17) return "ERROR";
  if (n >= 13) return "WARN";
  if (n >= 9) return "INFO";
  if (n >= 5) return "DEBUG";
  return "TRACE";
}

function severityColor(n: number): string {
  if (n >= 17) return "var(--color-error)";
  if (n >= 13) return "var(--color-warn, var(--color-ink))";
  return "var(--color-ink-muted)";
}

function formatMetricValue(v: number): string {
  if (!Number.isFinite(v)) return String(v);
  return v.toLocaleString();
}

function Centered({ children }: { children: React.ReactNode }) {
  return (
    <div className="h-full w-full flex items-center justify-center" style={{ color: "var(--color-ink-muted)" }}>
      {children}
    </div>
  );
}

function Th({ children }: { children?: React.ReactNode }) {
  return (
    <th className="px-3 py-2 font-semibold text-left border-b" style={{ borderColor: "var(--color-border)" }}>
      {children}
    </th>
  );
}

function Td({
  children,
  muted,
  className,
  style,
  onClick,
}: {
  children?: React.ReactNode;
  muted?: boolean;
  className?: string;
  style?: React.CSSProperties;
  onClick?: (e: React.MouseEvent) => void;
}) {
  return (
    <td
      onClick={onClick}
      className={clsx("px-3 py-1.5 align-top", className)}
      style={{
        color: muted ? "var(--color-ink-muted)" : undefined,
        ...style,
      }}
    >
      {children}
    </td>
  );
}
