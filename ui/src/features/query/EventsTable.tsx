import { useQuery } from "@tanstack/react-query";
import { Link } from "@tanstack/react-router";
import clsx from "clsx";
import { X } from "lucide-react";
import type {
  Dataset,
  QueryColumn,
  QueryResult,
  QuerySearch,
} from "../../lib/query";
import { refreshIntervalMs, resolveSearchRange, runQuery } from "../../lib/query";
import { formatDuration } from "../../lib/format";
import { CopyButton } from "../../components/ui/CopyButton";
import { AttributesPanel } from "../../components/ui/AttributesPanel";

/** Selection shape lifted to the page. Pairs the clicked row with the
 *  result's column schema so a detail pane rendered at page level can
 *  decode the row without re-fetching. */
export interface SelectedRow {
  row: unknown[];
  columns: QueryColumn[];
}

interface Props {
  dataset: Dataset;
  querySearch: QuerySearch;
  runCount: number;
  /** Reports the scroll container's vertical offset on every scroll event so
   *  the page can collapse the query/chart header once the user drills in. */
  onScrollY?: (y: number) => void;
  /** Currently-selected row (driven by the page). When this matches a row
   *  in the current result, that row is highlighted. */
  selected?: SelectedRow | null;
  /** Fires when the user clicks a row. Passing `null` clears. */
  onSelect?: (next: SelectedRow | null) => void;
}

/**
 * Events table. Sits below the chart and shows matching rows for the current
 * dataset + WHERE + time range. Backed by the same /api/query endpoint the
 * chart uses; empty SELECT switches that endpoint into raw-rows mode.
 */
export function EventsTable({ dataset, querySearch, runCount, onScrollY, selected, onSelect }: Props) {
  const result = useQuery({
    queryKey: [
      "events",
      dataset,
      querySearch.where,
      querySearch.range,
      querySearch.from,
      querySearch.to,
      runCount,
    ],
    queryFn: ({ signal }) => {
      const resolved = resolveSearchRange(querySearch);
      return runQuery(
        {
          dataset,
          time_range: {
            from: new Date(resolved.fromMs).toISOString(),
            to: new Date(resolved.toMs).toISOString(),
          },
          select: [], // raw-rows mode
          where: querySearch.where,
          limit: 500,
        },
        signal,
      );
    },
    refetchInterval: refreshIntervalMs(querySearch.refresh),
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

  if (dataset === "spans") {
    return <SpansTable result={result.data!} onScrollY={onScrollY} />;
  }
  if (dataset === "metrics") {
    return (
      <MetricsTable
        result={result.data!}
        onScrollY={onScrollY}
        selected={selected ?? null}
        onSelect={onSelect}
      />
    );
  }
  return (
    <LogsTable
      result={result.data!}
      onScrollY={onScrollY}
      selected={selected ?? null}
      onSelect={onSelect}
    />
  );
}

// ---------------------------------------------------------------------------
// Spans (no side pane — the trace-detail view owns that pattern)
// ---------------------------------------------------------------------------

function SpansTable({
  result,
  onScrollY,
}: {
  result: QueryResult;
  onScrollY?: (y: number) => void;
}) {
  const idx = columnIndex(result);
  return (
    <div
      className="h-full overflow-auto"
      onScroll={onScrollY ? (e) => onScrollY(e.currentTarget.scrollTop) : undefined}
    >
      <table className="w-full border-separate border-spacing-0 text-sm">
        <thead
          className="sticky top-0 z-10 text-left text-xs"
          style={{ background: "var(--color-surface)", color: "var(--color-ink-muted)" }}
        >
          <tr>
            <Th>Time</Th>
            <Th>Span</Th>
            <Th>Duration</Th>
            <Th>Status</Th>
            <Th>Trace</Th>
          </tr>
        </thead>
        <tbody>
          {result.rows.map((row, i) => {
            const traceID = String(row[idx.trace_id] ?? "").toLowerCase();
            const name = String(row[idx.name] ?? "");
            const startNS = Number(row[idx.start_time_ns] ?? 0);
            const durNS = Number(row[idx.duration_ns] ?? 0);
            const errorFlag = Number(row[idx.error] ?? 0);
            return (
              <tr
                key={i}
                className={clsx(
                  "hover:bg-[var(--color-card-hover)]",
                  i % 2 === 1 && "bg-[var(--color-card-stripe)]",
                )}
              >
                <Td muted>{formatWall(startNS)}</Td>
                <Td>{name}</Td>
                <Td className="tabular-nums">{formatDuration(durNS)}</Td>
                <Td>
                  {errorFlag ? (
                    <span style={{ color: "var(--color-error)" }}>error</span>
                  ) : (
                    <span style={{ color: "var(--color-ok)" }}>ok</span>
                  )}
                </Td>
                <Td>
                  {traceID ? (
                    <span className="inline-flex items-center gap-1">
                      <Link
                        to="/traces/$traceId"
                        params={{ traceId: traceID }}
                        className="underline font-mono text-xs"
                        style={{ color: "var(--color-accent)" }}
                      >
                        {traceID.slice(0, 8)}
                      </Link>
                      <CopyButton value={traceID} label="Copy trace ID" />
                    </span>
                  ) : null}
                </Td>
              </tr>
            );
          })}
        </tbody>
      </table>
    </div>
  );
}

// ---------------------------------------------------------------------------
// Logs — row click opens a right-hand detail pane (not inline).
// ---------------------------------------------------------------------------

function LogsTable({
  result,
  onScrollY,
  selected,
  onSelect,
}: {
  result: QueryResult;
  onScrollY?: (y: number) => void;
  selected: SelectedRow | null;
  onSelect?: (next: SelectedRow | null) => void;
}) {
  const idx = columnIndex(result);
  const selectedRef = selected?.row;

  const setSelected = (row: unknown[] | null) => {
    if (!onSelect) return;
    onSelect(row ? { row, columns: result.columns } : null);
  };

  return (
    <div
      className="h-full overflow-auto font-mono text-xs leading-5"
      onScroll={
        onScrollY ? (e) => onScrollY(e.currentTarget.scrollTop) : undefined
      }
    >
      <table className="w-full border-separate border-spacing-0">
        <thead
          className="sticky top-0 z-10 text-[10px] uppercase tracking-wide"
          style={{ background: "var(--color-surface)", color: "var(--color-ink-muted)" }}
        >
          <tr>
            <Th>Time</Th>
            <Th>Severity</Th>
            <Th>Body</Th>
            <Th>Trace</Th>
          </tr>
        </thead>
        <tbody>
          {result.rows.map((row, i) => {
            const timeNS = Number(row[idx.time_ns] ?? 0);
            const severityText = String(row[idx.severity_text] ?? "");
            const severityNum = Number(row[idx.severity_number] ?? 0);
            const body = String(row[idx.body] ?? "");
            const traceID = String(row[idx.trace_id] ?? "").toLowerCase();
            const spanID = String(row[idx.span_id] ?? "").toLowerCase();
            const isActive = selectedRef === row;
            return (
              <tr
                key={i}
                className={clsx(
                  "align-top cursor-pointer hover:bg-[var(--color-card-hover)]",
                  !isActive && i % 2 === 1 && "bg-[var(--color-card-stripe)]",
                )}
                style={
                  isActive
                    ? {
                        background:
                          "color-mix(in srgb, var(--color-accent) 12%, transparent)",
                      }
                    : undefined
                }
                onClick={() => setSelected(isActive ? null : row)}
              >
                <Td muted className="whitespace-nowrap">
                  {formatWall(timeNS)}
                </Td>
                <Td style={{ color: severityColor(severityNum) }}>
                  {severityText || severityName(severityNum)}
                </Td>
                <Td className="whitespace-pre-wrap break-all">{body}</Td>
                <Td onClick={(e) => e.stopPropagation()}>
                  {traceID ? (
                    <span className="inline-flex items-center gap-1">
                      <Link
                        to="/traces/$traceId"
                        params={{ traceId: traceID }}
                        search={spanID ? { span: spanID } : {}}
                        className="underline"
                        style={{ color: "var(--color-accent)" }}
                      >
                        {traceID.slice(0, 8)}
                      </Link>
                      <CopyButton value={traceID} label="Copy trace ID" />
                    </span>
                  ) : (
                    <span style={{ color: "var(--color-ink-muted)" }}>·</span>
                  )}
                </Td>
              </tr>
            );
          })}
        </tbody>
      </table>
    </div>
  );
}

/**
 * LogDetailPane renders a single log row's facts + attributes. Exported
 * so pages can render it as a full-height sibling of the main content
 * column (rather than cramped inside the table's flex row).
 */
export function LogDetailPane({
  columns,
  row,
  onClose,
}: {
  columns: QueryColumn[];
  row: unknown[];
  onClose: () => void;
}) {
  const idx = columnIndexFromColumns(columns);
  const timeNS = Number(row[idx.time_ns] ?? 0);
  const severityText = String(row[idx.severity_text] ?? "");
  const severityNum = Number(row[idx.severity_number] ?? 0);
  const body = String(row[idx.body] ?? "");
  const service = String(row[idx.service_name] ?? "");
  const traceID = String(row[idx.trace_id] ?? "").toLowerCase();
  const spanID = String(row[idx.span_id] ?? "").toLowerCase();
  const attributes = String(row[idx.attributes] ?? "{}");
  return (
    <PaneShell onClose={onClose} title="Log record">
      <div
        className="px-4 py-3 border-b flex flex-col gap-1.5"
        style={{
          borderColor: "var(--color-border)",
          background: "var(--color-surface)",
        }}
      >
        <div
          className="text-xs flex items-center gap-2"
          style={{ color: "var(--color-ink-muted)" }}
        >
          <span
            className="px-1.5 py-0.5 rounded text-[10px] font-medium"
            style={{
              color: severityColor(severityNum),
              background:
                "color-mix(in srgb, " +
                severityColor(severityNum) +
                " 12%, transparent)",
            }}
          >
            {severityText || severityName(severityNum)}
          </span>
          <span>{service}</span>
          <span>· {formatWall(timeNS)}</span>
        </div>
        <div className="text-sm font-mono break-words whitespace-pre-wrap">
          {body}
        </div>
        {traceID ? (
          <div
            className="text-[11px] font-mono flex items-center gap-1 flex-wrap"
            style={{ color: "var(--color-ink-muted)" }}
          >
            <span>trace_id:</span>
            <Link
              to="/traces/$traceId"
              params={{ traceId: traceID }}
              search={spanID ? { span: spanID } : {}}
              className="underline truncate"
              style={{ color: "var(--color-accent)" }}
            >
              {traceID}
            </Link>
            <CopyButton value={traceID} label="Copy trace ID" />
          </div>
        ) : null}
      </div>
      <div className="flex-1 overflow-auto">
        <AttributesPanel attributesJson={attributes} filterTarget="/logs" />
      </div>
    </PaneShell>
  );
}

// ---------------------------------------------------------------------------
// Metrics — same side-pane pattern. Columns come from
// rawColumnsFor(metrics): time_ns, service_name, name, kind, value,
// attributes.
// ---------------------------------------------------------------------------

function MetricsTable({
  result,
  onScrollY,
  selected,
  onSelect,
}: {
  result: QueryResult;
  onScrollY?: (y: number) => void;
  selected: SelectedRow | null;
  onSelect?: (next: SelectedRow | null) => void;
}) {
  const idx = columnIndex(result);
  const selectedRef = selected?.row;
  const setSelected = (row: unknown[] | null) => {
    if (!onSelect) return;
    onSelect(row ? { row, columns: result.columns } : null);
  };
  return (
    <div
      className="h-full overflow-auto text-sm"
      onScroll={
        onScrollY ? (e) => onScrollY(e.currentTarget.scrollTop) : undefined
      }
    >
      <table className="w-full border-separate border-spacing-0">
        <thead
          className="sticky top-0 z-10 text-left text-xs"
          style={{ background: "var(--color-surface)", color: "var(--color-ink-muted)" }}
        >
          <tr>
            <Th>Time</Th>
            <Th>Service</Th>
            <Th>Metric</Th>
            <Th>Kind</Th>
            <Th>Value</Th>
          </tr>
        </thead>
        <tbody>
          {result.rows.map((row, i) => {
            const timeNS = Number(row[idx.time_ns] ?? 0);
            const service = String(row[idx.service_name] ?? "");
            const name = String(row[idx.name] ?? "");
            const kind = String(row[idx.kind] ?? "");
            const value = Number(row[idx.value] ?? 0);
            const isActive = selectedRef === row;
            return (
              <tr
                key={i}
                className={clsx(
                  "cursor-pointer hover:bg-[var(--color-card-hover)]",
                  !isActive && i % 2 === 1 && "bg-[var(--color-card-stripe)]",
                )}
                style={
                  isActive
                    ? {
                        background:
                          "color-mix(in srgb, var(--color-accent) 12%, transparent)",
                      }
                    : undefined
                }
                onClick={() => setSelected(isActive ? null : row)}
              >
                <Td muted className="whitespace-nowrap font-mono text-xs">
                  {formatWall(timeNS)}
                </Td>
                <Td>{service}</Td>
                <Td className="font-mono">{name}</Td>
                <Td muted>{kind}</Td>
                <Td className="tabular-nums font-mono text-xs">
                  {formatMetricValue(value)}
                </Td>
              </tr>
            );
          })}
        </tbody>
      </table>
    </div>
  );
}

/**
 * MetricDetailPane — exported twin of LogDetailPane for /metrics. The
 * page renders this as a full-height sibling next to the main content.
 */
export function MetricDetailPane({
  columns,
  row,
  onClose,
}: {
  columns: QueryColumn[];
  row: unknown[];
  onClose: () => void;
}) {
  const idx = columnIndexFromColumns(columns);
  const timeNS = Number(row[idx.time_ns] ?? 0);
  const service = String(row[idx.service_name] ?? "");
  const name = String(row[idx.name] ?? "");
  const kind = String(row[idx.kind] ?? "");
  const value = Number(row[idx.value] ?? 0);
  const attributes = String(row[idx.attributes] ?? "{}");
  return (
    <PaneShell onClose={onClose} title="Metric point">
      <div
        className="px-4 py-3 border-b flex flex-col gap-1.5"
        style={{
          borderColor: "var(--color-border)",
          background: "var(--color-surface)",
        }}
      >
        <div
          className="text-xs flex items-center gap-2"
          style={{ color: "var(--color-ink-muted)" }}
        >
          <span
            className="px-1.5 py-0.5 rounded text-[10px] font-medium uppercase"
            style={{
              background: "var(--color-card-stripe)",
              color: "var(--color-ink-muted)",
            }}
          >
            {kind}
          </span>
          <span>{service}</span>
          <span>· {formatWall(timeNS)}</span>
        </div>
        <div className="text-base font-mono break-all">{name}</div>
        <div
          className="text-xl font-mono tabular-nums"
          style={{ color: "var(--color-ink)" }}
        >
          {formatMetricValue(value)}
        </div>
      </div>
      <div className="flex-1 overflow-auto">
        <AttributesPanel attributesJson={attributes} filterTarget="/metrics" />
      </div>
    </PaneShell>
  );
}

// ---------------------------------------------------------------------------
// Shared pane chrome — pages render the selected row's detail here as a
// full-height sibling of the main content column.
// ---------------------------------------------------------------------------

function PaneShell({
  onClose,
  title,
  children,
}: {
  onClose: () => void;
  title: string;
  children: React.ReactNode;
}) {
  return (
    <div className="h-full flex flex-col overflow-hidden">
      <div
        className="flex items-center justify-between px-3 py-2 border-b text-xs uppercase tracking-wide"
        style={{
          borderColor: "var(--color-border)",
          color: "var(--color-ink-muted)",
        }}
      >
        <span>{title}</span>
        <button
          type="button"
          onClick={onClose}
          className="p-1 rounded hover:bg-[var(--color-card-hover)]"
          title="Close"
          aria-label="Close detail pane"
        >
          <X className="w-3.5 h-3.5" />
        </button>
      </div>
      {children}
    </div>
  );
}

// ---------------------------------------------------------------------------
// helpers
// ---------------------------------------------------------------------------

function columnIndex(result: QueryResult): Record<string, number> {
  return columnIndexFromColumns(result.columns);
}

function columnIndexFromColumns(columns: QueryColumn[]): Record<string, number> {
  const map: Record<string, number> = {};
  columns.forEach((c, i) => {
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
  if (n >= 13) return "#b45f06";
  if (n >= 9) return "var(--color-ink)";
  return "var(--color-ink-muted)";
}

function formatMetricValue(v: number): string {
  if (!Number.isFinite(v)) return String(v);
  if (Number.isInteger(v)) return v.toLocaleString();
  return v.toLocaleString(undefined, { maximumFractionDigits: 4 });
}

function Th({ children }: { children?: React.ReactNode }) {
  return (
    <th
      className="px-4 py-2 border-b font-medium uppercase tracking-wide"
      style={{ borderColor: "var(--color-border)" }}
    >
      {children}
    </th>
  );
}

function Td({
  children,
  className,
  style,
  muted,
  onClick,
}: {
  children: React.ReactNode;
  className?: string;
  style?: React.CSSProperties;
  muted?: boolean;
  onClick?: React.MouseEventHandler<HTMLTableCellElement>;
}) {
  return (
    <td
      className={"px-4 py-2 border-b " + (className ?? "")}
      style={{
        borderColor: "var(--color-border)",
        color: muted ? "var(--color-ink-muted)" : undefined,
        ...style,
      }}
      onClick={onClick}
    >
      {children}
    </td>
  );
}

function Centered({ children }: { children: React.ReactNode }) {
  return <div className="h-full flex items-center justify-center">{children}</div>;
}
