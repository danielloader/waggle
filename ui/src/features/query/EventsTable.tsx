import { useQuery } from "@tanstack/react-query";
import { Link } from "@tanstack/react-router";
import type {
  Dataset,
  QueryResult,
  QuerySearch,
} from "../../lib/query";
import { resolveSearchRange, runQuery } from "../../lib/query";
import { formatDuration } from "../../lib/format";
import { CopyButton } from "../../components/ui/CopyButton";

interface Props {
  dataset: Dataset;
  search: QuerySearch;
  /** Reports the scroll container's vertical offset on every scroll event so
   *  the page can collapse the query/chart header once the user drills in. */
  onScrollY?: (y: number) => void;
}

/**
 * Events table. Sits below the chart and shows matching rows for the current
 * dataset + WHERE + time range. Backed by the same /api/query endpoint the
 * chart uses; empty SELECT switches that endpoint into raw-rows mode.
 */
export function EventsTable({ dataset, search, onScrollY }: Props) {
  // The cache key includes the raw search (range preset OR from/to).
  // Resolving inside queryFn would be cleaner but TanStack needs a stable
  // key for de-dup — so we key on the raw inputs and resolve inside the
  // query body each fetch (picking up fresh "now" for presets).
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
    refetchInterval: 10_000,
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

  return dataset === "spans" ? (
    <SpansTable result={result.data!} onScrollY={onScrollY} />
  ) : (
    <LogsTable result={result.data!} onScrollY={onScrollY} />
  );
}

// ---------------------------------------------------------------------------
// Spans
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
            // `error` is the synthetic field combining status_code=ERROR with
            // the presence of an `exception` span event. Always emitted by
            // the server in raw-rows mode.
            const errorFlag = Number(row[idx.error] ?? 0);
            return (
              <tr key={i}>
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
// Logs
// ---------------------------------------------------------------------------

function LogsTable({
  result,
  onScrollY,
}: {
  result: QueryResult;
  onScrollY?: (y: number) => void;
}) {
  const idx = columnIndex(result);
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
            return (
              <tr key={i} className="align-top">
                <Td muted className="whitespace-nowrap">
                  {formatWall(timeNS)}
                </Td>
                <Td style={{ color: severityColor(severityNum) }}>
                  {severityText || severityName(severityNum)}
                </Td>
                <Td className="whitespace-pre-wrap break-all">{body}</Td>
                <Td>
                  {traceID ? (
                    <span className="inline-flex items-center gap-1">
                      <Link
                        to="/traces/$traceId"
                        params={{ traceId: traceID }}
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
  if (n >= 13) return "#b45f06";
  if (n >= 9) return "var(--color-ink)";
  return "var(--color-ink-muted)";
}

function Th({ children }: { children: React.ReactNode }) {
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
}: {
  children: React.ReactNode;
  className?: string;
  style?: React.CSSProperties;
  muted?: boolean;
}) {
  return (
    <td
      className={"px-4 py-2 border-b " + (className ?? "")}
      style={{
        borderColor: "var(--color-border)",
        color: muted ? "var(--color-ink-muted)" : undefined,
        ...style,
      }}
    >
      {children}
    </td>
  );
}

function Centered({ children }: { children: React.ReactNode }) {
  return <div className="h-full flex items-center justify-center">{children}</div>;
}
