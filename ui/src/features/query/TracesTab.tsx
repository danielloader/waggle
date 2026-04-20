import { useQuery } from "@tanstack/react-query";
import { Link } from "@tanstack/react-router";
import type { QueryResult, QuerySearch } from "../../lib/query";
import { resolveSearchRange, runQuery } from "../../lib/query";
import { serviceColor } from "../../lib/colors";
import { formatDuration, formatRelativeTimestamp } from "../../lib/format";
import { CopyButton } from "../../components/ui/CopyButton";

interface Props {
  querySearch: QuerySearch;
  runCount: number;
}

/**
 * Top-N slowest root spans — Honeycomb's Traces tab. A single /api/query in
 * raw-rows mode scoped to is_root = true, ordered by duration descending.
 * Each row is one trace (by virtue of being its root span) and links into
 * the full waterfall view.
 */
export function TracesTab({ querySearch, runCount }: Props) {
  const result = useQuery({
    queryKey: ["traces-tab", querySearch, runCount],
    queryFn: ({ signal }) => {
      const resolved = resolveSearchRange(querySearch);
      return runQuery(
        {
          dataset: "spans",
          time_range: {
            from: new Date(resolved.fromMs).toISOString(),
            to: new Date(resolved.toMs).toISOString(),
          },
          select: [], // raw rows
          where: [
            ...querySearch.where,
            { field: "is_root", op: "=", value: true },
          ],
          order_by: [{ field: "duration_ns", dir: "desc" }],
          limit: 10,
        },
        signal,
      );
    },
    refetchInterval: 10_000,
  });

  if (result.isPending) return <Centered>Finding slowest traces…</Centered>;
  if (result.isError)
    return <Centered>Error: {(result.error as Error).message}</Centered>;
  const data = result.data;
  if (!data || data.rows.length === 0) {
    return (
      <Centered>
        <div className="max-w-md text-center">
          <div className="font-medium mb-2">No root spans match</div>
          <div className="text-sm" style={{ color: "var(--color-ink-muted)" }}>
            Widen the time range or remove a filter.
          </div>
        </div>
      </Centered>
    );
  }

  return (
    <div className="h-full overflow-auto">
      <table className="w-full border-separate border-spacing-0 text-sm">
        <thead
          className="sticky top-0 z-10 text-left text-xs"
          style={{
            background: "var(--color-card)",
            color: "var(--color-ink-muted)",
          }}
        >
          <tr>
            <Th>Root service</Th>
            <Th>Root name</Th>
            <Th align="right">Duration</Th>
            <Th>Started</Th>
            <Th>Status</Th>
            <Th>Trace</Th>
          </tr>
        </thead>
        <tbody>
          {data.rows.map((row, i) => (
            <TraceRow key={i} row={row} result={data} />
          ))}
        </tbody>
      </table>
    </div>
  );
}

function TraceRow({ row, result }: { row: unknown[]; result: QueryResult }) {
  const col = (name: string) => result.columns.findIndex((c) => c.name === name);

  const traceID = String(row[col("trace_id")] ?? "").toLowerCase();
  const service = String(row[col("service_name")] ?? "");
  const name = String(row[col("name")] ?? "");
  const startNS = Number(row[col("start_time_ns")] ?? 0);
  const durNS = Number(row[col("duration_ns")] ?? 0);
  const errorFlag = Number(row[col("error")] ?? 0);

  return (
    <tr>
      <Td>
        <span className="inline-flex items-center gap-1.5 text-xs font-medium">
          <span
            className="w-2 h-2 rounded-full"
            style={{ background: serviceColor(service) }}
          />
          {service}
        </span>
      </Td>
      <Td>{name}</Td>
      <Td align="right" className="tabular-nums">
        {formatDuration(durNS)}
      </Td>
      <Td muted>{formatRelativeTimestamp(startNS)}</Td>
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
              {traceID.slice(0, 12)}…
            </Link>
            <CopyButton value={traceID} label="Copy trace ID" />
          </span>
        ) : null}
      </Td>
    </tr>
  );
}

function Th({
  children,
  align = "left",
}: {
  children: React.ReactNode;
  align?: "left" | "right";
}) {
  return (
    <th
      className="px-4 py-2 border-b font-medium uppercase tracking-wide"
      style={{ borderColor: "var(--color-border)", textAlign: align }}
    >
      {children}
    </th>
  );
}

function Td({
  children,
  align = "left",
  className,
  muted,
}: {
  children: React.ReactNode;
  align?: "left" | "right";
  className?: string;
  muted?: boolean;
}) {
  return (
    <td
      className={"px-4 py-2 border-b " + (className ?? "")}
      style={{
        borderColor: "var(--color-border)",
        textAlign: align,
        color: muted ? "var(--color-ink-muted)" : undefined,
      }}
    >
      {children}
    </td>
  );
}

function Centered({ children }: { children: React.ReactNode }) {
  return (
    <div className="h-full flex items-center justify-center">{children}</div>
  );
}
