import { useQuery } from "@tanstack/react-query";
import type { Dataset, QueryResult, QuerySearch } from "../../lib/query";
import { buildOverviewQuery, runQuery } from "../../lib/query";
import { serviceColor } from "../../lib/colors";

interface Props {
  dataset: Dataset;
  search: QuerySearch;
}

/**
 * Honeycomb-style Overview tab. Renders the aggregation result rolled up
 * across the whole time range: a single scalar when the query has no
 * GROUP BY, a compact table when it does. Uses an unbucketed /api/query
 * request — same SELECT/WHERE as the chart but with bucket_ms omitted, so
 * each group tuple collapses to a single row.
 */
export function OverviewTab({ dataset, search }: Props) {
  const overview = useQuery({
    queryKey: ["overview", dataset, search],
    queryFn: ({ signal }) =>
      runQuery(buildOverviewQuery(dataset, search), signal),
    refetchInterval: 10_000,
  });

  if (overview.isPending) return <Msg>Rolling up…</Msg>;
  if (overview.isError)
    return <Msg>Error: {(overview.error as Error).message}</Msg>;
  const result = overview.data!;
  if (result.rows.length === 0) return <Msg>No rows in selected range.</Msg>;

  const groupCount = result.columns.length - aggregationCount(result);
  if (groupCount === 0) {
    return <ScalarView result={result} />;
  }
  return <GroupedView result={result} groupCount={groupCount} />;
}

// A crude heuristic: every column whose name matches one of the builder's
// aggregation alias prefixes is an aggregation output. Everything else is a
// GROUP BY key. Matches the server's naming convention
// (e.g. `count`, `p95_duration_ns`, `rate_sum_bytes_out`).
function aggregationCount(result: QueryResult): number {
  return result.columns.filter((c) => isAggAlias(c.name)).length;
}

function isAggAlias(name: string): boolean {
  if (name === "count") return true;
  const prefixes = [
    "count_",
    "sum_",
    "avg_",
    "min_",
    "max_",
    "p001_",
    "p01_",
    "p05_",
    "p10_",
    "p25_",
    "p50_",
    "p75_",
    "p90_",
    "p95_",
    "p99_",
    "p999_",
    "rate_sum_",
    "rate_avg_",
    "rate_max_",
  ];
  return prefixes.some((p) => name.startsWith(p));
}

// ---------------------------------------------------------------------------
// Scalar: no GROUP BY. Each select column becomes a single large number.
// ---------------------------------------------------------------------------

function ScalarView({ result }: { result: QueryResult }) {
  const row = result.rows[0];
  return (
    <div className="flex flex-col divide-y" style={{ borderColor: "var(--color-border)" }}>
      {result.columns.map((col, i) => (
        <div key={col.name} className="flex items-baseline gap-4 py-3 px-4">
          <div
            className="text-xs uppercase tracking-wider"
            style={{ color: "var(--color-ink-muted)", minWidth: 180 }}
          >
            {col.name}
          </div>
          <div className="text-2xl font-semibold tabular-nums">
            {formatScalar(row[i])}
          </div>
        </div>
      ))}
    </div>
  );
}

// ---------------------------------------------------------------------------
// Grouped: GROUP BY present. Render a table with one row per group tuple,
// sorted by the first aggregation column descending.
// ---------------------------------------------------------------------------

function GroupedView({
  result,
  groupCount,
}: {
  result: QueryResult;
  groupCount: number;
}) {
  // First aggregation column is the primary sort target (matches Honeycomb
  // behavior). User's explicit ORDER BY still takes effect if they set one.
  const primaryAggIdx = groupCount;
  const sortedRows = [...result.rows].sort((a, b) => {
    const av = Number(a[primaryAggIdx] ?? 0);
    const bv = Number(b[primaryAggIdx] ?? 0);
    return bv - av;
  });

  return (
    <div className="h-full overflow-auto">
      <table className="w-full border-separate border-spacing-0 text-sm">
        <thead
          className="sticky top-0 z-10 text-left text-xs"
          style={{
            background: "var(--color-surface)",
            color: "var(--color-ink-muted)",
          }}
        >
          <tr>
            {result.columns.map((col, i) => (
              <Th key={col.name} align={i >= groupCount ? "right" : "left"}>
                {col.name}
              </Th>
            ))}
          </tr>
        </thead>
        <tbody>
          {sortedRows.map((row, i) => (
            <tr key={i}>
              {result.columns.map((col, j) => (
                <Td key={col.name} align={j >= groupCount ? "right" : "left"}>
                  {j < groupCount ? (
                    <GroupValueCell
                      name={col.name}
                      value={row[j]}
                      rowIndex={i}
                      groupIndex={j}
                    />
                  ) : (
                    <span className="tabular-nums">{formatScalar(row[j])}</span>
                  )}
                </Td>
              ))}
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}

function GroupValueCell({
  name,
  value,
  rowIndex,
  groupIndex,
}: {
  name: string;
  value: unknown;
  rowIndex: number;
  groupIndex: number;
}) {
  const v = String(value ?? "·");
  // When the first group column is service-ish, render with the series dot
  // so the overview table looks consistent with the rest of the UI.
  const isService =
    groupIndex === 0 && (name === "service_name" || name.includes("service"));
  return (
    <span className="inline-flex items-center gap-2 font-mono">
      {isService && (
        <span
          className="w-2 h-2 rounded-full"
          style={{ background: serviceColor(v) }}
        />
      )}
      {!isService && groupIndex === 0 && (
        <span
          className="w-2 h-2 rounded-sm"
          style={{ background: paletteColor(rowIndex) }}
        />
      )}
      <span>{v}</span>
    </span>
  );
}

const PALETTE = [
  "#3c78d8",
  "#6aa84f",
  "#c27ba0",
  "#e06666",
  "#f6b26b",
  "#8e63ce",
  "#45818e",
  "#b45f06",
  "#a64d79",
  "#674ea7",
  "#38761d",
  "#cc0000",
];

function paletteColor(n: number): string {
  return PALETTE[Math.abs(n) % PALETTE.length];
}

// ---------------------------------------------------------------------------
// shared
// ---------------------------------------------------------------------------

function formatScalar(v: unknown): string {
  if (v === null || v === undefined) return "—";
  if (typeof v === "number") {
    if (!Number.isFinite(v)) return String(v);
    if (Number.isInteger(v)) return v.toLocaleString();
    return v.toLocaleString(undefined, { maximumFractionDigits: 3 });
  }
  return String(v);
}

function Msg({ children }: { children: React.ReactNode }) {
  return (
    <div
      className="px-4 py-3 text-sm"
      style={{ color: "var(--color-ink-muted)" }}
    >
      {children}
    </div>
  );
}

function Th({
  children,
  align,
}: {
  children: React.ReactNode;
  align: "left" | "right";
}) {
  return (
    <th
      className="px-4 py-2 border-b font-medium uppercase tracking-wide"
      style={{
        borderColor: "var(--color-border)",
        textAlign: align,
      }}
    >
      {children}
    </th>
  );
}

function Td({
  children,
  align,
}: {
  children: React.ReactNode;
  align: "left" | "right";
}) {
  return (
    <td
      className="px-4 py-2 border-b"
      style={{
        borderColor: "var(--color-border)",
        textAlign: align,
      }}
    >
      {children}
    </td>
  );
}
