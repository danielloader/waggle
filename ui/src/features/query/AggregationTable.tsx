import type { QueryResult } from "../../lib/query";

interface Props {
  result: QueryResult | undefined;
  loading?: boolean;
}

/**
 * Shown when the query has GROUP BY but we want a flat table of the totals
 * (not the time-series view). Rolls up the bucketed rows by summing the
 * aggregation column per group tuple.
 */
export function AggregationTable({ result, loading }: Props) {
  if (loading && !result) {
    return <EmptyState>Running query…</EmptyState>;
  }
  if (!result || result.rows.length === 0) {
    return <EmptyState>No rows</EmptyState>;
  }

  const bucketIdx = result.has_bucket ? 0 : -1;
  const aggIdx = result.columns.length - 1;
  const groupIdxs = result.columns
    .map((_, i) => i)
    .filter((i) => i !== bucketIdx && i !== aggIdx);

  // Roll up by group tuple.
  const rolled = new Map<string, { keys: unknown[]; total: number }>();
  for (const row of result.rows) {
    const keys = groupIdxs.map((i) => row[i]);
    const k = JSON.stringify(keys);
    const prev = rolled.get(k) ?? { keys, total: 0 };
    prev.total += Number(row[aggIdx] ?? 0);
    rolled.set(k, prev);
  }
  const totals = Array.from(rolled.values()).sort((a, b) => b.total - a.total);

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
            {groupIdxs.map((i) => (
              <Th key={i}>{result.columns[i].name}</Th>
            ))}
            <Th>{result.columns[aggIdx].name}</Th>
          </tr>
        </thead>
        <tbody>
          {totals.map((r, i) => (
            <tr key={i}>
              {r.keys.map((v, j) => (
                <Td key={j} className="font-mono">
                  {String(v ?? "·")}
                </Td>
              ))}
              <Td className="tabular-nums">{r.total.toLocaleString()}</Td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}

function EmptyState({ children }: { children: React.ReactNode }) {
  return (
    <div className="h-full flex items-center justify-center text-sm" style={{ color: "var(--color-ink-muted)" }}>
      {children}
    </div>
  );
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

function Td({ children, className }: { children: React.ReactNode; className?: string }) {
  return (
    <td
      className={"px-4 py-2 border-b " + (className ?? "")}
      style={{ borderColor: "var(--color-border)" }}
    >
      {children}
    </td>
  );
}
