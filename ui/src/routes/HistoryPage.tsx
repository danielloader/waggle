import { useQuery } from "@tanstack/react-query";
import { useNavigate } from "@tanstack/react-router";
import { Repeat } from "lucide-react";
import { api, type QueryHistoryEntry } from "../lib/api";
import { queryToUrlSearch, type Query } from "../lib/query";

/**
 * Query history — Honeycomb-style "Recent Queries" tab. One row per
 * dedup'd AST, most-recent-first, showing dataset + one-glance summary
 * + relative timestamp + run count. Click a row to re-run the stored
 * query with the original time window; the chart preview on each row is
 * intentionally absent in this pass (rehydrating and running every row
 * on list-render would cost a query per row).
 */
export function HistoryPage() {
  const navigate = useNavigate();
  const q = useQuery({
    queryKey: ["history"],
    queryFn: ({ signal }) => api.listHistory(100, signal),
    staleTime: 10_000,
  });

  return (
    <div className="h-full flex flex-col">
      <div
        className="px-6 py-4 border-b"
        style={{ borderColor: "var(--color-border)" }}
      >
        <h1 className="text-xl font-semibold">Query History</h1>
        <p
          className="text-sm mt-1"
          style={{ color: "var(--color-ink-muted)" }}
        >
          Recent queries, deduplicated. Click a row to re-run with the
          original time window.
        </p>
      </div>
      <div className="flex-1 overflow-auto px-6 py-4">
        {q.isPending && (
          <div
            className="text-sm py-8 text-center"
            style={{ color: "var(--color-ink-muted)" }}
          >
            Loading…
          </div>
        )}
        {q.isError && (
          <div
            className="text-sm py-8 text-center"
            style={{ color: "var(--color-error)" }}
          >
            Error: {(q.error as Error).message}
          </div>
        )}
        {q.data && q.data.entries.length === 0 && (
          <div
            className="text-sm py-8 text-center"
            style={{ color: "var(--color-ink-muted)" }}
          >
            No history yet. Run a query on the Events page and come back.
          </div>
        )}
        {q.data && q.data.entries.length > 0 && (
          <ul className="flex flex-col gap-2">
            {q.data.entries.map((e) => (
              <HistoryRow
                key={e.id}
                entry={e}
                onClick={() => {
                  const query = safeParseQuery(e.query_json);
                  if (!query) return;
                  const search = queryToUrlSearch(query);
                  navigate({
                    to: "/events",
                    search: search as unknown as Record<string, unknown>,
                  });
                }}
              />
            ))}
          </ul>
        )}
      </div>
    </div>
  );
}

function HistoryRow({
  entry,
  onClick,
}: {
  entry: QueryHistoryEntry;
  onClick: () => void;
}) {
  return (
    <li>
      <button
        type="button"
        onClick={onClick}
        className="w-full text-left rounded-md border px-4 py-3 hover:bg-[var(--color-card-hover)] flex items-start gap-4"
        style={{
          borderColor: "var(--color-border)",
          background: "var(--color-card)",
        }}
      >
        <div className="flex-1 min-w-0">
          <div className="flex items-center gap-2 mb-1.5">
            <DatasetPill dataset={entry.dataset} />
            <span
              className="text-xs"
              style={{ color: "var(--color-ink-muted)" }}
            >
              {relativeTime(entry.last_run_ns)}
            </span>
            {entry.run_count > 1 && (
              <span
                className="inline-flex items-center gap-1 text-[10px] tabular-nums px-1.5 py-0.5 rounded"
                style={{
                  color: "var(--color-ink-muted)",
                  background: "var(--color-surface-muted)",
                }}
                title={`Run ${entry.run_count} times`}
              >
                <Repeat className="w-3 h-3" />
                {entry.run_count}
              </span>
            )}
          </div>
          <div className="font-mono text-xs leading-relaxed break-words whitespace-pre-wrap">
            {entry.display_text}
          </div>
        </div>
      </button>
    </li>
  );
}

function DatasetPill({ dataset }: { dataset: string }) {
  return (
    <span
      className="inline-flex items-center px-1.5 py-0.5 rounded text-[10px] font-medium uppercase tracking-wider"
      style={{
        background: "color-mix(in srgb, var(--color-accent) 14%, transparent)",
        color: "var(--color-accent)",
      }}
    >
      {dataset}
    </span>
  );
}

// Convert a nanosecond wall-clock stamp into a short "X minutes ago"
// relative label. Honeycomb does the same on their recent-queries list.
function relativeTime(ns: number): string {
  const nowMs = Date.now();
  const ms = ns / 1e6;
  const diff = Math.max(0, nowMs - ms);
  const s = Math.floor(diff / 1000);
  if (s < 60) return `${s}s ago`;
  const m = Math.floor(s / 60);
  if (m < 60) return `${m}m ago`;
  const h = Math.floor(m / 60);
  if (h < 24) return `${h}h ago`;
  const d = Math.floor(h / 24);
  if (d < 7) return `${d}d ago`;
  return new Date(ms).toLocaleDateString();
}

function safeParseQuery(json: string): Query | null {
  try {
    return JSON.parse(json) as Query;
  } catch {
    return null;
  }
}
