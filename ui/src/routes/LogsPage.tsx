import { useQuery } from "@tanstack/react-query";
import { useNavigate } from "@tanstack/react-router";
import { DefinePanel } from "../features/query/DefinePanel";
import { QueryChart } from "../features/query/QueryChart";
import { ResultTabs } from "../features/query/ResultTabs";
import { LogSearchInput } from "../features/query/LogSearchInput";
import {
  bucketMsFor,
  buildCountQuery,
  resolveSearchRange,
  runQuery,
  type QuerySearch,
} from "../lib/query";
import { logsRoute } from "../router";

export function LogsPage() {
  const search = logsRoute.useSearch();
  const navigate = useNavigate();

  const setSearch = (next: QuerySearch) =>
    navigate({ to: "/logs", search: next as unknown as Record<string, unknown> });

  const chart = useQuery({
    queryKey: ["query", "logs", search],
    queryFn: ({ signal }) => runQuery(buildCountQuery("logs", search), signal),
    refetchInterval: 10_000,
  });

  // Click a chart bucket → ZOOM the time window to that bucket. Using
  // search.from/to (resolveSearchRange treats them as overriding `range`)
  // keeps chart, overview and events tab perfectly in sync.
  const resolved = resolveSearchRange(search);
  const handleBucketClick = (tMs: number) => {
    const bucketMs = bucketMsFor(resolved.durationMs, search.granularity);
    setSearch({
      ...search,
      from: tMs,
      to: tMs + bucketMs,
      // Reset to auto so the zoomed window gets a finer bucket — keeping
      // a coarse manual granularity would collapse the new view to one
      // bucket and defeat the purpose of drilling in.
      granularity: "auto",
      tab: "explore",
    });
  };

  return (
    <div className="h-full flex flex-col">
      <DefinePanel
        dataset="logs"
        search={search}
        onChange={setSearch}
        onRun={() => chart.refetch()}
        isRunning={chart.isFetching}
      />
      <div className="p-3" style={{ background: "var(--color-surface-muted)" }}>
        <QueryChart
          result={chart.data}
          loading={chart.isPending}
          error={chart.error}
          bucketMs={bucketMsFor(resolved.durationMs, search.granularity)}
          fromMs={resolved.fromMs}
          toMs={resolved.toMs}
          onBucketClick={handleBucketClick}
        />
      </div>
      <div
        className="flex-1 overflow-hidden border-t"
        style={{ borderColor: "var(--color-border)" }}
      >
        <ResultTabs
          dataset="logs"
          search={search}
          onTabChange={(tab) => setSearch({ ...search, tab })}
          rightSlot={
            <LogSearchInput
              value={search.q}
              onChange={(q) => setSearch({ ...search, q })}
            />
          }
        />
      </div>
    </div>
  );
}
