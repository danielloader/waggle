import { useQuery } from "@tanstack/react-query";
import { useNavigate } from "@tanstack/react-router";
import { DefinePanel } from "../features/query/DefinePanel";
import { QueryChart } from "../features/query/QueryChart";
import { ResultTabs } from "../features/query/ResultTabs";
import {
  bucketMsFor,
  buildCountQuery,
  resolveSearchRange,
  runQuery,
  type QuerySearch,
} from "../lib/query";
import { tracesRoute } from "../router";

export function TracesPage() {
  const search = tracesRoute.useSearch();
  const navigate = useNavigate();

  const setSearch = (next: QuerySearch) =>
    navigate({ to: "/traces", search: next as unknown as Record<string, unknown> });

  const chart = useQuery({
    queryKey: ["query", "spans", search],
    queryFn: ({ signal }) => runQuery(buildCountQuery("spans", search), signal),
    refetchInterval: 10_000,
  });

  // Click a chart bucket → ZOOM the time window to that bucket and jump to
  // Explore Data. Writes to search.from/to rather than adding WHERE filters,
  // so every downstream (chart axis, overview, events) sees the narrowed
  // window consistently — no more WHERE-filtered-but-chart-unclamped state.
  const resolved = resolveSearchRange(search);
  const handleBucketClick = (tMs: number) => {
    const bucketMs = bucketMsFor(resolved.durationMs, search.granularity);
    setSearch({
      ...search,
      from: tMs,
      to: tMs + bucketMs,
      // Reset to auto so the new (smaller) window gets a proportionally
      // smaller bucket — otherwise keeping e.g. "1m" on a 1m zoom would
      // leave you staring at a single bucket.
      granularity: "auto",
      tab: "explore",
    });
  };

  return (
    <div className="h-full flex flex-col">
      <DefinePanel
        dataset="spans"
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
          dataset="spans"
          search={search}
          onTabChange={(tab) => setSearch({ ...search, tab })}
        />
      </div>
    </div>
  );
}
