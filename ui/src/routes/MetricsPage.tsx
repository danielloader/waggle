import { useEffect, useRef, useState, useMemo } from "react";
import { useQuery } from "@tanstack/react-query";
import { useNavigate } from "@tanstack/react-router";
import { Accordion } from "../components/ui/Accordion";
import { DefinePanel } from "../features/query/DefinePanel";
import { QueryChart } from "../features/query/QueryChart";
import { ResultTabs } from "../features/query/ResultTabs";
import {
  bucketMsFor,
  buildCountQuery,
  refreshIntervalMs,
  resolveSearchRange,
  runQuery,
  type QuerySearch,
} from "../lib/query";
import { useRefreshPersistence } from "../lib/refreshPersistence";
import { metricsRoute } from "../router";

const COLLAPSE_DISTANCE = 220;

/**
 * Metrics explorer. Same skeleton as traces / logs — the Define panel
 * drives filtering via WHERE. Scoping to a single metric is just a
 * `name = <metric>` WHERE filter, which the shared filter editor's
 * value-autocomplete populates from /api/fields/name/values.
 *
 * The chart only renders once a `name =` filter is present — otherwise
 * we'd be aggregating MAX(value) across every metric in the store,
 * which never means anything useful.
 */
export function MetricsPage() {
  const search = metricsRoute.useSearch();
  const navigate = useNavigate();
  const [queryOpen, setQueryOpen] = useState(true);
  const [chartOpen, setChartOpen] = useState(true);
  const [scrollProgress, setScrollProgress] = useState(0);
  const rafRef = useRef<number | null>(null);

  useEffect(() => {
    setQueryOpen(true);
    setChartOpen(true);
    setScrollProgress(0);
  }, [search.tab]);
  useEffect(
    () => () => {
      if (rafRef.current !== null) cancelAnimationFrame(rafRef.current);
    },
    [],
  );

  const handleExploreScrollY = (y: number) => {
    if (rafRef.current !== null) cancelAnimationFrame(rafRef.current);
    rafRef.current = requestAnimationFrame(() => {
      rafRef.current = null;
      const p = Math.max(0, Math.min(1, y / COLLAPSE_DISTANCE));
      setScrollProgress((prev) => (prev === p ? prev : p));
    });
  };
  const reopen = (set: (v: boolean) => void) => {
    setScrollProgress(0);
    set(true);
  };

  const setSearch = (next: QuerySearch) =>
    navigate({ to: "/metrics", search: next as unknown as Record<string, unknown> });

  useRefreshPersistence(search, setSearch);

  // Gate the chart on the user having scoped to one metric. An aggregate
  // over "all metrics in the store" is meaningless; better to prompt
  // than render a misleading line.
  const pickedName = useMemo(() => {
    const nameFilter = search.where.find(
      (f) => f.field === "name" && f.op === "=",
    );
    return typeof nameFilter?.value === "string" ? nameFilter.value : "";
  }, [search.where]);

  const chart = useQuery({
    queryKey: ["query", "metrics", search],
    queryFn: ({ signal }) =>
      runQuery(buildCountQuery("metrics", search), signal),
    refetchInterval: refreshIntervalMs(search.refresh),
    enabled: pickedName !== "",
  });

  const resolved = resolveSearchRange(search);
  const handleBucketClick = (tMs: number) => {
    const bucketMs = bucketMsFor(resolved.durationMs, search.granularity);
    setSearch({
      ...search,
      from: tMs,
      to: tMs + bucketMs,
      granularity: "auto",
      tab: "explore",
    });
  };

  return (
    <div className="h-full flex flex-col">
      <Accordion
        label="Query"
        open={queryOpen}
        collapseProgress={scrollProgress}
        onToggle={() => (queryOpen ? setQueryOpen(false) : reopen(setQueryOpen))}
        collapsedSummary={querySummary(search, pickedName)}
      >
        <DefinePanel
          dataset="metrics"
          search={search}
          onChange={setSearch}
          onRun={() => chart.refetch()}
          isRunning={chart.isFetching}
        />
      </Accordion>
      <Accordion
        label="Chart"
        open={chartOpen}
        onToggle={() => setChartOpen((o) => !o)}
      >
        <div className="p-3" style={{ background: "var(--color-surface)" }}>
          {pickedName === "" ? (
            <div
              className="flex items-center justify-center text-sm px-4 text-center"
              style={{ color: "var(--color-ink-muted)", height: 125 }}
            >
              Add a <code className="mx-1 font-mono">name = …</code> filter
              above to chart a metric.
            </div>
          ) : (
            <QueryChart
              result={chart.data}
              loading={chart.isPending}
              error={chart.error}
              bucketMs={bucketMsFor(resolved.durationMs, search.granularity)}
              fromMs={resolved.fromMs}
              toMs={resolved.toMs}
              onBucketClick={handleBucketClick}
            />
          )}
        </div>
      </Accordion>
      <div className="flex-1 overflow-hidden">
        <ResultTabs
          dataset="metrics"
          search={search}
          onTabChange={(tab) => setSearch({ ...search, tab })}
          onExploreScrollY={handleExploreScrollY}
        />
      </div>
    </div>
  );
}

function querySummary(search: QuerySearch, picked: string): string {
  const parts: string[] = [];
  if (picked) parts.push(picked);
  const otherFilters = search.where.filter(
    (f) => !(f.field === "name" && f.op === "="),
  );
  if (otherFilters.length)
    parts.push(
      `${otherFilters.length} filter${otherFilters.length === 1 ? "" : "s"}`,
    );
  if (search.group_by.length)
    parts.push(`group by ${search.group_by.join(", ")}`);
  return parts.join(" · ");
}
