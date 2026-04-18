import { useEffect, useRef, useState, useMemo } from "react";
import { useQuery } from "@tanstack/react-query";
import { useNavigate } from "@tanstack/react-router";
import { Accordion } from "../components/ui/Accordion";
import { DefinePanel } from "../features/query/DefinePanel";
import { QueryChart } from "../features/query/QueryChart";
import { ResultTabs } from "../features/query/ResultTabs";
import { MetricPicker } from "../features/query/MetricPicker";
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
 * Metrics explorer. Top-of-page picker selects a metric by name; the
 * name+kind land as a WHERE filter pinned to the rest of the query, so
 * the chart and tables only show that metric's data. Everything below
 * (DefinePanel, accordions, tabs) reuses the shared components the
 * traces/logs pages use — metrics is just another `dataset` to the
 * query engine.
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

  // The picker drives the query by pinning a `name = <metric>` WHERE
  // filter. Whatever filters the user has set in the Define panel stay
  // orthogonal to the picker.
  const pickedName = useMemo(() => {
    const nameFilter = search.where.find(
      (f) => f.field === "name" && f.op === "=",
    );
    return typeof nameFilter?.value === "string" ? nameFilter.value : "";
  }, [search.where]);

  const chart = useQuery({
    // Gate the chart on having picked a metric. Metrics datasets without
    // a name filter would try to aggregate across every series in the
    // store — usually not what the user wants.
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
        <div
          className="flex items-center gap-3 px-4 pt-3"
          style={{ color: "var(--color-ink-muted)" }}
        >
          <span className="text-sm">Metric</span>
          <MetricPicker
            value={pickedName}
            onChange={(m) => {
              const rest = search.where.filter(
                (f) => !(f.field === "name" && f.op === "="),
              );
              if (m) {
                setSearch({
                  ...search,
                  where: [...rest, { field: "name", op: "=", value: m.name }],
                });
              } else {
                setSearch({ ...search, where: rest });
              }
            }}
          />
        </div>
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
              className="flex items-center justify-center text-sm"
              style={{ color: "var(--color-ink-muted)", height: 125 }}
            >
              Pick a metric above to chart it.
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
