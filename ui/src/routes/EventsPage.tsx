import { useEffect, useRef, useState } from "react";
import { useQuery } from "@tanstack/react-query";
import { useNavigate, useSearch } from "@tanstack/react-router";
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
  type Dataset,
  type QuerySearch,
} from "../lib/query";
import { useRefreshPersistence } from "../lib/refreshPersistence";

// Scroll distance (in Explore-Data pixels) over which the query/chart
// accordions collapse linearly from fully open to fully closed.
const COLLAPSE_DISTANCE = 220;

export type EventsPagePath = "/traces" | "/logs" | "/metrics" | "/events";

interface Props {
  /** Signal the route pins this page to. The Define panel's dataset pill
   *  switches by navigating to a sibling route, so this is always the
   *  dataset of the current URL. */
  dataset: Dataset;
  /** Current route path — used to compose `navigate({ to: path, … })`
   *  calls so search-state mutations stay on the right URL. */
  path: EventsPagePath;
}

/**
 * Unified query/chart/results page. Four routes (/traces, /logs, /metrics,
 * /events) all render this component with different `dataset` props —
 * signal-specific navigation with no code duplication.
 */
export function EventsPage({ dataset, path }: Props) {
  // Pull the search params without binding to a specific route — that way
  // one component can live under any of the four peer routes.
  const search = useSearch({ strict: false }) as QuerySearch;
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
    navigate({
      to: path,
      search: next as unknown as Record<string, unknown>,
    });

  // Switching dataset = navigating to the sibling route. Preserve the
  // rest of the search state so filters carry across.
  const changeDataset = (d: Dataset) => {
    const next: EventsPagePath =
      d === "spans" ? "/traces"
      : d === "logs" ? "/logs"
      : d === "metrics" ? "/metrics"
      : "/events";
    navigate({ to: next, search: search as unknown as Record<string, unknown> });
  };

  useRefreshPersistence(search, setSearch);

  const chart = useQuery({
    queryKey: ["query", dataset, search],
    queryFn: ({ signal }) => runQuery(buildCountQuery(dataset, search), signal),
    refetchInterval: refreshIntervalMs(search.refresh),
    // Metrics across "all metrics in the store" is meaningless — gate on
    // a `name = …` filter before we issue the query.
    enabled: dataset !== "metrics" || pickedMetricName(search) !== "",
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

  const metricsNeedsName =
    dataset === "metrics" && pickedMetricName(search) === "";

  return (
    <div className="h-full flex flex-col">
      <Accordion
        label="Query"
        open={queryOpen}
        collapseProgress={scrollProgress}
        onToggle={() => (queryOpen ? setQueryOpen(false) : reopen(setQueryOpen))}
        collapsedSummary={querySummary(dataset, search)}
      >
        <DefinePanel
          dataset={dataset}
          onDatasetChange={changeDataset}
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
          {metricsNeedsName ? (
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
          dataset={dataset}
          search={search}
          onTabChange={(tab) => setSearch({ ...search, tab })}
          onExploreScrollY={handleExploreScrollY}
        />
      </div>
    </div>
  );
}

function pickedMetricName(search: QuerySearch): string {
  const f = search.where.find((f) => f.field === "name" && f.op === "=");
  return typeof f?.value === "string" ? f.value : "";
}

function querySummary(dataset: Dataset, search: QuerySearch): string {
  const parts: string[] = [dataset];
  if (search.where.length)
    parts.push(`${search.where.length} filter${search.where.length === 1 ? "" : "s"}`);
  if (search.group_by.length)
    parts.push(`group by ${search.group_by.join(", ")}`);
  return parts.join(" · ");
}
