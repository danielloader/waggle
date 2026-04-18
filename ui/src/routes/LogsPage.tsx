import { useEffect, useRef, useState } from "react";
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
import { logsRoute } from "../router";

// Scroll distance (in Explore-Data pixels) over which the query/chart
// accordions collapse linearly from fully open to fully closed. Shorter
// → more aggressive; longer → gentler.
const COLLAPSE_DISTANCE = 220;

export function LogsPage() {
  const search = logsRoute.useSearch();
  const navigate = useNavigate();
  // Two accordions: the query builder and the chart. Both default open.
  // Click toggles the manual open/closed state; the scroll-driven progress
  // below multiplies the effective openness when open, so a user who's
  // scrolled halfway sees the accordion at half height.
  const [queryOpen, setQueryOpen] = useState(true);
  const [chartOpen, setChartOpen] = useState(true);
  const [scrollProgress, setScrollProgress] = useState(0);
  const rafRef = useRef<number | null>(null);

  // Reset on tab change — Explore is the only tab whose scroll drives the
  // collapse, so switching away should restore both accordions and drop
  // any in-flight progress.
  useEffect(() => {
    setQueryOpen(true);
    setChartOpen(true);
    setScrollProgress(0);
  }, [search.tab]);

  // Clean up any queued rAF on unmount.
  useEffect(
    () => () => {
      if (rafRef.current !== null) cancelAnimationFrame(rafRef.current);
    },
    [],
  );

  const handleExploreScrollY = (y: number) => {
    // Coalesce rapid scroll events into at most one setState per frame —
    // otherwise a single physical scroll fires ~10 setStates and thrashes
    // the React tree unnecessarily.
    if (rafRef.current !== null) cancelAnimationFrame(rafRef.current);
    rafRef.current = requestAnimationFrame(() => {
      rafRef.current = null;
      const p = Math.max(0, Math.min(1, y / COLLAPSE_DISTANCE));
      setScrollProgress((prev) => (prev === p ? prev : p));
    });
  };

  const reopen = (set: (v: boolean) => void) => {
    // Clicking to open a scroll-collapsed accordion is useless if scroll
    // progress stays at 1 — the multiplier would pin openness at 0. Reset
    // progress so the click actually reveals content; the next real scroll
    // event will re-collapse proportionally.
    setScrollProgress(0);
    set(true);
  };

  const setSearch = (next: QuerySearch) =>
    navigate({ to: "/logs", search: next as unknown as Record<string, unknown> });

  useRefreshPersistence(search, setSearch);

  const chart = useQuery({
    queryKey: ["query", "logs", search],
    queryFn: ({ signal }) => runQuery(buildCountQuery("logs", search), signal),
    refetchInterval: refreshIntervalMs(search.refresh),
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
      <Accordion
        label="Query"
        open={queryOpen}
        collapseProgress={scrollProgress}
        onToggle={() => (queryOpen ? setQueryOpen(false) : reopen(setQueryOpen))}
        collapsedSummary={querySummary(search)}
      >
        <DefinePanel
          dataset="logs"
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
      </Accordion>
      <div className="flex-1 overflow-hidden">
        <ResultTabs
          dataset="logs"
          search={search}
          onTabChange={(tab) => setSearch({ ...search, tab })}
          onExploreScrollY={handleExploreScrollY}
        />
      </div>
    </div>
  );
}

// Short, at-a-glance summary of the current query — shown on the collapsed
// accordion header so the user can tell what's active without expanding.
function querySummary(search: QuerySearch): string {
  const parts: string[] = [];
  if (search.where.length) parts.push(`${search.where.length} filter${search.where.length === 1 ? "" : "s"}`);
  if (search.group_by.length) parts.push(`group by ${search.group_by.join(", ")}`);
  return parts.join(" · ");
}
