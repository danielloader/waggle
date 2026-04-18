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
import { tracesRoute } from "../router";

// See LogsPage — scroll distance over which the accordions collapse.
const COLLAPSE_DISTANCE = 220;

export function TracesPage() {
  const search = tracesRoute.useSearch();
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
    navigate({ to: "/traces", search: next as unknown as Record<string, unknown> });

  const chart = useQuery({
    queryKey: ["query", "spans", search],
    queryFn: ({ signal }) => runQuery(buildCountQuery("spans", search), signal),
    refetchInterval: refreshIntervalMs(search.refresh),
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
      <Accordion
        label="Query"
        open={queryOpen}
        collapseProgress={scrollProgress}
        onToggle={() => (queryOpen ? setQueryOpen(false) : reopen(setQueryOpen))}
        collapsedSummary={querySummary(search)}
      >
        <DefinePanel
          dataset="spans"
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
          dataset="spans"
          search={search}
          onTabChange={(tab) => setSearch({ ...search, tab })}
          onExploreScrollY={handleExploreScrollY}
        />
      </div>
    </div>
  );
}

function querySummary(search: QuerySearch): string {
  const parts: string[] = [];
  if (search.where.length) parts.push(`${search.where.length} filter${search.where.length === 1 ? "" : "s"}`);
  if (search.group_by.length) parts.push(`group by ${search.group_by.join(", ")}`);
  return parts.join(" · ");
}
