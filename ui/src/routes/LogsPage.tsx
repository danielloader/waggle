import { useEffect, useState } from "react";
import { useQuery } from "@tanstack/react-query";
import { useNavigate } from "@tanstack/react-router";
import { Accordion } from "../components/ui/Accordion";
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
import { logsRoute } from "../router";

// Hysteresis thresholds for the scroll-driven accordion collapse. Collapse
// once the user is clearly scrolling (>120px); expand only when they're
// back near the top (<20px). The gap keeps the accordions from flickering
// when you hover around the trigger point.
const COLLAPSE_AT = 120;
const EXPAND_AT = 20;

export function LogsPage() {
  const search = logsRoute.useSearch();
  const navigate = useNavigate();
  // Two accordions: the query builder and the chart. Both default open;
  // either the user clicks to toggle, or scrolling Explore Data past the
  // threshold collapses both, and scrolling back to the top reopens them.
  const [queryOpen, setQueryOpen] = useState(true);
  const [chartOpen, setChartOpen] = useState(true);

  // Reset on tab change — Explore is the only tab whose scroll drives the
  // collapse, so switching away should restore both accordions.
  useEffect(() => {
    setQueryOpen(true);
    setChartOpen(true);
  }, [search.tab]);

  const handleExploreScrollY = (y: number) => {
    if (y > COLLAPSE_AT) {
      setQueryOpen((o) => (o ? false : o));
      setChartOpen((o) => (o ? false : o));
    } else if (y < EXPAND_AT) {
      setQueryOpen((o) => (o ? o : true));
      setChartOpen((o) => (o ? o : true));
    }
  };

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
      <Accordion
        label="Query"
        open={queryOpen}
        onToggle={() => setQueryOpen((o) => !o)}
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
