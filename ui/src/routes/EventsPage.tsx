import { useEffect, useRef, useState } from "react";
import { useQuery } from "@tanstack/react-query";
import { useNavigate } from "@tanstack/react-router";
import { Settings2 } from "lucide-react";
import { Accordion } from "../components/ui/Accordion";
import { Popover } from "../components/ui/Popover";
import { DefinePanel } from "../features/query/DefinePanel";
import {
  aggregationIndices,
  QueryChart,
  type MissingValuesMode,
} from "../features/query/QueryChart";
import { ResultTabs } from "../features/query/ResultTabs";
import {
  bucketMsFor,
  buildCountQuery,
  refreshIntervalMs,
  resolveSearchRange,
  runQuery,
  type QueryResult,
  type QuerySearch,
} from "../lib/query";
import { useRefreshPersistence } from "../lib/refreshPersistence";
import { eventsRoute } from "../router";

// Scroll distance (in Explore-Data pixels) over which the query/chart
// accordions collapse linearly from fully open to fully closed.
const COLLAPSE_DISTANCE = 220;

/**
 * Unified events page. Supersedes the per-signal /traces, /logs, /metrics
 * pages — the signal type is just one more filter the user can set. The
 * dataset selector inside the Define panel switches between "events"
 * (all signals), "spans", "logs", "metrics"; that choice drives a
 * signal_type='…' preset filter on the backend + the chart gate for
 * metrics (which only makes sense scoped to a single metric name).
 */
export function EventsPage() {
  const search = eventsRoute.useSearch();
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
    navigate({ to: "/events", search: next as unknown as Record<string, unknown> });

  // Switching datasets invalidates most of the query shape — SELECT
  // items almost always reference fields that only exist on the old
  // dataset (e.g. MAX(requests.total) on spans yields NULLs; p99
  // (duration_ns) on metrics references a column metric_events doesn't
  // have). Same for WHERE and GROUP BY. Reset those; keep the time
  // controls, refresh cadence, and active tab so the user's window
  // context carries over.
  const changeDataset = (next: typeof search.dataset) => {
    setSearch({
      dataset: next,
      range: search.range,
      from: search.from,
      to: search.to,
      granularity: search.granularity,
      refresh: search.refresh,
      tab: search.tab,
      select: [],
      where: [],
      group_by: [],
      order_by: [],
      having: [],
      limit: search.limit,
    });
  };

  useRefreshPersistence(search, setSearch);

  const chart = useQuery({
    queryKey: ["query", search.dataset, search],
    queryFn: ({ signal }) =>
      runQuery(buildCountQuery(search.dataset, search), signal),
    refetchInterval: refreshIntervalMs(search.refresh),
    // For metrics the default COUNT-of-metric-events isn't informative on
    // its own — the user needs to pick a specific metric field (e.g.
    // MAX(requests.total)) before the chart means anything. Disable the
    // fetch until they have.
    enabled: search.dataset !== "metrics" || hasMetricField(search),
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

  const metricsNeedsField =
    search.dataset === "metrics" && !hasMetricField(search);

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
          dataset={search.dataset}
          search={search}
          onChange={setSearch}
          onDatasetChange={changeDataset}
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
          {metricsNeedsField ? (
            <div
              className="flex items-center justify-center text-sm px-4 text-center"
              style={{ color: "var(--color-ink-muted)", height: 125 }}
            >
              Add a metric field to Select — e.g.{" "}
              <code className="mx-1 font-mono">MAX(requests.total)</code>{" "}
              or <code className="mx-1 font-mono">P99(memory.used)</code>.
            </div>
          ) : (
            <ChartStack
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
          dataset={search.dataset}
          search={search}
          onTabChange={(tab) => setSearch({ ...search, tab })}
          onExploreScrollY={handleExploreScrollY}
        />
      </div>
    </div>
  );
}

// hasMetricField reports whether the user's SELECT references a metric
// field. Under the folded metric_events model a metric's name is just
// an attribute key (not a row-identifier), so "the user is asking about
// a metric" = "the user put a field-bound aggregation in Select".
function hasMetricField(search: QuerySearch): boolean {
  return search.select.some((a) => a.field !== undefined && a.field !== "");
}

// ChartStack renders one chart per SELECT aggregation. Multi-metric queries
// like `COUNT + P99(duration_ns)` previously collapsed onto one y-axis with
// wildly incompatible scales; splitting into stacked charts gives each
// aggregation its own y-axis and its own labelled header. All charts share
// the time window / bucket size so they stay aligned.
interface ChartSettings {
  missingValues: MissingValuesMode;
}

const DEFAULT_CHART_SETTINGS: ChartSettings = { missingValues: "auto" };

function ChartStack({
  result,
  loading,
  error,
  bucketMs,
  fromMs,
  toMs,
  onBucketClick,
}: {
  result: QueryResult | undefined;
  loading: boolean;
  error: unknown;
  bucketMs: number;
  fromMs: number;
  toMs: number;
  onBucketClick?: (tMs: number) => void;
}) {
  const aggs = aggregationIndices(result);
  // Per-chart settings keyed by the aggregation alias (stable across
  // ephemeral re-renders; resets when the user changes the query). Not
  // currently persisted to URL — deliberate for now, the Edit Chart
  // controls are exploratory rather than something users need to share.
  const [settingsByLabel, setSettingsByLabel] = useState<
    Record<string, ChartSettings>
  >({});

  const updateSetting = (label: string, patch: Partial<ChartSettings>) =>
    setSettingsByLabel((prev) => ({
      ...prev,
      [label]: { ...(prev[label] ?? DEFAULT_CHART_SETTINGS), ...patch },
    }));

  // While the query is in-flight (and we still have no prior result), we
  // fall back to a single chart so the loading spinner appears where the
  // chart will land.
  if (aggs.length === 0) {
    const settings = settingsByLabel["__default__"] ?? DEFAULT_CHART_SETTINGS;
    return (
      <ChartWithEdit
        label=""
        settings={settings}
        onSettingsChange={(patch) => updateSetting("__default__", patch)}
      >
        <QueryChart
          result={result}
          loading={loading}
          error={error}
          bucketMs={bucketMs}
          fromMs={fromMs}
          toMs={toMs}
          onBucketClick={onBucketClick}
          missingValues={settings.missingValues}
        />
      </ChartWithEdit>
    );
  }
  return (
    <div className="flex flex-col gap-4">
      {aggs.map((a) => {
        const settings = settingsByLabel[a.label] ?? DEFAULT_CHART_SETTINGS;
        return (
          <ChartWithEdit
            key={a.idx}
            label={a.label}
            settings={settings}
            onSettingsChange={(patch) => updateSetting(a.label, patch)}
          >
            <QueryChart
              result={result}
              loading={loading}
              error={error}
              bucketMs={bucketMs}
              fromMs={fromMs}
              toMs={toMs}
              onBucketClick={onBucketClick}
              aggIdx={a.idx}
              missingValues={settings.missingValues}
            />
          </ChartWithEdit>
        );
      })}
    </div>
  );
}

function ChartWithEdit({
  label,
  settings,
  onSettingsChange,
  children,
}: {
  label: string;
  settings: ChartSettings;
  onSettingsChange: (patch: Partial<ChartSettings>) => void;
  children: React.ReactNode;
}) {
  return (
    <div>
      <div className="flex items-center justify-between pb-1 px-1">
        <div
          className="text-[11px] uppercase tracking-wide font-medium"
          style={{ color: "var(--color-ink-muted)" }}
        >
          {label}
        </div>
        <Popover
          align="end"
          trigger={
            <button
              type="button"
              className="p-1 rounded cursor-pointer hover:bg-card-hover"
              title="Edit chart"
              aria-label="Edit chart"
            >
              <Settings2
                className="w-3.5 h-3.5"
                style={{ color: "var(--color-ink-muted)" }}
              />
            </button>
          }
        >
          <ChartSettingsEditor
            settings={settings}
            onChange={onSettingsChange}
          />
        </Popover>
      </div>
      {children}
    </div>
  );
}

function ChartSettingsEditor({
  settings,
  onChange,
}: {
  settings: ChartSettings;
  onChange: (patch: Partial<ChartSettings>) => void;
}) {
  return (
    <div className="flex flex-col gap-3 p-1 min-w-55">
      <fieldset className="flex flex-col gap-1">
        <legend
          className="text-[11px] uppercase tracking-wide font-medium pb-1"
          style={{ color: "var(--color-ink-muted)" }}
        >
          Missing values
        </legend>
        {(
          [
            { v: "auto", label: "Auto (smart default)" },
            { v: "zero", label: "Fill with zeros" },
            { v: "omit", label: "Omit missing values" },
          ] as const
        ).map((opt) => (
          <label key={opt.v} className="flex items-center gap-2 text-sm cursor-pointer">
            <input
              type="radio"
              name="missing"
              value={opt.v}
              checked={settings.missingValues === opt.v}
              onChange={() => onChange({ missingValues: opt.v })}
            />
            {opt.label}
          </label>
        ))}
      </fieldset>
    </div>
  );
}

function querySummary(search: QuerySearch): string {
  const parts: string[] = [search.dataset];
  if (search.where.length)
    parts.push(`${search.where.length} filter${search.where.length === 1 ? "" : "s"}`);
  if (search.group_by.length)
    parts.push(`group by ${search.group_by.join(", ")}`);
  return parts.join(" · ");
}

