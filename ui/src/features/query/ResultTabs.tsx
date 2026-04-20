import clsx from "clsx";
import type { Dataset, QuerySearch } from "../../lib/query";
import { OverviewTab } from "./OverviewTab";
import { EventsTable, type SelectedRow } from "./EventsTable";
import { TracesTab } from "./TracesTab";

type TabID = "overview" | "traces" | "explore";

interface Props {
  dataset: Dataset;
  /** URL-driven search — used only for tab routing state, never for queries. */
  search: QuerySearch;
  /** The last-committed query params (set when the user clicks Run). All
   *  tab queries execute against this, not the live edit state. */
  querySearch: QuerySearch;
  /** Increments on every Run click, forcing a refetch even when params
   *  are unchanged since the last run. */
  runCount: number;
  onTabChange: (tab: TabID) => void;
  /** Optional slot for a logs-only FTS search input rendered next to tabs. */
  rightSlot?: React.ReactNode;
  /** Scroll callback from the Explore Data tab — the page uses it to
   *  collapse the query/chart header once the user drills in. */
  onExploreScrollY?: (y: number) => void;
  /** Row selection for the Explore Data tab's detail pane. Lifted to
   *  the page so the pane can render as a full-height sibling of the
   *  main content column rather than cramped inside the tab body. */
  selectedRow?: SelectedRow | null;
  onSelectRow?: (next: SelectedRow | null) => void;
}

type Tab = { id: TabID; label: string; datasets?: Dataset[] };

// The Traces tab only makes sense on the spans dataset — it's top-N by
// root-span duration. Logs don't have a trace/root concept we can sort by.
const TABS: Tab[] = [
  { id: "overview", label: "Overview" },
  { id: "traces", label: "Traces", datasets: ["spans"] },
  { id: "explore", label: "Explore Data" },
];

/**
 * Tab shell that sits below the chart. Overview is the default, cheap tab —
 * it just rolls up the existing aggregation across the time range. Explore
 * Data lazily mounts the events table, so the heavier raw-rows query only
 * runs when the user actually opens that tab.
 */
export function ResultTabs({
  dataset,
  search,
  querySearch,
  runCount,
  onTabChange,
  rightSlot,
  onExploreScrollY,
  selectedRow,
  onSelectRow,
}: Props) {
  const visibleTabs = TABS.filter(
    (t) => !t.datasets || t.datasets.includes(dataset),
  );
  // Fall back to "overview" if the persisted tab is no longer valid for the
  // current dataset (e.g. user switched from spans → logs with `traces`
  // selected).
  const requested = search.tab ?? "overview";
  const active: TabID = visibleTabs.some((t) => t.id === requested)
    ? requested
    : "overview";

  return (
    <div className="h-full flex flex-col">
      {/* Tab bar gets its own top + bottom border so it reads as a
          distinct strip wedged between the chart (or accordion header,
          when chart is collapsed) and the results below. */}
      <div
        className="flex items-center justify-between px-4 border-t border-b"
        style={{
          background: "var(--color-surface)",
          borderColor: "var(--color-border)",
        }}
      >
        <nav className="flex items-center gap-1">
          {visibleTabs.map((t) => (
            <button
              key={t.id}
              type="button"
              onClick={() => onTabChange(t.id)}
              className={clsx(
                "px-3 py-2 text-sm border-b-2 -mb-px transition-colors",
                active === t.id
                  ? "font-semibold"
                  : "font-normal hover:bg-[var(--color-surface-hover)]",
              )}
              style={{
                borderColor:
                  active === t.id ? "var(--color-accent)" : "transparent",
                color: active === t.id ? "var(--color-accent)" : undefined,
              }}
            >
              {t.label}
            </button>
          ))}
        </nav>
        {rightSlot ? <div className="py-1">{rightSlot}</div> : null}
      </div>
      <div className="flex-1 overflow-hidden">
        {active === "overview" && <OverviewTab dataset={dataset} querySearch={querySearch} runCount={runCount} />}
        {active === "traces" && <TracesTab querySearch={querySearch} runCount={runCount} />}
        {active === "explore" && (
          <EventsTable
            dataset={dataset}
            querySearch={querySearch}
            runCount={runCount}
            onScrollY={onExploreScrollY}
            selected={selectedRow ?? null}
            onSelect={onSelectRow}
          />
        )}
      </div>
    </div>
  );
}
