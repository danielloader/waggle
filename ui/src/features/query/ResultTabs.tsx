import clsx from "clsx";
import type { Dataset, QuerySearch } from "../../lib/query";
import { OverviewTab } from "./OverviewTab";
import { EventsTable } from "./EventsTable";
import { TracesTab } from "./TracesTab";

type TabID = "overview" | "traces" | "explore";

interface Props {
  dataset: Dataset;
  search: QuerySearch;
  onTabChange: (tab: TabID) => void;
  /** Optional slot for a logs-only FTS search input rendered next to tabs. */
  rightSlot?: React.ReactNode;
  /** Scroll callback from the Explore Data tab — the page uses it to
   *  collapse the query/chart header once the user drills in. */
  onExploreScrollY?: (y: number) => void;
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
  onTabChange,
  rightSlot,
  onExploreScrollY,
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
        {active === "overview" && <OverviewTab dataset={dataset} search={search} />}
        {active === "traces" && <TracesTab search={search} />}
        {active === "explore" && (
          <EventsTable
            dataset={dataset}
            search={search}
            onScrollY={onExploreScrollY}
          />
        )}
      </div>
    </div>
  );
}
