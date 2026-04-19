import { Play, RotateCw } from "lucide-react";
import type { Dataset } from "../../lib/query";
import type { QuerySearch } from "../../lib/query";
import {
  DATASETS,
  selectOrDefault,
  summarizeFilters,
  summarizeGroupBy,
  summarizeOrderBy,
  summarizeSelect,
} from "../../lib/query";
import { TimeRangePicker } from "./TimeRangePicker";
import { DefineCell } from "./DefineCell";
import { SelectEditor } from "./SelectEditor";
import { WhereEditor } from "./WhereEditor";
import { GroupByEditor } from "./GroupByEditor";
import { OrderByEditor } from "./OrderByEditor";
import { LimitEditor } from "./LimitEditor";
import { ServicePicker } from "./ServicePicker";

interface Props {
  dataset: Dataset;
  search: QuerySearch;
  onChange: (next: QuerySearch) => void;
  onRun: () => void;
  isRunning?: boolean;
}

/**
 * Honeycomb-style "Define" panel: a 3×2 grid of cells (SELECT / WHERE /
 * GROUP BY above, ORDER BY / HAVING / LIMIT below). Each cell shows its
 * current value as a one-line summary and opens a popover editor on click.
 * Time range picker sits outside the grid next to the title so it's always
 * visible, and the Run button is right-aligned against the second row.
 */
export function DefinePanel({ dataset, search, onChange, onRun, isRunning }: Props) {
  // Any service filter set in WHERE scopes the field autocomplete in the
  // GROUP BY picker / AddFilterButton so we only fetch relevant keys.
  const serviceFilter = search.where.find(
    (f) => f.field === "service.name" && f.op === "=",
  );
  const service = typeof serviceFilter?.value === "string" ? serviceFilter.value : undefined;

  const datasetLabel = dataset;
  const suggestionsForOrder = [
    ...selectOrDefault(search.select).map((a) =>
      a.op === "count" ? "count" : `${a.op}_${(a.field ?? "").replace(/[^a-zA-Z0-9_]/g, "_")}`,
    ),
    ...search.group_by.map((g) => g.replace(/[^a-zA-Z0-9_]/g, "_")),
  ];

  return (
    <div
      className="sticky top-0 z-20 border-b"
      style={{ background: "var(--color-surface)", borderColor: "var(--color-border)" }}
    >
      {/* Title row */}
      <div className="flex items-center justify-between px-4 pt-3 pb-1 gap-3">
        <div className="flex items-center gap-2 flex-wrap">
          <h1 className="text-lg font-semibold">Query in</h1>
          <DatasetSelect
            value={datasetLabel}
            onChange={(d) => onChange({ ...search, dataset: d })}
          />
          <span style={{ color: "var(--color-ink-muted)" }}>·</span>
          <ServicePicker
            where={search.where}
            onChange={(where) => onChange({ ...search, where })}
          />
        </div>
        <TimeRangePicker
          search={search}
          onChange={(patch) => onChange({ ...search, ...patch })}
        />
      </div>

      {/* Define panel — shadowed card, no outline border; the accordion
          header already names it "QUERY", so no inner sub-header. The
          Run button is tucked against the top-right so it's always the
          same fixed target regardless of cell content. */}
      <div
        className="mx-4 mb-3 rounded-md relative"
        style={{
          background: "var(--color-card)",
          boxShadow: "var(--shadow-card)",
        }}
      >
        <button
          type="button"
          onClick={onRun}
          className="absolute top-2 right-2 flex items-center gap-1.5 px-3 py-1.5 rounded-md text-sm font-medium text-white z-10"
          style={{ background: "var(--color-accent)" }}
        >
          {isRunning ? <RotateCw className="w-4 h-4 animate-spin" /> : <Play className="w-4 h-4" />}
          Run
        </button>

        <div
          className="grid grid-cols-3 gap-4 px-5 border-b"
          style={{ borderColor: "var(--color-border-subtle)" }}
        >
          <DefineCell
            label="Select"
            description="What to compute. COUNT returns one number per result row; aggregations like P95, AVG, SUM, MIN/MAX, and COUNT_DISTINCT take a field and reduce its values. An empty Select returns raw events instead."
            isEmpty={false}
            value={summarizeSelect(search.select, dataset)}
            editor={
              <SelectEditor
                select={search.select}
                onChange={(select) => onChange({ ...search, select })}
              />
            }
          />
          <DefineCell
            label="Where"
            description="Row-level filter applied BEFORE aggregation. Events that don't match are dropped, so they won't be counted or included in percentiles. Filter on first-class fields (service.name, http.route) or any attribute key."
            isEmpty={search.where.length === 0}
            placeholder="None; include all events"
            value={summarizeFilters(search.where, "None; include all events")}
            editor={
              <WhereEditor
                dataset={dataset}
                service={service}
                filters={search.where}
                onChange={(where) => onChange({ ...search, where })}
              />
            }
          />
          <DefineCell
            label="Group by"
            description="Split results into one row per unique value of the chosen field(s) — each aggregation in Select is computed separately for each group. Example: group by service.name + aggregate P95(duration_ns) → one p95 per service."
            isEmpty={search.group_by.length === 0}
            placeholder="None; don't segment"
            value={summarizeGroupBy(search.group_by)}
            editor={
              <GroupByEditor
                dataset={dataset}
                service={service}
                groupBy={search.group_by}
                onChange={(group_by) => onChange({ ...search, group_by })}
              />
            }
          />
        </div>

        <div className="grid grid-cols-3 gap-4 px-5">
          <DefineCell
            label="Order by"
            description="Sort the result rows. Reference an aggregation alias (count, p95_duration_ns, …) or a Group by field. Pair with Limit to get top-N."
            isEmpty={search.order_by.length === 0}
            placeholder="None"
            value={summarizeOrderBy(search.order_by)}
            editor={
              <OrderByEditor
                orderBy={search.order_by}
                onChange={(order_by) => onChange({ ...search, order_by })}
                suggestions={suggestionsForOrder}
              />
            }
          />
          <DefineCell
            label="Having"
            description="Group-level filter applied AFTER aggregation. Use it to keep only groups whose computed value matches, e.g. count > 100 or p95_duration_ns > 500000000. Where can't do this because it filters events before they're grouped."
            isEmpty={search.having.length === 0}
            placeholder="None; include all results"
            value={summarizeFilters(search.having, "None; include all results")}
            editor={
              <WhereEditor
                dataset={dataset}
                service={service}
                title="Having"
                filters={search.having}
                onChange={(having) => onChange({ ...search, having })}
              />
            }
          />
          <DefineCell
            label="Limit"
            description="Maximum number of result rows returned. Defaults to 1000. Combine with Order by to keep the top-N slowest / noisiest / busiest and drop the rest."
            isEmpty={search.limit === undefined}
            placeholder="1000"
            value={String(search.limit ?? "1000")}
            editor={
              <LimitEditor
                limit={search.limit}
                onChange={(limit) => onChange({ ...search, limit })}
              />
            }
          />
        </div>
      </div>
    </div>
  );
}

function DatasetSelect({
  value,
  onChange,
}: {
  value: Dataset;
  onChange: (next: Dataset) => void;
}) {
  return (
    <div className="relative">
      <select
        value={value}
        onChange={(e) => onChange(e.target.value as Dataset)}
        className="appearance-none pl-2.5 pr-6 py-0.5 rounded border text-sm font-medium cursor-pointer"
        style={{
          background: "var(--color-card)",
          borderColor: "var(--color-border)",
          color: "var(--color-ink)",
        }}
      >
        {DATASETS.map((d) => (
          <option key={d} value={d}>
            {d}
          </option>
        ))}
      </select>
      <span
        className="pointer-events-none absolute right-1.5 top-1/2 -translate-y-1/2 text-xs"
        style={{ color: "var(--color-ink-muted)" }}
      >
        ▾
      </span>
    </div>
  );
}
