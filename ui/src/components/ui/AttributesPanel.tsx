import { useState } from "react";
import { useNavigate } from "@tanstack/react-router";
import { Filter as FilterIcon, Search } from "lucide-react";
import type { Filter } from "../../lib/query";

export interface AttrRow {
  key: string;
  value: unknown;
  type: string;
}

interface Props {
  /** Pre-built rows. Use when the caller has already merged multiple
   *  sources (e.g. span meta + resource + attrs). */
  rows?: AttrRow[];
  /** Raw JSON string — parsed and sorted alphabetically. Use when the
   *  caller just has the backend-supplied attributes blob. */
  attributesJson?: string;
  /** Placeholder for the filter input. */
  filterPlaceholder?: string;
  /**
   * Where clicking the filter button on a scalar row navigates to. In the
   * unified-events world this is always /events, but span-detail still
   * links to /traces for backward-compat; leave the union open.
   */
  filterTarget?: "/events" | "/traces" | "/logs";
}

/**
 * Flat, filterable list of key-value attributes. Shared between the span
 * detail panel and the logs Explore-Data row expander so both surfaces
 * show structured data in a consistent way. Scalar rows get a
 * hover-reveal filter button that jumps to the query builder with an
 * equality filter on that field — parity with Honeycomb's "click to
 * filter" workflow.
 */
export function AttributesPanel({
  rows,
  attributesJson,
  filterPlaceholder = "Filter fields and values",
  filterTarget = "/events",
}: Props) {
  const navigate = useNavigate();
  const [filter, setFilter] = useState("");

  const allRows = rows ?? parseAttributes(attributesJson ?? "{}");

  const applyFilter = (row: AttrRow) => {
    const value = filterableValue(row);
    if (value === null) return;
    const f: Filter = { field: row.key, op: "=", value };
    navigate({
      to: filterTarget,
      search: { where: [f] } as unknown as Record<string, unknown>,
    });
  };

  const f = filter.toLowerCase();
  const filtered =
    f === ""
      ? allRows
      : allRows.filter(
          (r) =>
            r.key.toLowerCase().includes(f) ||
            String(r.value).toLowerCase().includes(f),
        );

  return (
    <div className="p-3 flex flex-col gap-3">
      <div
        className="flex items-center gap-2 px-2 py-1 rounded border sticky top-0"
        style={{
          borderColor: "var(--color-border)",
          background: "var(--color-surface)",
        }}
      >
        <Search className="w-3.5 h-3.5" style={{ color: "var(--color-ink-muted)" }} />
        <input
          value={filter}
          onChange={(e) => setFilter(e.target.value)}
          placeholder={filterPlaceholder}
          className="bg-transparent outline-none text-sm flex-1"
        />
      </div>
      <div className="flex flex-col">
        {filtered.map((r) => {
          const canFilter = filterableValue(r) !== null;
          return (
            <div
              key={r.key}
              className="group flex flex-col gap-0.5 py-2 border-b"
              style={{ borderColor: "var(--color-border)" }}
            >
              <div className="flex items-center gap-1">
                <TypePill type={r.type} />
                <span
                  className="text-xs font-mono truncate"
                  style={{ color: "var(--color-ink-muted)" }}
                >
                  {r.key}
                </span>
                {canFilter && (
                  <button
                    type="button"
                    onClick={() => applyFilter(r)}
                    className="ml-auto opacity-0 group-hover:opacity-100 shrink-0 p-1 rounded hover:bg-[var(--color-card-hover)]"
                    title={`Filter ${filterTarget} to ${r.key} = ${String(r.value)}`}
                  >
                    <FilterIcon
                      className="w-3 h-3"
                      style={{ color: "var(--color-accent)" }}
                    />
                  </button>
                )}
              </div>
              <div className="text-xs font-mono break-all">
                {formatValue(r.value)}
              </div>
            </div>
          );
        })}
        {filtered.length === 0 && (
          <div
            className="py-3 text-xs text-center"
            style={{ color: "var(--color-ink-muted)" }}
          >
            No matching fields.
          </div>
        )}
      </div>
    </div>
  );
}

function TypePill({ type }: { type: string }) {
  return (
    <span
      className="inline-block text-[9px] uppercase tracking-wide mr-2 px-1 rounded"
      style={{
        background: "var(--color-surface-muted)",
        color: "var(--color-ink-muted)",
      }}
    >
      {type}
    </span>
  );
}

function filterableValue(row: AttrRow): string | number | boolean | null {
  const v = row.value;
  if (v === null || v === undefined) return null;
  if (typeof v === "string" || typeof v === "number" || typeof v === "boolean") return v;
  return null;
}

/**
 * Deterministic alphabetical sort. Case-insensitive codepoint comparison
 * avoids the surprises of locale-aware sorts where "." and "_" can flip
 * order depending on the user's Intl config.
 */
export function compareByKey(a: AttrRow, b: AttrRow): number {
  const ka = a.key.toLowerCase();
  const kb = b.key.toLowerCase();
  if (ka < kb) return -1;
  if (ka > kb) return 1;
  return 0;
}

export function parseAttributes(json: string): AttrRow[] {
  if (!json || json === "{}") return [];
  try {
    const obj = JSON.parse(json) as Record<string, unknown>;
    return Object.entries(obj)
      .map(([key, value]) => ({ key, value, type: detectType(value) }))
      .sort(compareByKey);
  } catch {
    return [];
  }
}

export function detectType(v: unknown): string {
  if (v === null || v === undefined) return "null";
  if (Array.isArray(v)) return "arr";
  if (typeof v === "object") return "kv";
  if (typeof v === "boolean") return "bool";
  if (typeof v === "number") return Number.isInteger(v) ? "int" : "flt";
  return "str";
}

export function formatValue(v: unknown): string {
  if (v === null || v === undefined) return "—";
  if (typeof v === "string") return v;
  return JSON.stringify(v, null, 2);
}
