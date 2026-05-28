import { useState } from "react";
import { X } from "lucide-react";
import type { Dataset, Filter } from "../../lib/query";
import { filterToText } from "../../lib/filterText";
import { FilterInput } from "./FilterInput";

interface Props {
  filter: Filter;
  dataset?: Dataset;
  service?: string;
  onChange: (f: Filter) => void;
  onRemove: () => void;
}

const OPS_WITHOUT_VALUE = new Set(["exists", "!exists"]);

/**
 * A single committed filter. The whole `field op value` reads as one
 * clickable entity: clicking it drops back into the free-form FilterInput
 * (prefilled with this filter's text) so the field, operator, and value are
 * all editable as one string via the same autocomplete pathway used to add a
 * filter. Enter re-commits it to a chip; Escape cancels.
 */
export function FilterChip({ filter, dataset = "spans", service, onChange, onRemove }: Props) {
  const [editing, setEditing] = useState(false);

  if (editing) {
    return (
      <div style={{ minWidth: 360 }}>
        <FilterInput
          dataset={dataset}
          service={service}
          initial={filterToText(filter)}
          onSubmit={(f) => {
            onChange(f);
            setEditing(false);
          }}
          onCancel={() => setEditing(false)}
        />
      </div>
    );
  }

  return (
    <div
      className="flex items-center rounded-md border text-sm overflow-hidden"
      style={{
        background: "var(--color-surface)",
        borderColor: "var(--color-border)",
      }}
    >
      <button
        type="button"
        onClick={() => setEditing(true)}
        className="flex items-center gap-1.5 px-2 py-1 font-mono min-w-0 hover:bg-[var(--color-card-hover)]"
        title="Click to edit"
      >
        <span>{filter.field}</span>
        <span style={{ color: "var(--color-ink-muted)" }}>{filter.op}</span>
        {!OPS_WITHOUT_VALUE.has(filter.op) && (
          <span className="truncate max-w-xs">{formatValue(filter.value)}</span>
        )}
      </button>

      <button
        type="button"
        onClick={onRemove}
        className="px-1.5 py-1 hover:bg-[var(--color-card-hover)]"
        title="Remove filter"
      >
        <X className="w-3.5 h-3.5" />
      </button>
    </div>
  );
}

function formatValue(v: Filter["value"]): string {
  if (Array.isArray(v)) return `[${v.join(", ")}]`;
  if (v === undefined || v === null) return "";
  return String(v);
}
