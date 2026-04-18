import { X } from "lucide-react";
import type { Filter } from "../../lib/query";
import { filterToText } from "../../lib/filterText";
import { Popover } from "../../components/ui/Popover";
import { FilterInput } from "./FilterInput";

interface Props {
  filter: Filter;
  dataset?: "spans" | "logs";
  service?: string;
  onChange: (f: Filter) => void;
  onRemove: () => void;
}

const OPS_WITHOUT_VALUE = new Set(["exists", "!exists"]);

/**
 * Compact chip for a single committed filter. Clicking the field opens the
 * free-form FilterInput prefilled with this filter's text, so editing uses
 * the same autocomplete pathway as adding a new filter.
 */
export function FilterChip({ filter, dataset = "spans", service, onChange, onRemove }: Props) {
  return (
    <div
      className="flex items-center rounded-md border text-sm overflow-hidden"
      style={{
        background: "var(--color-surface)",
        borderColor: "var(--color-border)",
      }}
    >
      <Popover
        trigger={
          <button
            type="button"
            className="px-2 py-1 font-mono hover:bg-[var(--color-card-hover)]"
          >
            {filter.field}
          </button>
        }
      >
        <div style={{ minWidth: 360 }}>
          <FilterInput
            dataset={dataset}
            service={service}
            initial={filterToText(filter)}
            onSubmit={onChange}
          />
        </div>
      </Popover>

      <div
        className="px-2 py-1"
        style={{ color: "var(--color-ink-muted)", background: "var(--color-surface-muted)" }}
      >
        {filter.op}
      </div>

      {!OPS_WITHOUT_VALUE.has(filter.op) && (
        <div className="px-2 py-1 font-mono truncate max-w-xs">{formatValue(filter.value)}</div>
      )}

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
