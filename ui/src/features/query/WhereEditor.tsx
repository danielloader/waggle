import type { Dataset, Filter } from "../../lib/query";
import { FilterChip } from "./FilterChip";
import { FilterInput } from "./FilterInput";

interface Props {
  dataset: Dataset;
  service?: string;
  title?: string;
  filters: Filter[];
  onChange: (next: Filter[]) => void;
}

/**
 * WHERE / HAVING editor for the Define panel. Each existing filter renders
 * as a removable chip (with click-to-edit via FilterChip's popover). The
 * free-form input at the bottom is the primary add/edit affordance —
 * type-to-autocomplete across field names, operators, and values.
 */
export function WhereEditor({ dataset, service, title = "Where", filters, onChange }: Props) {
  return (
    <div className="flex flex-col gap-2 min-w-[360px]">
      <div className="text-xs uppercase tracking-wide" style={{ color: "var(--color-ink-muted)" }}>
        {title}
      </div>
      {filters.length > 0 && (
        <div className="flex flex-col gap-1.5">
          {filters.map((f, i) => (
            <FilterChip
              key={`${f.field}-${f.op}-${i}`}
              filter={f}
              dataset={dataset}
              service={service}
              onChange={(next) => {
                const copy = [...filters];
                copy[i] = next;
                onChange(copy);
              }}
              onRemove={() => onChange(filters.filter((_, idx) => idx !== i))}
            />
          ))}
        </div>
      )}
      <FilterInput
        dataset={dataset}
        service={service}
        onSubmit={(f) => onChange([...filters, f])}
      />
      <div className="text-xs" style={{ color: "var(--color-ink-muted)" }}>
        Type a field name to autocomplete, then an operator (<code>=</code>,{" "}
        <code>contains</code>, <code>&gt;=</code>…), then a value. Bare field
        = <code>exists</code>; bare booleans like <code>is_root</code> ={" "}
        <code>true</code>.
      </div>
    </div>
  );
}
