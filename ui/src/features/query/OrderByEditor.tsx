import { Plus, X } from "lucide-react";
import type { Order } from "../../lib/query";

interface Props {
  orderBy: Order[];
  onChange: (next: Order[]) => void;
  // Optional list of aliases (select result column names) to suggest.
  suggestions?: string[];
}

export function OrderByEditor({ orderBy, onChange, suggestions = [] }: Props) {
  return (
    <div className="flex flex-col gap-2 min-w-[320px]">
      <div className="text-xs uppercase tracking-wide" style={{ color: "var(--color-ink-muted)" }}>
        Order by
      </div>
      {orderBy.length === 0 && (
        <div className="text-sm" style={{ color: "var(--color-ink-muted)" }}>
          Unordered.
        </div>
      )}
      {orderBy.map((o, i) => (
        <div key={i} className="flex items-center gap-2">
          <input
            className="flex-1 px-2 py-1 rounded border font-mono text-sm"
            style={{ borderColor: "var(--color-border)" }}
            list={`orderby-suggest-${i}`}
            value={o.field}
            placeholder="field or alias"
            onChange={(e) => {
              const next = [...orderBy];
              next[i] = { ...next[i], field: e.target.value };
              onChange(next);
            }}
          />
          <datalist id={`orderby-suggest-${i}`}>
            {suggestions.map((s) => (
              <option key={s} value={s} />
            ))}
          </datalist>
          <select
            className="px-2 py-1 rounded border text-sm"
            style={{ borderColor: "var(--color-border)" }}
            value={o.dir ?? "desc"}
            onChange={(e) => {
              const next = [...orderBy];
              next[i] = { ...next[i], dir: e.target.value as "asc" | "desc" };
              onChange(next);
            }}
          >
            <option value="desc">desc</option>
            <option value="asc">asc</option>
          </select>
          <button
            type="button"
            className="px-1.5 py-1 rounded hover:bg-[var(--color-surface-muted)]"
            onClick={() => onChange(orderBy.filter((_, idx) => idx !== i))}
            title="Remove"
          >
            <X className="w-3.5 h-3.5" />
          </button>
        </div>
      ))}
      <button
        type="button"
        onClick={() => onChange([...orderBy, { field: suggestions[0] ?? "count", dir: "desc" }])}
        className="self-start flex items-center gap-1 px-2 py-1 rounded border text-sm text-[var(--color-ink-muted)] hover:bg-[var(--color-surface-muted)]"
        style={{
          background: "var(--color-surface)",
          borderColor: "var(--color-border)",
          borderStyle: "dashed",
        }}
      >
        <Plus className="w-3.5 h-3.5" />
        Add sort
      </button>
    </div>
  );
}
