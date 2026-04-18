import { X } from "lucide-react";
import { useQuery } from "@tanstack/react-query";
import { useState } from "react";
import type { Dataset } from "../../lib/query";
import { api, type FieldInfo } from "../../lib/api";

interface Props {
  dataset: Dataset;
  service?: string;
  groupBy: string[];
  onChange: (next: string[]) => void;
}

/**
 * GROUP BY editor for the Define panel cell. Shows the selected fields as
 * removable chips and a field picker below, backed by /api/fields autocomplete
 * (scoped to the given service when one is set via a WHERE filter).
 */
export function GroupByEditor({ dataset, service, groupBy, onChange }: Props) {
  const [q, setQ] = useState("");
  const signal = dataset === "spans" ? "span" : "log";
  const fields = useQuery({
    queryKey: ["fields", signal, service, q],
    queryFn: ({ signal: abort }) => {
      const p = new URLSearchParams({ dataset: signal });
      if (service) p.set("service", service);
      if (q) p.set("prefix", q);
      p.set("limit", "30");
      return api.listFields(p, abort);
    },
    staleTime: 30_000,
  });
  const items = (fields.data?.fields ?? []).filter((f: FieldInfo) => !groupBy.includes(f.key));

  return (
    <div className="flex flex-col gap-2 min-w-[280px]">
      <div className="text-xs uppercase tracking-wide" style={{ color: "var(--color-ink-muted)" }}>
        Group by
      </div>
      {groupBy.length === 0 && (
        <div className="text-sm" style={{ color: "var(--color-ink-muted)" }}>
          No fields selected.
        </div>
      )}
      <div className="flex flex-wrap gap-1.5">
        {groupBy.map((g) => (
          <span
            key={g}
            className="flex items-center rounded-md border text-sm overflow-hidden"
            style={{ background: "var(--color-surface)", borderColor: "var(--color-border)" }}
          >
            <span className="px-2 py-0.5 font-mono">{g}</span>
            <button
              type="button"
              className="px-1.5 py-0.5 hover:bg-[var(--color-surface-muted)]"
              onClick={() => onChange(groupBy.filter((x) => x !== g))}
            >
              <X className="w-3.5 h-3.5" />
            </button>
          </span>
        ))}
      </div>
      <input
        className="px-2 py-1 rounded border font-mono text-sm"
        style={{ borderColor: "var(--color-border)" }}
        placeholder="Filter fields…"
        value={q}
        onChange={(e) => setQ(e.target.value)}
      />
      <div className="max-h-64 overflow-auto">
        {items.length === 0 && (
          <div className="px-2 py-1 text-sm" style={{ color: "var(--color-ink-muted)" }}>
            {fields.isLoading ? "Loading…" : "No matches"}
          </div>
        )}
        {items.map((f: FieldInfo) => (
          <button
            key={f.key}
            type="button"
            onClick={() => onChange([...groupBy, f.key])}
            className="flex items-center justify-between w-full px-2 py-1 text-left text-sm rounded hover:bg-[var(--color-surface-muted)]"
          >
            <span className="font-mono truncate">{f.key}</span>
            <span className="text-xs" style={{ color: "var(--color-ink-muted)" }}>
              {f.type}
            </span>
          </button>
        ))}
      </div>
    </div>
  );
}
