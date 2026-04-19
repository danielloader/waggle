import { useEffect, useMemo, useRef, useState } from "react";
import { useQuery } from "@tanstack/react-query";
import { Plus, X } from "lucide-react";
import { api, type FieldInfo } from "../../lib/api";
import {
  AGG_OPS,
  type Aggregation,
  type AggOp,
  type Dataset,
} from "../../lib/query";

interface Props {
  dataset: Dataset;
  service?: string;
  select: Aggregation[];
  onChange: (next: Aggregation[]) => void;
}

// COUNT is the only op that doesn't take a field; everything else needs one.
const OPS_NO_FIELD = new Set<AggOp>(["count"]);

export function SelectEditor({ dataset, service, select, onChange }: Props) {
  const rows = select.length === 0 ? [{ op: "count" as AggOp }] : select;

  return (
    <div className="flex flex-col gap-2 w-[28rem]">
      <div className="text-xs uppercase tracking-wide" style={{ color: "var(--color-ink-muted)" }}>
        Select
      </div>

      {rows.map((agg, i) => (
        <div key={i} className="flex items-center gap-2">
          <select
            className="px-2 py-1 rounded border text-sm"
            style={{ borderColor: "var(--color-border)" }}
            value={agg.op}
            onChange={(e) => {
              const op = e.target.value as AggOp;
              const next = [...rows];
              next[i] = { ...next[i], op, field: OPS_NO_FIELD.has(op) ? undefined : next[i].field };
              onChange(next);
            }}
          >
            {AGG_OPS.map((o) => (
              <option key={o} value={o}>
                {o.toUpperCase()}
              </option>
            ))}
          </select>
          {!OPS_NO_FIELD.has(agg.op) && (
            <FieldAutocomplete
              dataset={dataset}
              service={service}
              value={agg.field ?? ""}
              onChange={(field) => {
                const next = [...rows];
                next[i] = { ...next[i], field };
                onChange(next);
              }}
            />
          )}
          <button
            type="button"
            className="px-1.5 py-1 rounded hover:bg-[var(--color-card-hover)]"
            onClick={() => {
              const next = rows.filter((_, idx) => idx !== i);
              onChange(next);
            }}
            title="Remove"
          >
            <X className="w-3.5 h-3.5" />
          </button>
        </div>
      ))}

      <button
        type="button"
        onClick={() => onChange([...rows, { op: "count" }])}
        className="self-start flex items-center gap-1 px-2 py-1 rounded border text-sm text-[var(--color-ink-muted)] hover:bg-[var(--color-card-hover)]"
        style={{
          background: "var(--color-surface)",
          borderColor: "var(--color-border)",
          borderStyle: "dashed",
        }}
      >
        <Plus className="w-3.5 h-3.5" />
        Add aggregation
      </button>
    </div>
  );
}

// -----------------------------------------------------------------------------
// FieldAutocomplete — free-text input with a /api/fields-backed suggestion
// dropdown. Opens on focus, filters on the typed prefix, Tab/Enter commits
// the highlighted suggestion, Escape closes.
// -----------------------------------------------------------------------------

function FieldAutocomplete({
  dataset,
  service,
  value,
  onChange,
}: {
  dataset: Dataset;
  service?: string;
  value: string;
  onChange: (next: string) => void;
}) {
  const [open, setOpen] = useState(false);
  const [highlight, setHighlight] = useState(0);
  const inputRef = useRef<HTMLInputElement | null>(null);
  const signal =
    dataset === "spans" ? "span" : dataset === "logs" ? "log" : "metric";

  const fields = useQuery({
    queryKey: ["fields", signal, service, value],
    queryFn: ({ signal: abort }) => {
      const p = new URLSearchParams({ dataset: signal });
      if (service) p.set("service", service);
      if (value) p.set("prefix", value);
      p.set("limit", "30");
      return api.listFields(p, abort);
    },
    staleTime: 30_000,
    enabled: open,
  });

  const suggestions = useMemo<FieldInfo[]>(() => {
    const items = fields.data?.fields ?? [];
    // When the input is non-empty, prefer items that actually start with
    // the text — the server filters by prefix but we also include
    // synthetic fields that start anywhere.
    return items;
  }, [fields.data]);

  useEffect(() => {
    setHighlight((h) => Math.min(h, Math.max(0, suggestions.length - 1)));
  }, [suggestions.length]);

  const commit = (s: string) => {
    onChange(s);
    setOpen(false);
  };

  return (
    <div className="flex-1 relative">
      <input
        ref={inputRef}
        className="w-full px-2 py-1 rounded border font-mono text-sm"
        style={{ borderColor: "var(--color-border)" }}
        placeholder="field, e.g. duration_ns"
        value={value}
        onFocus={() => setOpen(true)}
        onBlur={() => {
          // Small delay so a click on a suggestion lands before blur
          // closes the panel.
          setTimeout(() => setOpen(false), 120);
        }}
        onChange={(e) => {
          onChange(e.target.value);
          setOpen(true);
        }}
        onKeyDown={(e) => {
          if (!open && (e.key === "ArrowDown" || e.key === "Tab")) {
            setOpen(true);
            return;
          }
          if (!open) return;
          switch (e.key) {
            case "ArrowDown":
              e.preventDefault();
              setHighlight((h) => Math.min(h + 1, suggestions.length - 1));
              break;
            case "ArrowUp":
              e.preventDefault();
              setHighlight((h) => Math.max(h - 1, 0));
              break;
            case "Tab":
            case "Enter":
              if (suggestions[highlight]) {
                e.preventDefault();
                commit(suggestions[highlight].key);
              }
              break;
            case "Escape":
              setOpen(false);
              break;
          }
        }}
      />
      {open && suggestions.length > 0 && (
        <div
          className="absolute left-0 right-0 top-full mt-1 rounded border shadow-sm z-30 max-h-72 overflow-auto"
          style={{
            background: "var(--color-surface)",
            borderColor: "var(--color-border)",
          }}
        >
          {suggestions.map((f, i) => (
            <button
              key={f.key + ":" + f.type}
              type="button"
              onMouseDown={(e) => {
                // mousedown not click, so we commit before input blurs.
                e.preventDefault();
                commit(f.key);
              }}
              onMouseEnter={() => setHighlight(i)}
              className="flex items-center justify-between w-full px-2 py-1 text-left font-mono text-xs"
              style={{
                background: i === highlight ? "var(--color-card-hover)" : "transparent",
              }}
            >
              <span>{f.key}</span>
              <span style={{ color: "var(--color-ink-muted)" }}>
                {f.type}
                {f.count > 0 ? ` · ${f.count.toLocaleString()}` : ""}
              </span>
            </button>
          ))}
        </div>
      )}
    </div>
  );
}
