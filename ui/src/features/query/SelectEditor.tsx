import { Plus, X } from "lucide-react";
import { AGG_OPS, type Aggregation, type AggOp } from "../../lib/query";

interface Props {
  select: Aggregation[];
  onChange: (next: Aggregation[]) => void;
}

// COUNT is the only op that doesn't take a field; everything else needs one.
const OPS_NO_FIELD = new Set<AggOp>(["count"]);

export function SelectEditor({ select, onChange }: Props) {
  const rows = select.length === 0 ? [{ op: "count" as AggOp }] : select;

  return (
    <div className="flex flex-col gap-2 w-96">
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
            <input
              className="flex-1 px-2 py-1 rounded border font-mono text-sm"
              style={{ borderColor: "var(--color-border)" }}
              placeholder="field, e.g. duration_ns"
              value={agg.field ?? ""}
              onChange={(e) => {
                const next = [...rows];
                next[i] = { ...next[i], field: e.target.value };
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
