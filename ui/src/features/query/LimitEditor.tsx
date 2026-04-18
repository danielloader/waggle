interface Props {
  limit: number | undefined;
  onChange: (next: number | undefined) => void;
}

export function LimitEditor({ limit, onChange }: Props) {
  return (
    <div className="flex flex-col gap-2 w-48">
      <div className="text-xs uppercase tracking-wide" style={{ color: "var(--color-ink-muted)" }}>
        Limit
      </div>
      <input
        type="number"
        min={1}
        max={10000}
        step={100}
        className="px-2 py-1 rounded border text-sm"
        style={{ borderColor: "var(--color-border)" }}
        value={limit ?? ""}
        placeholder="1000"
        onChange={(e) => {
          const v = e.target.value;
          if (v === "") {
            onChange(undefined);
            return;
          }
          const n = Number(v);
          if (Number.isFinite(n)) onChange(n);
        }}
      />
      <div className="text-xs" style={{ color: "var(--color-ink-muted)" }}>
        Between 1 and 10,000.
      </div>
    </div>
  );
}
