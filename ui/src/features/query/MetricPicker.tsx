import { useState } from "react";
import { useQuery } from "@tanstack/react-query";
import { ChevronDown, Check } from "lucide-react";
import { Popover } from "../../components/ui/Popover";
import { api, type MetricSummary } from "../../lib/api";

interface Props {
  /** The currently-picked metric name, or empty when none is selected. */
  value: string;
  /** Fires when the user picks a metric. */
  onChange: (metric: MetricSummary | null) => void;
  /** Scope the /api/metrics listing to one service (optional). */
  service?: string;
}

/**
 * Popover combobox for picking a metric by name. The server returns
 * one row per unique (name, kind) tuple so we display both — rare but
 * legal to have the same name with different kinds, and the UI must
 * disambiguate.
 */
export function MetricPicker({ value, onChange, service }: Props) {
  const [q, setQ] = useState("");
  const metrics = useQuery({
    queryKey: ["metrics.list", service, q],
    queryFn: ({ signal }) => {
      const p = new URLSearchParams();
      if (service) p.set("service", service);
      if (q) p.set("prefix", q);
      p.set("limit", "100");
      return api.listMetrics(p, signal);
    },
    staleTime: 10_000,
  });
  const items: MetricSummary[] = metrics.data?.metrics ?? [];

  return (
    <Popover
      align="start"
      trigger={
        <button
          type="button"
          className="flex items-center gap-2 px-3 py-1.5 rounded-md border text-sm hover:bg-[var(--color-card-hover)]"
          style={{
            background: "var(--color-surface)",
            borderColor: "var(--color-border)",
          }}
        >
          <span className="font-mono">
            {value || <span style={{ color: "var(--color-ink-muted)" }}>Pick metric…</span>}
          </span>
          <ChevronDown className="w-4 h-4" />
        </button>
      }
    >
      <div className="flex flex-col gap-2" style={{ minWidth: 320 }}>
        <input
          autoFocus
          value={q}
          onChange={(e) => setQ(e.target.value)}
          placeholder="Filter metric names…"
          className="px-2 py-1 rounded border font-mono text-sm"
          style={{ borderColor: "var(--color-border)" }}
        />
        <div className="max-h-80 overflow-auto">
          {metrics.isLoading && (
            <div
              className="px-2 py-1 text-sm"
              style={{ color: "var(--color-ink-muted)" }}
            >
              Loading…
            </div>
          )}
          {!metrics.isLoading && items.length === 0 && (
            <div
              className="px-2 py-1 text-sm"
              style={{ color: "var(--color-ink-muted)" }}
            >
              No metrics {q ? `matching "${q}"` : "ingested yet"}.
            </div>
          )}
          {items.map((m) => {
            const active = m.name === value;
            return (
              <button
                key={`${m.name}-${m.kind}`}
                type="button"
                onClick={() => onChange(active ? null : m)}
                className="flex items-center gap-2 w-full px-2 py-1.5 text-left text-sm rounded hover:bg-[var(--color-card-hover)]"
                style={active ? { background: "var(--color-card-stripe)" } : undefined}
              >
                <Check
                  className="w-3.5 h-3.5 shrink-0"
                  style={{ opacity: active ? 1 : 0 }}
                />
                <span className="flex-1 min-w-0">
                  <span className="font-mono truncate block">{m.name}</span>
                  {m.description ? (
                    <span
                      className="text-xs truncate block"
                      style={{ color: "var(--color-ink-muted)" }}
                    >
                      {m.description}
                    </span>
                  ) : null}
                </span>
                <span
                  className="text-[10px] uppercase px-1 rounded shrink-0"
                  style={{
                    background: "var(--color-card-stripe)",
                    color: "var(--color-ink-muted)",
                  }}
                >
                  {m.kind}
                </span>
                {m.unit ? (
                  <span
                    className="text-[10px] font-mono shrink-0"
                    style={{ color: "var(--color-ink-muted)" }}
                  >
                    {m.unit}
                  </span>
                ) : null}
                <span
                  className="text-xs shrink-0"
                  style={{ color: "var(--color-ink-muted)" }}
                >
                  {m.series_count}
                </span>
              </button>
            );
          })}
        </div>
      </div>
    </Popover>
  );
}
