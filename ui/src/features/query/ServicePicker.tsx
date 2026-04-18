import { useQuery } from "@tanstack/react-query";
import { ChevronDown } from "lucide-react";
import { api, type ServiceSummary } from "../../lib/api";
import type { Filter } from "../../lib/query";
import { serviceColor } from "../../lib/colors";
import { Popover } from "../../components/ui/Popover";

interface Props {
  /** Current WHERE list — picker reads/writes the service.name = filter within. */
  where: Filter[];
  onChange: (next: Filter[]) => void;
}

/**
 * Dataset-style service selector. Sits in the Define panel header alongside
 * the dataset pill; emulates Honeycomb's "[workbench ▾]" dropdown for
 * switching between logical groupings of data. Under the hood it just
 * mutates a service.name = "<name>" equality filter in the WHERE list.
 */
export function ServicePicker({ where, onChange }: Props) {
  const services = useQuery({
    queryKey: ["services"],
    queryFn: ({ signal }) => api.listServices(signal),
    staleTime: 30_000,
  });

  const current = extractCurrent(where);
  const label = current ?? "All services";

  const select = (service: string | null) => {
    // Replace (or remove) the unique service.name = filter without
    // touching other predicates on the same key (e.g. !=, starts-with).
    const withoutEq = where.filter(
      (f) => !(f.field === "service.name" && f.op === "="),
    );
    if (service === null) {
      onChange(withoutEq);
    } else {
      onChange([...withoutEq, { field: "service.name", op: "=", value: service }]);
    }
  };

  return (
    <Popover
      align="start"
      trigger={
        <button
          type="button"
          className="inline-flex items-center gap-2 px-2.5 py-1 rounded-md border text-sm font-medium hover:bg-[var(--color-surface-muted)]"
          style={{
            background: "var(--color-surface)",
            borderColor: "var(--color-border)",
          }}
        >
          {current ? (
            <span
              className="w-2 h-2 rounded-full"
              style={{ background: serviceColor(current) }}
            />
          ) : null}
          {label}
          <ChevronDown className="w-3.5 h-3.5" style={{ color: "var(--color-ink-muted)" }} />
        </button>
      }
    >
      <div style={{ minWidth: 240, maxHeight: 320, overflow: "auto" }}>
        <button
          type="button"
          onClick={() => select(null)}
          className="w-full text-left px-2 py-1.5 text-sm rounded hover:bg-[var(--color-surface-muted)]"
          style={current === null ? { background: "var(--color-surface-muted)", fontWeight: 500 } : {}}
        >
          All services
        </button>
        {services.isLoading && (
          <div
            className="px-2 py-1 text-sm"
            style={{ color: "var(--color-ink-muted)" }}
          >
            Loading…
          </div>
        )}
        {(services.data?.services ?? []).map((s: ServiceSummary) => (
          <button
            key={s.service}
            type="button"
            onClick={() => select(s.service)}
            className="w-full text-left px-2 py-1.5 text-sm rounded hover:bg-[var(--color-surface-muted)] flex items-center justify-between gap-3"
            style={current === s.service ? { background: "var(--color-surface-muted)", fontWeight: 500 } : {}}
          >
            <span className="inline-flex items-center gap-2">
              <span
                className="w-2 h-2 rounded-full"
                style={{ background: serviceColor(s.service) }}
              />
              {s.service}
            </span>
            <span
              className="text-xs tabular-nums"
              style={{ color: "var(--color-ink-muted)" }}
            >
              {s.span_count.toLocaleString()}
            </span>
          </button>
        ))}
      </div>
    </Popover>
  );
}

function extractCurrent(where: Filter[]): string | null {
  const f = where.find((f) => f.field === "service.name" && f.op === "=");
  return typeof f?.value === "string" ? f.value : null;
}
