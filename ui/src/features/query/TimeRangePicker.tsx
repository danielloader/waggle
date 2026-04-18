import { Clock } from "lucide-react";
import {
  allowedGranularities,
  clampGranularity,
  granularityLabel,
  resolveSearchRange,
  TIME_RANGES,
  timeRangeLabel,
  type Granularity,
  type QuerySearch,
  type TimeRangeKey,
} from "../../lib/query";
import { Popover } from "../../components/ui/Popover";

interface Props {
  search: QuerySearch;
  onChange: (next: Partial<QuerySearch>) => void;
}

/**
 * Combined time-range + granularity picker. Accepts either a preset
 * (range) OR a custom [from, to] window, stored as URL-persisted `from` /
 * `to` ms timestamps. Click a preset to clear the custom window; type
 * custom start/end to override the preset.
 */
export function TimeRangePicker({ search, onChange }: Props) {
  const resolved = resolveSearchRange(search);
  const label = resolved.isCustom
    ? `${formatLocal(resolved.fromMs)} → ${formatLocal(resolved.toMs)}`
    : timeRangeLabel(search.range);

  return (
    <Popover
      align="end"
      trigger={
        <button
          type="button"
          className="flex items-center gap-2 px-3 py-1.5 rounded-md border text-sm hover:bg-[var(--color-surface-muted)]"
          style={{
            background: "var(--color-surface)",
            borderColor: "var(--color-border)",
          }}
        >
          <Clock className="w-4 h-4" />
          {resolved.isCustom && (
            <span
              className="text-[10px] uppercase tracking-wide px-1 rounded"
              style={{
                background: "var(--color-surface-muted)",
                color: "var(--color-ink-muted)",
              }}
            >
              custom
            </span>
          )}
          <span className="font-mono text-xs">{label}</span>
          {search.granularity !== "auto" && (
            <span className="text-xs" style={{ color: "var(--color-ink-muted)" }}>
              · {granularityLabel(search.granularity)}
            </span>
          )}
        </button>
      }
    >
      <div className="flex gap-4" style={{ minWidth: 500 }}>
        {/* Granularity + custom range column */}
        <div className="flex-1 flex flex-col gap-3">
          <div>
            <div
              className="text-[11px] uppercase tracking-wide mb-1"
              style={{ color: "var(--color-ink-muted)" }}
            >
              Granularity
            </div>
            <select
              className="w-full px-2 py-1.5 rounded border text-sm"
              style={{ borderColor: "var(--color-border)" }}
              value={search.granularity}
              onChange={(e) =>
                onChange({ granularity: e.target.value as Granularity })
              }
            >
              {allowedGranularities(resolved.durationMs).map((g) => (
                <option key={g} value={g}>
                  {granularityLabel(g)}
                </option>
              ))}
            </select>
          </div>

          <div>
            <div
              className="text-[11px] uppercase tracking-wide mb-1"
              style={{ color: "var(--color-ink-muted)" }}
            >
              Custom range
            </div>
            <CustomRangeInputs
              fromMs={resolved.fromMs}
              toMs={resolved.toMs}
              onApply={(fromMs, toMs) => {
                const duration = Math.max(1_000, toMs - fromMs);
                onChange({
                  from: fromMs,
                  to: toMs,
                  granularity: clampGranularity(duration, search.granularity),
                });
              }}
            />
          </div>
        </div>

        <div className="w-px" style={{ background: "var(--color-border)" }} />

        {/* Preset column */}
        <div className="flex-1">
          <div
            className="text-[11px] uppercase tracking-wide mb-1"
            style={{ color: "var(--color-ink-muted)" }}
          >
            Presets
          </div>
          <div className="flex flex-col gap-0.5">
            {TIME_RANGES.map((r) => (
              <button
                key={r}
                type="button"
                onClick={() =>
                  onChange({
                    range: r,
                    from: undefined,
                    to: undefined,
                    granularity: clampGranularity(
                      // Use the preset's duration, not the current resolved one.
                      // Easier than threading rangeMs — just look it up.
                      durationOfPreset(r),
                      search.granularity,
                    ),
                  })
                }
                className="px-2 py-1 text-left text-sm rounded hover:bg-[var(--color-surface-muted)]"
                style={
                  !resolved.isCustom && search.range === r
                    ? { background: "var(--color-surface-muted)", fontWeight: 500 }
                    : {}
                }
              >
                {timeRangeLabel(r)}
              </button>
            ))}
          </div>
        </div>
      </div>
    </Popover>
  );
}

// ---------------------------------------------------------------------------

function CustomRangeInputs({
  fromMs,
  toMs,
  onApply,
}: {
  fromMs: number;
  toMs: number;
  onApply: (fromMs: number, toMs: number) => void;
}) {
  // <input type="datetime-local"> takes a string in the user's local
  // timezone without a zone suffix. We round-trip through that form so
  // timezone handling stays the user's business.
  const fromStr = toDatetimeLocal(fromMs);
  const toStr = toDatetimeLocal(toMs);
  return (
    <form
      className="flex flex-col gap-1.5"
      onSubmit={(e) => {
        e.preventDefault();
        const form = e.currentTarget as HTMLFormElement;
        const f = (form.elements.namedItem("from") as HTMLInputElement).value;
        const t = (form.elements.namedItem("to") as HTMLInputElement).value;
        const fm = fromDatetimeLocal(f);
        const tm = fromDatetimeLocal(t);
        if (Number.isFinite(fm) && Number.isFinite(tm) && tm > fm) {
          onApply(fm, tm);
        }
      }}
    >
      <label className="flex items-center justify-between gap-2 text-xs">
        <span style={{ color: "var(--color-ink-muted)" }}>From</span>
        <input
          name="from"
          type="datetime-local"
          defaultValue={fromStr}
          step="1"
          className="px-2 py-1 rounded border text-xs font-mono"
          style={{ borderColor: "var(--color-border)" }}
        />
      </label>
      <label className="flex items-center justify-between gap-2 text-xs">
        <span style={{ color: "var(--color-ink-muted)" }}>To</span>
        <input
          name="to"
          type="datetime-local"
          defaultValue={toStr}
          step="1"
          className="px-2 py-1 rounded border text-xs font-mono"
          style={{ borderColor: "var(--color-border)" }}
        />
      </label>
      <button
        type="submit"
        className="self-start px-2 py-1 text-xs rounded-md text-white"
        style={{ background: "var(--color-accent)" }}
      >
        Apply custom
      </button>
    </form>
  );
}

function toDatetimeLocal(ms: number): string {
  const d = new Date(ms);
  const pad = (n: number) => String(n).padStart(2, "0");
  return (
    d.getFullYear() +
    "-" +
    pad(d.getMonth() + 1) +
    "-" +
    pad(d.getDate()) +
    "T" +
    pad(d.getHours()) +
    ":" +
    pad(d.getMinutes()) +
    ":" +
    pad(d.getSeconds())
  );
}

function fromDatetimeLocal(s: string): number {
  // datetime-local has no zone; new Date(s) interprets as local time.
  return new Date(s).getTime();
}

function formatLocal(ms: number): string {
  const d = new Date(ms);
  const date = d.toLocaleDateString([], { month: "short", day: "numeric" });
  const time = d.toLocaleTimeString([], { hour: "2-digit", minute: "2-digit", second: "2-digit" });
  return `${date} ${time}`;
}

function durationOfPreset(r: TimeRangeKey): number {
  switch (r) {
    case "15m":
      return 15 * 60_000;
    case "1h":
      return 60 * 60_000;
    case "6h":
      return 6 * 60 * 60_000;
    case "24h":
      return 24 * 60 * 60_000;
    case "7d":
      return 7 * 24 * 60 * 60_000;
  }
}
