import { useEffect, useMemo, useRef, useState } from "react";
import { useQuery } from "@tanstack/react-query";
import { api, type FieldInfo } from "../../lib/api";
import type { Dataset, Filter } from "../../lib/query";
import {
  applySuggestion,
  FILTER_OP_DISPLAY,
  parseFilter,
  textToFilter,
} from "../../lib/filterText";

/**
 * Small debounce — delays a value by `ms`. Used to throttle autocomplete
 * lookups so every keystroke doesn't fire a new request while the user is
 * mid-type. Short enough that the dropdown still feels responsive.
 */
function useDebouncedValue<T>(value: T, ms: number): T {
  const [debounced, setDebounced] = useState(value);
  useEffect(() => {
    const id = setTimeout(() => setDebounced(value), ms);
    return () => clearTimeout(id);
  }, [value, ms]);
  return debounced;
}

interface Props {
  dataset: Dataset;
  service?: string;
  /** Prefill when editing an existing filter. */
  initial?: string;
  /** Called when the user commits a valid filter (Enter on complete text). */
  onSubmit: (f: Filter) => void;
  /** Escape key (or loss of intent). Optional. */
  onCancel?: () => void;
  placeholder?: string;
}

/**
 * Honeycomb-style free-form filter input. The user types naturally; as the
 * parser progresses through `field`, `op`, and `value` phases, the dropdown
 * swaps to the appropriate suggestion source. Tab accepts the highlighted
 * suggestion, Enter commits the whole expression, Escape cancels.
 */
export function FilterInput({
  dataset,
  service,
  initial = "",
  onSubmit,
  onCancel,
  placeholder = 'Filter…   e.g. "is_root", "service.name = cart", "http.route contains /api"',
}: Props) {
  const [text, setText] = useState(initial);
  const [selected, setSelected] = useState(0);
  const inputRef = useRef<HTMLInputElement | null>(null);

  useEffect(() => {
    inputRef.current?.focus();
  }, []);

  const parsed = useMemo(() => parseFilter(text), [text]);
  const signal =
    dataset === "spans" ? "span" : dataset === "logs" ? "log" : "metric";

  // Debounce only the prefix. The field/phase flip immediately so the
  // dropdown contents switch at once; only the prefix-filtered fetch
  // waits the debounce interval so we don't hammer the API per keystroke.
  const debouncedPartial = useDebouncedValue(parsed.partial, 150);

  // Field suggestions — fetched while the user is in field phase.
  const fields = useQuery({
    queryKey: ["fields", signal, service, parsed.phase === "field" ? debouncedPartial : ""],
    queryFn: ({ signal: abort }) => {
      const p = new URLSearchParams({ dataset: signal });
      if (service) p.set("service", service);
      if (parsed.phase === "field" && debouncedPartial) p.set("prefix", debouncedPartial);
      p.set("limit", "30");
      return api.listFields(p, abort);
    },
    staleTime: 30_000,
    enabled: parsed.phase === "field",
  });

  // Value suggestions — fetched once we know the field and are in value phase.
  const values = useQuery({
    queryKey: ["values", signal, service, parsed.field, debouncedPartial],
    queryFn: ({ signal: abort }) => {
      const p = new URLSearchParams({ dataset: signal });
      if (service) p.set("service", service);
      if (debouncedPartial) p.set("prefix", debouncedPartial);
      p.set("limit", "10");
      return api.listFieldValues(parsed.field!, p, abort);
    },
    staleTime: 30_000,
    enabled: parsed.phase === "value" && !!parsed.field,
  });

  const suggestions = useMemo((): Suggestion[] => {
    switch (parsed.phase) {
      case "field":
        return buildFieldSuggestions(dataset, fields.data?.fields ?? [], parsed.partial);
      case "op":
        return FILTER_OP_DISPLAY
          .filter((o) => o.startsWith(parsed.partial))
          .map((o) => ({ value: o, label: o, hint: opHint(o) }));
      case "value":
        return (values.data?.values ?? [])
          .filter((v) => v.startsWith(parsed.partial))
          .map((v) => ({ value: v, label: v }));
    }
  }, [parsed, fields.data, values.data]);

  // Keep the highlight in range when the suggestion list shrinks.
  useEffect(() => {
    if (selected >= suggestions.length) setSelected(0);
  }, [suggestions.length, selected]);

  const commit = () => {
    const f = textToFilter(text);
    if (f) {
      onSubmit(f);
      setText("");
    }
  };

  const takeSuggestion = (s: Suggestion) => {
    const next = applySuggestion(text, parsed, s.value);
    setText(next);
    setSelected(0);
    // Keep focus so user can continue typing the next phase.
    requestAnimationFrame(() => inputRef.current?.focus());
  };

  return (
    <div className="relative">
      <input
        ref={inputRef}
        value={text}
        onChange={(e) => {
          setText(e.target.value);
          setSelected(0);
        }}
        onKeyDown={(e) => {
          if (e.key === "ArrowDown") {
            e.preventDefault();
            setSelected((i) => Math.min(suggestions.length - 1, i + 1));
          } else if (e.key === "ArrowUp") {
            e.preventDefault();
            setSelected((i) => Math.max(0, i - 1));
          } else if (e.key === "Tab" && suggestions.length > 0) {
            e.preventDefault();
            takeSuggestion(suggestions[selected]);
          } else if (e.key === "Enter") {
            e.preventDefault();
            // Enter commits when the text parses to a valid filter; otherwise
            // accept the highlighted suggestion to keep the flow going.
            const f = textToFilter(text);
            if (f) {
              commit();
            } else if (suggestions.length > 0) {
              takeSuggestion(suggestions[selected]);
            }
          } else if (e.key === "Escape") {
            e.preventDefault();
            onCancel?.();
          }
        }}
        placeholder={placeholder}
        className="w-full px-2 py-1 rounded border font-mono text-sm outline-none"
        style={{
          background: "var(--color-surface)",
          borderColor: "var(--color-border)",
        }}
        spellCheck={false}
        autoComplete="off"
      />
      {suggestions.length > 0 && (
        <div
          className="absolute left-0 right-0 z-50 mt-1 rounded-md border shadow-sm max-h-64 overflow-auto"
          style={{
            background: "var(--color-surface)",
            borderColor: "var(--color-border)",
          }}
        >
          <div
            className="px-2 py-1 text-[10px] uppercase tracking-wide"
            style={{
              color: "var(--color-ink-muted)",
              borderBottom: "1px solid var(--color-border)",
            }}
          >
            {phaseLabel(parsed.phase)}
          </div>
          {suggestions.map((s, i) => (
            <button
              key={`${parsed.phase}-${s.value}`}
              type="button"
              onMouseDown={(e) => {
                // onMouseDown beats the input's blur so the selection
                // registers before focus leaves the input.
                e.preventDefault();
                takeSuggestion(s);
              }}
              onMouseEnter={() => setSelected(i)}
              className="flex items-center justify-between w-full px-2 py-1 text-left text-sm"
              style={
                i === selected
                  ? { background: "var(--color-surface-muted)" }
                  : undefined
              }
            >
              <span className="font-mono truncate">{s.label}</span>
              {s.hint && (
                <span
                  className="text-xs ml-4 shrink-0"
                  style={{ color: "var(--color-ink-muted)" }}
                >
                  {s.hint}
                </span>
              )}
            </button>
          ))}
        </div>
      )}
    </div>
  );
}

// ---------------------------------------------------------------------------
// helpers
// ---------------------------------------------------------------------------

interface Suggestion {
  value: string;
  label: string;
  hint?: string;
}

// Synthetic / built-in fields, scoped per dataset. These mirror the keys
// the Go query builder resolves natively via realColumn(), so offering
// them first in autocomplete gives users a fast path to the common ones
// without having to know the attribute-key catalog.
const SYNTHETIC_FIELDS_BY_DATASET: Record<Dataset, { key: string; type: string }[]> = {
  spans: [
    { key: "is_root", type: "bool" },
    { key: "error", type: "bool" },
    { key: "trace_id", type: "string" },
    { key: "parent_span_id", type: "string" },
    { key: "duration_ns", type: "int" },
    { key: "duration_ms", type: "int" },
  ],
  logs: [
    { key: "body", type: "string" },
    { key: "severity_text", type: "string" },
    { key: "severity_number", type: "int" },
    { key: "error", type: "bool" },
    { key: "trace_id", type: "string" },
    { key: "time_ns", type: "time" },
  ],
  metrics: [
    { key: "name", type: "string" },
    { key: "kind", type: "string" },
    { key: "unit", type: "string" },
    { key: "temporality", type: "string" },
    { key: "value", type: "float" },
    { key: "time_ns", type: "time" },
  ],
};

function buildFieldSuggestions(
  dataset: Dataset,
  catalog: FieldInfo[],
  prefix: string,
): Suggestion[] {
  const seen = new Set<string>();
  const out: Suggestion[] = [];
  // Synthetic / built-in fields first, then user-attribute keys.
  for (const f of SYNTHETIC_FIELDS_BY_DATASET[dataset]) {
    if (!seen.has(f.key) && f.key.startsWith(prefix)) {
      out.push({ value: f.key, label: f.key, hint: f.type });
      seen.add(f.key);
    }
  }
  for (const f of catalog) {
    if (!seen.has(f.key) && f.key.startsWith(prefix)) {
      out.push({ value: f.key, label: f.key, hint: f.type });
      seen.add(f.key);
    }
  }
  return out.slice(0, 30);
}

function opHint(op: string): string {
  switch (op) {
    case "exists":
      return "field is present";
    case "!exists":
      return "field absent";
    case "contains":
      return "substring match";
    case "!contains":
      return "substring not";
    case "starts-with":
      return "prefix";
    case "!starts-with":
      return "not prefix";
    case "ends-with":
      return "suffix";
    case "!ends-with":
      return "not suffix";
    case "in":
      return "in a list";
    case "!in":
      return "not in list";
    default:
      return "";
  }
}

function phaseLabel(phase: string): string {
  switch (phase) {
    case "field":
      return "Fields";
    case "op":
      return "Operators";
    case "value":
      return "Values";
    default:
      return "";
  }
}
