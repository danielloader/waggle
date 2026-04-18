import { useEffect, useMemo, useState } from "react";
import * as Tabs from "@radix-ui/react-tabs";
import { Link } from "@tanstack/react-router";
import { ArrowLeft, ExternalLink } from "lucide-react";
import type { SpanOut, TraceDetail, TraceResource } from "../../lib/api";
import { serviceColor } from "../../lib/colors";
import { formatDuration } from "../../lib/format";
import { spanStatusLabel } from "./tree";
import { CopyButton } from "../../components/ui/CopyButton";
import {
  AttributesPanel,
  compareByKey,
  detectType,
  formatValue,
  parseAttributes,
  type AttrRow,
} from "../../components/ui/AttributesPanel";

interface Props {
  span: SpanOut | null;
  detail: TraceDetail;
  /** Externally-selected event index (e.g. from a waterfall marker click). */
  selectedEventIdx?: number | null;
  /** Notify parent when the user switches events inside the panel. */
  onSelectEvent?: (idx: number | null) => void;
  /** Controlled tab, if the parent wants to drive it from outside. */
  activeTab?: string;
  /** Fires when the user clicks a tab header. Parent should sync its state. */
  onTabChange?: (tab: string) => void;
}

/**
 * Honeycomb-style right-hand panel. Shows the full attribute set + events
 * + links for the currently-selected span, plus a compact metadata header
 * (service, status, timing). Mirrors the Fields / Span events / Links tab
 * layout.
 */
export function SpanDetail({
  span,
  detail,
  selectedEventIdx = null,
  onSelectEvent,
  activeTab,
  onTabChange,
}: Props) {
  const [localTab, setLocalTab] = useState("fields");
  const tab = activeTab ?? localTab;
  const setTab = (t: string) => {
    if (onTabChange) onTabChange(t);
    else setLocalTab(t);
  };

  if (!span) {
    return (
      <div
        className="h-full flex items-center justify-center text-sm px-4 text-center"
        style={{ color: "var(--color-ink-muted)" }}
      >
        Select a span on the left to inspect its fields.
      </div>
    );
  }

  // Single flat list of every attribute visible on this span — the span's
  // first-class columns, its user attributes, and the resource attributes
  // carried with it. Honeycomb treats these the same way (a span event's
  // `Fields` panel lists everything flat), and the user-facing query
  // language already does too. Duplicates are collapsed key-first.
  const resource = detail.resources?.[String(span.resource_id)];
  const allFields = useMemo(
    () => mergeAttrs(buildSpanMetaAttrs(span), parseAttributes(span.attributes), buildResourceAttrs(resource)),
    [span, resource],
  );

  return (
    <div className="h-full flex flex-col overflow-hidden">
      <Header span={span} />
      <Tabs.Root
        value={tab}
        onValueChange={setTab}
        className="flex-1 flex flex-col overflow-hidden"
      >
        <Tabs.List
          className="flex border-b sticky top-0"
          style={{
            background: "var(--color-surface)",
            borderColor: "var(--color-border)",
          }}
        >
          <Tab value="fields">Fields ({allFields.length})</Tab>
          <Tab value="events">Events ({span.events?.length ?? 0})</Tab>
          <Tab value="links">Links ({span.links?.length ?? 0})</Tab>
        </Tabs.List>
        <Tabs.Content value="fields" className="flex-1 overflow-auto">
          <AttributesPanel rows={allFields} />
        </Tabs.Content>
        <Tabs.Content value="events" className="flex-1 overflow-auto">
          <EventsTab
            span={span}
            selectedIdx={selectedEventIdx}
            onSelect={onSelectEvent}
          />
        </Tabs.Content>
        <Tabs.Content value="links" className="flex-1 overflow-auto">
          <LinksTab span={span} />
        </Tabs.Content>
      </Tabs.Root>
    </div>
  );
}

function Tab({ value, children }: { value: string; children: React.ReactNode }) {
  return (
    <Tabs.Trigger
      value={value}
      className="px-4 py-2 text-sm border-b-2 -mb-px data-[state=active]:font-medium"
      style={
        {
          borderColor: "transparent",
        } as React.CSSProperties
      }
    >
      <span
        className="
          data-[state=active]:text-[var(--color-accent)]
          hover:text-[var(--color-ink)]
        "
      >
        {children}
      </span>
    </Tabs.Trigger>
  );
}

// ---------------------------------------------------------------------------
// Header
// ---------------------------------------------------------------------------

function Header({ span }: { span: SpanOut }) {
  const status = spanStatusLabel(span.status_code);
  const color = serviceColor(span.service_name);
  return (
    <div
      className="px-4 py-3 border-b flex flex-col gap-1.5"
      style={{
        borderColor: "var(--color-border)",
        background: "var(--color-surface)",
      }}
    >
      <div
        className="text-xs flex items-center gap-1.5"
        style={{ color: "var(--color-ink-muted)" }}
      >
        <span className="w-2 h-2 rounded-full shrink-0" style={{ background: color }} />
        <span className="truncate">{span.service_name}</span>
        <span>›</span>
      </div>
      <div className="text-base font-semibold truncate">{span.name}</div>
      <div
        className="text-xs flex flex-wrap items-center gap-x-4 gap-y-1"
        style={{ color: "var(--color-ink-muted)" }}
      >
        <span>{formatDuration(span.duration_ns)}</span>
        <span className="capitalize">
          status:{" "}
          <span
            style={{
              color:
                status === "error"
                  ? "var(--color-error)"
                  : status === "ok"
                    ? "var(--color-ok)"
                    : undefined,
            }}
          >
            {status}
          </span>
        </span>
        {span.status_message && (
          <span
            className="italic truncate max-w-[260px]"
            style={{ color: "var(--color-error)" }}
          >
            {span.status_message}
          </span>
        )}
      </div>
      <div
        className="text-[11px] font-mono flex items-center gap-1 flex-wrap"
        style={{ color: "var(--color-ink-muted)" }}
      >
        <span>span_id:</span>
        <span className="truncate">{span.span_id}</span>
        <CopyButton value={span.span_id} label="Copy span ID" />
        {span.parent_span_id ? (
          <>
            <span>· parent:</span>
            <span className="truncate">{span.parent_span_id}</span>
            <CopyButton value={span.parent_span_id} label="Copy parent span ID" />
          </>
        ) : (
          <span>· root</span>
        )}
      </div>
    </div>
  );
}

// ---------------------------------------------------------------------------
// Events tab
// ---------------------------------------------------------------------------

function EventsTab({
  span,
  selectedIdx,
  onSelect,
}: {
  span: SpanOut;
  selectedIdx: number | null;
  onSelect?: (idx: number | null) => void;
}) {
  // Fallback local state when the parent doesn't want to own this.
  const [localIdx, setLocalIdx] = useState<number | null>(null);
  const effectiveIdx = selectedIdx ?? localIdx;

  const setIdx = (i: number | null) => {
    if (onSelect) onSelect(i);
    else setLocalIdx(i);
  };

  // Reset drill-down when the caller picks a different span. Indexes
  // aren't comparable across spans. Only affects the local fallback;
  // parent-owned state is the parent's responsibility to reset.
  useEffect(() => {
    setLocalIdx(null);
  }, [span.span_id]);

  const events = span.events ?? [];
  if (events.length === 0) {
    return <Empty>No events on this span.</Empty>;
  }

  if (
    effectiveIdx !== null &&
    effectiveIdx >= 0 &&
    effectiveIdx < events.length
  ) {
    return (
      <EventDetail
        event={events[effectiveIdx]}
        span={span}
        indexLabel={`${effectiveIdx + 1} / ${events.length}`}
        onBack={() => setIdx(null)}
      />
    );
  }

  return (
    <div className="p-3 flex flex-col gap-3">
      {events.map((ev, i) => (
        <button
          key={i}
          type="button"
          onClick={() => setIdx(i)}
          className="text-left border rounded overflow-hidden hover:bg-[var(--color-surface-muted)]"
          style={{
            borderColor: "var(--color-border)",
            background: "var(--color-surface)",
          }}
        >
          <div
            className="px-3 py-1.5 text-sm font-medium border-b flex justify-between"
            style={{ borderColor: "var(--color-border)" }}
          >
            <span>{ev.name}</span>
            <span
              className="text-xs font-mono"
              style={{ color: "var(--color-ink-muted)" }}
            >
              +{formatDuration(ev.time_ns - span.start_ns)}
            </span>
          </div>
          <EventAttrs json={ev.attributes} />
        </button>
      ))}
    </div>
  );
}

/**
 * Single-event drill-down. Mirrors the layout of the span's Fields tab —
 * one big header (name + timing) and the full attribute set rendered as a
 * flat, filterable list. Back button returns to the multi-event view.
 */
function EventDetail({
  event,
  span,
  indexLabel,
  onBack,
}: {
  event: NonNullable<SpanOut["events"]>[number];
  span: SpanOut;
  indexLabel: string;
  onBack: () => void;
}) {
  const rows = useMemo(() => {
    // Synthesise meta rows alongside the user-set attributes so the whole
    // event is browsable in one place, just like the span Fields tab.
    const meta: AttrRow[] = [
      { key: "name", value: event.name, type: "str" },
      { key: "time_ns", value: event.time_ns, type: "int" },
      {
        key: "time_offset_ns",
        value: event.time_ns - span.start_ns,
        type: "int",
      },
    ];
    return mergeAttrs(meta, parseAttributes(event.attributes));
  }, [event, span.start_ns]);

  return (
    <div className="flex flex-col h-full">
      <div
        className="px-3 py-2 border-b flex items-start gap-2"
        style={{
          borderColor: "var(--color-border)",
          background: "var(--color-surface)",
        }}
      >
        <button
          type="button"
          onClick={onBack}
          className="mt-0.5 p-1 rounded hover:bg-[var(--color-surface-muted)]"
          title="Back to events list"
        >
          <ArrowLeft className="w-4 h-4" />
        </button>
        <div className="min-w-0 flex-1">
          <div className="flex items-center gap-2">
            <span className="text-sm font-semibold truncate">{event.name}</span>
            <span
              className="text-[10px] uppercase tracking-wide"
              style={{ color: "var(--color-ink-muted)" }}
            >
              event {indexLabel}
            </span>
          </div>
          <div
            className="text-xs"
            style={{ color: "var(--color-ink-muted)" }}
          >
            +{formatDuration(event.time_ns - span.start_ns)} from span start
          </div>
        </div>
      </div>
      <div className="flex-1 overflow-auto">
        <AttributesPanel rows={rows} />
      </div>
    </div>
  );
}

function EventAttrs({ json }: { json: string }) {
  const rows = useMemo(() => parseAttributes(json), [json]);
  if (rows.length === 0) {
    return (
      <div className="px-3 py-2 text-xs" style={{ color: "var(--color-ink-muted)" }}>
        No attributes
      </div>
    );
  }
  return (
    <div className="px-3 py-2 flex flex-col gap-1">
      {rows.map((r) => (
        <div key={r.key} className="flex gap-3 text-xs font-mono">
          <span className="shrink-0" style={{ width: 140, color: "var(--color-ink-muted)" }}>
            {r.key}
          </span>
          <span className="break-all whitespace-pre-wrap">{formatValue(r.value)}</span>
        </div>
      ))}
    </div>
  );
}

// ---------------------------------------------------------------------------
// Links tab
// ---------------------------------------------------------------------------

function LinksTab({ span }: { span: SpanOut }) {
  if (!span.links || span.links.length === 0) {
    return <Empty>No links on this span.</Empty>;
  }
  return (
    <div className="p-3 flex flex-col gap-2">
      <div
        className="text-[10px] uppercase tracking-wider"
        style={{ color: "var(--color-ink-muted)" }}
      >
        Points to {span.links.length} other span{span.links.length === 1 ? "" : "s"}
      </div>
      {span.links.map((ln, i) => {
        const linkedAttrs = parseAttributes(ln.attributes ?? "{}");
        return (
          <div
            key={i}
            className="border rounded"
            style={{ borderColor: "var(--color-border)" }}
          >
            <Link
              to="/traces/$traceId"
              params={{ traceId: ln.linked_trace_id.toLowerCase() }}
              className="flex items-center gap-2 px-3 py-2 border-b text-sm hover:bg-[var(--color-surface-muted)]"
              style={{ borderColor: "var(--color-border)" }}
            >
              <ExternalLink className="w-3.5 h-3.5" style={{ color: "var(--color-accent)" }} />
              <div className="flex-1 min-w-0">
                <div className="font-mono text-xs truncate" style={{ color: "var(--color-accent)" }}>
                  trace {ln.linked_trace_id}
                </div>
                <div
                  className="font-mono text-[10px] truncate"
                  style={{ color: "var(--color-ink-muted)" }}
                >
                  span {ln.linked_span_id}
                </div>
              </div>
            </Link>
            {linkedAttrs.length > 0 && (
              <div className="px-3 py-2">
                {linkedAttrs.map((r) => (
                  <div key={r.key} className="flex gap-3 text-xs font-mono">
                    <span
                      className="shrink-0"
                      style={{ width: 140, color: "var(--color-ink-muted)" }}
                    >
                      {r.key}
                    </span>
                    <span className="break-all">{formatValue(r.value)}</span>
                  </div>
                ))}
              </div>
            )}
          </div>
        );
      })}
    </div>
  );
}

// ---------------------------------------------------------------------------
// helpers
// ---------------------------------------------------------------------------

function Empty({ children }: { children: React.ReactNode }) {
  return (
    <div
      className="px-4 py-6 text-sm text-center"
      style={{ color: "var(--color-ink-muted)" }}
    >
      {children}
    </div>
  );
}

/**
 * Merge several AttrRow sources into a single key-sorted list. Dedupes by
 * key, with earlier arrays winning — so a span-meta `service.name`
 * overrides a resource-level one, etc.
 */
function mergeAttrs(...sources: AttrRow[][]): AttrRow[] {
  const seen = new Map<string, AttrRow>();
  for (const src of sources) {
    for (const r of src) {
      if (!seen.has(r.key)) seen.set(r.key, r);
    }
  }
  return Array.from(seen.values()).sort(compareByKey);
}

/**
 * Convert a span's first-class columns into attribute-style rows —
 * the same fields the user can reference in the query builder. Synthetic
 * meta helpers (`is_root`, `error`) live in the query language only; they
 * aren't real attributes of the span so we don't render them here.
 */
function buildSpanMetaAttrs(span: SpanOut): AttrRow[] {
  const rows: AttrRow[] = [
    { key: "name", value: span.name, type: "str" },
    { key: "service.name", value: span.service_name, type: "str" },
    { key: "kind", value: spanKindName(span.kind), type: "str" },
    { key: "duration_ns", value: span.duration_ns, type: "int" },
    { key: "start_time_ns", value: span.start_ns, type: "int" },
    { key: "end_time_ns", value: span.end_ns, type: "int" },
    { key: "status_code", value: statusCodeName(span.status_code), type: "str" },
    { key: "trace.trace_id", value: span.trace_id, type: "str" },
    { key: "trace.span_id", value: span.span_id, type: "str" },
  ];
  if (span.parent_span_id) {
    rows.push({ key: "trace.parent_id", value: span.parent_span_id, type: "str" });
  }
  if (span.status_message) {
    rows.push({ key: "status_message", value: span.status_message, type: "str" });
  }
  return rows.sort(compareByKey);
}

/**
 * Flatten the server's TraceResource into attribute-style rows, preferring
 * the named columns (service_name, etc.) plus whatever's in AttributesJSON.
 * Dedupes keys so repeats (service.name appearing both as a named column
 * and in the JSON blob) collapse into one row.
 */
function buildResourceAttrs(resource: TraceResource | undefined): AttrRow[] {
  if (!resource) return [];
  const seen = new Set<string>();
  const rows: AttrRow[] = [];
  const push = (key: string, value: unknown) => {
    if (value === null || value === undefined || value === "") return;
    if (seen.has(key)) return;
    seen.add(key);
    rows.push({ key, value, type: detectType(value) });
  };
  push("service.name", resource.ServiceName);
  push("service.namespace", resource.ServiceNamespace);
  push("service.version", resource.ServiceVersion);
  push("service.instance.id", resource.ServiceInstanceID);
  push("telemetry.sdk.name", resource.SDKName);
  push("telemetry.sdk.language", resource.SDKLanguage);
  push("telemetry.sdk.version", resource.SDKVersion);
  for (const a of parseAttributes(resource.AttributesJSON ?? "{}")) {
    push(a.key, a.value);
  }
  return rows.sort(compareByKey);
}

function spanKindName(kind: number): string {
  switch (kind) {
    case 1:
      return "internal";
    case 2:
      return "server";
    case 3:
      return "client";
    case 4:
      return "producer";
    case 5:
      return "consumer";
    default:
      return "unspecified";
  }
}

function statusCodeName(code: number): string {
  switch (code) {
    case 1:
      return "ok";
    case 2:
      return "error";
    default:
      return "unset";
  }
}

