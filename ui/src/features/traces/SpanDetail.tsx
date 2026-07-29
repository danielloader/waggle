import { useCallback, useEffect, useMemo, useState } from "react";
import * as Tabs from "@radix-ui/react-tabs";
import { Link, useNavigate } from "@tanstack/react-router";
import { ArrowLeft, ChevronRight, ExternalLink } from "lucide-react";
import { api, HttpError, type SpanOut, type TraceDetail, type TraceResource } from "../../lib/api";
import { serviceColor } from "../../lib/colors";
import { formatDuration } from "../../lib/format";
import { spanStatusLabel } from "./tree";
import { CopyButton } from "../../components/ui/CopyButton";
import {
  AttributesPanel,
  compareByKey,
  detectType,
  InlineAttrList,
  parseAttributes,
  type AttrRow,
} from "../../components/ui/AttributesPanel";
import {
  appendWhere,
  queryToUrlSearch,
  type Filter,
  type Query,
} from "../../lib/query";

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
  /**
   * Hex content-hash of the originating /events query. When set, the
   * filter button on attribute rows fetches the source query from
   * /api/history/<hash>, appends the new filter to its WHERE, and
   * navigates to /events with the merged search. When unset (or the
   * server says 404 because the row was pruned), we fall back to a
   * minimal /events?dataset=spans&where=[<just-this-filter>] navigation
   * so the click still produces something useful.
   */
  fromHash?: string | null;
  /**
   * Originating /events tab — restored on the merged navigation so the
   * user lands back where they came from. The tab isn't part of the
   * stored Query AST (UI-only state), so it rides through the trace URL
   * as its own search param. Defaults to "explore" when absent — the
   * dataset-agnostic raw-rows view, which is where trace links live for
   * spans/logs anyway.
   */
  fromTab?: "overview" | "traces" | "explore" | "tail" | null;
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
  fromHash = null,
  fromTab = null,
}: Props) {
  const [localTab, setLocalTab] = useState("fields");
  const tab = activeTab ?? localTab;
  const setTab = (t: string) => {
    if (onTabChange) onTabChange(t);
    else setLocalTab(t);
  };

  const navigate = useNavigate();
  const onFilter = useCallback(
    async (f: Filter) => {
      // Restore the source tab so the user lands back where they
      // clicked from. queryToUrlSearch can't recover this (tab is
      // UI-only state, not in the Query AST), so the tab rides the
      // trace URL alongside fromHash. Default to "explore" — that's
      // where trace links physically live, so it's the right guess
      // when fromTab is missing (deep-link, older session, etc).
      const tab = fromTab ?? "explore";
      // Best-effort recovery of the source query. We can't await
      // user-perceived latency here, but a single indexed sqlite
      // lookup is sub-ms in practice.
      let merged: Record<string, unknown>;
      if (fromHash) {
        try {
          const entry = await api.getHistoryByHash(fromHash);
          const src = JSON.parse(entry.query_json) as Query;
          const url = queryToUrlSearch(src);
          merged = {
            ...url,
            where: appendWhere(url.where, f),
            tab,
          } as unknown as Record<string, unknown>;
        } catch (err) {
          // 404: row was pruned mid-flight. Anything else: log and
          // fall through to the same minimal fallback. Either way the
          // user gets a meaningful click result.
          if (!(err instanceof HttpError) || err.status !== 404) {
            console.warn("filter-by: history lookup failed", err);
          }
          merged = { dataset: "spans", where: [f], tab } as unknown as Record<string, unknown>;
        }
      } else {
        merged = { dataset: "spans", where: [f], tab } as unknown as Record<string, unknown>;
      }
      navigate({ to: "/events", search: merged });
    },
    [fromHash, fromTab, navigate],
  );

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
          <AttributesPanel rows={allFields} onFilter={onFilter} />
        </Tabs.Content>
        <Tabs.Content value="events" className="flex-1 overflow-auto">
          <EventsTab
            span={span}
            selectedIdx={selectedEventIdx}
            onSelect={onSelectEvent}
            onFilter={onFilter}
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
        className="text-xs flex flex-wrap items-center gap-x-3 gap-y-1"
        style={{ color: "var(--color-ink-muted)" }}
      >
        <span>{formatDuration(span.duration_ns)}</span>
        <StatusPill status={status} />
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

// StatusPill — colour-codes the span status with a subtle tinted
// background and a small dot. Mirrors the hover-legible treatment the
// waterfall uses for error rows, so status reads at a glance.
function StatusPill({ status }: { status: string }) {
  const tone =
    status === "error"
      ? { color: "var(--color-error)", bg: "color-mix(in srgb, var(--color-error) 12%, transparent)" }
      : status === "ok"
        ? { color: "var(--color-ok)", bg: "color-mix(in srgb, var(--color-ok) 12%, transparent)" }
        : { color: "var(--color-ink-muted)", bg: "var(--color-card)" };
  return (
    <span
      className="inline-flex items-center gap-1 px-1.5 py-0.5 rounded-full text-[11px] capitalize"
      style={{ color: tone.color, background: tone.bg }}
    >
      <span
        className="w-1.5 h-1.5 rounded-full"
        style={{ background: tone.color }}
      />
      {status}
    </span>
  );
}

// ---------------------------------------------------------------------------
// Events tab
// ---------------------------------------------------------------------------

function EventsTab({
  span,
  selectedIdx,
  onSelect,
  onFilter,
}: {
  span: SpanOut;
  selectedIdx: number | null;
  onSelect?: (idx: number | null) => void;
  onFilter?: (filter: Filter) => void;
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
        onFilter={onFilter}
      />
    );
  }

  return (
    <div className="p-3 flex flex-col gap-1.5">
      {events.map((ev, i) => (
        <EventListItem
          key={i}
          event={ev}
          offsetNs={ev.time_ns - span.start_ns}
          onClick={() => setIdx(i)}
        />
      ))}
    </div>
  );
}

/**
 * One row of the events master list — name, offset from span start, and
 * how many attributes are waiting behind the click. The attributes
 * themselves are deliberately not previewed: this panel is 420px wide and
 * a span routinely carries several events with a dozen attributes each,
 * so an inline dump is both unreadable and long enough to bury the later
 * events.
 */
function EventListItem({
  event,
  offsetNs,
  onClick,
}: {
  event: NonNullable<SpanOut["events"]>[number];
  offsetNs: number;
  onClick: () => void;
}) {
  const count = useMemo(
    () => parseAttributes(event.attributes).length,
    [event.attributes],
  );
  return (
    <button
      type="button"
      onClick={onClick}
      className="text-left border rounded px-3 py-2 flex items-center gap-2 hover:bg-[var(--color-card-hover)]"
      style={{
        borderColor: "var(--color-border)",
        background: "var(--color-surface)",
      }}
    >
      <div className="min-w-0 flex-1">
        <div className="text-sm font-medium break-words">{event.name}</div>
        <div className="text-[11px]" style={{ color: "var(--color-ink-muted)" }}>
          {count === 0
            ? "no attributes"
            : `${count} attribute${count === 1 ? "" : "s"}`}
        </div>
      </div>
      <span
        className="text-xs font-mono shrink-0"
        style={{ color: "var(--color-ink-muted)" }}
      >
        +{formatDuration(offsetNs)}
      </span>
      <ChevronRight
        className="w-3.5 h-3.5 shrink-0"
        style={{ color: "var(--color-ink-muted)" }}
      />
    </button>
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
  onFilter,
}: {
  event: NonNullable<SpanOut["events"]>[number];
  span: SpanOut;
  indexLabel: string;
  onBack: () => void;
  onFilter?: (filter: Filter) => void;
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
          className="mt-0.5 p-1 rounded hover:bg-[var(--color-card-hover)]"
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
        <AttributesPanel rows={rows} onFilter={onFilter} />
      </div>
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
              className="flex items-center gap-2 px-3 py-2 border-b text-sm hover:bg-[var(--color-card-hover)]"
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
                <InlineAttrList rows={linkedAttrs} />
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

function spanKindName(kind: string): string {
  // Backend now sends the OTel enum name ("SERVER", "CLIENT", …) lifted
  // from meta.span_kind. Lowercase for display; fall back to unspecified
  // when empty (e.g. instrumentations that didn't set a kind).
  if (!kind || kind === "UNSPECIFIED") return "unspecified";
  return kind.toLowerCase();
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

