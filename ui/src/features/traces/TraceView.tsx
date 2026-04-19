import { useEffect, useMemo, useState } from "react";
import { useQuery } from "@tanstack/react-query";
import { ArrowLeft, Ghost, RotateCw } from "lucide-react";
import { Link } from "@tanstack/react-router";
import { api, HttpError } from "../../lib/api";
import { formatDuration } from "../../lib/format";
import { CopyButton } from "../../components/ui/CopyButton";
import {
  allExpandableIDs,
  ancestorsOf,
  buildTraceModel,
  errorRowIndices,
  searchRowIndices,
  visibleRows,
} from "./tree";
import { Waterfall } from "./Waterfall";
import { SpanDetail } from "./SpanDetail";
import { WaterfallToolbar } from "./WaterfallToolbar";
import { TraceSummary } from "./TraceSummary";

interface Props {
  traceID: string;
}

export function TraceView({ traceID }: Props) {
  const query = useQuery({
    queryKey: ["trace", traceID],
    queryFn: ({ signal }) => api.getTrace(traceID, signal),
  });

  const model = useMemo(() => (query.data ? buildTraceModel(query.data) : null), [query.data]);

  const [selectedSpanID, setSelectedSpanID] = useState<string | null>(null);
  const [selectedEventIdx, setSelectedEventIdx] = useState<number | null>(null);
  const [activeTab, setActiveTab] = useState("fields");
  const [collapsed, setCollapsed] = useState<Set<string>>(() => new Set());
  const [scrollTarget, setScrollTarget] = useState<string | null>(null);

  // Row click contract: "show this span's attributes" — drop any in-flight
  // event drill-down and flip the panel back to the Fields tab. Passed to
  // both the Waterfall's row click and the TraceSummary lane segments so
  // either entry point feels the same.
  const selectSpan = (spanID: string) => {
    setSelectedSpanID(spanID);
    setSelectedEventIdx(null);
    setActiveTab("fields");
  };

  const [searchText, setSearchText] = useState("");
  const [searchIdx, setSearchIdx] = useState(0);
  const [errorIdx, setErrorIdx] = useState(0);

  const searchMatches = useMemo(
    () => (model ? searchRowIndices(model, searchText) : []),
    [model, searchText],
  );
  const errorMatches = useMemo(
    () => (model ? errorRowIndices(model) : []),
    [model],
  );

  // Clamp the navigation cursors if the underlying arrays change size.
  useEffect(() => {
    if (searchIdx >= searchMatches.length) setSearchIdx(0);
  }, [searchMatches, searchIdx]);
  useEffect(() => {
    if (errorIdx >= errorMatches.length) setErrorIdx(0);
  }, [errorMatches, errorIdx]);

  // Default selection: first root span once the model loads.
  const selected = useMemo(() => {
    if (!model) return null;
    if (selectedSpanID) {
      const idx = model.byID.get(selectedSpanID);
      if (idx !== undefined) return model.rows[idx].span;
    }
    return model.rows[0]?.span ?? null;
  }, [model, selectedSpanID]);

  // Clear event drill-down whenever the selected span changes — event
  // indexes aren't meaningful across spans.
  useEffect(() => {
    setSelectedEventIdx(null);
  }, [selected?.span_id]);

  const rows = useMemo(
    () => (model ? visibleRows(model, collapsed) : []),
    [model, collapsed],
  );

  const highlight = useMemo(() => {
    const s = new Set<string>();
    if (!model) return s;
    for (const i of searchMatches) s.add(model.rows[i].span.span_id);
    return s;
  }, [searchMatches, model]);

  // Jump-to handlers: uncollapse ancestors so the target is visible, select
  // it, and request a scroll. The Waterfall's useEffect picks up the change
  // after the collapsed state has settled.
  const jumpTo = (spanID: string) => {
    if (!model) return;
    const ancestors = ancestorsOf(model, spanID);
    setCollapsed((prev) => {
      if (ancestors.length === 0) return prev;
      const next = new Set(prev);
      for (const a of ancestors) next.delete(a);
      return next;
    });
    setSelectedSpanID(spanID);
    setScrollTarget(spanID);
  };

  const onPrevMatch = () => {
    if (searchMatches.length === 0) return;
    const next = (searchIdx - 1 + searchMatches.length) % searchMatches.length;
    setSearchIdx(next);
    jumpTo(model!.rows[searchMatches[next]].span.span_id);
  };
  const onNextMatch = () => {
    if (searchMatches.length === 0) return;
    const next = (searchIdx + 1) % searchMatches.length;
    setSearchIdx(next);
    jumpTo(model!.rows[searchMatches[next]].span.span_id);
  };
  const onPrevError = () => {
    if (errorMatches.length === 0) return;
    const next = (errorIdx - 1 + errorMatches.length) % errorMatches.length;
    setErrorIdx(next);
    jumpTo(model!.rows[errorMatches[next]].span.span_id);
  };
  const onNextError = () => {
    if (errorMatches.length === 0) return;
    const next = (errorIdx + 1) % errorMatches.length;
    setErrorIdx(next);
    jumpTo(model!.rows[errorMatches[next]].span.span_id);
  };

  if (query.isPending) return <Centered>Loading trace…</Centered>;

  if (query.isError) {
    const err = query.error;
    if (err instanceof HttpError && err.status === 404) {
      return <NotFound traceID={traceID} reason="missing" />;
    }
    if (err instanceof HttpError && err.status === 400) {
      return <NotFound traceID={traceID} reason="malformed" />;
    }
    return <Centered>Error: {(err as Error).message}</Centered>;
  }

  if (!model || model.rows.length === 0) {
    return <NotFound traceID={traceID} reason="missing" />;
  }

  const traceDurationNS = model.traceEndNS - model.traceStartNS;

  return (
    <div className="h-full flex flex-col">
      <Header
        traceID={model.traceID}
        spanCount={model.spanCount}
        durationNS={traceDurationNS}
        onReload={() => query.refetch()}
        isReloading={query.isFetching}
      />
      <div className="flex-1 flex overflow-hidden">
        <div
          className="flex-1 min-w-0 flex flex-col border-r"
          style={{ borderColor: "var(--color-border)" }}
        >
          <WaterfallToolbar
            searchText={searchText}
            onSearchChange={(v) => {
              setSearchText(v);
              setSearchIdx(0);
            }}
            matchCount={searchMatches.length}
            currentMatch={searchMatches.length > 0 ? searchIdx : -1}
            onPrevMatch={onPrevMatch}
            onNextMatch={onNextMatch}
            errorCount={errorMatches.length}
            currentError={errorMatches.length > 0 ? errorIdx : -1}
            onPrevError={onPrevError}
            onNextError={onNextError}
            onExpandAll={() => setCollapsed(new Set())}
            onCollapseAll={() => setCollapsed(new Set(allExpandableIDs(model)))}
          />
          <TraceSummary
            model={model}
            selectedSpanID={selected?.span_id ?? null}
            onSelect={(id) => {
              selectSpan(id);
              setScrollTarget(id);
            }}
          />
          <Waterfall
            model={model}
            rows={rows}
            selectedSpanID={selected?.span_id ?? null}
            collapsed={collapsed}
            onSelect={selectSpan}
            onToggleCollapse={(id) =>
              setCollapsed((prev) => {
                const next = new Set(prev);
                if (next.has(id)) next.delete(id);
                else next.add(id);
                return next;
              })
            }
            highlightSpanIDs={highlight}
            scrollToSpanID={scrollTarget}
            onSelectEvent={(spanID, eventIdx) => {
              // Marker click: make the target span selected, then drop
              // the right panel onto that event and flip to the Events
              // tab so the drill-down is visible.
              if (spanID !== selected?.span_id) {
                setSelectedSpanID(spanID);
                setScrollTarget(spanID);
              }
              setSelectedEventIdx(eventIdx);
              setActiveTab("events");
            }}
          />
        </div>
        <div
          className="shrink-0 w-[420px] flex flex-col border-l"
          style={{
            background: "var(--color-surface)",
            borderColor: "var(--color-border)",
          }}
        >
          <SpanDetail
            span={selected}
            detail={query.data!}
            selectedEventIdx={selectedEventIdx}
            onSelectEvent={setSelectedEventIdx}
            activeTab={activeTab}
            onTabChange={setActiveTab}
          />
        </div>
      </div>
    </div>
  );
}

function Header({
  traceID,
  spanCount,
  durationNS,
  onReload,
  isReloading,
}: {
  traceID: string;
  spanCount: number;
  durationNS: number;
  onReload: () => void;
  isReloading: boolean;
}) {
  return (
    <div
      className="flex items-center gap-4 px-4 py-3 border-b"
      style={{
        background: "var(--color-surface)",
        borderColor: "var(--color-border)",
      }}
    >
      <Link
        to="/events"
        search={{ dataset: "spans" } as never}
        className="inline-flex items-center gap-1 text-sm hover:underline"
        style={{ color: "var(--color-ink-muted)" }}
      >
        <ArrowLeft className="w-4 h-4" />
        Back
      </Link>
      <div className="flex items-center gap-2 min-w-0">
        <span
          className="text-xs uppercase tracking-wider"
          style={{ color: "var(--color-ink-muted)" }}
        >
          Trace
        </span>
        <span className="font-mono text-sm truncate">{traceID}</span>
        <CopyButton value={traceID} label="Copy trace ID" />
      </div>
      <div className="flex-1" />
      <div className="text-sm" style={{ color: "var(--color-ink-muted)" }}>
        {spanCount} span{spanCount === 1 ? "" : "s"} · {formatDuration(durationNS)}
      </div>
      <button
        type="button"
        onClick={onReload}
        disabled={isReloading}
        className="inline-flex items-center gap-1.5 px-2.5 py-1 rounded-md border text-sm hover:bg-[var(--color-card-hover)]"
        style={{
          background: "var(--color-surface)",
          borderColor: "var(--color-border)",
        }}
        title="Reload trace"
      >
        <RotateCw
          className={"w-4 h-4 " + (isReloading ? "animate-spin" : "")}
        />
        Reload
      </button>
    </div>
  );
}

function Centered({ children }: { children: React.ReactNode }) {
  return (
    <div
      className="h-full flex items-center justify-center text-sm"
      style={{ color: "var(--color-ink-muted)" }}
    >
      {children}
    </div>
  );
}

function NotFound({
  traceID,
  reason,
}: {
  traceID: string;
  reason: "missing" | "malformed";
}) {
  const heading =
    reason === "malformed" ? "Invalid trace ID" : "Trace not found";
  const body =
    reason === "malformed"
      ? "The URL doesn't look like a valid 32-character hex trace ID. It may be truncated or corrupted from a shared link."
      : "No trace with that ID exists in the local store. It may have aged out of the retention window, or the link may have been copied before the trace was ingested.";
  return (
    <div className="h-full flex flex-col items-center justify-center gap-4 px-6 py-8 text-center">
      <Ghost className="w-10 h-10" style={{ color: "var(--color-ink-muted)" }} />
      <div className="text-lg font-semibold">{heading}</div>
      <div
        className="max-w-md text-sm"
        style={{ color: "var(--color-ink-muted)" }}
      >
        {body}
      </div>
      <code
        className="text-xs font-mono px-2 py-1 rounded"
        style={{
          color: "var(--color-ink-muted)",
          background: "var(--color-surface-muted)",
        }}
      >
        {traceID}
      </code>
      <Link
        to="/events"
        search={{ dataset: "spans" } as never}
        className="inline-flex items-center gap-2 px-3 py-1.5 rounded-md text-sm font-medium text-white mt-2"
        style={{ background: "var(--color-accent)" }}
      >
        <ArrowLeft className="w-4 h-4" />
        Back to traces
      </Link>
    </div>
  );
}
