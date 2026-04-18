import type { SpanOut, TraceDetail } from "../../lib/api";

/**
 * Waterfall-oriented view of one trace. Each row is a span plus the indexes
 * needed to render it without recomputing on every frame: depth, own
 * timing, and the extended range including any descendants whose clock
 * appears to have drifted past the parent.
 */
export interface WaterfallRow {
  span: SpanOut;
  depth: number;
  parentID: string | null;
  childCount: number;
  /** "1.2.3" — stable ordering key for virtualization. */
  path: string;
  /** Offset of the solid bar relative to traceStartNS, in ns. */
  offsetNS: number;
  /** Own duration in ns — width of the solid bar. */
  durationNS: number;
  /**
   * Extended range [from, to] covering descendants whose timing exceeds
   * this span's own range (apparent clock skew). Always ⊇ own range.
   */
  extendedFromNS: number;
  extendedToNS: number;
  /** True when extended range is strictly wider than own range. */
  hasSkew: boolean;
}

export interface TraceModel {
  traceID: string;
  spanCount: number;
  /** Earliest start_ns in the trace — waterfall's t=0. */
  traceStartNS: number;
  /** Latest end_ns in the trace (including skewed descendants). */
  traceEndNS: number;
  /** Preordered rows, parents before children. */
  rows: WaterfallRow[];
  /** Map from span_id → row index for fast selection lookup. */
  byID: Map<string, number>;
}

interface Node {
  span: SpanOut;
  children: Node[];
}

/**
 * Build a waterfall model from the server's TraceDetail response. Handles:
 *   - multiple roots (defensive; a well-formed OTLP trace has one)
 *   - orphaned spans whose parent_span_id doesn't match any sibling (treated
 *     as pseudo-roots so they still render)
 *   - clock skew: a child's [start,end] that falls outside its parent's,
 *     surfaced via extendedFromNS/extendedToNS and the hasSkew flag
 */
export function buildTraceModel(detail: TraceDetail): TraceModel {
  const spans = [...detail.spans].sort(
    (a, b) => a.start_ns - b.start_ns || a.span_id.localeCompare(b.span_id),
  );
  const byID = new Map<string, Node>();
  spans.forEach((s) => byID.set(s.span_id, { span: s, children: [] }));

  const roots: Node[] = [];
  for (const s of spans) {
    const n = byID.get(s.span_id)!;
    const parent = s.parent_span_id ? byID.get(s.parent_span_id) : undefined;
    if (parent) {
      parent.children.push(n);
    } else {
      roots.push(n);
    }
  }

  // Flatten preorder with depth + path tracking.
  const rows: WaterfallRow[] = [];
  const byIDToIdx = new Map<string, number>();
  const visit = (node: Node, depth: number, parentID: string | null, path: string) => {
    const idx = rows.length;
    rows.push({
      span: node.span,
      depth,
      parentID,
      childCount: node.children.length,
      path,
      offsetNS: 0, // fill below after we know traceStartNS
      durationNS: node.span.end_ns - node.span.start_ns,
      extendedFromNS: node.span.start_ns,
      extendedToNS: node.span.end_ns,
      hasSkew: false,
    });
    byIDToIdx.set(node.span.span_id, idx);
    node.children.forEach((c, i) => visit(c, depth + 1, node.span.span_id, `${path}.${i}`));
  };
  roots.forEach((r, i) => visit(r, 0, null, String(i)));

  // Compute extended ranges — walk children bottom-up. Because the array
  // is preorder, iterating in reverse guarantees children come before their
  // parent so we can just merge the child's extended range into the parent.
  for (let i = rows.length - 1; i >= 0; i--) {
    const row = rows[i];
    // Find children by scanning forward until depth drops back.
    // Simpler: iterate rows with parentID === row.span.span_id.
    for (let j = i + 1; j < rows.length; j++) {
      if (rows[j].parentID !== row.span.span_id) continue;
      if (rows[j].depth !== row.depth + 1) continue;
      if (rows[j].extendedFromNS < row.extendedFromNS) row.extendedFromNS = rows[j].extendedFromNS;
      if (rows[j].extendedToNS > row.extendedToNS) row.extendedToNS = rows[j].extendedToNS;
    }
    row.hasSkew =
      row.extendedFromNS < row.span.start_ns || row.extendedToNS > row.span.end_ns;
  }

  // Trace-level bounds: earliest of all extended ranges, latest of all.
  let traceStartNS = Number.POSITIVE_INFINITY;
  let traceEndNS = 0;
  for (const row of rows) {
    if (row.extendedFromNS < traceStartNS) traceStartNS = row.extendedFromNS;
    if (row.extendedToNS > traceEndNS) traceEndNS = row.extendedToNS;
  }
  if (!Number.isFinite(traceStartNS)) traceStartNS = 0;

  // Now that we have the trace origin, compute relative offsets for rendering.
  for (const row of rows) {
    row.offsetNS = row.span.start_ns - traceStartNS;
  }

  return {
    traceID: detail.trace_id,
    spanCount: rows.length,
    traceStartNS,
    traceEndNS,
    rows,
    byID: byIDToIdx,
  };
}

/** The collapsed-set filter: drop rows that are descendants of a collapsed span. */
export function visibleRows(model: TraceModel, collapsed: Set<string>): WaterfallRow[] {
  if (collapsed.size === 0) return model.rows;
  const out: WaterfallRow[] = [];
  const hidingUntilDepth: number[] = []; // stack of depth thresholds
  for (const row of model.rows) {
    // Pop any thresholds that no longer apply (we've left a collapsed subtree).
    while (hidingUntilDepth.length > 0 && row.depth <= hidingUntilDepth[hidingUntilDepth.length - 1]) {
      hidingUntilDepth.pop();
    }
    if (hidingUntilDepth.length > 0) continue;
    out.push(row);
    if (collapsed.has(row.span.span_id) && row.childCount > 0) {
      hidingUntilDepth.push(row.depth);
    }
  }
  return out;
}

/** Human-readable status string. Mirrors the events-table rules. */
export function spanStatusLabel(statusCode: number): "ok" | "error" | "unset" {
  if (statusCode === 2) return "error";
  if (statusCode === 1) return "ok";
  return "unset";
}

/**
 * True when the span should be treated as an error for UI purposes:
 * explicit ERROR status OR a recorded "exception" span event. Matches the
 * synthetic `error` field in the query engine.
 */
export function isSpanError(span: import("../../lib/api").SpanOut): boolean {
  if (span.status_code === 2) return true;
  return (span.events ?? []).some((e) => e.name === "exception");
}

/** All ancestor span_ids of the given span (walking up via parentID). */
export function ancestorsOf(model: TraceModel, spanID: string): string[] {
  const out: string[] = [];
  const idx = model.byID.get(spanID);
  if (idx === undefined) return out;
  let parentID = model.rows[idx].parentID;
  while (parentID) {
    out.push(parentID);
    const pidx = model.byID.get(parentID);
    if (pidx === undefined) break;
    parentID = model.rows[pidx].parentID;
  }
  return out;
}

/** Indices into model.rows where the span is an error. Stable order. */
export function errorRowIndices(model: TraceModel): number[] {
  const out: number[] = [];
  for (let i = 0; i < model.rows.length; i++) {
    if (isSpanError(model.rows[i].span)) out.push(i);
  }
  return out;
}

/**
 * Indices into model.rows that match a substring search across span name,
 * service name, and stringified attributes. Case-insensitive.
 */
export function searchRowIndices(model: TraceModel, q: string): number[] {
  const needle = q.trim().toLowerCase();
  if (!needle) return [];
  const out: number[] = [];
  for (let i = 0; i < model.rows.length; i++) {
    const s = model.rows[i].span;
    const hay =
      s.name.toLowerCase() +
      " " +
      s.service_name.toLowerCase() +
      " " +
      (s.attributes ?? "").toLowerCase();
    if (hay.includes(needle)) out.push(i);
  }
  return out;
}

/** Every span_id with children — used for collapse-all. */
export function allExpandableIDs(model: TraceModel): string[] {
  return model.rows.filter((r) => r.childCount > 0).map((r) => r.span.span_id);
}
