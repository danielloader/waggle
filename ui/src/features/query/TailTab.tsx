import { useLayoutEffect, useMemo, useRef, useState } from "react";
import { useQuery } from "@tanstack/react-query";
import {
  Pause,
  Play,
  WrapText,
  ArrowDownToLine,
  Braces,
  Copy,
  Check,
} from "lucide-react";
import clsx from "clsx";
import type { QueryResult, QuerySearch } from "../../lib/query";
import { resolveSearchRange, runQuery } from "../../lib/query";

interface Props {
  querySearch: QuerySearch;
  runCount: number;
}

// Tail refreshes independently of the page's refresh control — the point of
// this view is a live terminal feed, so we poll on a short fixed cadence
// whenever follow mode is on. Paused => no refetch.
const TAIL_POLL_MS = 1000;

/**
 * Terminal-style tail view for logs. Full-bleed dark background, monospace
 * white text, ANSI SGR escapes honoured in log bodies. Follow mode auto-
 * scrolls to the newest line on every refresh; scrolling up interrupts it
 * (less's behaviour — you can stop to read, then jump-to-bottom to resume).
 */
export function TailTab({ querySearch, runCount }: Props) {
  const [follow, setFollow] = useState(true);
  const [wrap, setWrap] = useState(true);
  const [showAttrs, setShowAttrs] = useState(true);
  const [copied, setCopied] = useState(false);

  const result = useQuery({
    queryKey: [
      "tail",
      querySearch.where,
      querySearch.range,
      querySearch.from,
      querySearch.to,
      runCount,
    ],
    queryFn: ({ signal }) => {
      const resolved = resolveSearchRange(querySearch);
      return runQuery(
        {
          dataset: "logs",
          time_range: {
            from: new Date(resolved.fromMs).toISOString(),
            to: new Date(resolved.toMs).toISOString(),
          },
          select: [],
          where: querySearch.where,
          limit: 1000,
        },
        signal,
      );
    },
    refetchInterval: follow ? TAIL_POLL_MS : false,
  });

  // Render from the latest response directly. We considered accumulating
  // across refetches so the feed grows like `tail -f`, but the backend
  // already returns the most recent N rows in the window — each refresh
  // already contains the new lines plus overlap with the previous. Less
  // state = less to go wrong.
  const lines = useMemo(
    () => (result.data ? extractLines(result.data) : []),
    [result.data],
  );

  const scrollRef = useRef<HTMLDivElement>(null);
  const stickToBottom = useRef(true);
  useLayoutEffect(() => {
    const el = scrollRef.current;
    if (!el) return;
    if (follow || stickToBottom.current) {
      el.scrollTop = el.scrollHeight;
    }
  }, [lines, follow]);

  const onScroll = () => {
    const el = scrollRef.current;
    if (!el) return;
    const atBottom = el.scrollHeight - el.scrollTop - el.clientHeight < 4;
    stickToBottom.current = atBottom;
    // Manual scroll-up while follow is on pauses follow mode — matches less.
    if (!atBottom && follow) setFollow(false);
  };

  const jumpToBottom = () => {
    const el = scrollRef.current;
    if (el) el.scrollTop = el.scrollHeight;
    stickToBottom.current = true;
    setFollow(true);
  };

  // Copy the whole visible buffer as plain text — format mirrors what the
  // user sees (time, severity, service, body, then logfmt-style attrs) but
  // stripped of ANSI escapes so the paste target gets a clean feed.
  const copyBuffer = async () => {
    const text = lines.map((ln) => serializeLine(ln, showAttrs)).join("\n");
    try {
      await navigator.clipboard.writeText(text);
      setCopied(true);
      window.setTimeout(() => setCopied(false), 1200);
    } catch {
      // clipboard API can fail in insecure contexts or if permission is
      // denied — no recourse from here, just let the user notice the lack
      // of the "Copied" flash.
    }
  };

  return (
    <div
      className="h-full flex flex-col"
      style={{ background: "#0b0b0b", color: "#e5e5e5" }}
    >
      <TailToolbar
        follow={follow}
        wrap={wrap}
        showAttrs={showAttrs}
        copied={copied}
        lineCount={lines.length}
        error={result.error}
        onToggleFollow={() => {
          if (follow) setFollow(false);
          else jumpToBottom();
        }}
        onToggleWrap={() => setWrap((w) => !w)}
        onToggleAttrs={() => setShowAttrs((a) => !a)}
        onJumpToBottom={jumpToBottom}
        onCopy={copyBuffer}
      />
      <div
        ref={scrollRef}
        onScroll={onScroll}
        className="flex-1 overflow-auto font-mono text-[12px] leading-[1.45] px-3 py-2"
        style={{ scrollbarColor: "#333 #0b0b0b" }}
      >
        {lines.length === 0 ? (
          <div style={{ color: "#777" }}>
            {result.isPending
              ? "Waiting for logs..."
              : "No logs in the current time range. Widen the range or adjust filters."}
          </div>
        ) : (
          <div className={clsx("flex flex-col", wrap ? "" : "w-max min-w-full")}>
            {lines.map((ln) => (
              <TailRow key={ln.key} line={ln} wrap={wrap} showAttrs={showAttrs} />
            ))}
          </div>
        )}
      </div>
    </div>
  );
}

function TailToolbar({
  follow,
  wrap,
  showAttrs,
  copied,
  lineCount,
  error,
  onToggleFollow,
  onToggleWrap,
  onToggleAttrs,
  onJumpToBottom,
  onCopy,
}: {
  follow: boolean;
  wrap: boolean;
  showAttrs: boolean;
  copied: boolean;
  lineCount: number;
  error: unknown;
  onToggleFollow: () => void;
  onToggleWrap: () => void;
  onToggleAttrs: () => void;
  onJumpToBottom: () => void;
  onCopy: () => void;
}) {
  return (
    <div
      className="flex items-center gap-2 px-3 py-1.5 border-b text-[11px]"
      style={{ background: "#111", borderColor: "#222", color: "#bbb" }}
    >
      <TailBtn
        onClick={onToggleFollow}
        active={follow}
        title={follow ? "Pause (stop following)" : "Resume follow"}
      >
        {follow ? <Pause className="w-3 h-3" /> : <Play className="w-3 h-3" />}
        <span>{follow ? "Following" : "Paused"}</span>
      </TailBtn>
      <TailBtn onClick={onToggleWrap} active={wrap} title="Toggle line wrapping">
        <WrapText className="w-3 h-3" />
        <span>{wrap ? "Wrap" : "No wrap"}</span>
      </TailBtn>
      <TailBtn
        onClick={onToggleAttrs}
        active={showAttrs}
        title="Show OTel attributes after body"
      >
        <Braces className="w-3 h-3" />
        <span>{showAttrs ? "Attrs" : "No attrs"}</span>
      </TailBtn>
      <TailBtn onClick={onJumpToBottom} title="Jump to latest">
        <ArrowDownToLine className="w-3 h-3" />
        <span>Jump</span>
      </TailBtn>
      <TailBtn
        onClick={onCopy}
        active={copied}
        title="Copy visible buffer to clipboard"
      >
        {copied ? (
          <Check className="w-3 h-3" />
        ) : (
          <Copy className="w-3 h-3" />
        )}
        <span>{copied ? "Copied" : "Copy"}</span>
      </TailBtn>
      <div className="ml-auto flex items-center gap-3 tabular-nums">
        <span>{lineCount.toLocaleString()} lines</span>
        {error ? (
          <span style={{ color: "#ef5350" }}>
            err: {(error as Error).message}
          </span>
        ) : null}
      </div>
    </div>
  );
}

function TailBtn({
  onClick,
  active,
  title,
  children,
}: {
  onClick: () => void;
  active?: boolean;
  title: string;
  children: React.ReactNode;
}) {
  return (
    <button
      type="button"
      onClick={onClick}
      title={title}
      className="inline-flex items-center gap-1 px-2 py-1 rounded cursor-pointer"
      style={{
        background: active ? "#2a2a2a" : "transparent",
        color: active ? "#e5e5e5" : "#bbb",
        border: "1px solid " + (active ? "#3a3a3a" : "transparent"),
      }}
    >
      {children}
    </button>
  );
}

// ---------------------------------------------------------------------------
// Row rendering
// ---------------------------------------------------------------------------

function TailRow({
  line,
  wrap,
  showAttrs,
}: {
  line: TailLine;
  wrap: boolean;
  showAttrs: boolean;
}) {
  const segments = useMemo(() => parseAnsi(line.body), [line.body]);
  const isError = line.severityNum >= 17;
  return (
    <div
      className={clsx(
        "flex gap-2",
        wrap ? "whitespace-pre-wrap break-all" : "whitespace-pre",
      )}
    >
      <span style={{ color: TIME_COLOR, flexShrink: 0 }}>{line.time}</span>
      <span
        style={{
          color: severityAnsiColor(line.severityNum),
          flexShrink: 0,
          width: "3.25em",
          fontWeight: 700,
        }}
      >
        {line.severityAbbrev}
      </span>
      {line.service ? (
        <span style={{ color: "#888", flexShrink: 0 }}>{line.service}</span>
      ) : null}
      <span className="flex-1 min-w-0">
        {/* Body rendered bold-white in zerolog console style — the message
            is the thing the user scans for, so it sits at the top of the
            visual hierarchy. ANSI SGR escapes in the body still override
            if present. */}
        <span style={{ color: BODY_COLOR, fontWeight: 700 }}>
          {segments.map((s, i) => (
            <span key={i} style={segmentStyle(s.style)}>
              {s.text}
            </span>
          ))}
        </span>
        {showAttrs && line.attrs.length > 0 ? (
          <AttrPairs pairs={line.attrs} isErrorRow={isError} />
        ) : null}
      </span>
    </div>
  );
}

// Zerolog-style `key=value` attribute rendering. Keys + equals share the
// cyan field-name tone; values are rendered in the default body-value
// colour. On error-level rows, the `error` field's value gets the bold-red
// highlight zerolog's ConsoleWriter uses — the one attribute the user
// almost always needs to see first.
function AttrPairs({
  pairs,
  isErrorRow,
}: {
  pairs: AttrPair[];
  isErrorRow: boolean;
}) {
  return (
    <>
      {pairs.map((p, i) => {
        const isErrorField = isErrorRow && ERROR_ATTR_KEYS.has(p.key);
        return (
          <span key={i}>
            {" "}
            <span style={{ color: KEY_COLOR }}>
              {p.key}
              <span>=</span>
            </span>
            <span
              style={
                isErrorField
                  ? { color: ERROR_VALUE_COLOR, fontWeight: 700 }
                  : { color: VALUE_COLOR }
              }
            >
              {formatAttrValue(p)}
            </span>
          </span>
        );
      })}
    </>
  );
}

type AttrKind = "string" | "number" | "bool" | "null" | "object";

interface AttrPair {
  key: string;
  value: unknown;
  kind: AttrKind;
}

// Serialise a line for clipboard copy — what the user sees, minus the
// colours. Body's ANSI escapes are stripped so a paste into a plain text
// target (Slack, editor, ticket) reads clean.
function serializeLine(ln: TailLine, includeAttrs: boolean): string {
  const parts: string[] = [];
  if (ln.time) parts.push(ln.time);
  parts.push(ln.severityAbbrev || "---");
  if (ln.service) parts.push(ln.service);
  parts.push(stripAnsi(ln.body));
  if (includeAttrs && ln.attrs.length > 0) {
    for (const a of ln.attrs) {
      parts.push(a.key + "=" + formatAttrValue(a));
    }
  }
  return parts.join(" ");
}

function stripAnsi(input: string): string {
  if (!input || input.indexOf("\x1b[") === -1) return input;
  let out = "";
  let cursor = 0;
  while (cursor < input.length) {
    const esc = input.indexOf("\x1b[", cursor);
    if (esc === -1) {
      out += input.slice(cursor);
      break;
    }
    out += input.slice(cursor, esc);
    let end = esc + 2;
    while (end < input.length) {
      const code = input.charCodeAt(end);
      if (code >= 0x40 && code <= 0x7e) break;
      end++;
    }
    cursor = end < input.length ? end + 1 : input.length;
  }
  return out;
}

function formatAttrValue(p: AttrPair): string {
  if (p.kind === "null") return "null";
  if (p.kind === "object") return JSON.stringify(p.value);
  if (p.kind === "string") {
    const s = String(p.value);
    return needsQuoting(s) ? JSON.stringify(s) : s;
  }
  return String(p.value);
}

const TIME_COLOR = "#8c8c8c";
const BODY_COLOR = "#ffffff";
const KEY_COLOR = "#12b2a6";
const VALUE_COLOR = "#d0d0d0";
const ERROR_VALUE_COLOR = "#ef4444";

// Attribute keys whose values get bold-red treatment on error-level rows.
// Covers the logfmt short form plus the OTel semantic conventions for
// errors + exceptions — those are the ones a user almost always needs to
// see first when scanning an ERR line. Extending this set is cheap.
const ERROR_ATTR_KEYS = new Set<string>([
  "error",
  "err",
  "error.message",
  "error.type",
  "exception.message",
  "exception.type",
  "exception.stacktrace",
]);

function needsQuoting(s: string): boolean {
  return s.length === 0 || /[\s="]/.test(s);
}

// Attribute keys that are noise in a terminal feed: `meta.*` is waggle's
// internal bookkeeping (dataset/signal type), and service.name is already
// shown as the grey prefix column.
function isHiddenAttrKey(k: string): boolean {
  return k.startsWith("meta.") || k === "service.name";
}

// ---------------------------------------------------------------------------
// Line extraction
// ---------------------------------------------------------------------------

interface TailLine {
  key: string;
  timeNs: number;
  time: string;
  severityNum: number;
  severityAbbrev: string;
  service: string;
  body: string;
  attrs: AttrPair[];
}

function extractLines(result: QueryResult): TailLine[] {
  const idx: Record<string, number> = {};
  result.columns.forEach((c, i) => {
    idx[c.name] = i;
  });
  const out: TailLine[] = [];
  for (const row of result.rows) {
    const timeNs = Number(row[idx.time_ns] ?? 0);
    const body = String(row[idx.body] ?? "");
    const severityNum = Number(row[idx.severity_number] ?? 0);
    const severityText = String(row[idx.severity_text] ?? "");
    const service = String(row[idx.service_name] ?? "");
    const traceID = String(row[idx.trace_id] ?? "");
    const spanID = String(row[idx.span_id] ?? "");
    const attributes = String(row[idx.attributes] ?? "");
    out.push({
      key: timeNs + "|" + traceID + "|" + spanID + "|" + body,
      timeNs,
      time: formatTailTime(timeNs),
      severityNum,
      severityAbbrev: abbrevSeverity(severityText, severityNum),
      service,
      body,
      attrs: parseAttrs(attributes),
    });
  }
  // Backend returns time-DESC; oldest-first is what a tail feed wants.
  out.sort((a, b) => a.timeNs - b.timeNs);
  return out;
}

function parseAttrs(json: string): AttrPair[] {
  if (!json || json === "{}") return [];
  let parsed: Record<string, unknown>;
  try {
    parsed = JSON.parse(json);
  } catch {
    return [];
  }
  if (!parsed || typeof parsed !== "object" || Array.isArray(parsed)) return [];
  const out: AttrPair[] = [];
  for (const key of Object.keys(parsed)) {
    if (isHiddenAttrKey(key)) continue;
    const value = parsed[key];
    out.push({ key, value, kind: attrKind(value) });
  }
  return out;
}

function attrKind(v: unknown): AttrKind {
  if (v === null) return "null";
  const t = typeof v;
  if (t === "string") return "string";
  if (t === "number") return "number";
  if (t === "boolean") return "bool";
  return "object";
}

function formatTailTime(ns: number): string {
  if (!ns) return "";
  const d = new Date(Math.floor(ns / 1_000_000));
  const hh = String(d.getHours()).padStart(2, "0");
  const mm = String(d.getMinutes()).padStart(2, "0");
  const ss = String(d.getSeconds()).padStart(2, "0");
  const ms = String(d.getMilliseconds()).padStart(3, "0");
  return hh + ":" + mm + ":" + ss + "." + ms;
}

function abbrevSeverity(text: string, num: number): string {
  const t = (text || severityFromNumber(num)).toUpperCase();
  if (t.startsWith("TRACE")) return "TRC";
  if (t.startsWith("DEBUG")) return "DBG";
  if (t.startsWith("INFO")) return "INF";
  if (t.startsWith("WARN")) return "WRN";
  if (t.startsWith("ERROR") || t.startsWith("ERR")) return "ERR";
  if (t.startsWith("FATAL")) return "FTL";
  return t.slice(0, 3);
}

function severityFromNumber(n: number): string {
  if (n >= 21) return "FATAL";
  if (n >= 17) return "ERROR";
  if (n >= 13) return "WARN";
  if (n >= 9) return "INFO";
  if (n >= 5) return "DEBUG";
  if (n > 0) return "TRACE";
  return "";
}

function severityAnsiColor(n: number): string {
  if (n >= 17) return "#ef5350";
  if (n >= 13) return "#fce94f";
  if (n >= 9) return "#8ae234";
  if (n >= 5) return "#729fcf";
  if (n > 0) return "#888";
  return "#888";
}

// ---------------------------------------------------------------------------
// Minimal ANSI SGR parser
// ---------------------------------------------------------------------------
//
// Walks the string looking for CSI "\x1b[...m" sequences and splits into
// styled segments. Supports bold/italic/underline/dim and the 8 standard +
// 8 bright colours (30-37, 90-97, background 40-47, 100-107). Extended
// 256-color and truecolor sequences parse cleanly but aren't styled.

interface SgrStyle {
  fg?: string;
  bg?: string;
  bold?: boolean;
  italic?: boolean;
  underline?: boolean;
  dim?: boolean;
}

interface AnsiSegment {
  text: string;
  style: SgrStyle;
}

const ESC = "\x1b";

function parseAnsi(input: string): AnsiSegment[] {
  if (!input) return [];
  if (input.indexOf(ESC + "[") === -1) {
    return [{ text: input, style: {} }];
  }
  const segments: AnsiSegment[] = [];
  let style: SgrStyle = {};
  let cursor = 0;
  const len = input.length;
  while (cursor < len) {
    const esc = input.indexOf(ESC + "[", cursor);
    if (esc === -1) {
      segments.push({ text: input.slice(cursor), style });
      break;
    }
    if (esc > cursor) {
      segments.push({ text: input.slice(cursor, esc), style });
    }
    // Scan until we find the SGR terminator 'm'. If the sequence is a CSI
    // other than SGR (e.g. cursor movement), we still want to strip it —
    // find the final byte in range 0x40-0x7e and skip.
    let end = esc + 2;
    while (end < len) {
      const code = input.charCodeAt(end);
      if (code >= 0x40 && code <= 0x7e) break;
      end++;
    }
    const terminator = input.charAt(end);
    if (terminator === "m") {
      const params = input.slice(esc + 2, end);
      style = applySgr(style, params);
    }
    cursor = end < len ? end + 1 : len;
  }
  return segments;
}

function applySgr(current: SgrStyle, params: string): SgrStyle {
  const codes =
    params === "" ? [0] : params.split(";").map((s) => Number(s) || 0);
  let next: SgrStyle = { ...current };
  for (let i = 0; i < codes.length; i++) {
    const c = codes[i];
    if (c === 0) {
      next = {};
    } else if (c === 1) next.bold = true;
    else if (c === 2) next.dim = true;
    else if (c === 3) next.italic = true;
    else if (c === 4) next.underline = true;
    else if (c === 22) {
      next.bold = false;
      next.dim = false;
    } else if (c === 23) next.italic = false;
    else if (c === 24) next.underline = false;
    else if (c >= 30 && c <= 37) next.fg = ANSI_FG[c - 30];
    else if (c === 39) next.fg = undefined;
    else if (c >= 40 && c <= 47) next.bg = ANSI_BG[c - 40];
    else if (c === 49) next.bg = undefined;
    else if (c >= 90 && c <= 97) next.fg = ANSI_FG_BRIGHT[c - 90];
    else if (c >= 100 && c <= 107) next.bg = ANSI_BG_BRIGHT[c - 100];
    else if (c === 38 || c === 48) {
      // Extended color: 38;5;N (256) or 38;2;R;G;B (truecolor). Consume
      // the right number of trailing params and skip styling for now.
      const sub = codes[i + 1];
      if (sub === 5) i += 2;
      else if (sub === 2) i += 4;
      else i += 1;
    }
  }
  return next;
}

// Tango-ish palette — readable on near-black.
const ANSI_FG = [
  "#2e3436",
  "#cc0000",
  "#4e9a06",
  "#c4a000",
  "#3465a4",
  "#75507b",
  "#06989a",
  "#d3d7cf",
];
const ANSI_FG_BRIGHT = [
  "#555753",
  "#ef2929",
  "#8ae234",
  "#fce94f",
  "#729fcf",
  "#ad7fa8",
  "#34e2e2",
  "#eeeeec",
];
const ANSI_BG = ANSI_FG;
const ANSI_BG_BRIGHT = ANSI_FG_BRIGHT;

function segmentStyle(s: SgrStyle): React.CSSProperties {
  const out: React.CSSProperties = {};
  if (s.fg) out.color = s.fg;
  if (s.bg) out.background = s.bg;
  if (s.bold) out.fontWeight = 700;
  if (s.italic) out.fontStyle = "italic";
  if (s.underline) out.textDecoration = "underline";
  if (s.dim) out.opacity = 0.7;
  return out;
}
