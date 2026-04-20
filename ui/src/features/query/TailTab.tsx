import { useEffect, useLayoutEffect, useMemo, useRef, useState } from "react";
import { useQuery } from "@tanstack/react-query";
import {
  Pause,
  Play,
  WrapText,
  ArrowDownToLine,
  Braces,
  Copy,
  Check,
  ChevronUp,
  ChevronDown,
  X,
  Search,
  Filter,
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
  // less-style command prompt at the bottom of the pane. Only one of the
  // two prompts is visible at a time (search `/` and filter `&` are modal
  // in less, same here), but both patterns persist when the prompt closes
  // so the user can flip between them without losing state.
  const [promptMode, setPromptMode] = useState<"search" | "filter" | null>(
    null,
  );
  const [searchPattern, setSearchPattern] = useState("");
  const [searchUseRegex, setSearchUseRegex] = useState(false);
  const [currentMatch, setCurrentMatch] = useState(0);
  const searchInputRef = useRef<HTMLInputElement>(null);
  // Filter mode (`&pattern` in less) hides non-matching lines. Negate
  // flips the test — less spells this `&!pattern`, we have a `!` button
  // to avoid confusing `!` in legitimate regex with the negation prefix.
  const [filterPattern, setFilterPattern] = useState("");
  const [filterUseRegex, setFilterUseRegex] = useState(false);
  const [filterNegate, setFilterNegate] = useState(false);
  const filterInputRef = useRef<HTMLInputElement>(null);
  const searchOpen = promptMode === "search";

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

  // Copy the currently visible buffer as plain text — so if a filter is
  // active, only the filtered lines land on the clipboard. Format mirrors
  // what the user sees (time, severity, service, body, then logfmt-style
  // attrs) with ANSI escapes stripped.
  const copyBuffer = async () => {
    const text = filteredLines
      .map((ln) => serializeLine(ln, showAttrs))
      .join("\n");
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

  // Regex-compile attempts — the UI distinguishes "no match" from "invalid
  // pattern" so the user can tell why `score=0.9[` returned nothing. One
  // per prompt, since search and filter have independent regex toggles.
  const searchRegexError = useMemo(
    () => regexCompileError(searchPattern, searchUseRegex),
    [searchPattern, searchUseRegex],
  );
  const filterRegexError = useMemo(
    () => regexCompileError(filterPattern, filterUseRegex),
    [filterPattern, filterUseRegex],
  );

  // Filter (`&pattern` in less) narrows the visible buffer to lines where
  // the pattern matches body, any attribute key, or any attribute value.
  // Negation flips the predicate. If the pattern is invalid (regex parse
  // failed), we fall through to showing all lines — that way the feed
  // doesn't vanish while the user is mid-edit on a regex.
  //
  // Computed inline (no useMemo) — 1000 rows × a string indexOf per row
  // is sub-millisecond, and the React Compiler auto-memoizes re-renders
  // when inputs don't change. Manual useMemo here tripped the compiler's
  // preserve-memoization check.
  const filterFinder =
    !filterPattern || filterRegexError
      ? null
      : buildFinder(
          filterPattern,
          filterUseRegex,
          /[A-Z]/.test(filterPattern),
        );
  const filteredLines = filterFinder
    ? lines.filter((ln) => {
        const hit = lineHasHit(ln, filterFinder);
        return filterNegate ? !hit : hit;
      })
    : lines;

  // Search (`/pattern` in less) highlights hits within the *currently
  // visible* buffer — so /-searching after &-filtering is natural (search
  // within the filtered slice). Matches cover body + attribute keys +
  // stringified attribute values.
  const matches = useMemo(() => {
    if (!searchPattern || searchRegexError) return [] as SearchMatch[];
    const caseSensitive = /[A-Z]/.test(searchPattern);
    const finder = buildFinder(searchPattern, searchUseRegex, caseSensitive);
    if (!finder) return [];
    const out: SearchMatch[] = [];
    for (let li = 0; li < filteredLines.length; li++) {
      const ln = filteredLines[li];
      for (const m of finder(stripAnsi(ln.body))) {
        out.push({
          lineIdx: li,
          loc: { kind: "body", start: m.start, end: m.end },
        });
      }
      for (let ai = 0; ai < ln.attrs.length; ai++) {
        const attr = ln.attrs[ai];
        for (const m of finder(attr.key)) {
          out.push({
            lineIdx: li,
            loc: { kind: "attrKey", attrIdx: ai, start: m.start, end: m.end },
          });
        }
        const valStr = formatAttrValue(attr);
        for (const m of finder(valStr)) {
          out.push({
            lineIdx: li,
            loc: { kind: "attrValue", attrIdx: ai, start: m.start, end: m.end },
          });
        }
      }
    }
    return out;
  }, [filteredLines, searchPattern, searchUseRegex, searchRegexError]);

  const matchesByLine = useMemo(() => {
    const by = new Map<number, { match: SearchMatch; matchIdx: number }[]>();
    matches.forEach((m, idx) => {
      const arr = by.get(m.lineIdx) ?? [];
      arr.push({ match: m, matchIdx: idx });
      by.set(m.lineIdx, arr);
    });
    return by;
  }, [matches]);

  // `currentMatch` is kept in range by clamping at read time rather than
  // with a self-correcting effect — matches can churn with every poll, and
  // a setState-in-effect triggers cascading renders the React Compiler lint
  // rule flags. `safeMatchIdx` is what the render should use everywhere.
  const safeMatchIdx =
    matches.length === 0 ? 0 : currentMatch % matches.length;

  // Scroll the active match into the middle of the pane. Searching always
  // pauses follow so we don't fight auto-scroll on the next poll.
  useEffect(() => {
    if (!searchOpen || matches.length === 0) return;
    const m = matches[safeMatchIdx];
    if (!m) return;
    const el = scrollRef.current?.querySelector(
      `[data-line-idx="${m.lineIdx}"]`,
    ) as HTMLElement | null;
    el?.scrollIntoView({ block: "center" });
  }, [safeMatchIdx, matches, searchOpen]);

  const openSearch = () => {
    setPromptMode("search");
    setFollow(false);
    stickToBottom.current = false;
    queueMicrotask(() => searchInputRef.current?.focus());
  };
  const closeSearch = () => {
    setPromptMode(null);
    // Clear the search pattern on close — tail is a live view, stale
    // highlight markers persisting after Esc feels wrong. Filter pattern
    // is deliberately *not* cleared on close (see below).
    setSearchPattern("");
    setCurrentMatch(0);
  };
  const openFilter = () => {
    setPromptMode("filter");
    setFollow(false);
    stickToBottom.current = false;
    queueMicrotask(() => filterInputRef.current?.focus());
  };
  const closeFilter = () => {
    setPromptMode(null);
    // Filter pattern persists when the prompt closes — the user can Esc
    // out of the bar to see the filtered view without the prompt
    // chrome, and reopen with `&` to edit. To clear, open and erase.
  };
  const nextMatch = () => {
    if (matches.length === 0) return;
    setCurrentMatch((c) => (c + 1) % matches.length);
  };
  const prevMatch = () => {
    if (matches.length === 0) return;
    setCurrentMatch((c) => (c - 1 + matches.length) % matches.length);
  };

  // Global key handler: bindings land on the window when the Tail tab is
  // mounted. Anything typed into an input is left alone — we don't want to
  // hijack the Define panel or either prompt's own input, which has its
  // own handlers for Enter / Esc.
  useEffect(() => {
    const onKey = (e: KeyboardEvent) => {
      const ae = document.activeElement as HTMLElement | null;
      const tag = ae?.tagName;
      const typingInField =
        tag === "INPUT" || tag === "TEXTAREA" || ae?.isContentEditable;
      if (typingInField) return;
      if (e.metaKey || e.ctrlKey || e.altKey) return;
      switch (e.key) {
        case "/":
          e.preventDefault();
          openSearch();
          return;
        case "&":
          e.preventDefault();
          openFilter();
          return;
        case "Escape":
          if (promptMode !== null) {
            e.preventDefault();
            if (promptMode === "search") closeSearch();
            else closeFilter();
          }
          return;
        case "n":
          if (matches.length > 0) {
            e.preventDefault();
            nextMatch();
          }
          return;
        case "N":
          if (matches.length > 0) {
            e.preventDefault();
            prevMatch();
          }
          return;
        case "F":
          e.preventDefault();
          if (follow) setFollow(false);
          else jumpToBottom();
          return;
        case "G":
          e.preventDefault();
          jumpToBottom();
          return;
        case "g": {
          e.preventDefault();
          const el = scrollRef.current;
          if (el) {
            el.scrollTop = 0;
            stickToBottom.current = false;
            setFollow(false);
          }
          return;
        }
      }
    };
    window.addEventListener("keydown", onKey);
    return () => window.removeEventListener("keydown", onKey);
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [promptMode, matches.length, follow]);

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
        lineCount={filteredLines.length}
        totalLineCount={lines.length}
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
        {filteredLines.length === 0 ? (
          <div style={{ color: "#777" }}>
            {result.isPending
              ? "Waiting for logs..."
              : lines.length === 0
                ? "No logs in the current time range. Widen the range or adjust filters."
                : `No lines match filter "${filterPattern}". Press & to edit.`}
          </div>
        ) : (
          <div className={clsx("flex flex-col", wrap ? "" : "w-max min-w-full")}>
            {filteredLines.map((ln, idx) => (
              <TailRow
                key={ln.key}
                line={ln}
                lineIdx={idx}
                wrap={wrap}
                showAttrs={showAttrs}
                lineMatches={matchesByLine.get(idx) ?? EMPTY_LINE_MATCHES}
                activeMatchIdx={safeMatchIdx}
                searching={searchOpen && searchPattern.length > 0}
              />
            ))}
          </div>
        )}
      </div>
      <TailCommandBar
        mode={promptMode}
        search={{
          pattern: searchPattern,
          useRegex: searchUseRegex,
          regexError: searchRegexError,
          inputRef: searchInputRef,
          matchCount: matches.length,
          currentMatch: safeMatchIdx,
          onPatternChange: setSearchPattern,
          onToggleRegex: () => setSearchUseRegex((r) => !r),
          onNext: nextMatch,
          onPrev: prevMatch,
          onClose: closeSearch,
        }}
        filter={{
          pattern: filterPattern,
          useRegex: filterUseRegex,
          negate: filterNegate,
          regexError: filterRegexError,
          inputRef: filterInputRef,
          filteredCount: filteredLines.length,
          totalCount: lines.length,
          onPatternChange: setFilterPattern,
          onToggleRegex: () => setFilterUseRegex((r) => !r),
          onToggleNegate: () => setFilterNegate((n) => !n),
          onClose: closeFilter,
        }}
        filterActive={filterPattern.length > 0 && !filterRegexError}
        filterPattern={filterPattern}
        filterNegate={filterNegate}
      />
    </div>
  );
}

// Reused across every non-matching row so we don't allocate fresh empty
// arrays on every render (1000 rows × every keystroke adds up).
const EMPTY_LINE_MATCHES: { match: SearchMatch; matchIdx: number }[] = [];

type MatchLocation =
  | { kind: "body"; start: number; end: number }
  | { kind: "attrKey"; attrIdx: number; start: number; end: number }
  | { kind: "attrValue"; attrIdx: number; start: number; end: number };

interface SearchMatch {
  lineIdx: number;
  loc: MatchLocation;
}

function regexCompileError(pattern: string, useRegex: boolean): string | null {
  if (!useRegex || !pattern) return null;
  try {
    new RegExp(pattern);
    return null;
  } catch (e) {
    return (e as Error).message;
  }
}

// Used by filter mode — does *any* of the line's searchable text hit?
// Body, attribute keys, attribute values all count. Used with
// `buildFinder` so the same smart-case + regex rules apply as search.
function lineHasHit(
  ln: TailLine,
  finder: (text: string) => { start: number; end: number }[],
): boolean {
  if (finder(stripAnsi(ln.body)).length > 0) return true;
  for (const a of ln.attrs) {
    if (finder(a.key).length > 0) return true;
    if (finder(formatAttrValue(a)).length > 0) return true;
  }
  return false;
}

function buildFinder(
  pattern: string,
  useRegex: boolean,
  caseSensitive: boolean,
): ((text: string) => { start: number; end: number }[]) | null {
  if (!pattern) return null;
  if (useRegex) {
    let re: RegExp;
    try {
      re = new RegExp(pattern, caseSensitive ? "g" : "gi");
    } catch {
      return null;
    }
    return (text: string) => {
      const out: { start: number; end: number }[] = [];
      re.lastIndex = 0;
      let m: RegExpExecArray | null;
      // Cap iterations so a pathological /(.*)*/ can't freeze the tab.
      for (let iter = 0; iter < 10_000; iter++) {
        m = re.exec(text);
        if (m === null) break;
        if (m[0].length === 0) {
          re.lastIndex++;
          continue;
        }
        out.push({ start: m.index, end: m.index + m[0].length });
      }
      return out;
    };
  }
  const pat = caseSensitive ? pattern : pattern.toLowerCase();
  return (text: string) => {
    const hay = caseSensitive ? text : text.toLowerCase();
    const out: { start: number; end: number }[] = [];
    let from = 0;
    while (from <= hay.length - pat.length) {
      const idx = hay.indexOf(pat, from);
      if (idx === -1) break;
      out.push({ start: idx, end: idx + pat.length });
      from = Math.max(idx + pat.length, idx + 1);
    }
    return out;
  };
}

interface SearchPromptProps {
  pattern: string;
  useRegex: boolean;
  regexError: string | null;
  inputRef: React.RefObject<HTMLInputElement | null>;
  matchCount: number;
  currentMatch: number;
  onPatternChange: (s: string) => void;
  onToggleRegex: () => void;
  onNext: () => void;
  onPrev: () => void;
  onClose: () => void;
}

interface FilterPromptProps {
  pattern: string;
  useRegex: boolean;
  negate: boolean;
  regexError: string | null;
  inputRef: React.RefObject<HTMLInputElement | null>;
  filteredCount: number;
  totalCount: number;
  onPatternChange: (s: string) => void;
  onToggleRegex: () => void;
  onToggleNegate: () => void;
  onClose: () => void;
}

// Bottom command bar — modelled on less's `/`-prompt and idle status line.
// Three states: search prompt, filter prompt, or idle key-hint strip.
// Search (`/`) and filter (`&`) are modal in less too, so only one is
// rendered at a time. When a filter is active but its prompt is closed,
// the idle bar shows a compact summary so the user knows the buffer is
// narrowed.
function TailCommandBar({
  mode,
  search,
  filter,
  filterActive,
  filterPattern,
  filterNegate,
}: {
  mode: "search" | "filter" | null;
  search: SearchPromptProps;
  filter: FilterPromptProps;
  filterActive: boolean;
  filterPattern: string;
  filterNegate: boolean;
}) {
  if (mode === "search") return <SearchPrompt {...search} />;
  if (mode === "filter") return <FilterPrompt {...filter} />;
  return (
    <div
      className="flex items-center gap-3 px-3 py-1.5 border-t text-[11px] font-mono"
      style={{ background: "#111", borderColor: "#222", color: "#666" }}
    >
      <span style={{ color: "#888" }}>:</span>
      <KeyHint k="/" label="search" />
      <KeyHint k="&" label="filter" />
      <KeyHint k="n" label="next" />
      <KeyHint k="N" label="prev" />
      <KeyHint k="F" label="follow" />
      <KeyHint k="g" label="top" />
      <KeyHint k="G" label="bottom" />
      {filterActive ? (
        <span className="ml-auto" style={{ color: "#12b2a6" }}>
          <Filter className="w-3 h-3 inline -mt-0.5" />{" "}
          <span style={{ color: "#bbb" }}>
            {filterNegate ? "!" : ""}
            {filterPattern}
          </span>
        </span>
      ) : null}
    </div>
  );
}

function SearchPrompt({
  pattern,
  useRegex,
  regexError,
  inputRef,
  matchCount,
  currentMatch,
  onPatternChange,
  onToggleRegex,
  onNext,
  onPrev,
  onClose,
}: SearchPromptProps) {
  const counter = regexError
    ? "invalid regex"
    : matchCount === 0
      ? pattern
        ? "no match"
        : "—"
      : `${currentMatch + 1} / ${matchCount}`;
  return (
    <div
      className="flex items-center gap-2 px-3 py-1.5 border-t text-[11px] font-mono"
      style={{ background: "#111", borderColor: "#222", color: "#bbb" }}
    >
      <Search className="w-3 h-3" style={{ color: "#888" }} />
      <span style={{ color: "#12b2a6" }}>/</span>
      <input
        ref={inputRef}
        value={pattern}
        onChange={(e) => onPatternChange(e.target.value)}
        onKeyDown={(e) => {
          if (e.key === "Enter") {
            e.preventDefault();
            if (e.shiftKey) onPrev();
            else onNext();
          } else if (e.key === "Escape") {
            e.preventDefault();
            onClose();
          }
        }}
        autoFocus
        spellCheck={false}
        placeholder={
          useRegex
            ? "regex (smart case: lowercase = insensitive)"
            : "literal (smart case: lowercase = insensitive)"
        }
        className="flex-1 bg-transparent outline-none"
        style={{ color: "#e5e5e5" }}
      />
      <ToggleChip label=".*" active={useRegex} title="Toggle regex mode" onClick={onToggleRegex} />
      <span
        style={{
          color: regexError ? "#ef5350" : matchCount === 0 ? "#777" : "#bbb",
        }}
        title={regexError ?? undefined}
      >
        {counter}
      </span>
      <IconBtn onClick={onPrev} title="Previous match (Shift+Enter, N)">
        <ChevronUp className="w-3 h-3" />
      </IconBtn>
      <IconBtn onClick={onNext} title="Next match (Enter, n)">
        <ChevronDown className="w-3 h-3" />
      </IconBtn>
      <IconBtn onClick={onClose} title="Close search (Esc)">
        <X className="w-3 h-3" />
      </IconBtn>
    </div>
  );
}

function FilterPrompt({
  pattern,
  useRegex,
  negate,
  regexError,
  inputRef,
  filteredCount,
  totalCount,
  onPatternChange,
  onToggleRegex,
  onToggleNegate,
  onClose,
}: FilterPromptProps) {
  const status = regexError
    ? "invalid regex"
    : pattern
      ? `${filteredCount.toLocaleString()} / ${totalCount.toLocaleString()}`
      : `${totalCount.toLocaleString()} lines`;
  return (
    <div
      className="flex items-center gap-2 px-3 py-1.5 border-t text-[11px] font-mono"
      style={{ background: "#111", borderColor: "#222", color: "#bbb" }}
    >
      <Filter className="w-3 h-3" style={{ color: "#888" }} />
      <span style={{ color: "#12b2a6" }}>&amp;{negate ? "!" : ""}</span>
      <input
        ref={inputRef}
        value={pattern}
        onChange={(e) => onPatternChange(e.target.value)}
        onKeyDown={(e) => {
          if (e.key === "Enter" || e.key === "Escape") {
            e.preventDefault();
            onClose();
          }
        }}
        autoFocus
        spellCheck={false}
        placeholder={
          negate
            ? "pattern to EXCLUDE (lines not matching)"
            : "pattern to include (smart case: lowercase = insensitive)"
        }
        className="flex-1 bg-transparent outline-none"
        style={{ color: "#e5e5e5" }}
      />
      <ToggleChip label="!" active={negate} title="Invert — show lines that DON'T match" onClick={onToggleNegate} />
      <ToggleChip label=".*" active={useRegex} title="Toggle regex mode" onClick={onToggleRegex} />
      <span
        style={{ color: regexError ? "#ef5350" : "#bbb" }}
        title={regexError ?? undefined}
      >
        {status}
      </span>
      <IconBtn onClick={onClose} title="Close filter prompt (Esc). Pattern is kept.">
        <X className="w-3 h-3" />
      </IconBtn>
    </div>
  );
}

function ToggleChip({
  label,
  active,
  title,
  onClick,
}: {
  label: string;
  active: boolean;
  title: string;
  onClick: () => void;
}) {
  return (
    <button
      type="button"
      onClick={onClick}
      title={title}
      className="px-1.5 py-0.5 rounded cursor-pointer"
      style={{
        background: active ? "#2a2a2a" : "transparent",
        color: active ? "#e5e5e5" : "#888",
        border: "1px solid " + (active ? "#3a3a3a" : "#222"),
        fontFamily: "inherit",
      }}
    >
      {label}
    </button>
  );
}

function IconBtn({
  onClick,
  title,
  children,
}: {
  onClick: () => void;
  title: string;
  children: React.ReactNode;
}) {
  return (
    <button
      type="button"
      onClick={onClick}
      title={title}
      className="p-1 rounded hover:bg-[#2a2a2a] cursor-pointer"
    >
      {children}
    </button>
  );
}

function KeyHint({ k, label }: { k: string; label: string }) {
  return (
    <span>
      <span
        className="px-1 rounded"
        style={{ background: "#1a1a1a", color: "#aaa" }}
      >
        {k}
      </span>
      <span style={{ color: "#666" }}> {label}</span>
    </span>
  );
}

function TailToolbar({
  follow,
  wrap,
  showAttrs,
  copied,
  lineCount,
  totalLineCount,
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
  totalLineCount: number;
  error: unknown;
  onToggleFollow: () => void;
  onToggleWrap: () => void;
  onToggleAttrs: () => void;
  onJumpToBottom: () => void;
  onCopy: () => void;
}) {
  const filtered = lineCount !== totalLineCount;
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
        <span>
          {filtered ? (
            <>
              <span style={{ color: "#12b2a6" }}>
                {lineCount.toLocaleString()}
              </span>{" "}
              / {totalLineCount.toLocaleString()} lines
            </>
          ) : (
            <>{totalLineCount.toLocaleString()} lines</>
          )}
        </span>
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
  lineIdx,
  wrap,
  showAttrs,
  lineMatches,
  activeMatchIdx,
  searching,
}: {
  line: TailLine;
  lineIdx: number;
  wrap: boolean;
  showAttrs: boolean;
  lineMatches: { match: SearchMatch; matchIdx: number }[];
  activeMatchIdx: number;
  searching: boolean;
}) {
  const segments = useMemo(() => parseAnsi(line.body), [line.body]);
  const isError = line.severityNum >= 17;
  // Fan matches out by location so body / attrKey / attrValue each get
  // just the hits that belong to them, with the global matchIdx preserved
  // for active-highlight detection.
  const { bodyHits, attrKeyHitsByIdx, attrValHitsByIdx } = useMemo(() => {
    const body: Highlight[] = [];
    const keys = new Map<number, Highlight[]>();
    const vals = new Map<number, Highlight[]>();
    for (const lm of lineMatches) {
      const h: Highlight = {
        start: lm.match.loc.start,
        end: lm.match.loc.end,
        matchIdx: lm.matchIdx,
      };
      switch (lm.match.loc.kind) {
        case "body":
          body.push(h);
          break;
        case "attrKey": {
          const arr = keys.get(lm.match.loc.attrIdx) ?? [];
          arr.push(h);
          keys.set(lm.match.loc.attrIdx, arr);
          break;
        }
        case "attrValue": {
          const arr = vals.get(lm.match.loc.attrIdx) ?? [];
          arr.push(h);
          vals.set(lm.match.loc.attrIdx, arr);
          break;
        }
      }
    }
    return { bodyHits: body, attrKeyHitsByIdx: keys, attrValHitsByIdx: vals };
  }, [lineMatches]);

  return (
    <div
      data-line-idx={lineIdx}
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
        {/* While a search is active we render the ANSI-stripped body so
            highlight offsets line up cleanly; ANSI colouring returns once
            the user closes the search bar. */}
        <span style={{ color: BODY_COLOR, fontWeight: 700 }}>
          {searching ? (
            <HighlightedText
              text={stripAnsi(line.body)}
              highlights={bodyHits}
              activeMatchIdx={activeMatchIdx}
            />
          ) : (
            segments.map((s, i) => (
              <span key={i} style={segmentStyle(s.style)}>
                {s.text}
              </span>
            ))
          )}
        </span>
        {showAttrs && line.attrs.length > 0 ? (
          <AttrPairs
            pairs={line.attrs}
            isErrorRow={isError}
            keyHits={attrKeyHitsByIdx}
            valueHits={attrValHitsByIdx}
            activeMatchIdx={activeMatchIdx}
            searching={searching}
          />
        ) : null}
      </span>
    </div>
  );
}

interface Highlight {
  start: number;
  end: number;
  matchIdx: number;
}

// Splits arbitrary text into plain + <mark> runs according to the search
// hits passed in. The currently-active match gets a distinct colour so the
// user can see which one `n` / `N` is on.
function HighlightedText({
  text,
  highlights,
  activeMatchIdx,
}: {
  text: string;
  highlights: Highlight[];
  activeMatchIdx: number;
}) {
  if (highlights.length === 0) return <>{text}</>;
  const parts: React.ReactNode[] = [];
  let cursor = 0;
  for (let i = 0; i < highlights.length; i++) {
    const h = highlights[i];
    if (h.start > cursor) parts.push(text.slice(cursor, h.start));
    const isActive = h.matchIdx === activeMatchIdx;
    parts.push(
      <mark
        key={i}
        style={{
          background: isActive ? "#ffaa00" : "#3a2f00",
          color: isActive ? "#111" : "#ffcc66",
          padding: "0 1px",
          borderRadius: 2,
        }}
      >
        {text.slice(h.start, h.end)}
      </mark>,
    );
    cursor = h.end;
  }
  if (cursor < text.length) parts.push(text.slice(cursor));
  return <>{parts}</>;
}

// Zerolog-style `key=value` attribute rendering. Keys + equals share the
// cyan field-name tone; values are rendered in the default body-value
// colour. On error-level rows, the `error` field's value gets the bold-red
// highlight zerolog's ConsoleWriter uses — the one attribute the user
// almost always needs to see first. When a search is active, keys and
// values are rendered via HighlightedText so matches in either half show
// the yellow <mark> treatment.
function AttrPairs({
  pairs,
  isErrorRow,
  keyHits,
  valueHits,
  activeMatchIdx,
  searching,
}: {
  pairs: AttrPair[];
  isErrorRow: boolean;
  keyHits: Map<number, Highlight[]>;
  valueHits: Map<number, Highlight[]>;
  activeMatchIdx: number;
  searching: boolean;
}) {
  return (
    <>
      {pairs.map((p, i) => {
        const isErrorField = isErrorRow && ERROR_ATTR_KEYS.has(p.key);
        const kHits = searching ? keyHits.get(i) ?? [] : [];
        const vHits = searching ? valueHits.get(i) ?? [] : [];
        const valueStr = formatAttrValue(p);
        return (
          <span key={i}>
            {" "}
            <span style={{ color: KEY_COLOR }}>
              {kHits.length > 0 ? (
                <HighlightedText
                  text={p.key}
                  highlights={kHits}
                  activeMatchIdx={activeMatchIdx}
                />
              ) : (
                p.key
              )}
              <span>=</span>
            </span>
            <span
              style={
                isErrorField
                  ? { color: ERROR_VALUE_COLOR, fontWeight: 700 }
                  : { color: VALUE_COLOR }
              }
            >
              {vHits.length > 0 ? (
                <HighlightedText
                  text={valueStr}
                  highlights={vHits}
                  activeMatchIdx={activeMatchIdx}
                />
              ) : (
                valueStr
              )}
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
