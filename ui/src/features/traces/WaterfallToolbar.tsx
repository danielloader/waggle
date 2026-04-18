import {
  AlertCircle,
  ChevronDown,
  ChevronUp,
  FoldVertical,
  Search,
  UnfoldVertical,
  X,
} from "lucide-react";
import clsx from "clsx";

interface Props {
  searchText: string;
  onSearchChange: (v: string) => void;
  matchCount: number;
  currentMatch: number; // 0-based; -1 when no matches
  onPrevMatch: () => void;
  onNextMatch: () => void;

  errorCount: number;
  currentError: number; // 0-based; -1 when no errors
  onPrevError: () => void;
  onNextError: () => void;

  onExpandAll: () => void;
  onCollapseAll: () => void;
}

/**
 * Compact control strip over the waterfall. Three groups:
 *   1. Expand / collapse-all toggles
 *   2. Substring search with prev/next navigation
 *   3. Error count + prev/next jump (hidden when the trace is all green)
 */
export function WaterfallToolbar({
  searchText,
  onSearchChange,
  matchCount,
  currentMatch,
  onPrevMatch,
  onNextMatch,
  errorCount,
  currentError,
  onPrevError,
  onNextError,
  onExpandAll,
  onCollapseAll,
}: Props) {
  return (
    <div
      className="flex items-center gap-2 px-3 py-1.5 border-b text-xs"
      style={{
        background: "var(--color-surface)",
        borderColor: "var(--color-border)",
      }}
    >
      <IconButton onClick={onExpandAll} title="Expand all">
        <UnfoldVertical className="w-4 h-4" />
      </IconButton>
      <IconButton onClick={onCollapseAll} title="Collapse all">
        <FoldVertical className="w-4 h-4" />
      </IconButton>

      <div className="h-4 w-px" style={{ background: "var(--color-border)" }} />

      <label
        className="flex items-center gap-1.5 px-2 py-1 rounded border flex-1 max-w-xs"
        style={{
          borderColor: "var(--color-border)",
          background: "var(--color-surface-muted)",
        }}
      >
        <Search className="w-3.5 h-3.5" style={{ color: "var(--color-ink-muted)" }} />
        <input
          value={searchText}
          onChange={(e) => onSearchChange(e.target.value)}
          placeholder="Search spans…"
          className="flex-1 bg-transparent outline-none font-mono"
        />
        {searchText && (
          <>
            <span
              className="tabular-nums"
              style={{ color: "var(--color-ink-muted)" }}
            >
              {matchCount === 0 ? "0" : `${currentMatch + 1}/${matchCount}`}
            </span>
            <IconButton
              onClick={onPrevMatch}
              disabled={matchCount === 0}
              title="Previous match"
              small
            >
              <ChevronUp className="w-3.5 h-3.5" />
            </IconButton>
            <IconButton
              onClick={onNextMatch}
              disabled={matchCount === 0}
              title="Next match"
              small
            >
              <ChevronDown className="w-3.5 h-3.5" />
            </IconButton>
            <IconButton
              onClick={() => onSearchChange("")}
              title="Clear search"
              small
            >
              <X className="w-3.5 h-3.5" />
            </IconButton>
          </>
        )}
      </label>

      {errorCount > 0 && (
        <>
          <div className="h-4 w-px" style={{ background: "var(--color-border)" }} />
          <div
            className="flex items-center gap-1 px-2 py-0.5 rounded"
            style={{
              background: "rgba(192, 54, 47, 0.08)",
              color: "var(--color-error)",
            }}
          >
            <AlertCircle className="w-3.5 h-3.5" />
            <span className="tabular-nums">
              {currentError >= 0 ? `${currentError + 1}/${errorCount}` : errorCount}
            </span>
            <span className="uppercase tracking-wide text-[10px] font-medium">
              error{errorCount === 1 ? "" : "s"}
            </span>
            <IconButton onClick={onPrevError} title="Previous error" small>
              <ChevronUp className="w-3.5 h-3.5" />
            </IconButton>
            <IconButton onClick={onNextError} title="Next error" small>
              <ChevronDown className="w-3.5 h-3.5" />
            </IconButton>
          </div>
        </>
      )}
    </div>
  );
}

function IconButton({
  children,
  onClick,
  title,
  disabled,
  small,
}: {
  children: React.ReactNode;
  onClick: () => void;
  title: string;
  disabled?: boolean;
  small?: boolean;
}) {
  return (
    <button
      type="button"
      onClick={onClick}
      title={title}
      disabled={disabled}
      className={clsx(
        "inline-flex items-center justify-center rounded",
        small ? "p-0.5" : "p-1",
        disabled
          ? "opacity-40 cursor-not-allowed"
          : "hover:bg-[var(--color-border)]/50",
      )}
    >
      {children}
    </button>
  );
}
