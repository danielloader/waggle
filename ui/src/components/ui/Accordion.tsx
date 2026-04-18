import { ChevronDown, ChevronRight } from "lucide-react";
import clsx from "clsx";

interface Props {
  label: string;
  open: boolean;
  onToggle: () => void;
  children: React.ReactNode;
  /** Optional compact summary shown on the header bar — rendered when the
   *  accordion is collapsed (manually or by scroll) so the user can see the
   *  key state without expanding. */
  collapsedSummary?: React.ReactNode;
  /** Scroll-driven collapse progress in [0, 1]. 0 = fully open, 1 = fully
   *  collapsed from scroll. Combined with `open`: the effective openness is
   *  `open ? (1 - collapseProgress) : 0`, so a scroll-collapsed accordion
   *  will re-expand as the user scrolls back to the top, but a user-closed
   *  accordion stays closed. Defaults to 0 (no scroll effect). */
  collapseProgress?: number;
}

// A single accordion — clickable header bar plus a collapsible body. The
// body uses `grid-template-rows: Nfr` where N ∈ [0, 1] so the content
// height scales linearly between 0 (collapsed) and its intrinsic size
// (fully open), and we can interpolate smoothly from scroll.
export function Accordion({
  label,
  open,
  onToggle,
  children,
  collapsedSummary,
  collapseProgress = 0,
}: Props) {
  const clamped = Math.max(0, Math.min(1, collapseProgress));
  const openness = open ? 1 - clamped : 0;
  // Show the collapsed summary once we're mostly shut — the content
  // underneath is effectively invisible by then, so the header text is the
  // only way to tell what the accordion holds.
  const lookCollapsed = openness < 0.1;
  return (
    <div className="border-b" style={{ borderColor: "var(--color-border)" }}>
      <button
        type="button"
        onClick={onToggle}
        className={clsx(
          "w-full flex items-center gap-2 px-4 py-1 text-xs",
          "hover:bg-[var(--color-surface-muted)]",
        )}
        style={{ color: "var(--color-ink-muted)" }}
        aria-expanded={!lookCollapsed}
      >
        {lookCollapsed ? (
          <ChevronRight className="w-3.5 h-3.5" />
        ) : (
          <ChevronDown className="w-3.5 h-3.5" />
        )}
        <span className="uppercase tracking-wide">{label}</span>
        {lookCollapsed && collapsedSummary ? (
          <span className="ml-2 normal-case tracking-normal truncate">
            {collapsedSummary}
          </span>
        ) : null}
      </button>
      <div
        className="grid"
        style={{
          gridTemplateRows: `${openness}fr`,
          // Short transition smooths a manual click (0→1 or 1→0) but stays
          // quick enough that scroll-driven updates don't lag visibly.
          transition: "grid-template-rows 80ms linear",
        }}
      >
        <div className="overflow-hidden">{children}</div>
      </div>
    </div>
  );
}
