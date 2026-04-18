import { ChevronDown, ChevronRight } from "lucide-react";
import clsx from "clsx";

interface Props {
  label: string;
  open: boolean;
  onToggle: () => void;
  children: React.ReactNode;
  /** Optional compact summary shown on the header bar — typically only
   *  rendered when the accordion is collapsed, so the user can see the
   *  key state without expanding. */
  collapsedSummary?: React.ReactNode;
}

// A single accordion — clickable header bar plus a collapsible body. The
// body uses grid-template-rows 0fr→1fr so the content height animates
// without having to know its measured height.
export function Accordion({
  label,
  open,
  onToggle,
  children,
  collapsedSummary,
}: Props) {
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
        aria-expanded={open}
      >
        {open ? (
          <ChevronDown className="w-3.5 h-3.5" />
        ) : (
          <ChevronRight className="w-3.5 h-3.5" />
        )}
        <span className="uppercase tracking-wide">{label}</span>
        {!open && collapsedSummary ? (
          <span className="ml-2 normal-case tracking-normal truncate">
            {collapsedSummary}
          </span>
        ) : null}
      </button>
      <div
        className="grid transition-[grid-template-rows] duration-150 ease-out"
        style={{ gridTemplateRows: open ? "1fr" : "0fr" }}
      >
        <div className="overflow-hidden">{children}</div>
      </div>
    </div>
  );
}
