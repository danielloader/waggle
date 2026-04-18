import * as RadixPopover from "@radix-ui/react-popover";
import * as Tooltip from "@radix-ui/react-tooltip";
import type { ReactNode } from "react";

interface Props {
  label: string;
  value: ReactNode;
  placeholder?: string;
  isEmpty?: boolean;
  /** Short explanation of what this cell does, shown on hover of the label. */
  description?: string;
  editor: ReactNode;
}

/**
 * One cell of Honeycomb's Define grid. Shows an uppercase label (rendered
 * with a dotted underline to hint at its role as a help affordance) with
 * the current value beneath it. Clicking anywhere on the cell opens the
 * popover editor; hovering the label surfaces a short description of what
 * the clause does. Empty state renders muted placeholder text ("None;
 * include all events").
 */
export function DefineCell({
  label,
  value,
  placeholder,
  isEmpty,
  description,
  editor,
}: Props) {
  const labelNode = (
    <span
      className="text-[11px] uppercase tracking-wider font-medium"
      style={{
        color: "var(--color-ink-muted)",
        textDecoration: "underline dotted",
        textUnderlineOffset: "3px",
      }}
    >
      {label}
    </span>
  );
  return (
    <RadixPopover.Root>
      <RadixPopover.Trigger asChild>
        <button
          type="button"
          className="text-left w-full group flex flex-col gap-1 py-2 outline-none focus-visible:ring-2 focus-visible:ring-[var(--color-accent)] rounded"
        >
          {description ? (
            <Tooltip.Root>
              <Tooltip.Trigger asChild>
                {/* span makes the tooltip trigger inline and keyboard-focusable
                    independent of the outer popover button. */}
                <span className="inline-block w-fit">{labelNode}</span>
              </Tooltip.Trigger>
              <Tooltip.Portal>
                <Tooltip.Content
                  side="top"
                  align="start"
                  sideOffset={6}
                  className="z-50 max-w-xs rounded-md border px-2.5 py-1.5 text-xs leading-snug shadow-lg"
                  style={{
                    background: "var(--color-surface)",
                    borderColor: "var(--color-border)",
                    color: "var(--color-ink)",
                  }}
                  onPointerDownOutside={(e) => e.preventDefault()}
                >
                  {description}
                  <Tooltip.Arrow style={{ fill: "var(--color-surface)" }} />
                </Tooltip.Content>
              </Tooltip.Portal>
            </Tooltip.Root>
          ) : (
            labelNode
          )}
          <span
            className="text-sm"
            style={{
              color: isEmpty ? "var(--color-ink-muted)" : "var(--color-ink)",
              fontStyle: isEmpty && placeholder ? "normal" : "normal",
            }}
          >
            {isEmpty && placeholder ? placeholder : value}
          </span>
        </button>
      </RadixPopover.Trigger>
      <RadixPopover.Portal>
        <RadixPopover.Content
          align="start"
          side="bottom"
          sideOffset={4}
          className="z-50 rounded-md border p-3 shadow-lg"
          style={{
            background: "var(--color-surface)",
            borderColor: "var(--color-border)",
            minWidth: 280,
            maxWidth: 520,
          }}
        >
          {editor}
        </RadixPopover.Content>
      </RadixPopover.Portal>
    </RadixPopover.Root>
  );
}
