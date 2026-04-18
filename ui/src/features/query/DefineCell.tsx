import * as RadixPopover from "@radix-ui/react-popover";
import type { ReactNode } from "react";

interface Props {
  label: string;
  value: ReactNode;
  placeholder?: string;
  isEmpty?: boolean;
  editor: ReactNode;
}

/**
 * One cell of Honeycomb's Define grid. Shows an uppercase label (rendered
 * as a link so the field-name is visually discoverable) with the current
 * value beneath it. Click anywhere on the cell to open the popover editor.
 * Empty state renders muted placeholder text ("None; include all events").
 */
export function DefineCell({ label, value, placeholder, isEmpty, editor }: Props) {
  return (
    <RadixPopover.Root>
      <RadixPopover.Trigger asChild>
        <button
          type="button"
          className="text-left w-full group flex flex-col gap-1 py-2 outline-none focus-visible:ring-2 focus-visible:ring-[var(--color-accent)] rounded"
        >
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
