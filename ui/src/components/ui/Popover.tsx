import * as RadixPopover from "@radix-ui/react-popover";
import type { ReactNode } from "react";

interface PopoverProps {
  trigger: ReactNode;
  children: ReactNode;
  align?: "start" | "center" | "end";
  side?: "top" | "right" | "bottom" | "left";
}

/**
 * Thin styled wrapper around Radix Popover. All our query-editor popovers
 * use this so they share border/shadow/portal behaviour.
 */
export function Popover({ trigger, children, align = "start", side = "bottom" }: PopoverProps) {
  return (
    <RadixPopover.Root>
      <RadixPopover.Trigger asChild>{trigger}</RadixPopover.Trigger>
      <RadixPopover.Portal>
        <RadixPopover.Content
          align={align}
          side={side}
          sideOffset={4}
          className="z-50 min-w-[220px] rounded-md border p-2 shadow-lg"
          style={{
            background: "var(--color-surface)",
            borderColor: "var(--color-border)",
          }}
        >
          {children}
        </RadixPopover.Content>
      </RadixPopover.Portal>
    </RadixPopover.Root>
  );
}
