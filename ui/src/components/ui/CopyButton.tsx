import { useState } from "react";
import { Check, Copy } from "lucide-react";
import clsx from "clsx";

interface Props {
  value: string;
  label?: string;
  className?: string;
}

/**
 * One-click copy affordance. Writes `value` to the clipboard and flashes a
 * ✓ tick for ~1.2s as confirmation. Sized small so it slots inline next to
 * IDs and other monospace values without disturbing layout.
 */
export function CopyButton({ value, label, className }: Props) {
  const [copied, setCopied] = useState(false);

  const onClick = (e: React.MouseEvent) => {
    e.stopPropagation();
    e.preventDefault();
    navigator.clipboard
      .writeText(value)
      .then(() => {
        setCopied(true);
        setTimeout(() => setCopied(false), 1200);
      })
      .catch(() => {
        // Clipboard API can fail outside secure contexts (http://); ignore
        // silently rather than throwing a red error on a dev tool.
      });
  };

  return (
    <button
      type="button"
      onClick={onClick}
      title={copied ? "Copied" : (label ?? "Copy to clipboard")}
      aria-label={label ?? "Copy to clipboard"}
      className={clsx(
        "inline-flex items-center justify-center p-1 rounded",
        "hover:bg-[var(--color-border)]/50",
        className,
      )}
    >
      {copied ? (
        <Check className="w-3 h-3" style={{ color: "var(--color-ok)" }} />
      ) : (
        <Copy className="w-3 h-3" style={{ color: "var(--color-ink-muted)" }} />
      )}
    </button>
  );
}
