import { Search } from "lucide-react";

interface Props {
  value: string;
  onChange: (v: string) => void;
}

export function LogSearchInput({ value, onChange }: Props) {
  return (
    <label
      className="flex items-center gap-2 px-3 py-1.5 rounded-md border text-sm"
      style={{
        background: "var(--color-surface)",
        borderColor: "var(--color-border)",
      }}
    >
      <Search className="w-4 h-4" style={{ color: "var(--color-ink-muted)" }} />
      <input
        className="outline-none bg-transparent w-64 font-mono"
        placeholder="FTS search… e.g. refused OR timeout"
        value={value}
        onChange={(e) => onChange(e.target.value)}
      />
    </label>
  );
}
