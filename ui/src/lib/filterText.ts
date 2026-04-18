/**
 * Parser + serializer for the free-form filter text input.
 *
 * Grammar (informally):
 *
 *   filter    := field (WS op (WS value)?)?
 *   field     := IDENT                   // e.g. "service.name", "is_root"
 *   op        := = | != | > | >= | < | <= | contains | !contains
 *              | starts-with | !starts-with | ends-with | !ends-with
 *              | in | !in | exists | !exists
 *   value     := quoted | bare | comma_list   // bare = non-whitespace token
 *
 * A bare field with no op is handled by the serializer: booleans default
 * to `= true`, everything else defaults to `exists`. That mirrors how
 * Honeycomb's input accepts `is_root` or `http.status_code` on their own.
 */
import {
  BOOLEAN_META_FIELDS,
  FILTER_OPS,
  type Filter,
  type FilterOp,
} from "./query";

// Longest-first so "!=" doesn't match "!" and "starts-with" doesn't get
// chopped to "s". These are the literal surface strings.
const OP_TOKENS: readonly FilterOp[] = [
  "!starts-with",
  "starts-with",
  "!ends-with",
  "ends-with",
  "!contains",
  "contains",
  "!exists",
  "exists",
  "!in",
  "in",
  ">=",
  "<=",
  "!=",
  "=",
  ">",
  "<",
] as const;

// Ops that don't take a value — once parsed, the filter is complete.
const OPS_NO_VALUE = new Set<FilterOp>(["exists", "!exists"]);
// Ops whose value is a comma-separated list rather than a scalar.
const OPS_ARRAY = new Set<FilterOp>(["in", "!in"]);

export type ParsePhase = "field" | "op" | "value";

export interface ParsedFilter {
  field?: string;
  op?: FilterOp;
  /** The value text after the op, if any. */
  valueText?: string;
  /** Which token is the cursor on right now — drives suggestion selection. */
  phase: ParsePhase;
  /** The text of the token being completed (empty if cursor is on whitespace). */
  partial: string;
}

/** Incremental parse used by the combobox to decide which suggestions to show. */
export function parseFilter(text: string): ParsedFilter {
  const input = text.replace(/^\s+/, "");

  const firstWs = indexOfWhitespace(input);
  if (firstWs === -1) {
    return { phase: "field", partial: input };
  }

  const field = input.slice(0, firstWs);
  const afterField = input.slice(firstWs).replace(/^\s+/, "");
  if (afterField === "") {
    return { phase: "op", field, partial: "" };
  }

  const op = matchOp(afterField);
  if (!op) {
    // Still typing the operator (may be a valid prefix like "!", "s", "c").
    return { phase: "op", field, partial: afterField };
  }

  const remainder = afterField.slice(op.length);
  // Operator is "complete" when it either fills the rest of the string or
  // is followed by whitespace. Otherwise treat as still-typing.
  const completed =
    remainder === "" || /^\s/.test(remainder) || OPS_NO_VALUE.has(op);
  if (!completed) {
    return { phase: "op", field, partial: afterField };
  }

  if (OPS_NO_VALUE.has(op)) {
    return { phase: "value", field, op, valueText: "", partial: "" };
  }

  const valueText = remainder.replace(/^\s+/, "");
  return { phase: "value", field, op, valueText, partial: valueText };
}

/** Try to compile the current text into a committable Filter. */
export function textToFilter(
  text: string,
  opts: { booleanFields?: Set<string> } = {},
): Filter | null {
  const p = parseFilter(text);
  if (!p.field) return null;
  const booleans = opts.booleanFields ?? BOOLEAN_META_FIELDS;
  const isBool = booleans.has(p.field);

  // Bare field: boolean → "= true", everything else → "exists".
  if (!p.op) {
    return isBool
      ? { field: p.field, op: "=", value: true }
      : { field: p.field, op: "exists" };
  }

  if (OPS_NO_VALUE.has(p.op)) {
    return { field: p.field, op: p.op };
  }

  const raw = (p.valueText ?? "").trim();
  if (raw === "") return null; // op chosen but no value yet

  if (OPS_ARRAY.has(p.op)) {
    const items = raw
      .split(",")
      .map((s) => s.trim())
      .filter(Boolean)
      .map(coerceScalar);
    if (items.length === 0) return null;
    // Drop booleans from the "in" list — not supported on the wire.
    const safe = items.filter(
      (v): v is string | number => typeof v !== "boolean",
    );
    return { field: p.field, op: p.op, value: safe };
  }

  return {
    field: p.field,
    op: p.op,
    value: coerceScalar(raw),
  };
}

/** Serialize a Filter back to text for edit-in-place. */
export function filterToText(f: Filter): string {
  const field = f.field;
  if (f.op === "exists" || f.op === "!exists") {
    return `${field} ${f.op}`;
  }
  if (f.op === "=" && f.value === true && BOOLEAN_META_FIELDS.has(field)) {
    // Render boolean metas as bare field; round-trips back via textToFilter.
    return field;
  }
  const val = Array.isArray(f.value) ? f.value.join(",") : formatScalar(f.value);
  return `${field} ${f.op} ${val}`;
}

// ---------------------------------------------------------------------------
// helpers
// ---------------------------------------------------------------------------

function indexOfWhitespace(s: string): number {
  for (let i = 0; i < s.length; i++) {
    if (/\s/.test(s[i])) return i;
  }
  return -1;
}

function matchOp(s: string): FilterOp | null {
  for (const op of OP_TOKENS) {
    if (s.startsWith(op)) return op;
  }
  return null;
}

function coerceScalar(raw: string): string | number | boolean {
  // Strip surrounding quotes if the user quoted (for values with spaces).
  const unquoted =
    (raw.startsWith('"') && raw.endsWith('"') && raw.length >= 2) ||
    (raw.startsWith("'") && raw.endsWith("'") && raw.length >= 2)
      ? raw.slice(1, -1)
      : raw;
  if (unquoted === "true") return true;
  if (unquoted === "false") return false;
  if (unquoted !== "" && !Number.isNaN(Number(unquoted))) return Number(unquoted);
  return unquoted;
}

function formatScalar(v: unknown): string {
  if (v === null || v === undefined) return "";
  if (typeof v === "string") {
    return /\s/.test(v) ? `"${v}"` : v;
  }
  return String(v);
}

/**
 * Build an updated input string by replacing the current partial token with
 * a chosen suggestion plus a trailing space so the next phase can start
 * typing. If the suggestion advances us past the current phase (e.g.
 * "exists" is a complete op that takes no value), the returned text is a
 * terminal filter with no trailing space.
 */
export function applySuggestion(
  text: string,
  parsed: ParsedFilter,
  suggestion: string,
): string {
  switch (parsed.phase) {
    case "field": {
      return `${suggestion} `;
    }
    case "op": {
      const fieldPart = parsed.field ?? "";
      if (OPS_NO_VALUE.has(suggestion as FilterOp)) {
        return `${fieldPart} ${suggestion}`;
      }
      return `${fieldPart} ${suggestion} `;
    }
    case "value": {
      const prefix = text.slice(0, text.length - parsed.partial.length);
      return `${prefix}${suggestion}`;
    }
  }
}

/** All filter ops in display order (for op-phase suggestions). */
export const FILTER_OP_DISPLAY: readonly FilterOp[] = FILTER_OPS;
