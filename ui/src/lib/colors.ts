/**
 * Deterministic service-name to color mapping. 12 distinguishable hues
 * sampled around the color wheel at a fixed saturation/lightness that works
 * on the warm-neutral surface (matches the Honeycomb light theme).
 */
const PALETTE = [
  "#3c78d8",
  "#6aa84f",
  "#c27ba0",
  "#e06666",
  "#f6b26b",
  "#8e63ce",
  "#45818e",
  "#b45f06",
  "#a64d79",
  "#674ea7",
  "#38761d",
  "#cc0000",
] as const;

export function serviceColor(service: string): string {
  let h = 0;
  for (let i = 0; i < service.length; i++) {
    h = (h * 31 + service.charCodeAt(i)) | 0;
  }
  return PALETTE[Math.abs(h) % PALETTE.length];
}
