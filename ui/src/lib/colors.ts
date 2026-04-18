/**
 * Deterministic service-name to color mapping. 12 pastel hues — light
 * enough that dark ink rendered on top (duration labels, etc.) stays
 * readable without a heavyweight backing, and that the red error bar
 * still stands out against its service-coloured siblings.
 */
const PALETTE = [
  "#93c5fd", // blue
  "#86efac", // green
  "#fcd34d", // amber
  "#fca5a5", // red
  "#d8b4fe", // purple
  "#fdba74", // orange
  "#67e8f9", // cyan
  "#f9a8d4", // pink
  "#a5b4fc", // indigo
  "#6ee7b7", // emerald
  "#c7d2fe", // periwinkle
  "#fde68a", // warm yellow
] as const;

export function serviceColor(service: string): string {
  let h = 0;
  for (let i = 0; i < service.length; i++) {
    h = (h * 31 + service.charCodeAt(i)) | 0;
  }
  return PALETTE[Math.abs(h) % PALETTE.length];
}
