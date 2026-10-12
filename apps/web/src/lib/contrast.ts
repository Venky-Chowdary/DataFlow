/**
 * WCAG 2.1 relative luminance and contrast.
 *
 * The studio gates text at 4.5:1 and non-text boundaries at 3:1. Pairs that
 * fail today are named in the test so a later visual change has to update
 * the record instead of silently repainting the app.
 */

export function relativeLuminance(hex: string): number {
  const channels = rgb(hex).map((value) => {
    const scaled = value / 255;
    return scaled <= 0.03928 ? scaled / 12.92 : ((scaled + 0.055) / 1.055) ** 2.4;
  });
  return 0.2126 * channels[0] + 0.7152 * channels[1] + 0.0722 * channels[2];
}

export function contrastRatio(foreground: string, background: string): number {
  const lighter = Math.max(relativeLuminance(foreground), relativeLuminance(background));
  const darker = Math.min(relativeLuminance(foreground), relativeLuminance(background));
  return (lighter + 0.05) / (darker + 0.05);
}

function rgb(hex: string): [number, number, number] {
  const raw = hex.trim().replace("#", "");
  const full = raw.length === 3 ? raw.split("").map((part) => part + part).join("") : raw;
  if (!/^[0-9a-fA-F]{6}$/.test(full)) {
    throw new Error(`Expected a hex color, received ${hex}`);
  }
  return [
    Number.parseInt(full.slice(0, 2), 16),
    Number.parseInt(full.slice(2, 4), 16),
    Number.parseInt(full.slice(4, 6), 16),
  ];
}
