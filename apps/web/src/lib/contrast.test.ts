import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";
import { describe, it } from "node:test";
function relativeLuminance(hex: string): number {
  const channels = rgb(hex).map((value) => {
    const scaled = value / 255;
    return scaled <= 0.03928 ? scaled / 12.92 : ((scaled + 0.055) / 1.055) ** 2.4;
  });
  return 0.2126 * channels[0] + 0.7152 * channels[1] + 0.0722 * channels[2];
}

function contrastRatio(foreground: string, background: string): number {
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

const root = join(dirname(fileURLToPath(import.meta.url)), "../../../..");
const tokens = readFileSync(join(root, "apps/web/src/styles/tokens.css"), "utf8");
const mark = readFileSync(join(root, "packages/design-system/src/brand/mark.ts"), "utf8");

/** Current text pairs that fail 4.5:1. Named so they stay visible, not fixed here. */
const KNOWN_TEXT_FAILURES: { name: string; fg: string; bg: string }[] = [
  { name: "legacy-brand-on-white", fg: "#0d9488", bg: "#ffffff" },
  { name: "white-on-legacy-brand", fg: "#ffffff", bg: "#0d9488" },
  { name: "teal-400-on-white", fg: "#14b8a6", bg: "#ffffff" },
  { name: "arrow-on-white", fg: "#2DD4BF", bg: "#ffffff" },
  { name: "attention-on-white", fg: "#F59E0B", bg: "#ffffff" },
  { name: "tertiary-on-white", fg: "#94a3b8", bg: "#ffffff" },
  { name: "legacy-success-on-white", fg: "#059669", bg: "#ffffff" },
  { name: "legacy-warning-on-white", fg: "#d97706", bg: "#ffffff" },
  { name: "legacy-info-on-white", fg: "#0284c7", bg: "#ffffff" },
];

const LOGO_TEXT: { name: string; fg: string; bg: string }[] = [
  { name: "tile-on-white", fg: "#0A3D3A", bg: "#ffffff" },
  { name: "brand-text-on-white", fg: "#0F766E", bg: "#ffffff" },
  { name: "on-tile", fg: "#F8FAFC", bg: "#0A3D3A" },
  { name: "tile-on-attention", fg: "#0A3D3A", bg: "#F59E0B" },
  { name: "arrow-on-tile", fg: "#2DD4BF", bg: "#0A3D3A" },
  { name: "arrow-on-dark-canvas", fg: "#2DD4BF", bg: "#0c1219" },
  { name: "brand-text-on-dark", fg: "#5eead4", bg: "#0c1219" },
  { name: "success-text-on-white", fg: "#047857", bg: "#ffffff" },
  { name: "warning-text-on-white", fg: "#92400E", bg: "#ffffff" },
  { name: "warning-text-on-warning-bg", fg: "#92400E", bg: "#fffbeb" },
  { name: "info-text-on-white", fg: "#0369a1", bg: "#ffffff" },
  { name: "danger-on-white", fg: "#dc2626", bg: "#ffffff" },
  { name: "muted-on-white", fg: "#64748b", bg: "#ffffff" },
  { name: "success-text-on-dark", fg: "#6ee7b7", bg: "#0c1219" },
  { name: "warning-text-on-dark", fg: "#fcd34d", bg: "#0c1219" },
  { name: "info-text-on-dark", fg: "#7dd3fc", bg: "#0c1219" },
];

describe("contrast", () => {
  it("records the legacy text pairs that fail WCAG AA", () => {
    for (const pair of KNOWN_TEXT_FAILURES) {
      const ratio = contrastRatio(pair.fg, pair.bg);
      assert.ok(ratio < 4.5, `${pair.name} was ${ratio.toFixed(2)}; the failure record is stale`);
    }
  });

  it("passes AA for logo-derived text and status text", () => {
    for (const pair of LOGO_TEXT) {
      const ratio = contrastRatio(pair.fg, pair.bg);
      assert.ok(ratio >= 4.5, `${pair.name} is ${ratio.toFixed(2)}:1`);
    }
  });

  it("keeps the selected-node border above the non-text boundary", () => {
    assert.ok(contrastRatio("#0A3D3A", "#f3f5f7") >= 3);
    assert.ok(contrastRatio("#2DD4BF", "#0c1219") >= 3);
  });

  it("locks studio token names to the mark hex and leaves the legacy fill", () => {
    for (const hex of ["#0A3D3A", "#F59E0B", "#F8FAFC", "#0F766E", "#2DD4BF"]) {
      assert.match(mark, new RegExp(hex, "i"));
      assert.match(tokens, new RegExp(hex, "i"));
    }
    assert.match(tokens, /--df-brand-tile:\s*var\(--df-logo-tile\)/);
    assert.match(tokens, /--df-brand-text:\s*var\(--df-logo-ink\)/);
    assert.match(tokens, /--df-brand-arrow:\s*var\(--df-logo-arrow\)/);
    assert.match(tokens, /--df-attention:\s*var\(--df-logo-strap\)/);
    assert.match(tokens, /--df-on-attention:\s*var\(--df-logo-tile\)/);
    assert.match(tokens, /--df-brand:\s*#0d9488/i);
  });
});
