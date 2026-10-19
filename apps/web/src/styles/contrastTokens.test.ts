import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";
import { describe, it } from "node:test";

const stylesDir = dirname(fileURLToPath(import.meta.url));

function sheet(name: string): string {
  return readFileSync(join(stylesDir, name), "utf8");
}

/** WCAG 2.x relative luminance of a #rrggbb or rgb() colour. */
function luminance(color: string): number {
  let n: number;
  const hex = /^#([0-9a-f]{6})$/i.exec(color.trim());
  const rgb = /^rgb\((\d+),\s*(\d+),\s*(\d+)\)$/.exec(color.trim());
  if (hex) n = parseInt(hex[1], 16);
  else if (rgb) n = (Number(rgb[1]) << 16) | (Number(rgb[2]) << 8) | Number(rgb[3]);
  else assert.fail(`expected #rrggbb or rgb(r,g,b), got ${color}`);
  const channel = (v: number) => {
    const c = v / 255;
    return c <= 0.03928 ? c / 12.92 : Math.pow((c + 0.055) / 1.055, 2.4);
  };
  return (
    0.2126 * channel((n >> 16) & 0xff) +
    0.7152 * channel((n >> 8) & 0xff) +
    0.0722 * channel(n & 0xff)
  );
}

function ratio(a: string, b: string): number {
  const [hi, lo] = [luminance(a), luminance(b)].sort((x, y) => y - x);
  return (hi + 0.05) / (lo + 0.05);
}

/** Read a token's literal value from a theme block of tokens.css. */
function tokenValue(css: string, blockRe: RegExp, name: string): string {
  const block = css.match(blockRe);
  assert.ok(block, `theme block not found for ${name}`);
  const re = new RegExp(`${name.replace(/[.*+?^${}()|[\]\\]/g, "\\$&")}:\\s*(#[0-9a-f]{6})`, "i");
  const m = block[0].match(re);
  assert.ok(m, `${name} has no literal hex value`);
  return m[1];
}

const tokens = sheet("tokens.css");
const LIGHT = /:root\s*\{[\s\S]*?\n\}/;
const DARK = /html\[data-theme="dark"\][\s\S]*?\{[\s\S]*?\n\}/;

// Measured surfaces (independent Playwright harness, alpha-composited):
// the light active-nav tint is --df-brand-muted; the dark active-nav tint
// composites to rgb(21,49,56); white text surface is --df-surface.
const WHITE = tokenValue(tokens, LIGHT, "--df-surface");
const LIGHT_TINT = tokenValue(tokens, LIGHT, "--df-brand-muted");
const LIGHT_MUTED = tokenValue(tokens, LIGHT, "--df-surface-muted");
const DARK_TINT = "rgb(21,49,56)";

describe("contrast tokens meet WCAG AA (>=4.5:1)", () => {
  it("light tertiary text on white, muted, and the nav tint", () => {
    const ink = tokenValue(tokens, LIGHT, "--df-text-tertiary");
    for (const bg of [WHITE, LIGHT_MUTED, LIGHT_TINT]) {
      assert.ok(ratio(ink, bg) >= 4.5, `${ink} on ${bg} = ${ratio(ink, bg).toFixed(2)}:1`);
    }
  });

  it("dark tertiary text on the dark surface and tint composite", () => {
    const ink = tokenValue(tokens, DARK, "--df-text-tertiary");
    const surface = tokenValue(tokens, DARK, "--df-surface");
    for (const bg of [surface, DARK_TINT]) {
      assert.ok(ratio(ink, bg) >= 4.5, `${ink} on ${bg} = ${ratio(ink, bg).toFixed(2)}:1`);
    }
  });

  it("action fill carries white text in BOTH themes", () => {
    for (const [name, block] of [["light", LIGHT], ["dark", DARK]] as const) {
      const fill = tokenValue(tokens, block, "--df-action-bg");
      assert.ok(
        ratio(fill, WHITE) >= 4.5,
        `${name}: white on ${fill} = ${ratio(fill, WHITE).toFixed(2)}:1`,
      );
    }
  });

  it("action ink on the light nav tint clears AA in both themes", () => {
    for (const [name, block] of [["light", LIGHT], ["dark", DARK]] as const) {
      const ink = tokenValue(tokens, block, "--df-action-bg");
      assert.ok(
        ratio(ink, LIGHT_TINT) >= 4.5,
        `${name}: ${ink} on ${LIGHT_TINT} = ${ratio(ink, LIGHT_TINT).toFixed(2)}:1`,
      );
    }
  });

  it("--df-brand-strong stays a text colour, never a white-text fill", () => {
    // Dark brand-strong is a pale teal — white on it is ~1.5:1. Assert the
    // token is never used as a background that also carries white text.
    const files = [
      "dataflow-ui.css",
      "enterprise-platform.css",
      "enterprise-ui.css",
      "shell-polish.css",
      "premium-theme.css",
    ];
    for (const f of files) {
      const css = sheet(f);
      const bad = css.match(/\.[^{]*\{[^}]*background:[^;}]*var\(--df-brand-strong\)[^}]*color:\s*(#fff|white|var\(--df-surface\))/);
      assert.equal(bad, null, `${f}: white text on var(--df-brand-strong) fill`);
    }
  });

  it("OCR/policy-toggle heading routes through --df-text-primary", () => {
    const ui = sheet("dataflow-ui.css");
    const pinned = tokenValue(tokens, LIGHT, "--df-text-primary");
    assert.match(ui, /\.df2-policy-toggle\s*\{[\s\S]{0,300}?color:\s*var\(--df-text-primary/);
    assert.doesNotMatch(
      ui,
      new RegExp(`\\.df2-policy-toggle\\s*\\{[\\s\\S]{0,300}?color:\\s*${pinned}`),
      `policy-toggle pins the light ink ${pinned} instead of the remapping token`,
    );
  });
});

describe("text-bearing controls route through --df-action-bg", () => {
  it("primary button fill (the real painted rule, incl. !important)", () => {
    const premium = sheet("premium-theme.css");
    assert.match(premium, /\.df2-btn-primary\s*\{[^}]*var\(--df-action-bg/);
    const accent = tokenValue(tokens, LIGHT, "--df-accent");
    assert.doesNotMatch(
      premium,
      new RegExp(`\\.df2-btn-primary\\s*\\{[^}]*var\\(--df-accent, ${accent}\\)`),
      `primary fill fell back to --df-accent (${accent}, below AA for white text)`,
    );
    const platform = sheet("enterprise-platform.css");
    assert.match(platform, /\.df2-btn-primary\s*\{[^}]*var\(--df-action-bg/);
  });

  it("sidebar CTA + wizard numeral + source-aside marker", () => {
    const dataflow = sheet("dataflow-ui.css");
    const platform = sheet("enterprise-platform.css");
    const enterprise = sheet("enterprise-ui.css");
    assert.match(platform, /\.df2-sidebar-cta\s*\{[^}]*var\(--df-action-bg/);
    assert.match(dataflow, /\.df2-wizard-step\.active \.df2-wizard-num\s*\{[^}]*var\(--df-action-bg/);
    assert.match(enterprise, /\.df2-source-aside-steps li\.active \.df2-source-aside-step-marker\s*\{[^}]*var\(--df-action-bg/);
  });

  it("every .df2-nav-item.active ink rule uses the action token", () => {
    for (const f of ["dataflow-ui.css", "shell-polish.css", "enterprise-platform.css"]) {
      const css = sheet(f);
      const rules = css.match(/\.df2-nav-item\.active\s*\{[^}]*\}/g) ?? [];
      assert.ok(rules.length >= 1, `${f}: expected a .df2-nav-item.active rule`);
      for (const rule of rules) {
        if (/color:/.test(rule)) {
          assert.match(rule, /color:\s*var\(--df-action-bg/, `${f}: ${rule}`);
        }
      }
      const icons = css.match(/\.df2-nav-item\.active[^{]*\.dt-nav-icon[^{]*\{[^}]*\}/g) ?? [];
      for (const rule of icons) {
        if (/color:/.test(rule)) {
          assert.match(rule, /color:\s*var\(--df-action-bg/, `${f}: ${rule}`);
        }
      }
    }
  });
});
