/**
 * Chrome contracts for the auth shell — review-9 screenshot items.
 * Each rule is pinned because the pixel shots alone couldn't catch it in CI.
 */
import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";
import { describe, it } from "node:test";

const STYLES_DIR = dirname(fileURLToPath(import.meta.url));
const EDITORIAL = readFileSync(join(STYLES_DIR, "marketing-editorial.css"), "utf8");
const LANDING = readFileSync(join(STYLES_DIR, "landing.css"), "utf8");

function ruleBody(css: string, selector: string): string | null {
  const i = css.indexOf(`${selector} {`);
  if (i === -1) return null;
  const end = css.indexOf("}", i);
  return end === -1 ? null : css.slice(i + selector.length + 1, end);
}

/** sRGB relative-luminance contrast ratio. */
function contrastRatio(a: [number, number, number], b: [number, number, number]): number {
  const lum = ([r, g, b]: [number, number, number]) => {
    const f = (c: number) => {
      const s = c / 255;
      return s <= 0.03928 ? s / 12.92 : Math.pow((s + 0.055) / 1.055, 2.4);
    };
    return 0.2126 * f(r) + 0.7152 * f(g) + 0.0722 * f(b);
  };
  const [l1, l2] = [lum(a), lum(b)].sort((x, y) => y - x);
  return (l1 + 0.05) / (l2 + 0.05);
}

const hex = (h: string): [number, number, number] => {
  const n = parseInt(h.slice(1), 16);
  return [(n >> 16) & 255, (n >> 8) & 255, n & 255];
};

describe("brand wordmark on the always-dark panel", () => {
  it("uses the on-dark variant, not --df-text-primary", () => {
    const rule = ruleBody(EDITORIAL, ".lp-login-brand-logo .dw-wordmark-text");
    assert.ok(rule, "no explicit wordmark color on the brand panel");
    const m = rule.match(/color:\s*(#[0-9a-fA-F]{6})/);
    assert.ok(m, "wordmark must declare an explicit on-dark color");
    // #f0fdfa on the panel's darkest stop (#071c1a) is far beyond 4.5:1.
    assert.ok(contrastRatio(hex(m[1]), hex("#071c1a")) >= 4.5,
              `wordmark ${m[1]} on the brand panel fails AA`);
    assert.doesNotMatch(rule, /color:\s*var\(--df-text-primary\)/);
  });
});

describe("resend hint row", () => {
  it("stays on one line at mobile widths", () => {
    const rule = ruleBody(LANDING, ".lp-login-resend .lp-login-legal");
    assert.ok(rule, "no nowrap rule for the resend hint");
    assert.match(rule, /white-space:\s*nowrap/);
  });
});

describe("mobile brand header edge", () => {
  it("ends on a deliberate rounded seam, not a hard cut", () => {
    assert.match(EDITORIAL, /\.lp-login-brand\s*\{[^}]*border-radius:\s*0 0 26px 26px/);
  });
});
