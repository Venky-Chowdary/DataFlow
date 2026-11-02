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
  const i = css.indexOf(selector);
  if (i === -1) return null;
  const open = css.indexOf("{", i);
  const end = open === -1 ? -1 : css.indexOf("}", open);
  return end === -1 ? null : css.slice(open + 1, end);
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

describe("disabled brand CTA", () => {
  it("keeps a readable label and a >=3:1 boundary instead of a faded tint", () => {
    const rule = ruleBody(LANDING, ".lp-btn--brand:disabled");
    assert.ok(rule, "no dedicated disabled style for the brand CTA");
    const fill = rule.match(/background:\s*(#[0-9a-fA-F]{6})/);
    const label = rule.match(/color:\s*(#[0-9a-fA-F]{6})/);
    const edge = rule.match(/border-color:\s*(#[0-9a-fA-F]{6})/);
    assert.ok(fill && label && edge, "disabled CTA needs fill/label/border");
    assert.ok(contrastRatio(hex(label[1]), hex(fill[1])) >= 4.5,
              "disabled label must stay readable on its fill");
    assert.ok(contrastRatio(hex(edge[1]), [255, 255, 255]) >= 3,
              "disabled boundary must reach 3:1 against a white card");
    assert.doesNotMatch(rule, /opacity:\s*0\./, "an opacity fade is not a style");
  });
});

describe("settings card spacing (review-10 item 6)", () => {
  it("metric + change-email cards use the section's 12px gap token", () => {
    const rule = ruleBody(
      readFileSync(join(STYLES_DIR, "settings-enterprise.css"), "utf8"),
      ".df2-settings-policy-grid");
    assert.ok(rule, "no .df2-settings-policy-grid rule");
    assert.match(rule, /gap:\s*12px/);
    // Both card stacks must be on the grid token — a bare
    // .df2-settings-policy container has no gap and rows stack flush.
    for (const p of ["../pages/SettingsPage.tsx",
                     "../components/ChangeEmailCard.tsx"]) {
      const src = readFileSync(join(STYLES_DIR, p), "utf8");
      assert.doesNotMatch(src, /className="df2-settings-policy"/,
        `${p} still uses the gapless .df2-settings-policy`);
    }
  });
});

describe("accent rule under the brand H1 (review-10 item 8)", () => {
  it("is a flow element with a clear top margin, never absolute", () => {
    const rule = ruleBody(EDITORIAL, ".lp-login-brand-rule");
    assert.ok(rule, "no .lp-login-brand-rule rule");
    assert.doesNotMatch(rule, /position:\s*absolute/);
    const m = rule.match(/margin:\s*(\d+)px/);
    assert.ok(m && parseInt(m[1], 10) >= 12,
            "accent rule must keep >=12px clear of the heading");
    // The old absolutely-positioned rail is gone from markup and styles.
    const panel = readFileSync(
      join(STYLES_DIR, "../components/AuthBrandPanel.tsx"), "utf8");
    assert.doesNotMatch(panel, /lp-login-brand-rail/);
    assert.doesNotMatch(EDITORIAL, /\.lp-login-brand-rail/);
    assert.doesNotMatch(LANDING, /\.lp-login-brand-rail/);
  });
});

describe("mobile auth header (review-10 item 9)", () => {
  it("collapses the brand to content height — no 100% fill", () => {
    // The narrow-viewport block must undo the base height:100% on both the
    // brand and its inner wrapper, or the compact header still stretches
    // to the viewport and pushes the form below the fold.
    // The compact-header ruleset is the one that redefines the brand as
    // `min-height: auto` — slice from it, then read the same ruleset.
    const marker = ".lp-login-brand {\n    min-height: auto;";
    const i = EDITORIAL.indexOf(marker);
    assert.ok(i !== -1, "no mobile compact-header ruleset");
    const media = EDITORIAL.slice(i);
    const brand = ruleBody(media, ".lp-login-brand {");
    assert.ok(brand, "no mobile .lp-login-brand rule");
    assert.match(brand, /height:\s*auto/);
    assert.match(brand, /flex:\s*0 0 auto/);
    const inner = ruleBody(media, ".lp-login-brand-inner {");
    assert.ok(inner, "no mobile .lp-login-brand-inner rule");
    assert.match(inner, /height:\s*auto/);
  });
});

describe("mobile auth header rhythm (review-11 item 8)", () => {
  it("gives the head->title stack the tablet rhythm token at 390px", () => {
    // On mobile the title sat ~8px under the Back row. The <=560 block must
    // re-space it with the shared rhythm tokens, not another magic px.
    const media = EDITORIAL.match(
      /@media \(max-width: 559px\)\s*\{[^@]*lp-login-brand-inner[^@]*\}/s);
    assert.ok(media, "no <=559 rhythm override for the auth brand header");
    assert.match(media[0],
      /\.lp-login-brand-inner\s*\{[^}]*gap:\s*var\(--lp-brand-head-gap/s);
    assert.match(media[0],
      /\.lp-login-brand-body\s*\{[^}]*padding-top:\s*var\(--lp-brand-title-pad/s);
  });
});
