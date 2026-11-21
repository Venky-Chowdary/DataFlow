/**
 * Review UI2 #13: no ALL-CAPS labels or Title Case status words on the
 * screens UI pass 2 touched. The words are sentence case in the source; this
 * guards the stylesheet that stops CSS from re-casing them (verified in the
 * browser: a computed-style probe of Jobs, the job drawer, Validate, Run and
 * the connection test found no uppercase/capitalize text after it).
 * Run: npx tsx --test apps/web/src/styles/sentenceCase.test.ts
 */
import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { describe, it } from "node:test";

const css = readFileSync(new URL("./sentence-case.css", import.meta.url), "utf8").replace(/\/\*[\s\S]*?\*\//g, "");
const app = readFileSync(new URL("./app-styles.css", import.meta.url), "utf8");

function rulesFor(selector: string): string[] {
  const out: string[] = [];
  for (const m of css.matchAll(/([^{}]+)\{([^}]*)\}/g)) {
    const sels = m[1].split(",").map((x) => x.replace(/\/\*[\s\S]*?\*\//g, "").trim());
    if (sels.includes(selector)) out.push(m[2]);
  }
  return out;
}

describe("sentence case on the touched screens", () => {
  const labels = [
    ".df2-app .df2-jobs-grid-table thead th",
    ".df2-app .df2-jobs-kpis .df2-stat-label",
    ".df2-app .df2-jobs-v3-summary-route span",
    ".df2-app .df2-copy-id-label",
    ".df2-app .df2-quarantine-next-copy > strong",
    ".df2-app .df2-gate8-proof-grid dt",
    ".df2-app .df2-jobs-grid .df2-badge",
    ".df2-app .df2-jobs-detail-drawer .df2-badge",
    ".df2-app .df2-conn-probe .df2-badge",
    ".df2-app .df2-theater-v3 .df2-badge",
    ".df2-app .df2-copilot-edge-open",
  ];
  for (const selector of labels) {
    it(selector, () => {
      const rules = rulesFor(selector);
      assert.ok(rules.some((body) => /text-transform:\s*none/.test(body)), `${selector} keeps the source's case`);
    });
  }
  it("is loaded by the app", () => {
    assert.match(app, /^@import "\.\/sentence-case\.css";$/m);
    assert.doesNotMatch(css, /text-transform:\s*(uppercase|capitalize)/);
  });
});
