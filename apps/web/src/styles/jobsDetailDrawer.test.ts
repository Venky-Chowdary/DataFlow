/**
 * Two style contracts the Jobs detail drawer relies on:
 *
 * - Inside the drawer the header already names the job and shows its status,
 *   so the detail card hides its own copy of both (Rename, the row metric and
 *   the Job ID chip stay). Outside the drawer nothing is hidden.
 * - The CopyIdChip reads theme tokens, so it is not a white block in dark mode.
 */
import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";
import { describe, it } from "node:test";

const stylesDir = dirname(fileURLToPath(import.meta.url));
const sheet = (name: string) => readFileSync(join(stylesDir, name), "utf8");

/** [selector, body] pairs, comments removed. */
function blocks(css: string): Array<[string, string]> {
  const clean = css.replace(/\/\*[\s\S]*?\*\//g, "");
  const out: Array<[string, string]> = [];
  for (const match of clean.matchAll(/([^{}]+)\{([^{}]*)\}/g)) out.push([match[1].trim(), match[2]]);
  return out;
}

describe("Jobs detail drawer: no repeated name or status", () => {
  const rules = blocks(sheet("enterprise-ui.css"));
  const hiding = rules.filter(([, body]) => /display:\s*none/.test(body));

  it("hides the card's job title and status badge only inside the drawer", () => {
    const title = hiding.find(([selector]) => selector.includes(".df2-job-name-editor-title"));
    assert.ok(title, "a rule hides the inner job title");
    for (const part of title[0].split(",")) {
      assert.match(part, /\.df2-jobs-detail-drawer\b/, `scoped to the drawer: ${part.trim()}`);
      if (part.includes(".df2-job-name-editor-title")) {
        assert.match(part, /\.is-view\b/, "the rename input is never hidden");
      }
    }
    const badge = hiding.find(([selector]) => /\.df2-jobs-v3-summary-ids\s*>\s*\.df2-badge/.test(selector));
    assert.ok(badge, "a rule hides the inner status badge");
    assert.match(badge[0], /\.df2-jobs-detail-drawer\b/);
  });

  it("never hides the Rename button, the row metric or the Job ID chip", () => {
    for (const [selector] of hiding) {
      if (!selector.includes("df2-jobs-detail-drawer")) continue;
      assert.doesNotMatch(selector, /df2-job-name-rename-btn|df2-jobs-v3-summary-dest|df2-copy-id/, selector);
    }
  });

  it("the drawer mounts with the class those rules scope to", () => {
    const panel = readFileSync(join(stylesDir, "../components/jobs/JobDetailPanel.tsx"), "utf8");
    assert.match(panel, /className="df2-jobs-detail-drawer"/);
  });
});

describe("CopyIdChip colours come from theme tokens", () => {
  it("has no raw hex in any df2-copy-id rule", () => {
    const rules = blocks(sheet("dataflow-ui.css")).filter(([selector]) => selector.includes("df2-copy-id"));
    assert.ok(rules.length >= 4, "the chip, its label, value and button are styled");
    for (const [selector, body] of rules) {
      assert.doesNotMatch(body, /#[0-9a-fA-F]{3,8}\b/, `${selector} must use --df-* tokens`);
    }
    const chip = rules.find(([selector]) => selector === ".df2-copy-id")!;
    assert.match(chip[1], /background:\s*var\(--df-surface-muted\)/);
    assert.match(chip[1], /border:\s*1px solid var\(--df-border\)/);
    const button = rules.find(([selector]) => selector === ".df2-copy-id-btn")!;
    assert.match(button[1], /background:\s*var\(--df-surface\)/);
  });
});
