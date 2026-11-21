import assert from "node:assert/strict";
import { readFileSync, readdirSync } from "node:fs";
import path from "node:path";
import { test } from "node:test";
import { fileURLToPath } from "node:url";
import { createElement } from "react";
import { renderToStaticMarkup } from "react-dom/server";
import { JSDOM } from "jsdom";
import { Callout, type CalloutTone } from "./Callout";

const STYLES = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "../../styles");

function render(props: Parameters<typeof Callout>[0]) {
  return new JSDOM(renderToStaticMarkup(createElement(Callout, props))).window.document;
}

test("each tone shows a 16px status icon and names the tone for screen readers", () => {
  const glyph: Record<CalloutTone, string> = {
    info: "lucide-info", success: "lucide-circle-check", warning: "lucide-triangle-alert",
    held: "lucide-triangle-alert", danger: "lucide-circle-x", neutral: "lucide-info",
  };
  for (const tone of Object.keys(glyph) as CalloutTone[]) {
    const doc = render({ tone, title: "Title" });
    const svg = doc.querySelector(".df-callout-icon svg");
    assert.ok(svg?.classList.contains(glyph[tone]), tone);
    assert.equal(svg?.getAttribute("width"), "16");
    assert.ok(doc.querySelector(`.df-callout--${tone}`));
    assert.match(doc.querySelector(".df-callout-icon .df2-sr-only")?.textContent || "", /: $/);
  }
});

test("title, body and one action", () => {
  const doc = render({
    tone: "warning",
    title: "3 tables held",
    children: createElement("p", null, "Rows that fail the contract wait in quarantine."),
    action: createElement("button", { type: "button" }, "Review held rows"),
  });
  assert.equal(doc.querySelector(".df-callout-title")?.textContent, "3 tables held");
  assert.match(doc.querySelector(".df-callout-body")?.textContent || "", /quarantine/);
  assert.equal(doc.querySelectorAll(".df-callout-action button").length, 1);
});

test("administrator details render only for administrators", () => {
  const props = { tone: "danger" as const, title: "Couldn't save", adminDetails: "DATAWRAP_SECRET_KEY is not set" };
  assert.equal(render(props).querySelector(".df-callout-admin"), null);
  assert.equal(render({ ...props, isAdmin: false }).body.textContent?.includes("DATAWRAP_"), false);
  const admin = render({ ...props, isAdmin: true });
  assert.equal(admin.querySelector(".df-callout-admin summary")?.textContent, "Details for administrators");
  assert.match(admin.querySelector(".df-callout-admin-body")?.textContent || "", /DATAWRAP_SECRET_KEY/);
});

test("callouts never use a side stripe", () => {
  const css = readFileSync(path.join(STYLES, "feedback.css"), "utf8");
  const rules = css.match(/\.df-callout[^{]*\{[^}]*\}/g) ?? [];
  assert.ok(rules.length > 3);
  for (const rule of rules) assert.doesNotMatch(rule, /border-left\s*:\s*[2-9]px/, rule);
});

test("no app stylesheet draws a 2-5px left stripe on a callout or card", () => {
  // Kept on purpose: navigation/selection indicators and row status markers
  // (listed in the UI pass report), not callouts.
  const allowed = new Set([
    ".df2-job-item.failed", ".df2-mapping-col.high", ".df2-mapping-col.medium", ".df2-mapping-col.low",
    ".df2-preflight-step", ".df2-job-row.is-live", ".df2-job-row.is-failed", ".df2-ms-combo-option",
    ".df2-ms-dest-row", ".df2-settings-nav button.active", ".df2-qw-history-item", ".df2-repair-issues li",
    ".df2-xform-catalog-row.has-findings", ".df2-xform-col.has-findings", ".df2-run-result",
  ]);
  const offenders: string[] = [];
  for (const file of readdirSync(STYLES).filter((f) => f.endsWith(".css") && !/^(marketing-|landing)/.test(f))) {
    const css = readFileSync(path.join(STYLES, file), "utf8").replace(/\/\*[\s\S]*?\*\//g, "");
    for (const m of css.matchAll(/([^{}]+)\{([^{}]*)\}/g)) {
      if (!/border-left\s*:\s*[2-5]px solid/.test(m[2])) continue;
      const selector = m[1].trim().replace(/\s+/g, " ");
      if (!allowed.has(selector)) offenders.push(`${file}: ${selector}`);
    }
  }
  assert.deepEqual(offenders, []);
});
