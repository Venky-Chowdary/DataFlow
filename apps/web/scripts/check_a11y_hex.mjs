#!/usr/bin/env node
/** Lint/a11y + raw-hex ratchet (audit P1-5 / P2-6).
 *  Three one-way budgets in a11y_hex_budget.json — any rise fails the gate:
 *  - hex color literals in src ts and tsx files (logo tokens are the only
 *    sanctioned palette; migrate literals to var(--df-*) on touch)
 *  - <img> without alt
 *  - onClick on a non-interactive element without key handler / role /
 *    tabIndex (jsx-a11y click-events-have-key-events equivalent)
 *  Lower the numbers as sweeps land; never raise them. */
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const webRoot = path.resolve(__dirname, "..");
const srcRoot = path.join(webRoot, "src");
const budget = JSON.parse(
  fs.readFileSync(path.join(webRoot, "a11y_hex_budget.json"), "utf8"),
);

const HEX_RE = /#[0-9a-fA-F]{3}\b|#[0-9a-fA-F]{6}\b|#[0-9a-fA-F]{8}\b/g;
const IMG_RE = /<img\b[^>]*>/g;
const CLICK_RE = /<(div|span|li|td|tr|section|article|header)\b[^>]*onClick=[^>]*>/g;
const KEY_OK = /onKey(Down|Up|Press)=|role=|tabIndex=/;
const FIELD_RE = /<(input|select|textarea)\b[^>]*>/g;
const FIELD_OK =
  /aria-label(ledby)?=|\btitle=|\bid=|type="?(hidden|submit|button)"?|placeholder=/;

let hex = 0, hexCss = 0, imgNoAlt = 0, clickNoKey = 0, unlabeledFields = 0;
const hexFiles = new Map();
(function walk(dir) {
  for (const ent of fs.readdirSync(dir, { withFileTypes: true })) {
    const p = path.join(dir, ent.name);
    if (ent.isDirectory()) {
      if (ent.name === "node_modules" || ent.name.startsWith(".")) continue;
      walk(p);
      continue;
    }
    if (!/\.(ts|tsx|css)$/.test(ent.name) || ent.name.includes(".test.")) continue;
    const s = fs.readFileSync(p, "utf8");
    if (ent.name.endsWith(".css")) {
      hexCss += (s.match(HEX_RE) || []).length;
      continue;
    }
    const h = s.match(HEX_RE) || [];
    if (h.length) {
      hex += h.length;
      hexFiles.set(path.relative(srcRoot, p), h.length);
    }
    for (const m of s.matchAll(IMG_RE)) {
      if (!m[0].includes("alt=")) imgNoAlt += 1;
    }
    for (const m of s.matchAll(CLICK_RE)) {
      if (!KEY_OK.test(m[0])) clickNoKey += 1;
    }
    for (const m of s.matchAll(FIELD_RE)) {
      if (!FIELD_OK.test(m[0])) unlabeledFields += 1;
    }
  }
})(srcRoot);

console.log(`hex literals in ts/tsx: ${hex} (budget ${budget.max_hex_literals})`);
console.log(`hex literals in css: ${hexCss} (budget ${budget.max_hex_literals_css})`);
for (const [f, n] of [...hexFiles].sort((a, b) => b[1] - a[1]).slice(0, 8))
  console.log(`  ${n}\t${f}`);
console.log(`img without alt: ${imgNoAlt} (budget ${budget.max_img_without_alt})`);
console.log(
  `clickable without key/role: ${clickNoKey} (budget ${budget.max_clickable_without_key_or_role})`,
);
console.log(
  `fields without a label hook: ${unlabeledFields} (budget ${budget.max_unlabeled_fields ?? "n/a"})`,
);
let fail = 0;
if (hex > budget.max_hex_literals) fail = 1;
if (hexCss > budget.max_hex_literals_css) fail = 1;
if (imgNoAlt > budget.max_img_without_alt) fail = 1;
if (clickNoKey > budget.max_clickable_without_key_or_role) fail = 1;
if (budget.max_unlabeled_fields != null && unlabeledFields > budget.max_unlabeled_fields) fail = 1;
if (fail) {
  console.error("FAIL: a11y/hex budget exceeded — ratchets are one-way.");
  process.exit(1);
}
console.log("OK: within budgets");
