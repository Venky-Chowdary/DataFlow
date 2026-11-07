#!/usr/bin/env node
/** Cascade ratchet — the !important count in apps/web/src can only go down.
 *  Baseline 3,330 (SHA b8b5dcba) -> current budget in important_budget.json.
 *  Lower the budget after every successful tranche; never raise it. */
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const webRoot = path.resolve(__dirname, "..");
const srcRoot = path.join(webRoot, "src");
const budget = JSON.parse(
  fs.readFileSync(path.join(webRoot, "important_budget.json"), "utf8"),
);
const MAX = budget.max_important_occurrences;

const exts = new Set([".css", ".ts", ".tsx"]);
let count = 0;
const perFile = [];
(function walk(dir) {
  for (const ent of fs.readdirSync(dir, { withFileTypes: true })) {
    const p = path.join(dir, ent.name);
    if (ent.isDirectory()) {
      if (ent.name === "node_modules" || ent.name.startsWith(".")) continue;
      walk(p);
    } else if (exts.has(path.extname(ent.name))) {
      const n = (fs.readFileSync(p, "utf8").match(/!important/g) || []).length;
      if (n) {
        count += n;
        perFile.push([n, path.relative(srcRoot, p)]);
      }
    }
  }
})(srcRoot);

perFile.sort((a, b) => b[0] - a[0]);
console.log(`!important occurrences in apps/web/src: ${count} (budget ${MAX})`);
for (const [n, f] of perFile.slice(0, 10)) console.log(`  ${n}\t${f}`);
if (count > MAX) {
  console.error(
    `FAIL: ${count} > ${MAX}. The !important ratchet is one-way — ` +
      `remove declarations instead of adding them.`,
  );
  process.exit(1);
}
console.log("OK: within budget");
