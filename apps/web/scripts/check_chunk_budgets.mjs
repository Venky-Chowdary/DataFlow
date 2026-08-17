#!/usr/bin/env node
/** Phase F9 — enforce Vite chunk size budgets after build. */
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const webRoot = path.resolve(__dirname, "..");
const distAssets = path.join(webRoot, "dist", "assets");
const budgets = JSON.parse(
  fs.readFileSync(path.join(webRoot, "chunk_budgets.json"), "utf8"),
);

if (!fs.existsSync(distAssets)) {
  console.error("dist/assets missing — run npm run build first");
  process.exit(1);
}

const files = fs.readdirSync(distAssets);
const js = files.filter((f) => f.endsWith(".js"));
const css = files.filter((f) => f.endsWith(".css"));
const rows = [];
const violations = [];

for (const f of js) {
  const bytes = fs.statSync(path.join(distAssets, f)).size;
  rows.push({ file: f, bytes, kind: "js" });
  if (bytes > budgets.max_any_js_chunk_bytes) {
    violations.push(`${f}: ${bytes} > max_any_js_chunk_bytes ${budgets.max_any_js_chunk_bytes}`);
  }
}
for (const f of css) {
  const bytes = fs.statSync(path.join(distAssets, f)).size;
  rows.push({ file: f, bytes, kind: "css" });
  if (bytes > budgets.max_css_bytes) {
    violations.push(`${f}: ${bytes} > max_css_bytes ${budgets.max_css_bytes}`);
  }
}

const names = js.join(" ");
for (const need of budgets.require_named_chunks || []) {
  if (!names.includes(need)) {
    violations.push(`missing required chunk name substring: ${need}`);
  }
}

const entry = rows
  .filter((r) => r.kind === "js" && /index-|main-|DataTransferApp|entry/i.test(r.file))
  .sort((a, b) => a.bytes - b.bytes)[0];
if (entry && entry.bytes > budgets.max_entry_js_bytes) {
  violations.push(
    `entry-like ${entry.file}: ${entry.bytes} > max_entry_js_bytes ${budgets.max_entry_js_bytes}`,
  );
}

const out = {
  schema_version: 1,
  ok: violations.length === 0,
  violations,
  chunks: rows.sort((a, b) => b.bytes - a.bytes),
};
const dest = path.join(webRoot, ".ui-audit", "chunk_budgets_report.json");
fs.mkdirSync(path.dirname(dest), { recursive: true });
fs.writeFileSync(dest, JSON.stringify(out, null, 2) + "\n");
console.log(JSON.stringify({ ok: out.ok, violations, top: out.chunks.slice(0, 8) }, null, 2));
process.exit(violations.length ? 1 : 0);
