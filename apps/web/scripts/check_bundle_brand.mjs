#!/usr/bin/env node
/**
 * QA r12 #4b — the built web app must not carry the old brand or a hosting
 * provider's hostname to clients.
 *
 * QA found `DATAFLOW_API_BASE`, `DATAFLOW_EGRESS_ALLOWLIST`, a
 * `*.up.railway.app` fallback API URL and `dataflow-jsonld` in the production
 * bundle. Everything under dist/ (scripts, styles, index.html, public files)
 * is what a browser downloads, so all of it is scanned. Run after `npm run build`.
 */
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const dist = path.resolve(__dirname, "..", "dist");
const FORBIDDEN = /dataflow|railway/gi;
const TEXT = new Set([".js", ".mjs", ".css", ".html", ".json", ".webmanifest", ".txt", ".csv", ".svg", ".xml", ".map"]);

if (!fs.existsSync(dist)) {
  console.error("dist missing — run npm run build first");
  process.exit(1);
}

function* walk(dir) {
  for (const entry of fs.readdirSync(dir, { withFileTypes: true })) {
    const full = path.join(dir, entry.name);
    if (entry.isDirectory()) yield* walk(full);
    else if (TEXT.has(path.extname(entry.name).toLowerCase())) yield full;
  }
}

const hits = [];
for (const file of walk(dist)) {
  const text = fs.readFileSync(file, "utf8");
  for (const m of text.matchAll(FORBIDDEN)) {
    const at = m.index ?? 0;
    const context = text.slice(Math.max(0, at - 40), at + 40).replace(/\s+/g, " ");
    hits.push(`${path.relative(dist, file)}: …${context}…`);
  }
}

if (hits.length) {
  console.error(`Bundle brand check: ${hits.length} hit(s) for /dataflow|railway/i in dist/`);
  for (const hit of hits.slice(0, 50)) console.error(`  ${hit}`);
  process.exit(1);
}
console.log("Bundle brand check: no 'dataflow' or 'railway' in dist/");
