import assert from "node:assert/strict";
import { readdirSync, readFileSync } from "node:fs";
import path from "node:path";
import { test } from "node:test";
import { fileURLToPath } from "node:url";
import { createElement } from "react";
import { renderToStaticMarkup } from "react-dom/server";
import { JSDOM } from "jsdom";
import { DtIcon } from "./DtIcon";
import { ICONS, ICON_NAMES, ICON_STROKE, isIconName, snapIconSize } from "./icons/iconMap";
import { StructurePreview } from "./ui/StructurePreview";

const SRC = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");

function svgOf(markup: string) {
  return new JSDOM(markup).window.document.querySelector("svg");
}

test("entity icons use one library with stable decorative sizing", () => {
  for (const name of ["folder", "folder-open", "table", "file", "database", "cloud"]) {
    for (const chrome of ["nav", "product"] as const) {
      for (const size of [16, 20, 24]) {
        const markup = renderToStaticMarkup(createElement(DtIcon, { name, chrome, size }));
        const document = new JSDOM(markup).window.document;
        const svg = document.querySelector("svg");
        assert.ok(svg);
        assert.equal(document.querySelectorAll("svg").length, 1);
        assert.equal(svg.getAttribute("width"), String(size));
        assert.equal(svg.getAttribute("height"), String(size));
        assert.equal(svg.getAttribute("aria-hidden"), "true");
        assert.ok(svg.classList.contains("lucide"));
        assert.ok(svg.childElementCount > 0);
        assert.equal(Boolean(document.querySelector(".dt-nav-icon")), chrome === "nav");
      }
    }
  }
});

test("table preview uses the table symbol rather than database or layers", () => {
  const markup = renderToStaticMarkup(createElement(StructurePreview, {
    columns: ["id"], rows: [{ id: 1 }], allowJson: true,
  }));
  const document = new JSDOM(markup).window.document;
  const tab = document.querySelector('[role="tab"]');
  assert.ok(tab);
  assert.match(tab.textContent || "", /Table/);
  const table = svgOf(renderToStaticMarkup(createElement(DtIcon, { name: "table", size: 13 })));
  const database = svgOf(renderToStaticMarkup(createElement(DtIcon, { name: "database", size: 13 })));
  assert.equal(tab.querySelector("svg")?.innerHTML, table?.innerHTML);
  assert.notEqual(table?.innerHTML, database?.innerHTML);
});

test("an unknown name renders nothing — never a substitute glyph (P1-1)", () => {
  for (const name of ["unknown-entity", "constructor", "toString", "warn", ""]) {
    for (const chrome of ["nav", "product"] as const) {
      assert.equal(renderToStaticMarkup(createElement(DtIcon, { name, chrome })), "", `${name} (${chrome})`);
    }
  }
  assert.equal(isIconName("info"), true);
  assert.equal(isIconName("hasOwnProperty"), false);
});

test("every icon is lucide at one stroke and a size on the scale", () => {
  for (const name of ICON_NAMES) {
    for (const asked of [11, 13, 14, 15, 16, 18, 20, 22, 28]) {
      const svg = svgOf(renderToStaticMarkup(createElement(DtIcon, { name, size: asked, chrome: "product" })));
      assert.ok(svg, name);
      assert.ok(svg.classList.contains("lucide"), `${name} is drawn by lucide`);
      assert.equal(svg.getAttribute("stroke-width"), String(ICON_STROKE), `${name} stroke`);
      assert.equal(svg.getAttribute("width"), String(snapIconSize(asked)), `${name} at ${asked}`);
      assert.ok([14, 16, 20, 24, 32].includes(Number(svg.getAttribute("width"))));
    }
  }
});

test("one glyph per meaning: only documented aliases share a drawing (P1-2)", () => {
  const aliases: Record<string, string> = {
    gate: "verify", shield: "security", sparkle: "ai", activity: "live", alert: "warning", book: "docs", scan: "inspect",
  };
  const byIcon = new Map<unknown, string[]>();
  for (const name of ICON_NAMES) byIcon.set(ICONS[name], [...(byIcon.get(ICONS[name]) ?? []), name]);
  for (const names of byIcon.values()) {
    if (names.length < 2) continue;
    const canonical = names.filter((n) => !(n in aliases));
    assert.equal(canonical.length, 1, `${names.join(", ")} share one glyph`);
    for (const alias of names.filter((n) => n in aliases)) assert.equal(aliases[alias], canonical[0]);
  }
  // The meanings the audit found confused each get their own glyph.
  const distinct = ["copy", "map", "transform", "layers", "settings", "sun", "ai", "verify", "security", "contract",
    "sign-out", "live", "refresh", "test-connection", "schedule", "info", "warning", "error", "success", "help",
    "library", "docs", "timer", "inspect"] as const;
  assert.equal(new Set(distinct.map((n) => ICONS[n])).size, distinct.length);
});

function sourceFiles(dir: string, out: string[] = []): string[] {
  for (const entry of readdirSync(dir, { withFileTypes: true })) {
    const full = path.join(dir, entry.name);
    if (entry.isDirectory()) sourceFiles(full, out);
    else if (/\.tsx?$/.test(entry.name) && !/\.test\.tsx?$/.test(entry.name)) out.push(full);
  }
  return out;
}

/** Names a DtIcon can receive from a literal: `<DtIcon name="x">`, the result
 *  branches of `name={c ? "x" : "y"}`, `icon="x"` props and `icon: "x"` data. */
function literalIconNames(text: string): string[] {
  const found: string[] = [];
  for (const m of text.matchAll(/<DtIcon\b[^>]*?\bname=("[^"]*"|\{[^}]*\})/g)) {
    const expr = m[1];
    if (expr.startsWith('"')) found.push(expr.slice(1, -1));
    else for (const q of expr.matchAll(/(?:^\{|\?\?|[?:])\s*"([a-z0-9-]*)"/g)) found.push(q[1]);
  }
  for (const m of text.matchAll(/\bicon="([a-z0-9-]+)"/g)) found.push(m[1]);
  for (const m of text.matchAll(/\bicon:\s*"([a-z0-9-]+)"/g)) found.push(m[1]);
  return found;
}

test("every literal icon name in the app is in the map (P1-1)", () => {
  // Its `icon` values name industry pictograms, not DtIcon glyphs.
  const exempt = new Set([path.join(SRC, "lib", "domainIntelligence.ts")]);
  const unknown: string[] = [];
  for (const file of sourceFiles(SRC)) {
    if (exempt.has(file)) continue;
    for (const name of literalIconNames(readFileSync(file, "utf8"))) {
      if (!isIconName(name)) unknown.push(`${path.relative(SRC, file)}: "${name}"`);
    }
  }
  assert.deepEqual(unknown, []);
});
