/**
 * Run: npx --yes tsx --test apps/web/src/lib/helpDocs.test.ts
 */
import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import test from "node:test";

import { HELP_DOC_IDS, getHelpDoc, searchHelpDocs } from "./helpDocs.ts";

const SRC = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");

test("help articles exist for every registered id", () => {
  for (const id of HELP_DOC_IDS) {
    const doc = getHelpDoc(id);
    assert.equal(doc.id, id);
    assert.ok(doc.sections.length > 0, `${id} has no sections`);
  }
});

test("Team roles in Help match the product: Viewer / Editor / Admin", () => {
  const help = readFileSync(path.join(SRC, "lib", "helpDocs.ts"), "utf8");
  const portal = readFileSync(path.join(SRC, "pages", "marketing", "DocsPortal.tsx"), "utf8");
  assert.match(help, /Viewer \/ Editor \/ Admin/);
  assert.match(help, /There is no Owner role/);
  assert.doesNotMatch(help, /Viewer \/ Editor \/ Owner/);
  assert.doesNotMatch(help, /\*\*Owner\*\* — everything Editors/);
  assert.doesNotMatch(portal, /Viewer \/ Editor \/ Owner/);
  assert.match(portal, /Viewer \/ Editor \/ Admin/);
});

test("Help does not sell CDC as platform exactly-once", () => {
  const help = readFileSync(path.join(SRC, "lib", "helpDocs.ts"), "utf8");
  assert.match(help, /at-least-once upsert/);
  assert.doesNotMatch(help, /exactly-once CDC|CDC is exactly-once|exactly once by default/i);
});

test("search finds Team admin role copy", () => {
  const hits = searchHelpDocs("workspace admin");
  assert.ok(hits.some((h) => h.id === "help-installation" || h.id === "help-enterprise"));
});
