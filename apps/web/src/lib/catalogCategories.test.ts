/**
 * UX-29: the catalog reads as product copy, not ids or package names.
 * Run: npx --yes tsx --test apps/web/src/lib/catalogCategories.test.ts
 */
import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import path from "node:path";
import { describe, it } from "node:test";
import { fileURLToPath } from "node:url";
import { catalogCategoryLabel } from "./catalogCategories.js";

const SRC = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const API_DATA = path.resolve(SRC, "..", "..", "api", "data", "connector_catalog.json");

describe("catalog category labels (UX-29)", () => {
  it("names every category the catalog ships", () => {
    const catalog = JSON.parse(readFileSync(API_DATA, "utf8")) as { connectors: { category?: string }[] };
    const ids = new Set(catalog.connectors.map((c) => c.category || "other"));
    for (const id of ids) {
      const label = catalogCategoryLabel(id);
      assert.match(label, /^[A-Z]/, `${id} -> ${label}`);
      assert.ok(!label.includes("_"), `${id} -> ${label}`);
    }
    assert.equal(catalogCategoryLabel("vector"), "Vector Databases");
    assert.equal(catalogCategoryLabel("some_new_kind"), "Some New Kind");
  });

  it("shows no internal ids or package names on catalog surfaces", () => {
    const panel = readFileSync(path.join(SRC, "components", "ConnectorCatalogPanel.tsx"), "utf8");
    assert.ok(!panel.includes("TRANSFER_READY"), "TRANSFER_READY is an internal flag");
    const catalog = readFileSync(API_DATA, "utf8");
    assert.ok(!/openpyxl/i.test(catalog), "a Python package name is not catalog copy");
  });
});
