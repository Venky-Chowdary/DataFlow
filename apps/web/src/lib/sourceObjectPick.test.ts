import assert from "node:assert/strict";
import { test } from "node:test";
import { scheduleCreateOpensStudio, sourceTableMissingFromCatalog } from "./sourceObjectPick";

test("a typed name that is not in the catalog is missing", () => {
  assert.equal(sourceTableMissingFromCatalog("sample", ["orders", "public.customers"]), true);
  assert.equal(sourceTableMissingFromCatalog("orders", ["orders", "public.customers"]), false);
  assert.equal(sourceTableMissingFromCatalog("customers", ["public.customers"]), false);
  assert.equal(sourceTableMissingFromCatalog("sample", []), false);
  assert.equal(sourceTableMissingFromCatalog("", ["orders"]), false);
});

test("a new schedule without mappings opens Studio", () => {
  assert.equal(scheduleCreateOpensStudio({ mapping_count: 0 }), true);
  assert.equal(scheduleCreateOpensStudio({ mappings: [] }), true);
  assert.equal(scheduleCreateOpensStudio({ mapping_count: 3 }), false);
});
