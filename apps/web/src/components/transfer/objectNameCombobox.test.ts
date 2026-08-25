import assert from "node:assert/strict";
import { describe, it } from "node:test";
import { filterObjectNames } from "./objectNameFilter.js";

describe("filterObjectNames", () => {
  const tables = ["case_a_dst", "case_a_src", "orders", "public.case_a_dst"];

  it("keeps the exact selected name so the menu does not say no match", () => {
    // Dropping the exact hit left "No tables match 'case_a_dst'" on an existing table.
    assert.deepEqual(filterObjectNames(tables, "case_a_dst"), ["case_a_dst", "public.case_a_dst"]);
    assert.deepEqual(filterObjectNames(tables, "CASE_A_DST"), ["case_a_dst", "public.case_a_dst"]);
    assert.equal(filterObjectNames(tables, "case_a_dst")[0], "case_a_dst");
  });

  it("ranks exact, then prefix, then contains", () => {
    assert.deepEqual(
      filterObjectNames(tables, "case_a"),
      ["case_a_dst", "case_a_src", "public.case_a_dst"],
    );
  });

  it("returns the catalog unfiltered when the box is empty", () => {
    assert.deepEqual(filterObjectNames(tables, "  "), tables);
  });
});
