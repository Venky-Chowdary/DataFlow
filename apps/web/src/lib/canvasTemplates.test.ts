import assert from "node:assert/strict";
import { describe, it } from "node:test";
import {
  divertMatchingRows,
  dropEmptyColumns,
  hashIdentityKey,
  parseDateColumns,
  templateUsesHashPii,
  trimTextColumns,
} from "./canvasTemplates.js";
import type { ShapeSuggestion } from "./shape.js";

describe("canvas templates", () => {
  it("trims declared text columns and refuses to guess an untyped column", () => {
    const result = trimTextColumns([
      { name: "email", type: "varchar" },
      { name: "id", type: "integer" },
      { name: "mystery", type: "" },
    ], "customers");
    assert.equal(result.error, "");
    assert.deepEqual(result.steps, [{ op: "trim", column: "email", source_table: "customers" }]);
    assert.equal(templateUsesHashPii(result), false);
  });

  it("drops only columns the profile already suggested", () => {
    const suggestion: ShapeSuggestion = {
      id: "drop-notes",
      title: "Drop notes",
      reason: "Empty in the sample",
      rows_affected: 12,
      severity: "hygiene",
      step: { op: "drop_column", column: "notes" },
    };
    const result = dropEmptyColumns([suggestion]);
    assert.equal(result.steps[0].op, "drop_column");
    assert.equal(dropEmptyColumns([]).error.length > 0, true);
  });

  it("requires an explicit date format and a divert condition", () => {
    assert.match(parseDateColumns([{ name: "created", type: "date" }], "").error, /explicit format/);
    const parsed = parseDateColumns([{ name: "created", type: "timestamp" }], "%Y-%m-%d");
    assert.equal(parsed.steps[0].options?.format, "%Y-%m-%d");
    assert.match(divertMatchingRows("  ").error, /condition/);
    assert.equal(divertMatchingRows("[status] = 'void'").steps[0].op, "divert_rows");
  });

  it("builds a hash-identity key and never a browser hash_pii", () => {
    const result = hashIdentityKey(["id", "email"]);
    assert.equal(result.steps[0].op, "hash_identity");
    assert.deepEqual(result.steps[0].options?.columns, ["id", "email"]);
    assert.equal(templateUsesHashPii(result), false);
    assert.match(hashIdentityKey([]).error, /at least one/);
  });
});
