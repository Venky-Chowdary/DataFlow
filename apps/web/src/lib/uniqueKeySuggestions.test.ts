/**
 * Sample-unique key suggestions — honest preview-only ranking.
 * Run: npx --yes tsx --test apps/web/src/lib/uniqueKeySuggestions.test.ts
 */
import assert from "node:assert/strict";
import { describe, it } from "node:test";
import { suggestUniqueKeyCandidates, suggestCompositeUniqueKeyCandidates } from "./uniqueKeySuggestions.js";

describe("suggestUniqueKeyCandidates", () => {
  it("returns columns unique in the sample and prefers natural keys", () => {
    const rows = [
      { id: "a", fingerprint: "f1", external_id: "e1", name: "x" },
      { id: "a", fingerprint: "f2", external_id: "e2", name: "y" },
      { id: "b", fingerprint: "f3", external_id: "e3", name: "z" },
    ];
    const hits = suggestUniqueKeyCandidates(rows, ["id", "fingerprint", "external_id", "name"]);
    assert.ok(hits.every((h) => h.column !== "id"));
    assert.equal(hits[0]?.column, "fingerprint");
    assert.equal(hits[0]?.uniqueCount, 3);
  });

  it("excludes requested columns", () => {
    const rows = [
      { a: "1", b: "x" },
      { a: "2", b: "y" },
    ];
    const hits = suggestUniqueKeyCandidates(rows, ["a", "b"], { exclude: ["a"] });
    assert.deepEqual(hits.map((h) => h.column), ["b"]);
  });
});

describe("suggestCompositeUniqueKeyCandidates", () => {
  it("finds a unique pair when singles collide", () => {
    const rows = [
      { tenant: "t1", order: "1", note: "a" },
      { tenant: "t1", order: "2", note: "a" },
      { tenant: "t2", order: "1", note: "a" },
    ];
    const hits = suggestCompositeUniqueKeyCandidates(rows, ["tenant", "order", "note"]);
    assert.ok(hits.some((h) => h.columns.join(",") === "tenant,order"));
  });
});
