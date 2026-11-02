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

  it("ranks a surrogate id above a measure that is unique only by chance", () => {
    const rows = [
      { age: 34, id: 1, email: "a@x.io", signup_date: "2024-01-10", status: "active" },
      { age: 28, id: 2, email: "b@x.io", signup_date: "2024-02-14", status: "inactive" },
      { age: 45, id: 3, email: "c@x.io", signup_date: "2024-03-02", status: "active" },
      { age: -5, id: 4, email: "d@x.io", signup_date: "2024-04-07", status: "pending" },
    ];
    const hits = suggestUniqueKeyCandidates(rows, ["age", "email", "id", "signup_date", "status"]);
    assert.equal(hits[0]?.column, "id");
    assert.ok(hits.every((h) => h.column !== "age"), "a measure is never offered as identity");
    assert.equal(hits[1]?.column, "email");
    assert.ok(hits.every((h) => h.column !== "signup_date"), "a date is not offered as identity");
  });

  it("never offers decimals as a key and recognises camelCase / uuid ids", () => {
    const rows = [
      { price: "9.99", customerId: "c1", ref: "0b6f8f3e-1c2d-4e5f-8a9b-0c1d2e3f4a5b" },
      { price: "19.5", customerId: "c2", ref: "1b6f8f3e-1c2d-4e5f-8a9b-0c1d2e3f4a5b" },
    ];
    const hits = suggestUniqueKeyCandidates(rows, ["price", "ref", "customerId"]);
    assert.deepEqual(hits.map((h) => h.column), ["customerId", "ref"]);
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
