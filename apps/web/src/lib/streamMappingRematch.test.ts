/**
 * Run: npx --yes tsx --test apps/web/src/lib/streamMappingRematch.test.ts
 */
import assert from "node:assert/strict";
import { describe, it } from "node:test";
import {
  missingStreamMappings,
  streamColumnSignature,
  streamsNeedPerStreamRematch,
} from "./streamMappingRematch.js";

describe("streamColumnSignature", () => {
  it("is order- and case-insensitive", () => {
    assert.equal(streamColumnSignature(["B", "a"]), streamColumnSignature(["a", "b"]));
  });
});

describe("streamsNeedPerStreamRematch", () => {
  it("is false when schemas match", () => {
    assert.equal(
      streamsNeedPerStreamRematch([
        { name: "a", columns: ["id", "name"] },
        { name: "b", columns: ["ID", "Name"] },
      ]),
      false,
    );
  });

  it("is true when schemas diverge", () => {
    assert.equal(
      streamsNeedPerStreamRematch([
        { name: "a", columns: ["id", "amount"] },
        { name: "b", columns: ["id", "email"] },
      ]),
      true,
    );
  });
});

describe("missingStreamMappings", () => {
  it("reports streams without mappings", () => {
    const missing = missingStreamMappings(
      ["orders", "users"],
      { orders: [{ source: "id" }] },
      "orders",
      [{ source: "id" }],
    );
    assert.deepEqual(missing, ["users"]);
  });
});
