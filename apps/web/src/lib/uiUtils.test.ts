/**
 * Run: npx --yes tsx --test apps/web/src/lib/uiUtils.test.ts
 */
import assert from "node:assert/strict";
import { describe, it } from "node:test";
import { uniqueListKey } from "./uiUtils.js";

describe("uniqueListKey", () => {
  it("keeps colliding identities distinct by index", () => {
    assert.equal(uniqueListKey("id", 0), "row:id:0");
    assert.equal(uniqueListKey("id", 1), "row:id:1");
    assert.equal(uniqueListKey("", 3, "warn"), "warn:3");
    assert.equal(uniqueListKey(undefined, 2, "job"), "job:2");
  });
});
