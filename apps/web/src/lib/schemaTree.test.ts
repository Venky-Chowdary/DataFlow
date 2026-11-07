import assert from "node:assert/strict";
import { describe, it } from "node:test";
import { nextTreeIndex } from "./schemaTree.js";

describe("schema tree keyboard", () => {
  it("moves within the list and ignores other keys", () => {
    assert.equal(nextTreeIndex(0, "ArrowDown", 4), 1);
    assert.equal(nextTreeIndex(3, "ArrowDown", 4), 3);
    assert.equal(nextTreeIndex(2, "ArrowUp", 4), 1);
    assert.equal(nextTreeIndex(0, "Home", 4), 0);
    assert.equal(nextTreeIndex(1, "End", 4), 3);
    assert.equal(nextTreeIndex(1, "Enter", 4), null);
    assert.equal(nextTreeIndex(0, "ArrowDown", 0), null);
  });
});
