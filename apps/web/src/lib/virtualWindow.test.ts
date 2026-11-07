import assert from "node:assert/strict";
import { describe, it } from "node:test";
import { shouldVirtualize, virtualRange } from "./virtualWindow.js";

describe("virtual window", () => {
  it("covers the viewport plus overscan and never reads past the list", () => {
    const range = virtualRange(1000, 360, 200, 36, 2);
    assert.equal(range.start, 8);
    assert.ok(range.end > range.start);
    assert.ok(range.end <= 1000);
    assert.equal(range.offsetTop, range.start * 36);
    assert.equal(range.totalHeight, 36000);
  });

  it("stays empty for a zero-height viewport", () => {
    assert.deepEqual(virtualRange(10, 0, 0, 36), {
      start: 0,
      end: 0,
      offsetTop: 0,
      totalHeight: 360,
    });
  });

  it("virtualizes only long collapsed lists", () => {
    assert.equal(shouldVirtualize(200, false), false);
    assert.equal(shouldVirtualize(201, false), true);
    assert.equal(shouldVirtualize(400, true), false);
  });
});
