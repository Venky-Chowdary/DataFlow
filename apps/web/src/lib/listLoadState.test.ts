/**
 * Run: npx tsx --test src/lib/listLoadState.test.ts
 */
import assert from "node:assert/strict";
import { describe, it } from "node:test";
import { countsUnknown, listLoadState } from "./listLoadState.js";

describe("list load state: an error is never an empty state", () => {
  it("a failed first read is an error, not 'nothing here yet'", () => {
    assert.equal(listLoadState({ loading: false, error: "Service unavailable", loaded: false, count: 0 }), "error");
    // Answered once, then the refresh failed with nothing on screen: still an error.
    assert.equal(listLoadState({ loading: false, error: "Service unavailable", loaded: true, count: 0 }), "error");
  });

  it("is empty only when the server answered with zero rows", () => {
    assert.equal(listLoadState({ loading: false, error: "", loaded: true, count: 0 }), "empty");
    assert.equal(listLoadState({ loading: false, error: "", loaded: false, count: 0 }), "loading");
    assert.equal(listLoadState({ loading: true, error: "", loaded: true, count: 0 }), "loading");
  });

  it("rows on screen stay on screen while a refresh loads or fails", () => {
    assert.equal(listLoadState({ loading: true, error: "", loaded: true, count: 3 }), "ready");
    assert.equal(listLoadState({ loading: false, error: "timeout", loaded: true, count: 3 }), "ready");
  });
});

describe("list counts are unknown until the list answers", () => {
  it("treats loading and a failed first read as unknown counts", () => {
    assert.equal(countsUnknown("loading"), true);
    assert.equal(countsUnknown("error"), true);
    assert.equal(countsUnknown("empty"), false);
    assert.equal(countsUnknown("ready"), false);
  });
});
