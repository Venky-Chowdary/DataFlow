/**
 * Run: npx --yes tsx --test apps/web/src/lib/jobSelection.test.ts
 */
import assert from "node:assert/strict";
import { describe, it } from "node:test";
import { deepLinkPending, nextListSelection, shouldApplyInitialJobFocus } from "./jobSelection.js";

describe("jobSelection", () => {
  it("applies a deep-link once, not again on list poll", () => {
    const ids = ["a", "b", "c"];
    assert.equal(shouldApplyInitialJobFocus("b", null, ids), true);
    assert.equal(shouldApplyInitialJobFocus("b", "b", ids), false);
    assert.equal(shouldApplyInitialJobFocus("c", "b", ids), true);
    assert.equal(shouldApplyInitialJobFocus(undefined, null, ids), false);
    assert.equal(shouldApplyInitialJobFocus("z", null, ids), false);
  });

  it("holds a deep link until the requested job is applied (fresh-load race)", () => {
    // First poll returns only the newest page: the requested job is absent.
    assert.equal(deepLinkPending("old", null), true);
    assert.equal(shouldApplyInitialJobFocus("old", null, ["newest", "b"]), false);
    // Once applied, the list may resume normal selection.
    assert.equal(deepLinkPending("old", "old"), false);
    assert.equal(deepLinkPending(undefined, null), false);
  });

  it("keeps the operator pick when it is still in the filtered list", () => {
    assert.equal(nextListSelection("b", ["a", "b", "c"]), "b");
    assert.equal(nextListSelection("b", ["a", "c"]), "a");
    assert.equal(nextListSelection(null, ["a", "c"]), "a");
    assert.equal(nextListSelection("b", []), "b");
  });
});
