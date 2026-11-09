/**
 * Run: npx --yes tsx --test apps/web/src/lib/jobSelection.test.ts
 */
import assert from "node:assert/strict";
import { describe, it } from "node:test";
import { absentEvidenceLabel, deepLinkPending, latchDeepLink, nextListSelection, shouldApplyInitialJobFocus } from "./jobSelection.js";

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

describe("deep link survives the shell clearing its transient focus", () => {
  it("latches the requested job until a new one arrives", () => {
    const first = latchDeepLink({}, "older", "quarantine");
    assert.deepEqual(first, { jobId: "older", panel: "quarantine" });
    const cleared = latchDeepLink(first, undefined, undefined);
    assert.equal(cleared.jobId, "older");
    assert.equal(deepLinkPending(cleared.jobId, null), true);
    assert.deepEqual(latchDeepLink(cleared, "other", undefined), { jobId: "other", panel: undefined });
  });
});

describe("absentEvidenceLabel", () => {
  it("claims Not captured only after the job record was read", () => {
    assert.equal(absentEvidenceLabel("read"), "Not captured");
    assert.equal(absentEvidenceLabel("pending"), "Loading…");
    assert.equal(absentEvidenceLabel("failed"), "Detail unavailable — Refresh");
  });
});
