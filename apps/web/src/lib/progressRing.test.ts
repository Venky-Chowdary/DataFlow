/**
 * Run: npx --yes tsx --test apps/web/src/lib/progressRing.test.ts
 */
import assert from "node:assert/strict";
import { describe, it } from "node:test";
import { launchStageState, ratioPercent, ringDasharray, stagePercent, validateRingPercent } from "./progressRing.js";

describe("progressRing", () => {
  it("closes the dash at 100% instead of leaving a mid-ring gap", () => {
    assert.equal(ringDasharray(100), "100 0");
    assert.equal(ringDasharray(88), "88 100");
    assert.equal(ringDasharray(0), "0 100");
    assert.equal(ringDasharray(0, { indeterminate: true }), "28 72");
  });

  it("treats skipped gates as N/A so a passed Validate is 100%, not 88%", () => {
    const done = validateRingPercent({
      running: false,
      passed: true,
      decision: "approve",
      passedCount: 8,
      blockedCount: 0,
      readinessScore: 88.9,
    });
    assert.equal(done.indeterminate, false);
    assert.equal(done.pct, 100);

    const blocked = validateRingPercent({
      running: false,
      passed: false,
      decision: "block",
      passedCount: 7,
      blockedCount: 2,
      readinessScore: 70,
    });
    assert.equal(blocked.pct, 78);

    const live = validateRingPercent({
      running: true,
      passedCount: 0,
      blockedCount: 0,
    });
    assert.equal(live.indeterminate, true);
  });

  it("P0-4: 16/22 passed · 5 skipped · 1 review reads 94%, not 100%", () => {
    const ring = validateRingPercent({
      running: false,
      passed: true,
      decision: "approve",
      passedCount: 16,
      blockedCount: 0,
      totalCount: 22,
      skippedCount: 5,
      reviewCount: 1,
    });
    assert.deepEqual(ring, { pct: 94, indeterminate: false });
    // A review decision with nothing left in review fills the ring only because every applicable gate passed.
    assert.equal(validateRingPercent({ running: false, passed: true, decision: "review", passedCount: 17, blockedCount: 0, totalCount: 22, skippedCount: 5 }).pct, 100);
    // The decision alone never fills the ring.
    assert.equal(validateRingPercent({ running: false, passed: true, decision: "review", passedCount: 5, blockedCount: 0, reviewCount: 3 }).pct, 63);
    // Blocked gates count against it, skipped ones do not.
    assert.equal(validateRingPercent({ running: false, passed: false, decision: "block", passedCount: 10, blockedCount: 2, totalCount: 15, skippedCount: 3 }).pct, 83);
  });

  it("never rounds a partial pass up to 100%", () => {
    assert.equal(ratioPercent(199, 200), 99);
    assert.equal(ratioPercent(200, 200), 100);
    assert.equal(ratioPercent(0, 22), 0);
    assert.equal(ratioPercent(3, 0), 0);
    const nearly = validateRingPercent({ running: false, passed: true, decision: "approve", passedCount: 999, blockedCount: 0, totalCount: 1000 });
    assert.equal(nearly.pct, 99);
  });

  it("maps completed stages to percent without invented mid values", () => {
    assert.equal(stagePercent(0, 4), 0);
    assert.equal(stagePercent(1, 4), 25);
    assert.equal(stagePercent(4, 4), 100);
    assert.equal(stagePercent(3, 3), 100);
  });

  it("marks every launch chip done at 100%, not stuck mid-ring", () => {
    assert.equal(launchStageState(25, 0, 4), "done");
    assert.equal(launchStageState(25, 1, 4), "active");
    assert.equal(launchStageState(25, 2, 4), "pending");
    assert.equal(launchStageState(100, 0, 4), "done");
    assert.equal(launchStageState(100, 3, 4), "done");
  });
});
