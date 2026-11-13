import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { describe, it } from "node:test";

/**
 * The Studio half of plan concurrency (PS-1 / PS-18): every mapping save names
 * the revision it was based on, and a refused save or Map shows the reload
 * notice instead of failing silently or running a version the user never saw.
 */
const page = readFileSync(new URL("../pages/TransferPage.tsx", import.meta.url), "utf8");
const notice = readFileSync(new URL("../components/transfer/PlanChangedNotice.tsx", import.meta.url), "utf8");

describe("Transfer Studio plan-conflict wiring", () => {
  it("sends base_version with every mapping save", () => {
    const saves = page.match(/syncTransferPlanMappings\([^)]*\)/g) ?? [];
    assert.ok(saves.length >= 3, `expected the Validate and Execute saves, found ${saves.length}`);
    for (const call of saves) {
      assert.match(call, /baseVersion: planHeadRef\.current/, call);
    }
  });

  it("tracks the plan head from every plan response", () => {
    assert.match(page, /planHeadRef\.current = headVersion\(plan/);
    assert.match(page, /planHeadRef\.current = headVersion\(synced\?\.plan\)/);
    assert.match(page, /planHeadRef\.current = mappedVersion/);
  });

  it("turns a conflict from Map, Validate and Execute into the reload notice", () => {
    const handled = page.match(/isPlanVersionConflict\((e|planErr|transferErr|syncErr)\)/g) ?? [];
    for (const site of ["(e)", "(planErr)", "(transferErr)", "(syncErr)"]) {
      assert.ok(handled.some((h) => h.endsWith(site)), `conflict not handled at ${site}`);
    }
    assert.match(page, /<PlanChangedNotice/);
    assert.match(page, /getTransferPlan\(planId\)/, "Reload refetches the plan");
  });

  it("reload keeps the user's edits and says so", () => {
    assert.match(notice, /PLAN_CHANGED_MESSAGE/);
    assert.match(notice, /still on screen/);
    assert.match(notice, /Reload plan/);
  });
});
