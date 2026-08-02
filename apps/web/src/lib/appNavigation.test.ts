/**
 * Run: npx --yes tsx --test apps/web/src/lib/appNavigation.test.ts
 */
import assert from "node:assert/strict";
import { describe, it } from "node:test";
import { focusFromHash, screenFromHash } from "./appNavigation.js";

describe("screenFromHash aliases", () => {
  it("maps Pipelines label URL to schedules screen", () => {
    assert.equal(screenFromHash("#/pipelines"), "schedules");
    assert.equal(screenFromHash("#/pipeline"), "schedules");
    assert.equal(focusFromHash("#/pipelines")?.screen, "schedules");
  });

  it("keeps canonical screen ids", () => {
    assert.equal(screenFromHash("#/schedules"), "schedules");
    assert.equal(screenFromHash("#/jobs"), "jobs");
    assert.equal(screenFromHash("#/transfer"), "transfer");
  });

  it("maps overview alias to dashboard but leaves marketing #/home alone", () => {
    assert.equal(screenFromHash("#/overview"), "dashboard");
    assert.equal(screenFromHash("#/home"), null);
  });
});
