/**
 * Run: npx --yes tsx --test src/lib/schedulesGitops.test.ts
 */
import assert from "node:assert/strict";
import { describe, it } from "node:test";
import { fleetExportBlockedReason } from "./schedulesGitops.js";

describe("fleetExportBlockedReason", () => {
  it("names the empty fleet instead of inventing an unexpected error", () => {
    const reason = fleetExportBlockedReason(0);
    assert.ok(reason);
    assert.match(reason!, /no scheduled jobs to export/i);
    assert.doesNotMatch(reason!, /unexpected error/i);
  });

  it("allows export when at least one schedule exists", () => {
    assert.equal(fleetExportBlockedReason(1), null);
  });
});
