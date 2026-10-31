/**
 * QA enterprise (557f556c) #4: retention_days was stored but never applied.
 */
import assert from "node:assert/strict";
import { describe, it } from "node:test";
import { purgeConfirmCopy, RETENTION_HINT } from "./jobRetention.js";

describe("job retention copy", () => {
  it("states that nothing runs on a schedule", () => {
    assert.match(RETENTION_HINT, /Nothing is deleted on a schedule/);
    assert.doesNotMatch(RETENTION_HINT, /not wired/);
  });

  it("confirms with the dry-run count", () => {
    const c = purgeConfirmCopy({ would_remove: 2, cutoff: "2026-10-01T00:00:00+00:00", retention_days: 30, confirm_token: "t" });
    assert.equal(c.title, "Delete 2 finished jobs?");
    assert.match(c.message, /before 2026-10-01/);
    assert.equal(purgeConfirmCopy({ would_remove: 0, cutoff: "2026-10-01", retention_days: 30, confirm_token: "t" }).nothing, true);
  });
});
