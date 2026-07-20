/**
 * Run: npx --yes tsx --test apps/web/src/lib/jobTrustScore.test.ts
 */
import assert from "node:assert/strict";
import { describe, it } from "node:test";
import { computeJobTrustScore } from "./jobTrustScore.js";

describe("computeJobTrustScore", () => {
  it("scores a clean completed job highly", () => {
    const t = computeJobTrustScore({
      status: "completed",
      records_processed: 1000,
      rejected_rows: 0,
      coerced_null_rows: 0,
      reconciliation: { passed: true },
    });
    assert.ok(t.score >= 90);
    assert.equal(t.grade, "A");
    assert.equal(t.next_action.code, "ok");
  });

  it("drops score on quarantine and points next action", () => {
    const t = computeJobTrustScore({
      status: "completed_with_quarantine",
      records_processed: 100,
      rejected_rows: 40,
      reconciliation: { passed: true },
    });
    assert.ok(t.score < 85);
    assert.equal(t.next_action.code, "quarantine");
  });

  it("caps score on lease conflict", () => {
    const t = computeJobTrustScore({
      status: "failed",
      records_processed: 0,
      cdc_lease_conflict: true,
      reconciliation: { passed: false },
    });
    assert.ok(t.score <= 35);
    assert.equal(t.next_action.code, "lease");
  });

  it("caps score on CDC cursor gap and prefers reset watermark", () => {
    const t = computeJobTrustScore({
      status: "failed",
      records_processed: 10,
      cdc_cursor_gap: true,
      reconciliation: { passed: false },
    });
    assert.ok(t.score <= 28);
    assert.equal(t.cursor_gap, true);
    assert.equal(t.next_action.code, "cursor_gap");
  });
});
