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

  it("caps completeness when Gate-8 reconcile is missing", () => {
    const withRecon = computeJobTrustScore({
      status: "completed",
      records_processed: 1000,
      rejected_rows: 0,
      reconciliation: { passed: true },
    });
    const without = computeJobTrustScore({
      status: "completed",
      records_processed: 1000,
      rejected_rows: 0,
    });
    assert.ok(without.score < withRecon.score);
    const completeness = without.factors.find((f) => f.id === "completeness");
    assert.ok(completeness && (completeness.score as number) <= 82);
  });

  it("does not treat writer-ack as full Gate-8 pass", () => {
    const full = computeJobTrustScore({
      status: "completed",
      records_processed: 1000,
      rejected_rows: 0,
      reconciliation: { passed: true, phase: "post_write", source_checksum: "a", target_checksum: "a" },
    });
    const ack = computeJobTrustScore({
      status: "completed",
      records_processed: 1000,
      rejected_rows: 0,
      reconciliation: {
        passed: true,
        phase: "post_write_writer_ack",
        message: "Transfer verified by writer: 10 rows written (read-back verifier not available)",
        source_checksum: "abc",
      },
    });
    assert.ok(ack.score < full.score);
    const factor = ack.factors.find((f) => f.id === "reconcile");
    assert.ok(factor?.note.toLowerCase().includes("writer"));
    assert.ok((factor?.score as number) <= 58);
  });
});
