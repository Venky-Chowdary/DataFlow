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
      reconciliation: {
        passed: true,
        assurance_level: "full_checksum",
        source_checksum: "aaa",
        target_checksum: "aaa",
        phase: "post_write_verified",
      },
    });
    assert.ok(t.score >= 90);
    assert.equal(t.grade, "A");
    assert.equal(t.next_action.code, "ok");
  });

  it("does not grade-A on passed without full_checksum assurance", () => {
    const t = computeJobTrustScore({
      status: "completed",
      records_processed: 1000,
      rejected_rows: 0,
      reconciliation: { passed: true },
    });
    assert.ok(t.score <= 89);
    assert.notEqual(t.grade, "A");
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
      reconciliation: {
        passed: true,
        assurance_level: "full_checksum",
        source_checksum: "a",
        target_checksum: "a",
      },
    });
    const without = computeJobTrustScore({
      status: "completed",
      records_processed: 1000,
      rejected_rows: 0,
    });
    assert.ok(without.score < withRecon.score);
    assert.ok(without.score <= 84);
    const completeness = without.factors.find((f) => f.id === "completeness");
    assert.ok(completeness && (completeness.score as number) <= 82);
  });

  it("does not treat writer-ack as full Gate-8 pass", () => {
    const full = computeJobTrustScore({
      status: "completed",
      records_processed: 1000,
      rejected_rows: 0,
      reconciliation: {
        passed: true,
        phase: "post_write_verified",
        assurance_level: "full_checksum",
        source_checksum: "a",
        target_checksum: "a",
      },
    });
    const ack = computeJobTrustScore({
      status: "completed",
      records_processed: 1000,
      rejected_rows: 0,
      reconciliation: {
        passed: true,
        phase: "post_write_writer_ack",
        assurance_level: "writer_ack",
        message: "Transfer verified by writer: 10 rows written (read-back verifier not available)",
        source_checksum: "abc",
      },
    });
    assert.ok(ack.score < full.score);
    assert.notEqual(ack.grade, "A");
    const factor = ack.factors.find((f) => f.id === "reconcile");
    assert.ok(factor?.note.toLowerCase().includes("writer"));
    assert.ok((factor?.score as number) <= 58);
  });

  it("caps sample and file-export unproven below grade A", () => {
    const sample = computeJobTrustScore({
      status: "completed",
      records_processed: 1000,
      rejected_rows: 0,
      reconciliation: {
        passed: true,
        assurance_level: "sample",
        phase: "post_write_sample_verified",
      },
    });
    assert.ok(sample.score <= 89);
    assert.notEqual(sample.grade, "A");
    const exportJob = computeJobTrustScore({
      status: "completed",
      records_processed: 10,
      rejected_rows: 0,
      reconciliation: {
        passed: true,
        unproven: true,
        skipped_readback: true,
        phase: "post_write_skipped",
        assurance_level: "none",
        message: "File/object export wrote successfully — Gate-8 cell fidelity unproven",
      },
    });
    const factor = exportJob.factors.find((f) => f.id === "reconcile");
    assert.ok((factor?.score as number) <= 45);
    assert.ok(factor?.note.toLowerCase().includes("unproven"));
  });

  it("does not treat append dest-before delta as full checksum Verified", () => {
    const append = computeJobTrustScore({
      status: "completed",
      records_processed: 200,
      rejected_rows: 0,
      reconciliation: {
        passed: true,
        phase: "post_write_row_count",
        assurance_level: "row_count",
        coverage: "row_count",
        checksum_scope: "whole_table_not_comparable",
        source_checksum: "aaa",
        target_checksum: "bbb",
        checksum_match: false,
        migration_proven: false,
        message: "Append delta verified (200 row(s) appended: 100 → 300).",
      },
    });
    const factor = append.factors.find((f) => f.id === "reconcile");
    assert.ok(factor?.note.toLowerCase().includes("append delta"));
    assert.notEqual(append.grade, "A");
    assert.ok(append.score <= 89);
    assert.equal(append.next_action.code, "append_delta");
    assert.doesNotMatch(append.next_action.label, /investigate/i);
  });
});
