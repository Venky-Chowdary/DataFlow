/**
 * Gate-8 hold-out accounting + pre-write honesty —
 * run: npx --yes tsx --test apps/web/src/components/transfer/gate8Expected.test.ts
 */
import assert from "node:assert/strict";
import { describe, it } from "node:test";
import { classifyGate8Status, isGate8IdentityUnproven, isGate8PreWriteSimulation, isGate8SampleVerified, isGate8WriterAckOnly } from "./Gate8ProofCard";

/** Mirror of Gate8ProofCard expected-dest math (quarantine hold-out). */
function gate8ExpectedDest(sourceRows: number, rejectedRows: number, coercedNullRows: number) {
  const heldOut = Math.max(rejectedRows - coercedNullRows, 0);
  return {
    heldOut,
    expectedRows: Math.max(sourceRows - heldOut, 0),
    delta: (target: number) => target - Math.max(sourceRows - heldOut, 0),
  };
}

describe("Gate-8 quarantine hold-out delta", () => {
  it("passes when dest == source − held_out", () => {
    const g = gate8ExpectedDest(3, 1, 0);
    assert.equal(g.heldOut, 1);
    assert.equal(g.expectedRows, 2);
    assert.equal(g.delta(2), 0);
  });

  it("does not warn on raw source−dest when quarantine held out", () => {
    const g = gate8ExpectedDest(3, 1, 0);
    // Raw delta source−dest would be −1 and look like a failure; expected delta is 0.
    assert.equal(2 - 3, -1);
    assert.equal(g.delta(2), 0);
  });

  it("coerce_null does not lower expected dest count", () => {
    const g = gate8ExpectedDest(3, 1, 1);
    assert.equal(g.heldOut, 0);
    assert.equal(g.expectedRows, 3);
    assert.equal(g.delta(3), 0);
  });
});

describe("Gate-8 pre-write simulation honesty", () => {
  it("treats preview reconciliation as pre-write, not verified", () => {
    assert.equal(
      isGate8PreWriteSimulation({
        passed: true,
        preview: true,
        phase: "pre_write_simulation",
        source_rows: 25,
        target_rows: 0,
        message: "Pre-write simulation only",
      }),
      true,
    );
  });

  it("does not treat post-write proof with checksums as pre-write", () => {
    assert.equal(
      isGate8PreWriteSimulation({
        passed: true,
        source_rows: 100,
        target_rows: 100,
        source_checksum: "abc123",
        target_checksum: "abc123",
      }),
      false,
    );
  });

  it("treats writer-checksum-only fallback as pending, not verified", () => {
    assert.equal(
      isGate8PreWriteSimulation({
        passed: false,
        preview: true,
        phase: "post_write_pending",
        post_write_pending: true,
        target_checksum: "writer-digest",
        message: "Writer checksum captured — independent Gate-8 source/destination compare still pending",
      }),
      true,
    );
    // Defense: duplicated writer digest must not look verified.
    assert.equal(
      isGate8PreWriteSimulation({
        passed: true,
        source_checksum: "same",
        target_checksum: "same",
        message: "Writer checksum captured — full Gate-8 sample compare may still be loading",
      }),
      true,
    );
  });

  it("treats writer-ack phase as not Verified", () => {
    assert.equal(
      isGate8WriterAckOnly({
        passed: true,
        phase: "post_write_writer_ack",
        source_checksum: "abc",
        target_checksum: "",
        message: "Transfer verified by writer: 10 rows written (read-back verifier not available)",
      }),
      true,
    );
  });

  it("classifyGate8Status never labels writer-ack as Passed", () => {
    const view = classifyGate8Status({
      passed: true,
      phase: "post_write_writer_ack",
      message: "Transfer verified by writer: 10 rows written (read-back verifier not available)",
    });
    assert.equal(view.label, "Writer ack");
    assert.equal(view.fullPass, false);
    assert.equal(view.tone, "warn");
  });

  it("classifyGate8Status never labels file-export unproven as Passed", () => {
    const view = classifyGate8Status({
      passed: true,
      unproven: true,
      skipped_readback: true,
      migration_proven: false,
      coverage: "none",
      message: "File export checksum recorded (no destination read-back)",
    });
    assert.equal(view.label, "Unproven (no read-back)");
    assert.equal(view.fullPass, false);
    assert.equal(view.tone, "warn");
  });
});

describe("Gate-8 identity-proof honesty", () => {
  it("flags a no-primary-key reconcile as unproven identity", () => {
    assert.equal(
      isGate8IdentityUnproven({
        passed: false,
        verification_mode: "unproven_identity",
        identity: { column: null, proven: false, reason: "primary_key required" },
        sample_compare: { passed: false, compared: 0, mismatches: [], alignment: "unproven_identity" },
      }),
      true,
    );
  });

  it("flags duplicate/null keys as positional-only, never a clean pass", () => {
    const report = {
      passed: true,
      verification_mode: "positional_only",
      identity: { column: "id", proven: false, reason: "null or duplicate identity values" },
      sample_compare: {
        passed: true,
        compared: 10,
        mismatches: [],
        alignment: "positional_only",
        identity_warning: "weak or non-unique primary key — fidelity is sample/positional only",
      },
    };
    assert.equal(isGate8IdentityUnproven(report), true);
    const view = classifyGate8Status(report);
    assert.equal(view.label, "Unproven identity");
    assert.equal(view.fullPass, false);
    assert.equal(view.tone, "warn");
  });

  it("does not flag a genuinely key-aligned reconcile", () => {
    const report = {
      passed: true,
      verification_mode: "key_aligned",
      identity: { column: "id", proven: true, provenance: "explicit" },
      source_checksum: "abc",
      target_checksum: "abc",
      sample_compare: { passed: true, compared: 10, mismatches: [], alignment: "key_aligned" },
    };
    assert.equal(isGate8IdentityUnproven(report), false);
    assert.equal(classifyGate8Status(report).fullPass, true);
  });
});

describe("Gate-8 skipped-row accounting", () => {
  /** Mirror of the card's expected-dest math including LSN-guard skips. */
  function expectedDest(source: number, rejected: number, coerced: number, skipped: number) {
    const heldOut = Math.max(rejected - coerced, 0);
    return Math.max(source - heldOut - skipped, 0);
  }

  it("LSN-guard skips do not read as a shortfall", () => {
    // 100 source, 10 intentionally skipped on CDC redelivery, 90 written.
    assert.equal(expectedDest(100, 0, 0, 10), 90);
    assert.equal(90 - expectedDest(100, 0, 0, 10), 0);
    // Without rows_skipped the card showed a −10 warn delta on a clean pass.
    assert.equal(90 - expectedDest(100, 0, 0, 0), -10);
  });

  it("combines quarantine hold-outs and skips", () => {
    assert.equal(expectedDest(100, 5, 0, 10), 85);
  });
});

describe("Gate-8 sample-verified reverse-ETL honesty", () => {
  it("upgrades keyed sample proof above writer-ack", () => {
    const report = {
      passed: true,
      phase: "post_write_sample_verified",
      source_checksum: "abc",
      target_checksum: "",
      message: "Gate-8 sample-verified 4 key-aligned field(s) for 'hubspot'",
      sample_compare: { passed: true, compared: 4, mismatches: [] },
    };
    assert.equal(isGate8SampleVerified(report), true);
    assert.equal(isGate8WriterAckOnly(report), false);
    const view = classifyGate8Status(report);
    assert.equal(view.label, "Sample verified");
    assert.equal(view.tone, "warn");
    // Sample is not population / full-checksum proof.
    assert.equal(view.fullPass, false);
  });

  it("checksum mismatch with sample authority is sample, not fullPass", () => {
    const report = {
      passed: true,
      phase: "post_write_sample_verified",
      source_checksum: "aaa",
      target_checksum: "bbb",
      message:
        "Sample-only assurance: 5 key-aligned row(s) compared (10 rows; whole-table checksums differed). Population / full-checksum fidelity NOT proven — sample coverage only.",
      sample_compare: { passed: true, compared: 5, mismatches: [] },
      coverage: "sample",
      checksum_match: false,
      population_proof: false,
      assurance_level: "sample",
    };
    assert.equal(isGate8SampleVerified(report), true);
    const view = classifyGate8Status(report);
    assert.equal(view.label, "Sample verified");
    assert.equal(view.tone, "warn");
    assert.equal(view.fullPass, false);
  });
});
