/**
 * Gate-8 hold-out accounting + pre-write honesty —
 * run: npx --yes tsx --test apps/web/src/components/transfer/gate8Expected.test.ts
 */
import assert from "node:assert/strict";
import { describe, it } from "node:test";
import { classifyGate8Status, isGate8PreWriteSimulation, isGate8WriterAckOnly } from "./Gate8ProofCard";

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
});
