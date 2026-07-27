/**
 * Gate-8 hold-out accounting — run: npx --yes tsx --test apps/web/src/components/transfer/gate8Expected.test.ts
 */
import assert from "node:assert/strict";
import { describe, it } from "node:test";

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
