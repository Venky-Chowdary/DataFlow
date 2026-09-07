/**
 * Which quarantine findings replay can actually rebuild a row from.
 *
 * The panel used to offer Promote / Replay on the open count alone, so findings
 * that carry no column and no stored values produced a button whose every click
 * came back "Could not reconstruct rows from quarantine details" — and the
 * message lived in a toast, so it read as a silent no-op.
 */
import assert from "node:assert/strict";
import { describe, it } from "node:test";
import { isReplayable } from "./quarantineReplay";

describe("isReplayable", () => {
  it("accepts a finding that names the bad cell", () => {
    assert.equal(isReplayable({ row: 4, column: "amount", value: "1,234" }), true);
    assert.equal(isReplayable({ row: 4, target: "AMOUNT", value: "1,234" }), true);
  });

  it("accepts a finding that carries the whole row", () => {
    assert.equal(isReplayable({ row: 4, values: { id: "7", amount: "1,234" } }), true);
  });

  it("refuses a finding with no column and no values", () => {
    assert.equal(
      isReplayable({ row: 4, reason: "Referential integrity: 2 orphan rows" }),
      false,
    );
    assert.equal(isReplayable({ column: "   ", values: {} }), false);
    assert.equal(isReplayable({}), false);
  });
});
