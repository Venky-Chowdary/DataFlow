/**
 * Which quarantine findings replay can actually rebuild a row from.
 *
 * The panel used to offer Promote / Replay on the open count alone, so findings
 * that carry no column and no stored values produced a button whose every click
 * came back "Could not reconstruct rows from quarantine details" — and the
 * message lived in a toast, so it read as a silent no-op.
 */
import { describe, expect, it } from "vitest";
import { isReplayable } from "./QuarantinePanel";

describe("isReplayable", () => {
  it("accepts a finding that names the bad cell", () => {
    expect(isReplayable({ row: 4, column: "amount", value: "1,234" })).toBe(true);
    expect(isReplayable({ row: 4, target: "AMOUNT", value: "1,234" })).toBe(true);
  });

  it("accepts a finding that carries the whole row", () => {
    expect(isReplayable({ row: 4, values: { id: "7", amount: "1,234" } })).toBe(true);
  });

  it("refuses a finding with no column and no values", () => {
    expect(isReplayable({ row: 4, reason: "Referential integrity: 2 orphan rows" })).toBe(false);
    expect(isReplayable({ column: "   ", values: {} })).toBe(false);
    expect(isReplayable({})).toBe(false);
  });
});
