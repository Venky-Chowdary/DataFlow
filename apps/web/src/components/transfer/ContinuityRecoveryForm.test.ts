import assert from "node:assert/strict";
import { test } from "node:test";
import { CONTINUITY_GATE_ID, stripContinuityRecoveryProse } from "./ContinuityRecoveryForm";

test("G24 id does not collide with the control-totals gate", () => {
  assert.equal(CONTINUITY_GATE_ID, "g24_append_continuity");
});

test("recovery prose naming MCP tools / REST routes is replaced by controls", () => {
  const msg =
    "Append continuity — Verification failed for table 'orders': the table no longer exists. "
    + "Nothing was written. To recover: if the table was removed on purpose, accept the removal "
    + "(MCP tool rebaseline_transfer, or POST /api/v1/transfer/continuity/rebaseline).";
  const out = stripContinuityRecoveryProse(msg);
  assert.match(out, /no longer exists\. Nothing was written\.$/);
  assert.doesNotMatch(out, /rebaseline|POST/);
});

test("messages without recovery prose are unchanged", () => {
  assert.equal(stripContinuityRecoveryProse("Row count drifted."), "Row count drifted.");
});
