import assert from "node:assert/strict";
import { test } from "node:test";
import {
  CONTINUITY_FAILURE_REASONS,
  CONTINUITY_GATE_ID,
  stripContinuityRecoveryProse,
} from "./ContinuityRecoveryForm";

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

test("a cursor reset is recovered by controls, not cursor-clear API prose", () => {
  const msg =
    "Destination reset since the last incremental run: the stored watermark '2025-01-01' on "
    + "'updated_at' claims those rows are already at rest, but the destination table is missing "
    + "— resuming would skip the whole history and report success. Clear the cursor (MCP tool "
    + "clear_cursor_resnapshot with cursor_key 'k', or POST /api/v1/ops/cdc-cursors/clear) to re-snapshot.";
  const out = stripContinuityRecoveryProse(msg);
  assert.match(out, /report success\.$/);
  assert.doesNotMatch(out, /clear_cursor|POST/);
  assert.ok(CONTINUITY_FAILURE_REASONS.has("cursor_destination_reset"));
  assert.ok(CONTINUITY_FAILURE_REASONS.has("destination_continuity_mismatch"));
});
