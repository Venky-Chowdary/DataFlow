/**
 * Run: npx --yes tsx --test apps/web/src/lib/pilotConfirm.test.ts
 *
 * Avoid importing from ./api.js — that pulls Vite's import.meta.env and breaks
 * under node:test. The helpers under test only need the pending-action shape.
 */
import assert from "node:assert/strict";
import { describe, it } from "node:test";

import { destructiveActionButtonLabel, isDestructiveLifecycle, isDestructiveTransfer, isLifecycleOp, lifecycleConfirmPrompt, transferOverwriteMessage } from "./pilotConfirm.js";

function transfer(syncMode: string, destructive = false) {
  return {
    id: "t1",
    type: "start_transfer",
    destructive,
    payload: {
      ack_id: "ack_1",
      preview: {
        source: "Local Postgres.orders",
        destination: "Warehouse.orders",
        sync_mode: syncMode,
      },
    },
  } as Parameters<typeof isDestructiveTransfer>[0];
}

describe("pilotConfirm", () => {
  it("treats overwrite sync mode as destructive even without the flag", () => {
    assert.equal(isDestructiveTransfer(transfer("full_refresh_overwrite")), true);
    assert.equal(isDestructiveTransfer(transfer("full_refresh_append")), false);
  });

  it("honours the explicit destructive flag from the backend", () => {
    assert.equal(isDestructiveTransfer(transfer("incremental_upsert", true)), true);
  });

  it("treats a cadence change as a schedule lifecycle confirm", () => {
    assert.equal(isLifecycleOp("update_schedule"), true);
    assert.equal(
      isDestructiveLifecycle({
        id: "s1",
        type: "update_schedule",
        destructive: false,
        payload: { ack_id: "ack_1", preview: { name: "Nightly" } },
      }),
      false,
    );
  });

  it("treats confirmed CDC drift repair as a lifecycle action", () => {
    assert.equal(isLifecycleOp("repair_cdc_drift"), true);
    assert.equal(
      isDestructiveLifecycle({
        id: "d1",
        type: "repair_cdc_drift",
        destructive: true,
        payload: { ack_id: "ack_1", preview: { table: "orders_copy" } },
      }),
      true,
    );
  });

  it("uses a repair-specific confirmation prompt with the agreed semantics", () => {
    const prompt = lifecycleConfirmPrompt({
      id: "d1",
      type: "repair_cdc_drift",
      destructive: true,
      payload: {
        ack_id: "ack_1",
        preview: { table: "orders_copy", key_count: 2, committed_position: "0/100" },
      },
    });

    assert.equal(prompt.confirmLabel, "Re-copy and reconcile");
    assert.ok(prompt.message.includes("orders_copy"));
    assert.ok(prompt.message.includes("0/100"));
    assert.ok(prompt.message.includes("state convergence / reconciliation, not log replay"));
    assert.ok(!prompt.title.toLowerCase().includes("retry"));
  });

  it("labels the destructive CDC drift repair action by its selected rows", () => {
    assert.equal(
      destructiveActionButtonLabel("repair_cdc_drift"),
      "Repair selected rows",
    );
    assert.equal(
      destructiveActionButtonLabel("retry_job"),
      "Overwrite & run",
    );
  });

  it("names the destination in the overwrite warning", () => {
    const message = transferOverwriteMessage(transfer("full_refresh_overwrite"));
    assert.ok(message.includes("Warehouse.orders"));
    assert.ok(message.toLowerCase().includes("replace"));
  });
});
