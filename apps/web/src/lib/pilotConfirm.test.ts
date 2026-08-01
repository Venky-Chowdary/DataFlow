/**
 * Run: npx --yes tsx --test apps/web/src/lib/pilotConfirm.test.ts
 *
 * Avoid importing from ./api.js — that pulls Vite's import.meta.env and breaks
 * under node:test. The helpers under test only need the pending-action shape.
 */
import assert from "node:assert/strict";
import { describe, it } from "node:test";

import { isDestructiveTransfer, transferOverwriteMessage } from "./pilotConfirm.js";

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

  it("names the destination in the overwrite warning", () => {
    const message = transferOverwriteMessage(transfer("full_refresh_overwrite"));
    assert.ok(message.includes("Warehouse.orders"));
    assert.ok(message.toLowerCase().includes("replace"));
  });
});
