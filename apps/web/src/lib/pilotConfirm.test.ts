import { describe, expect, it } from "vitest";

import type { CopilotPendingAction } from "./api";
import { isDestructiveTransfer, transferOverwriteMessage } from "./pilotConfirm";

function transfer(syncMode: string, destructive = false): CopilotPendingAction {
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
  };
}

describe("pilotConfirm", () => {
  it("treats overwrite sync mode as destructive even without the flag", () => {
    expect(isDestructiveTransfer(transfer("full_refresh_overwrite"))).toBe(true);
    expect(isDestructiveTransfer(transfer("full_refresh_append"))).toBe(false);
  });

  it("honours the explicit destructive flag from the backend", () => {
    expect(isDestructiveTransfer(transfer("incremental_upsert", true))).toBe(true);
  });

  it("names the destination in the overwrite warning", () => {
    const message = transferOverwriteMessage(transfer("full_refresh_overwrite"));
    expect(message).toContain("Warehouse.orders");
    expect(message.toLowerCase()).toContain("replace");
  });
});
