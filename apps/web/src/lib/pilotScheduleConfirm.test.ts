/**
 * Run: npx --yes tsx --test apps/web/src/lib/pilotScheduleConfirm.test.ts
 */
import assert from "node:assert/strict";
import { describe, it } from "node:test";
import {
  isDestructiveSchedulePreview,
  scheduleConfirmBind,
  scheduleConfirmBlocksRun,
  schedulePreviewFromPayload,
} from "./pilotScheduleConfirm.ts";

describe("pilotScheduleConfirm", () => {
  it("reads route + bind from the redacted preview", () => {
    const preview = schedulePreviewFromPayload({
      ack_id: "ack_1",
      name: "Nightly",
      preview: {
        schedule_id: "s1",
        name: "Nightly",
        source_table: "orders",
        dest_table: "orders_wh",
        sync_mode: "incremental",
        contract_id: "dfc-1",
        require_signed_contract: true,
        enforce_contract: true,
        breaker_state: "closed",
      },
    });
    assert.equal(preview.source_table, "orders");
    assert.equal(preview.dest_table, "orders_wh");
    assert.deepEqual(scheduleConfirmBind(preview), {
      contractId: "dfc-1",
      requireSigned: true,
      breakerState: "closed",
    });
    assert.equal(scheduleConfirmBlocksRun(preview), "");
    assert.equal(isDestructiveSchedulePreview(preview), false);
  });

  it("blocks Confirm when the preview breaker is OPEN", () => {
    const preview = schedulePreviewFromPayload({
      preview: {
        contract_id: "dfc-1",
        require_signed_contract: true,
        breaker_state: "open",
        sync_mode: "full_refresh_overwrite",
      },
    });
    assert.match(scheduleConfirmBlocksRun(preview), /OPEN/i);
    assert.equal(isDestructiveSchedulePreview(preview), true);
  });

  it("does not invent a bind when the schedule is unbound", () => {
    const preview = schedulePreviewFromPayload({
      preview: { name: "Adhoc", source_table: "t", dest_table: "t" },
    });
    assert.deepEqual(scheduleConfirmBind(preview), {
      contractId: "",
      requireSigned: false,
      breakerState: "",
    });
    assert.equal(scheduleConfirmBlocksRun(preview), "");
  });
});
