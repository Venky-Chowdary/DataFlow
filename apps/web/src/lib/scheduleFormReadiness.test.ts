/**
 * QA D344: the schedule Save button was disabled with no reason, and the
 * primary-key field that gated it (beside the cursor column) looked optional.
 */
import assert from "node:assert/strict";
import { describe, it } from "node:test";
import {
  schedulePrimaryKeyRequired,
  scheduleSaveBlockedReason,
  scheduleSaveBlockers,
  type ScheduleFormState,
} from "./scheduleFormReadiness";

const ready: ScheduleFormState = {
  name: "Nightly", sourceId: "s", destId: "d", sourceTable: "orders", destTable: "orders_d",
  callable: false, procedureText: "", cadenceMode: "preset", cron: "",
  syncMode: "incremental_append", duplicatePolicy: "upsert", primaryKey: "id", contractBlock: "",
};

describe("schedule form readiness", () => {
  it("a complete form has no blocker and no reason", () => {
    assert.deepEqual(scheduleSaveBlockers(ready), []);
    assert.equal(scheduleSaveBlockedReason([]), "");
  });

  it("names the missing primary key and the way out on an incremental schedule", () => {
    const blockers = scheduleSaveBlockers({ ...ready, primaryKey: " " });
    assert.deepEqual(blockers.map((b) => b.field), ["sched-pk"]);
    assert.equal(
      scheduleSaveBlockedReason(blockers),
      "To save: enter the primary key, or set Duplicate policy to Append.",
    );
  });

  it("the cursor column never blocks Save (planning binds or asks for it)", () => {
    assert.deepEqual(scheduleSaveBlockers({ ...ready, syncMode: "cdc" }), []);
  });

  it("append needs no key, SCD2 / mirror always do", () => {
    assert.equal(schedulePrimaryKeyRequired("incremental_append", "append"), false);
    assert.equal(schedulePrimaryKeyRequired("scd2", "append"), true);
    assert.equal(schedulePrimaryKeyRequired("mirror", "append"), true);
    const scd2 = scheduleSaveBlockers({ ...ready, syncMode: "scd2", duplicatePolicy: "append", primaryKey: "" });
    assert.match(scheduleSaveBlockedReason(scd2), /scd2 needs it/);
  });

  it("lists every missing requirement once, in form order", () => {
    const blockers = scheduleSaveBlockers({
      ...ready, name: "", destTable: "", cadenceMode: "cron", cron: "", contractBlock: "Contract is not signed",
    });
    assert.deepEqual(blockers.map((b) => b.field), ["sched-name", "sched-dst-table", "sched-cron", "sched-contract"]);
    assert.match(scheduleSaveBlockedReason(blockers), /^To save: name the schedule; name the destination table; enter the cron expression; Contract is not signed\.$/);
  });
});
