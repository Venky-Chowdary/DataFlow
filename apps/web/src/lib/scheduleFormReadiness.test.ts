/**
 * QA D344 / PR 435: the schedule Save button was disabled with no reason.
 * Each rule here is the API's: the store's key rule for duplicate_policy, and
 * Validate's (g9) cursor rule per sync mode, which the server plan satisfies
 * itself on a new schedule.
 */
import assert from "node:assert/strict";
import { describe, it } from "node:test";
import {
  scheduleCursorRequired,
  scheduleModeNeedsCursorColumn,
  schedulePrimaryKeyRequired,
  scheduleSaveBlockedReason,
  scheduleSaveBlockers,
  type ScheduleFormState,
} from "./scheduleFormReadiness";

const ready: ScheduleFormState = {
  name: "Nightly", sourceId: "s", destId: "d", sourceTable: "orders", destTable: "orders_d",
  callable: false, procedureText: "", cadenceMode: "preset", cron: "",
  syncMode: "incremental_append", duplicatePolicy: "upsert", primaryKey: "id", contractBlock: "",
  cursorColumn: "updated_at", sourceType: "postgresql", serverPlans: false,
};

const fields = (s: Partial<ScheduleFormState>) => scheduleSaveBlockers({ ...ready, ...s }).map((b) => b.field);

describe("schedule form readiness", () => {
  it("a complete form has no blocker and no reason", () => {
    assert.deepEqual(scheduleSaveBlockers(ready), []);
    assert.equal(scheduleSaveBlockedReason([]), "");
  });

  it("names the missing primary key and the way out (the store's duplicate_policy rule)", () => {
    const blockers = scheduleSaveBlockers({ ...ready, primaryKey: " " });
    assert.deepEqual(blockers.map((b) => b.field), ["sched-pk"]);
    assert.equal(
      scheduleSaveBlockedReason(blockers),
      "To save: enter the primary key, or set Duplicate policy to Append.",
    );
    assert.equal(schedulePrimaryKeyRequired("upsert"), true);
    assert.equal(schedulePrimaryKeyRequired("skip_duplicates"), true);
    assert.equal(schedulePrimaryKeyRequired("append"), false);
  });

  it("append needs no key in any mode; SCD2 / mirror / CDC keys come from the source catalog at Validate", () => {
    for (const syncMode of ["full_refresh_overwrite", "full_refresh_append", "incremental_append", "incremental_deduped", "cdc", "scd2", "mirror", "upsert"]) {
      assert.deepEqual(fields({ syncMode, duplicatePolicy: "append", primaryKey: "" }), [], syncMode);
      assert.deepEqual(fields({ syncMode, duplicatePolicy: "upsert", primaryKey: "" }), ["sched-pk"], syncMode);
    }
  });

  it("modes whose Validate needs a cursor column", () => {
    assert.equal(scheduleModeNeedsCursorColumn("incremental_append"), true);
    assert.equal(scheduleModeNeedsCursorColumn("incremental_deduped"), true);
    assert.equal(scheduleModeNeedsCursorColumn("incremental"), true);
    // CDC's position is the change log, not a column.
    assert.equal(scheduleModeNeedsCursorColumn("cdc"), false);
    for (const mode of ["full_refresh_overwrite", "full_refresh_append", "upsert", "scd2", "mirror"]) {
      assert.equal(scheduleModeNeedsCursorColumn(mode), false, mode);
    }
    // Kafka advances on the consumer offset; a file re-reads on its key.
    assert.equal(scheduleModeNeedsCursorColumn("incremental_append", "kafka"), false);
    assert.equal(scheduleModeNeedsCursorColumn("incremental_deduped", "csv"), false);
    assert.equal(scheduleModeNeedsCursorColumn("incremental_append", "csv"), true);
  });

  it("an edit of a planned incremental schedule needs the cursor; the reason names it", () => {
    for (const syncMode of ["incremental_append", "incremental_deduped"]) {
      const blockers = scheduleSaveBlockers({ ...ready, syncMode, cursorColumn: "" });
      assert.deepEqual(blockers.map((b) => b.field), ["sched-cursor"], syncMode);
      assert.equal(
        scheduleSaveBlockedReason(blockers),
        `To save: enter the cursor column (${syncMode} advances on it).`,
      );
    }
    assert.deepEqual(fields({ syncMode: "cdc", cursorColumn: "" }), []);
  });

  it("the server plan binds the cursor on a new or mapping-less schedule, so Save is open", () => {
    assert.equal(scheduleCursorRequired({ syncMode: "incremental_append", serverPlans: true }), false);
    assert.deepEqual(fields({ syncMode: "incremental_append", cursorColumn: "", serverPlans: true }), []);
    // The key rule is the store's and holds either way.
    assert.deepEqual(fields({ cursorColumn: "", primaryKey: "", serverPlans: true }), ["sched-pk"]);
  });

  it("lists every missing requirement once, in form order", () => {
    const blockers = scheduleSaveBlockers({
      ...ready, name: "", destTable: "", cadenceMode: "cron", cron: "", cursorColumn: "", primaryKey: "",
      contractBlock: "Contract is not signed",
    });
    assert.deepEqual(blockers.map((b) => b.field), ["sched-name", "sched-dst-table", "sched-cron", "sched-cursor", "sched-pk", "sched-contract"]);
    assert.match(
      scheduleSaveBlockedReason(blockers),
      /^To save: name the schedule; name the destination table; enter the cron expression; enter the cursor column \(incremental_append advances on it\); enter the primary key, or set Duplicate policy to Append; Contract is not signed\.$/,
    );
  });
});
