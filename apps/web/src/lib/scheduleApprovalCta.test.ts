import assert from "node:assert/strict";
import { test } from "node:test";
import {
  inboxNeedsStudio,
  isEmptyMappingFinding,
  scheduleNeedsStudio,
  studioIntentFromSchedule,
} from "./scheduleApprovalCta";
import type { PipelineSchedule, ScheduleApprovalInboxItem } from "./types";

test("empty mapping is a plan change, not a signature", () => {
  assert.equal(isEmptyMappingFinding("EMPTY_MAPPING_CONTRACT", ""), true);
  assert.equal(
    isEmptyMappingFinding("RUN_REFUSED", "Schedule has no persisted column mappings — unattended"),
    true,
  );
  assert.equal(isEmptyMappingFinding("SOURCE_SCHEMA_DRIFT", "column added"), false);
});

test("inbox rows with no mappings send the operator to Studio", () => {
  const item = {
    schedule_id: "s1",
    schedule_name: "MySQL → Snowflake",
    workspace_id: "w",
    source: "mysql",
    destination: "snowflake",
    sync_mode: "full_refresh_append",
    enabled: false,
    approval: {
      id: "a1",
      status: "open",
      kind: "run_refused",
      code: "EMPTY_MAPPING_CONTRACT",
      finding: "Schedule has no persisted column mappings",
      corrective_action: "Open Transfer Studio",
      approvable: false,
      requested_scopes: [],
      occurrences: 1,
      created_at: "",
    },
  } as ScheduleApprovalInboxItem;
  assert.equal(inboxNeedsStudio(item), true);
});

test("a paused schedule with mapping_count 0 needs Studio, not Decide", () => {
  const sched = {
    id: "s1",
    mapping_count: 0,
    last_status: "needs_approval",
    enabled: false,
    source_connector_id: "src",
    dest_connector_id: "dst",
    source_table: "orders",
    dest_table: "orders_dw",
  } as PipelineSchedule;
  assert.equal(scheduleNeedsStudio(sched), true);
  assert.deepEqual(studioIntentFromSchedule(sched), {
    step: "source",
    sourceConnectorId: "src",
    destConnectorId: "dst",
    sourceTable: "orders",
    destTable: "orders_dw",
  });
});
