import assert from "node:assert/strict";
import { test } from "node:test";
import {
  DEST_EXISTS_AFTER_CREATE_NEW_CORRECTIVE,
  inboxCorrectiveAction,
  inboxNeedsRunNow,
  inboxNeedsStudio,
  isCreateNewDestExistsPark,
  isEmptyMappingFinding,
  NO_MAPPINGS_RUN_MESSAGE,
  persistedMappingRows,
  scheduleNeedsStudio,
  studioIntentConnectorsReady,
  scheduleCreateOpensStudio,
  studioIntentFromSchedule,
} from "./scheduleApprovalCta";
import type { PipelineSchedule, ScheduleApprovalInboxItem } from "./types";

test("Run now on a mapping-less schedule names the fix first and Studio second", () => {
  const plan = NO_MAPPINGS_RUN_MESSAGE.indexOf("Plan mappings");
  const studio = NO_MAPPINGS_RUN_MESSAGE.indexOf("open Transfer Studio");
  assert.ok(NO_MAPPINGS_RUN_MESSAGE.includes("no job was created"));
  assert.ok(plan >= 0 && studio > plan);
});

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
  assert.equal(scheduleNeedsStudio({ ...sched, enabled: true, last_status: "ok", needs_approval: false }), true);
  assert.deepEqual(studioIntentFromSchedule(sched), {
    step: "source",
    replaceRoute: true,
    scheduleId: "s1",
    sourceConnectorId: "src",
    destConnectorId: "dst",
    sourceTable: "orders",
    destTable: "orders_dw",
    destDatabase: "",
    syncMode: "",
    sourceReadMode: "table",
    sourceSql: "",
    primaryKey: "",
    cursorColumn: "",
    rowLimit: 0,
    mappings: [],
    workspaceId: "",
    shapeSteps: [],
    procedureParams: {},
  });
});

test("a schedule opened from another workspace does not carry its SQL across", () => {
  const sched = {
    id: "s1",
    workspace_id: "ws-a",
    source_connector_id: "src",
    dest_connector_id: "dst",
    source_table: "qa_my_seed",
    dest_table: "landed",
    source_read_mode: "query",
    source_query: "SELECT * FROM qa_my_seed LIMIT 3",
    primary_key: "id",
    cursor_column: "id",
    sync_mode: "incremental_deduped",
    mappings: [{ source: "id", target: "id" }],
  } as PipelineSchedule;
  const refused = studioIntentFromSchedule(sched, "ws-b");
  assert.equal(refused.refused, "wrong_workspace");
  assert.equal(refused.sourceSql, "");
  assert.equal(refused.sourceConnectorId, "");
  const same = studioIntentFromSchedule(sched, "ws-a");
  assert.equal(same.refused, undefined);
  assert.equal(same.sourceSql, "SELECT * FROM qa_my_seed LIMIT 3");
  assert.equal(same.primaryKey, "id");
  assert.equal(same.cursorColumn, "id");
  assert.equal(same.syncMode, "incremental_deduped");
  assert.equal(same.rowLimit, 0);
  assert.equal(same.mappings.length, 1);
});

test("inbox copy names Studio even when the stored finding says Open Validate", () => {
  const item = {
    schedule_id: "s1",
    schedule_name: "Crown → Snowflake",
    workspace_id: "w",
    source: "src:sample",
    destination: "dst:newtable",
    sync_mode: "full_refresh_append",
    enabled: false,
    approval: {
      id: "a1",
      status: "open",
      kind: "run_refused",
      code: "RUN_REFUSED",
      finding: "Schedule has no persisted column mappings — unattended runs must replay",
      corrective_action: "Open Validate for this job and resolve the blocking check, then run the schedule again.",
      approvable: false,
      requested_scopes: [],
      occurrences: 1,
      created_at: "",
    },
  } as ScheduleApprovalInboxItem;
  const copy = inboxCorrectiveAction(item);
  assert.match(copy, /Transfer Studio/);
  assert.equal(copy.includes("Open Validate for this job"), false);
});

test("persisted mapping rows require a source or target", () => {
  assert.equal(persistedMappingRows([{ source: "id", target: "id" }]).length, 1);
  assert.equal(persistedMappingRows([{ confidence: 1 }]).length, 0);
  assert.equal(persistedMappingRows(null).length, 0);
});

test("a schedule Studio seed waits until connectors have loaded", () => {
  const intent = { sourceConnectorId: "src", destConnectorId: "dst" };
  assert.equal(studioIntentConnectorsReady(intent, []), false);
  assert.equal(studioIntentConnectorsReady(intent, ["src"]), true);
  assert.equal(studioIntentConnectorsReady({}, []), true);
});

test("dest-exists after create-new is Run now, not a signature", () => {
  const item = {
    schedule_id: "s1",
    schedule_name: "Crown → newtable",
    workspace_id: "w",
    source: "src:sample",
    destination: "dst:newtable",
    sync_mode: "full_refresh_append",
    enabled: true,
    approval: {
      id: "a1",
      status: "open",
      kind: "run_refused",
      code: "RUN_REFUSED",
      finding: "Decision Artifact DDL identity diverged from current Map — re-run Validate before Execute.",
      corrective_action: "Open Validate for this job and resolve the blocking check, then run the schedule again.",
      approvable: false,
      requested_scopes: [],
      occurrences: 1,
      created_at: "",
    },
  } as ScheduleApprovalInboxItem;
  assert.equal(isCreateNewDestExistsPark(item.approval.code, item.approval.finding), true);
  assert.equal(inboxNeedsRunNow(item), true);
  assert.equal(inboxNeedsStudio(item), false);
  assert.equal(inboxCorrectiveAction(item), DEST_EXISTS_AFTER_CREATE_NEW_CORRECTIVE);
});

test("missing upsert business key sends the operator to Studio with a concrete fix", () => {
  const item = {
    schedule_id: "orders",
    schedule_name: "Orders Postgres → orders_feed",
    workspace_id: "w",
    source: "orders",
    destination: "orders_feed",
    sync_mode: "upsert",
    enabled: true,
    approval: {
      id: "key-needed",
      status: "open",
      kind: "run_refused",
      code: "RUN_REFUSED",
      finding: "Upsert needs a business key. Declare a primary key, or choose append to allow duplicate rows.",
      corrective_action: "Open Validate for this job and resolve the blocking check.",
      approvable: false,
      requested_scopes: [],
      occurrences: 2,
      created_at: "",
    },
  } as ScheduleApprovalInboxItem;
  assert.equal(inboxNeedsStudio(item), true);
  assert.match(inboxCorrectiveAction(item), /Destination \/ Advanced/);
  assert.match(inboxCorrectiveAction(item), /Append/);
});

test("zero-retry budget park still offers Run now", () => {
  const item = {
    schedule_id: "s1",
    schedule_name: "Crown → newtable",
    workspace_id: "w",
    source: "src:sample",
    destination: "dst:newtable",
    sync_mode: "full_refresh_append",
    enabled: true,
    approval: {
      id: "a1",
      status: "open",
      kind: "run_refused",
      code: "RUN_REFUSED",
      finding: "Retry budget exhausted after 0 attempt(s).",
      corrective_action: "Open Validate for this job and resolve the blocking check, then run the schedule again.",
      approvable: false,
      requested_scopes: [],
      occurrences: 1,
      created_at: "",
    },
  } as ScheduleApprovalInboxItem;
  assert.equal(inboxNeedsRunNow(item), true);
  assert.match(inboxCorrectiveAction(item), /Run now/);
  assert.equal(inboxCorrectiveAction(item).includes("Open Validate for this job"), false);
});

test("create without mappings opens Studio — the beat will not invent Map", () => {
  assert.equal(scheduleCreateOpensStudio({ mapping_count: 0 }), true);
  assert.equal(scheduleCreateOpensStudio({ mapping_count: 2 }), false);
});

// D435: a parked schedule names its route in an operator's words and gives the fix that fits.
const parkedOnDns = (extra: Partial<ScheduleApprovalInboxItem> = {}): ScheduleApprovalInboxItem => ({
  schedule_id: "s-dns",
  schedule_name: "QAR72-S4b-my-ora-iu",
  workspace_id: "",
  source: "c81fd66d-be79-4cf2-8720-5987b9e26069:r72_up_src2",
  destination: "42ca781c-83ec-4afa-9dba-60bf69983083:R72_UP_ORA2",
  source_connector: { id: "c81fd66d-be79-4cf2-8720-5987b9e26069", name: "QAR72-MY", type: "mysql", deleted: false },
  source_table: "r72_up_src2",
  destination_connector: { id: "42ca781c-83ec-4afa-9dba-60bf69983083", name: "QAR72-ORA", type: "oracle", deleted: false },
  dest_table: "R72_UP_ORA2",
  remedy: {
    target: "connection",
    code: "network.dns_not_found",
    corrective_action: "Check the spelling of the host name on the connection. Open the schedule's connection (Test, or Edit connection) to confirm it connects, then run the schedule again.",
  },
  sync_mode: "incremental_upsert",
  enabled: true,
  approval: {
    id: "a1", status: "open", kind: "run_refused", code: "RUN_REFUSED",
    finding: "Host not found: the connection's host name didn't resolve (DNS).",
    corrective_action: "Open Validate for this job and resolve the blocking check, then run the schedule again.",
    approvable: false, requested_scopes: [], occurrences: 1, created_at: "2026-11-25T00:00:00Z",
  },
  ...extra,
} as ScheduleApprovalInboxItem);

test("the inbox route shows connection names and tables; the id is hover text only", async () => {
  const { inboxRouteEnd } = await import("./scheduleApprovalCta");
  const src = inboxRouteEnd(parkedOnDns(), "source");
  const dst = inboxRouteEnd(parkedOnDns(), "destination");
  assert.equal(src.label, "QAR72-MY · r72_up_src2");
  assert.equal(dst.label, "QAR72-ORA · R72_UP_ORA2");
  assert.doesNotMatch(src.label + dst.label, /[0-9a-f]{8}-[0-9a-f]{4}/);
  assert.match(src.title, /c81fd66d-be79-4cf2-8720-5987b9e26069/);
  assert.equal(src.type, "mysql");
});

test("a DNS finding's fix points at the connection, not Validate", async () => {
  const { inboxNeedsConnectionFix } = await import("./scheduleApprovalCta");
  const item = parkedOnDns();
  assert.equal(inboxNeedsConnectionFix(item), true);
  assert.doesNotMatch(inboxCorrectiveAction(item), /Validate/);
  assert.match(inboxCorrectiveAction(item), /host name on the connection/);
});

test("a deleted connection is named as such with re-select guidance", async () => {
  const { inboxConnectionDeleted, inboxRouteEnd } = await import("./scheduleApprovalCta");
  const item = parkedOnDns({
    source_connector: { id: "c81fd66d-be79-4cf2-8720-5987b9e26069", name: "", type: "", deleted: true },
    remedy: {
      target: "connection_deleted", sides: "source",
      corrective_action: "Connection deleted: the source connection of this schedule no longer exists. Re-select the connection on this schedule (edit it, or open it in Transfer Studio), or restore the deleted connection, then resume.",
    },
  });
  assert.equal(inboxConnectionDeleted(item), true);
  assert.equal(inboxRouteEnd(item, "source").label, "Connection deleted · r72_up_src2");
  assert.match(inboxCorrectiveAction(item), /^Connection deleted.*Re-select/);
});

test("an inbox item from an API without names still hides the raw id", async () => {
  const { inboxRouteEnd } = await import("./scheduleApprovalCta");
  const item = parkedOnDns({ source_connector: undefined, source_table: undefined, remedy: undefined });
  const end = inboxRouteEnd(item, "source");
  assert.equal(end.label, "r72_up_src2");
  assert.match(end.title, /c81fd66d/);
});
