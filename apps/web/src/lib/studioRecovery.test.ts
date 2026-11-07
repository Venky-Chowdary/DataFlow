/**
 * Run: npx --yes tsx --test apps/web/src/lib/studioRecovery.test.ts
 *
 * QA 2026-11-06: Open Validate on a failed schedule run painted the studio's
 * previous 1,000,000-row route. These cases pin the replacement rules.
 */
import assert from "node:assert/strict";
import { describe, it } from "node:test";
import {
  applyStudioRecovery,
  decideStudioRecovery,
  type StudioRouteSnapshot,
} from "./studioRecovery.js";

const SQL = "SELECT * FROM qa_my_seed LIMIT 3";
const STALE_SQL = "SELECT * FROM mahi_qa_20261106_scale_1000000";
const STALE_TABLE = "mahi_qa_20261106_scale_1000000";

function staleRoute(): StudioRouteSnapshot {
  return {
    sourceConnectorId: "pg-reporting",
    destConnectorId: "mysql-inventory",
    sourceTable: "generate_series",
    destDatabase: "defaultdb",
    destTable: STALE_TABLE,
    syncMode: "full_refresh_append",
    sourceReadMode: "query",
    sourceSql: STALE_SQL,
    primaryKey: "series",
    cursorColumn: "series",
    rowLimit: 1000000,
    mappings: [{ source: "series", destination: "id" }],
    scheduleId: "",
  };
}

function scheduleJob() {
  return {
    _id: "6aee7fae586f58f1b23b772b",
    workspace_id: "ws-mahi",
    source_connector_id: "shop-mysql",
    dest_connector_id: "inventory-mysql",
    source_name: "qa_my_seed",
    destination_database: "defaultdb",
    destination_collection: "mahi_qa_20261106_1902_schedule",
    sync_mode: "incremental_deduped",
    transfer_request: {
      workspace_id: "ws-mahi",
      sync_mode: "incremental_deduped",
      limit: 3,
      mappings: [
        { source: "id", target: "id", target_type: "BIGINT" },
        { source: "label", target: "label", target_type: "LONGTEXT" },
      ],
      source: {
        connector_id: "shop-mysql",
        table: "qa_my_seed",
        connection_string: "mysql://shop:secret@db/shop",
        password: "secret",
        extra: {
          schedule_id: "sched-1902",
          source_read_mode: "query",
          source_query: SQL,
        },
      },
      destination: {
        connector_id: "inventory-mysql",
        database: "defaultdb",
        table: "mahi_qa_20261106_1902_schedule",
        extra: { schedule_id: "sched-1902", primary_key: "id" },
      },
      stream_contracts: [{ name: "qa_my_seed", cursor_field: "id", primary_key: "id" }],
    },
  };
}

const loaded = {
  activeWorkspaceId: "ws-mahi",
  knownConnectorIds: ["shop-mysql", "inventory-mysql", "pg-reporting"],
  connectorsLoaded: true,
  authenticated: true,
};

describe("studio recovery replaces a stale route", () => {
  it("hydrates the schedule job and drops the previous 1M route", () => {
    const decision = decideStudioRecovery({ job: scheduleJob(), ...loaded });
    assert.equal(decision.action, "apply");
    if (decision.action !== "apply") return;
    const next = applyStudioRecovery(staleRoute(), decision);
    assert.equal(next.sourceConnectorId, "shop-mysql");
    assert.equal(next.destConnectorId, "inventory-mysql");
    assert.equal(next.sourceTable, "qa_my_seed");
    assert.equal(next.destDatabase, "defaultdb");
    assert.equal(next.destTable, "mahi_qa_20261106_1902_schedule");
    assert.equal(next.sourceReadMode, "query");
    assert.equal(next.sourceSql, SQL);
    assert.equal(next.primaryKey, "id");
    assert.equal(next.cursorColumn, "id");
    assert.equal(next.syncMode, "incremental_deduped");
    assert.equal(next.rowLimit, 3);
    assert.equal(next.scheduleId, "sched-1902");
    assert.deepEqual(next.mappings.map((row) => row.source), ["id", "label"]);
    assert.equal(JSON.stringify(next).includes(STALE_TABLE), false);
    assert.equal(JSON.stringify(next).includes("secret"), false);
    assert.equal(JSON.stringify(next).includes("connection_string"), false);
  });

  it("a reload from an empty studio still hydrates the same route", () => {
    const decision = decideStudioRecovery({ job: scheduleJob(), ...loaded });
    assert.equal(decision.action, "apply");
    if (decision.action !== "apply") return;
    const next = applyStudioRecovery(
      {
        ...staleRoute(),
        sourceConnectorId: "",
        destConnectorId: "",
        destTable: "",
        sourceSql: "",
        mappings: [],
      },
      decision,
    );
    assert.equal(next.destTable, "mahi_qa_20261106_1902_schedule");
    assert.equal(next.sourceSql, SQL);
    assert.equal(next.primaryKey, "id");
  });

  it("a second schedule replaces the first; nothing from the first remains", () => {
    const first = decideStudioRecovery({ job: scheduleJob(), ...loaded });
    const other = scheduleJob();
    other.transfer_request.source.extra.schedule_id = "sched-other";
    other.transfer_request.destination.extra.schedule_id = "sched-other";
    other.transfer_request.source.table = "other_seed";
    other.transfer_request.source.extra.source_query = "SELECT id FROM other_seed LIMIT 1";
    other.transfer_request.destination.table = "other_dest";
    other.transfer_request.stream_contracts = [{ cursor_field: "updated_at", primary_key: "sku" }];
    other.transfer_request.destination.extra.primary_key = "sku";
    const second = decideStudioRecovery({ job: other, ...loaded });
    assert.equal(first.action, "apply");
    assert.equal(second.action, "apply");
    if (first.action !== "apply" || second.action !== "apply") return;
    const afterFirst = applyStudioRecovery(staleRoute(), first);
    const afterSecond = applyStudioRecovery(afterFirst, second);
    assert.equal(afterSecond.scheduleId, "sched-other");
    assert.equal(afterSecond.destTable, "other_dest");
    assert.equal(afterSecond.sourceSql, "SELECT id FROM other_seed LIMIT 1");
    assert.equal(afterSecond.primaryKey, "sku");
    assert.equal(afterSecond.cursorColumn, "updated_at");
    assert.equal(JSON.stringify(afterSecond).includes(SQL), false);
    assert.equal(JSON.stringify(afterSecond).includes("sched-1902"), false);
    assert.equal(JSON.stringify(afterSecond).includes(STALE_TABLE), false);
  });

  it("a list row clears the stale SQL and does not borrow another schedule", () => {
    const listRow = {
      _id: "6aee7fae586f58f1b23b772b",
      workspace_id: "ws-mahi",
      schedule_id: "sched-1902",
      source_connector_id: "shop-mysql",
      dest_connector_id: "inventory-mysql",
      source_name: "qa_my_seed",
      destination_database: "defaultdb",
      destination_collection: "mahi_qa_20261106_1902_schedule",
      sync_mode: "incremental_deduped",
      source_read_mode: "query",
    };
    const otherSchedule = {
      id: "sched-other",
      workspace_id: "ws-mahi",
      source_connector_id: "shop-mysql",
      dest_connector_id: "inventory-mysql",
      source_table: "other",
      dest_table: "other_dest",
      source_read_mode: "query",
      source_query: "SELECT * FROM other",
      primary_key: "sku",
      cursor_column: "sku",
      sync_mode: "full_refresh_append",
      mappings: [{ source: "sku", target: "sku" }],
    };
    const decision = decideStudioRecovery({
      job: listRow,
      schedule: otherSchedule,
      ...loaded,
    });
    assert.equal(decision.action, "apply");
    if (decision.action !== "apply") return;
    assert.equal(decision.fields.partial, true);
    assert.equal(decision.fields.scheduleId, "sched-1902");
    assert.equal(decision.fields.sourceSql, "");
    assert.equal(decision.fields.sourceReadMode, "query");
    assert.equal(decision.fields.destTable, "mahi_qa_20261106_1902_schedule");
    const next = applyStudioRecovery(staleRoute(), decision);
    assert.equal(next.sourceSql, "");
    assert.equal(next.destTable, "mahi_qa_20261106_1902_schedule");
    assert.equal(JSON.stringify(next).includes("other_dest"), false);
    assert.equal(JSON.stringify(next).includes(STALE_SQL), false);
  });

  it("the matching schedule fills SQL the list row does not carry", () => {
    const listRow = {
      workspace_id: "ws-mahi",
      schedule_id: "sched-1902",
      source_connector_id: "shop-mysql",
      dest_connector_id: "inventory-mysql",
      source_name: "qa_my_seed",
      destination_collection: "mahi_qa_20261106_1902_schedule",
      destination_database: "defaultdb",
    };
    const decision = decideStudioRecovery({
      job: listRow,
      schedule: {
        id: "sched-1902",
        workspace_id: "ws-mahi",
        source_connector_id: "shop-mysql",
        dest_connector_id: "inventory-mysql",
        source_table: "qa_my_seed",
        dest_table: "mahi_qa_20261106_1902_schedule",
        source_read_mode: "query",
        source_query: SQL,
        primary_key: "id",
        cursor_column: "id",
        sync_mode: "incremental_deduped",
        row_limit: 3,
        mappings: [{ source: "id", target: "id" }],
      },
      ...loaded,
    });
    assert.equal(decision.action, "apply");
    if (decision.action !== "apply") return;
    const next = applyStudioRecovery(staleRoute(), decision);
    assert.equal(next.sourceSql, SQL);
    assert.equal(next.primaryKey, "id");
    assert.equal(next.cursorColumn, "id");
    assert.equal(next.rowLimit, 3);
    assert.equal(next.syncMode, "incremental_deduped");
    assert.equal(next.mappings.length, 1);
  });
});

describe("studio recovery refuses the wrong tenant and missing auth", () => {
  it("a signed-out session does not hydrate the route", () => {
    const decision = decideStudioRecovery({
      job: scheduleJob(),
      ...loaded,
      authenticated: false,
    });
    assert.deepEqual(decision, { action: "refuse", reason: "unauthenticated" });
    const next = applyStudioRecovery(staleRoute(), decision);
    assert.equal(next.destTable, "");
    assert.equal(next.sourceSql, "");
    assert.equal(next.sourceConnectorId, "");
    assert.equal(JSON.stringify(next).includes("qa_my_seed"), false);
    assert.equal(JSON.stringify(next).includes(STALE_TABLE), false);
  });

  it("a job from another workspace is not applied onto this studio", () => {
    const decision = decideStudioRecovery({
      job: scheduleJob(),
      ...loaded,
      activeWorkspaceId: "ws-other",
    });
    assert.deepEqual(decision, { action: "refuse", reason: "wrong_workspace" });
    const next = applyStudioRecovery(staleRoute(), decision);
    assert.equal(next.destTable, "");
    assert.equal(next.scheduleId, "");
  });

  it("no selected workspace does not accept a workspace-scoped job", () => {
    const decision = decideStudioRecovery({
      job: scheduleJob(),
      ...loaded,
      activeWorkspaceId: "",
    });
    assert.equal(decision.action, "refuse");
  });

  it("a deleted source connector clears that side instead of keeping the stale one", () => {
    const decision = decideStudioRecovery({
      job: scheduleJob(),
      ...loaded,
      knownConnectorIds: ["inventory-mysql"],
    });
    assert.equal(decision.action, "apply");
    if (decision.action !== "apply") return;
    assert.equal(decision.missingSource, true);
    assert.equal(decision.missingDest, false);
    const next = applyStudioRecovery(staleRoute(), decision);
    assert.equal(next.sourceConnectorId, "");
    assert.equal(next.destConnectorId, "inventory-mysql");
    assert.equal(next.destTable, "mahi_qa_20261106_1902_schedule");
    assert.equal(next.sourceTable, "qa_my_seed");
    assert.equal(next.sourceSql, SQL);
  });

  it("a connector list that is still loading does not call the ids missing", () => {
    const decision = decideStudioRecovery({
      job: scheduleJob(),
      ...loaded,
      connectorsLoaded: false,
      knownConnectorIds: [],
    });
    assert.equal(decision.action, "apply");
    if (decision.action !== "apply") return;
    assert.equal(decision.missingSource, false);
    assert.equal(decision.missingDest, false);
  });
});
