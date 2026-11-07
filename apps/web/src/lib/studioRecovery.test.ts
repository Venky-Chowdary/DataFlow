/**
 * Run: npx --yes tsx --test apps/web/src/lib/studioRecovery.test.ts
 *
 * QA 2026-11-06: Open Validate on a failed schedule run painted the studio's
 * previous 1,000,000-row route. These cases pin the replacement rules.
 */
import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { describe, it } from "node:test";
import { applyDeclaredStreamFields, buildStreamContracts, seedStreamFieldsFromCandidates } from "./streamContracts.js";
import { clearSession } from "./session.js";
import {
  applyStudioRecovery,
  decideStudioRecovery,
  destinationEnginePaint,
  keepDeclaredCursorKey,
  loadStudioRecovery,
  persistStudioRecovery,
  RECOVERY_STORAGE_KEY,
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
    shapeSteps: [{ op: "rename", from: "series", to: "id" }],
    procedureParams: { p: "old" },
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

describe("Transfer Studio wires the recovery decisions into the page effects", () => {
  const page = readFileSync(new URL("../pages/TransferPage.tsx", import.meta.url), "utf8");
  const app = readFileSync(new URL("../DataTransferApp.tsx", import.meta.url), "utf8");

  it("the column effect calls keepDeclaredCursorKey and does not assign the heuristic directly", () => {
    assert.match(page, /keepDeclaredCursorKey\(/);
    assert.equal(page.includes("setCursorField(cursorCandidate)"), false);
    assert.equal(page.includes("setPrimaryKeyField(primaryKeyCandidate)"), false);
    assert.match(page, /applyDeclaredStreamFields\(/);
    assert.equal(page.includes("cursorCandidate || cursorField"), false);
    assert.equal(page.includes("primaryKeyCandidate || primaryKeyField"), false);
    assert.match(page, /setDestKindMode\(destPaint\.mode\)/);
  });

  it("a missing destination goes through destinationEnginePaint and a recovery owns shape steps", () => {
    assert.match(page, /destinationEnginePaint\(/);
    assert.match(page, /shapeOwnedByRecovery/);
    assert.match(page, /persistStudioRecovery\(/);
  });

  it("a hard reload reads the stored route before Studio mounts", () => {
    assert.match(app, /loadStudioRecovery\(/);
  });
});

describe("column effect keeps a declared cursor and key", () => {
  it("does not clear id/sku on the previous columns or replace them with updated_at/id", () => {
    const declared = { declaredCursor: "id", declaredPrimaryKey: "sku" };
    const afterStale = keepDeclaredCursorKey({
      cursorField: "id",
      primaryKeyField: "sku",
      columns: ["series"],
      cursorCandidate: "",
      primaryKeyCandidate: "",
      ...declared,
    });
    assert.deepEqual(afterStale, { cursorField: "id", primaryKeyField: "sku" });
    const afterFresh = keepDeclaredCursorKey({
      cursorField: afterStale.cursorField,
      primaryKeyField: afterStale.primaryKeyField,
      columns: ["id", "sku", "label", "updated_at"],
      cursorCandidate: "updated_at",
      primaryKeyCandidate: "id",
      ...declared,
    });
    assert.deepEqual(afterFresh, { cursorField: "id", primaryKeyField: "sku" });
  });

  it("keeps cursor id when the new columns have no timestamp", () => {
    const next = keepDeclaredCursorKey({
      cursorField: "id",
      primaryKeyField: "id",
      columns: ["id", "label"],
      cursorCandidate: "",
      primaryKeyCandidate: "id",
      declaredCursor: "id",
      declaredPrimaryKey: "id",
    });
    assert.deepEqual(next, { cursorField: "id", primaryKeyField: "id" });
  });

  it("a heuristic fills only a field the schedule left empty", () => {
    const next = keepDeclaredCursorKey({
      cursorField: "",
      primaryKeyField: "",
      columns: ["id", "updated_at"],
      cursorCandidate: "updated_at",
      primaryKeyCandidate: "id",
      declaredCursor: "",
      declaredPrimaryKey: "",
    });
    assert.deepEqual(next, { cursorField: "updated_at", primaryKeyField: "id" });
  });
});

function installWebStores(): Map<string, string> {
  const mem = new Map<string, string>();
  const store = {
    getItem: (key: string) => mem.get(key) ?? null,
    setItem: (key: string, value: string) => { mem.set(key, value); },
    removeItem: (key: string) => { mem.delete(key); },
  };
  Object.defineProperty(globalThis, "sessionStorage", { configurable: true, value: store });
  Object.defineProperty(globalThis, "localStorage", { configurable: true, value: store });
  return mem;
}

function signIn(email = "mahi@example.com"): void {
  localStorage.setItem("df2.session", JSON.stringify({
    email,
    name: "Mahi",
    role: "admin",
    token: "not-stored-on-the-route",
    expires_at: Date.now() + 3_600_000,
    signed_in_at: Date.now(),
  }));
}

describe("reload, refusal, recipe, and a missing destination", () => {
  it("a hard reload stores only the job identity for the signed-in workspace", () => {
    const mem = installWebStores();
    signIn();
    const job = scheduleJob();
    job.transfer_request.mappings = [{ source: "id", target: "id", transform: "hash" }];
    job.transfer_request.source.procedure_params = { token: "sekrit" };
    const decision = decideStudioRecovery({ job, ...loaded });
    assert.equal(decision.action, "apply");
    if (decision.action !== "apply") return;
    assert.equal(decision.fields.mappings[0]?.transform, "hash");
    assert.equal(decision.fields.procedureParams.token, "sekrit");
    persistStudioRecovery({
      activeWorkspaceId: "ws-mahi",
      routeWorkspaceId: "ws-mahi",
      jobId: "6aee7fae586f58f1b23b772b",
      scheduleId: decision.fields.scheduleId,
    });
    const stored = mem.get(RECOVERY_STORAGE_KEY) || "";
    assert.equal(stored.includes(SQL), false);
    assert.equal(stored.includes("sekrit"), false);
    assert.equal(stored.includes("hash"), false);
    const restored = loadStudioRecovery("ws-mahi");
    assert.equal(restored?.jobId, "6aee7fae586f58f1b23b772b");
    assert.equal(restored?.scheduleId, "sched-1902");
    assert.equal("sourceSql" in (restored || {}), false);
    assert.equal(loadStudioRecovery("ws-other"), null);
    assert.equal(loadStudioRecovery(""), null);
    assert.equal(loadStudioRecovery("__all__"), null);
    assert.equal(loadStudioRecovery("__unassigned__"), null);
    clearSession();
    assert.equal(mem.has(RECOVERY_STORAGE_KEY), false);
    signIn();
    assert.equal(loadStudioRecovery("ws-mahi"), null);
  });

  it("another operator in the same tab does not receive the stored identity", () => {
    installWebStores();
    signIn("mahi@example.com");
    persistStudioRecovery({
      activeWorkspaceId: "ws-mahi",
      routeWorkspaceId: "ws-mahi",
      jobId: "job-1",
      scheduleId: "sched-1",
    });
    signIn("other@example.com");
    assert.equal(loadStudioRecovery("ws-mahi"), null);
  });

  it("a refused open replaces the previous route with an empty one", () => {
    const decision = decideStudioRecovery({
      job: scheduleJob(),
      ...loaded,
      activeWorkspaceId: "ws-other",
    });
    const cleared = applyStudioRecovery(staleRoute(), decision);
    assert.equal(cleared.destTable, "");
    assert.equal(cleared.sourceSql, "");
    assert.equal(cleared.primaryKey, "");
    assert.equal(cleared.cursorColumn, "");
    assert.equal(cleared.shapeSteps.length, 0);
  });

  it("an empty schedule recipe replaces the previous shape steps", () => {
    const decision = decideStudioRecovery({
      schedule: {
        id: "sched-1902",
        workspace_id: "ws-mahi",
        source_connector_id: "shop-mysql",
        dest_connector_id: "inventory-mysql",
        source_table: "qa_my_seed",
        dest_table: "mahi_qa_20261106_1902_schedule",
        dest_database: "inventory",
        shape_recipe: { steps: [] },
        procedure_params: {},
      },
      ...loaded,
    });
    assert.equal(decision.action, "apply");
    if (decision.action !== "apply") return;
    const previous = { ...staleRoute(), shapeSteps: [{ op: "rename", from: "series", to: "id" }] };
    const next = applyStudioRecovery(previous, decision);
    assert.deepEqual(next.shapeSteps, []);
    assert.deepEqual(next.procedureParams, {});
    assert.equal(next.destDatabase, "inventory");
    assert.equal(JSON.stringify(next.shapeSteps).includes("series"), false);
  });

  it("a missing destination clears the previous engine and database", () => {
    const paint = destinationEnginePaint({
      destConnectorId: "",
      destTable: "mahi_qa_20261106_1902_schedule",
      destDatabase: "",
      connectorFound: false,
    });
    assert.equal(paint.clearEngine, true);
    assert.equal(paint.mode, "database");
    assert.equal(paint.connectorId, "");
    assert.equal(paint.database, "");
    assert.equal(paint.table, "mahi_qa_20261106_1902_schedule");
    const kept = destinationEnginePaint({
      destConnectorId: "inventory-mysql",
      destTable: "landed",
      destDatabase: "inventory",
      connectorFound: true,
    });
    assert.equal(kept.clearEngine, false);
    assert.equal(kept.mode, "database");
    assert.equal(kept.database, "inventory");
  });
});

describe("declared cursor is the stream contract Validate sends", () => {
  it("keeps id and sku when the heuristic would pick updated_at and id", () => {
    const columns = ["id", "sku", "label", "updated_at"];
    const declared = keepDeclaredCursorKey({
      cursorField: "id",
      primaryKeyField: "sku",
      columns,
      cursorCandidate: "updated_at",
      primaryKeyCandidate: "id",
      declaredCursor: "id",
      declaredPrimaryKey: "sku",
    });
    const stale = { qa_my_seed: { cursorField: "updated_at", primaryKeyField: "id" } };
    const seeded = applyDeclaredStreamFields(
      ["qa_my_seed"],
      stale,
      "updated_at",
      "id",
      columns,
      "id",
      "sku",
    );
    const contracts = buildStreamContracts({
      streamNames: ["qa_my_seed"],
      syncMode: "incremental_deduped",
      schemaPolicy: "evolve",
      validationMode: "strict",
      fieldCount: columns.length,
      requiresCursor: true,
      requiresPrimaryKey: true,
      defaultCursor: "updated_at",
      defaultPrimaryKey: "id",
      streamFields: seeded,
    });
    assert.equal(declared.cursorField, "id");
    assert.equal(declared.primaryKeyField, "sku");
    assert.equal(seeded.qa_my_seed?.cursorField, "id");
    assert.equal(seeded.qa_my_seed?.primaryKeyField, "sku");
    assert.equal(contracts[0]?.cursor_field, "id");
    assert.equal(contracts[0]?.primary_key, "sku");
    const heuristic = seedStreamFieldsFromCandidates(
      ["qa_my_seed"],
      {},
      "updated_at",
      "id",
      columns,
    );
    assert.equal(heuristic.qa_my_seed?.cursorField, "updated_at");
    assert.equal(heuristic.qa_my_seed?.primaryKeyField, "id");
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

  it("an all-workspaces view is not a tenant mismatch", () => {
    const decision = decideStudioRecovery({
      job: scheduleJob(),
      ...loaded,
      activeWorkspaceId: "__all__",
    });
    assert.equal(decision.action, "apply");
    if (decision.action !== "apply") return;
    const next = applyStudioRecovery(staleRoute(), decision);
    assert.equal(next.destTable, "mahi_qa_20261106_1902_schedule");
    assert.equal(next.cursorColumn, "id");
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
