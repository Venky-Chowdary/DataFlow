/**
 * Run: npx --yes tsx --test apps/web/src/lib/jobStudioRoute.test.ts
 */
import assert from "node:assert/strict";
import { describe, it } from "node:test";
import type { Connector } from "./types.js";
import { endpointAddress, matchSavedConnector, planJobStudioRoute, scheduleRecoveryRoute } from "./jobStudioRoute.js";

const conn = (over: Partial<Connector>): Connector => ({
  id: "c1", name: "Orders PG", type: "postgresql", host: "db.internal", port: 5432,
  database: "shop", status: "active", created_at: "", ...over,
} as Connector);

const job = (source: Record<string, unknown>, destination: Record<string, unknown>, extra: Record<string, unknown> = {}) => ({
  transfer_request: { source, destination, sync_mode: "full_refresh", ...extra },
});

describe("destination restore", () => {
  it("a file export destination reopens as file export (kind=file_export, no output_path)", () => {
    const plan = planJobStudioRoute(job({ kind: "file", extra: { file_id: "f1" } }, { kind: "file_export", format: "json" }), []);
    assert.equal(plan.destination.kind, "file_export");
    assert.equal(plan.destination.kind === "file_export" && plan.destination.format, "json");
  });

  it("an inline DB destination with a masked password matches the saved connector by address", () => {
    const plan = planJobStudioRoute(
      job({ kind: "file", extra: { file_id: "f1" } }, {
        kind: "database", format: "postgresql",
        connection_string: "postgresql://app:****@DB.internal:5432/shop", table: "orders",
      }),
      [conn({ id: "pg1" })],
    );
    assert.equal(plan.destination.kind, "database");
    if (plan.destination.kind !== "database") return;
    assert.equal(plan.destination.connectorId, "pg1");
    assert.equal(plan.destination.table, "orders");
    assert.equal(plan.destination.missingLabel, "");
  });

  it("an unmatched masked DB destination is reported, not silently half-filled", () => {
    const plan = planJobStudioRoute(
      job({ kind: "file", extra: { file_id: "f1" } }, { kind: "database", format: "mysql", host: "other", database: "x", password: "****" }),
      [conn({})],
    );
    assert.equal(plan.destination.kind === "database" && plan.destination.missingLabel, "mysql at other/x");
  });
});

describe("source restore", () => {
  it("a saved connector source restores by id", () => {
    const plan = planJobStudioRoute(job({ kind: "database", connector_id: "c1", table: "orders" }, { kind: "file_export" }), [conn({})]);
    assert.deepEqual(plan.source, { kind: "connector", connectorId: "c1", connectorType: "postgresql", table: "orders", collection: "" });
  });

  it("an MCP-style inline DB source (no connector_id) matches a saved connector by host/port/db", () => {
    const plan = planJobStudioRoute(
      job({ kind: "database", format: "postgres", connection_string: "postgres://u:****@db.internal/shop", table: "orders" }, { kind: "file_export" }),
      [conn({ id: "other", host: "elsewhere" }), conn({ id: "pg1" })],
    );
    assert.equal(plan.source.kind, "connector");
    assert.equal(plan.source.kind === "connector" && plan.source.connectorId, "pg1");
  });

  it("an inline DB source with no saved match says which connection to save", () => {
    const plan = planJobStudioRoute(job({ kind: "database", format: "mysql", host: "h", database: "d" }, { kind: "file_export" }), []);
    assert.deepEqual(plan.source, { kind: "missing", reason: "unsaved_connection", label: "mysql at h/d" });
  });

  it("two equally good saved matches are ambiguous, never a guess", () => {
    const plan = planJobStudioRoute(
      job({ kind: "database", format: "postgresql", host: "db.internal", database: "shop" }, { kind: "file_export" }),
      [conn({ id: "a", username: "x" }), conn({ id: "b", username: "y" })],
    );
    assert.equal(plan.source.kind === "missing" && plan.source.reason, "ambiguous_connection");
  });

  it("stored upload, journal and inline file sources", () => {
    assert.equal(planJobStudioRoute(job({ kind: "file", extra: { file_id: "f9" } }, {}, { source_filename: "a.csv" }), []).source.kind, "stored_file");
    assert.equal(planJobStudioRoute(job({ kind: "file", extra: { journal_id: "j1", journal_stream: "s" } }, {}), []).source.kind, "journal");
    const inline = planJobStudioRoute(job({ kind: "file" }, {}, { source_filename: "b.csv" }), []).source;
    assert.deepEqual(inline, { kind: "missing", reason: "inline_file", label: "b.csv" });
  });
});

describe("matching rules", () => {
  it("type aliases and default ports compare equal", () => {
    assert.deepEqual(endpointAddress({ connection_string: "postgres://u:****@H/db" }), { type: "postgresql", host: "h", port: 5432, database: "db" });
    assert.ok(matchSavedConnector({ format: "pg", host: "db.internal", database: "shop" }, [conn({})]).connector);
  });

  it("a different port or database is not the same connection", () => {
    assert.equal(matchSavedConnector({ format: "postgresql", host: "db.internal", port: 6543, database: "shop" }, [conn({})]).connector, undefined);
    assert.equal(matchSavedConnector({ format: "postgresql", host: "db.internal", database: "crm" }, [conn({})]).connector, undefined);
  });
});

describe("encrypted job connection strings (API address projection)", () => {
  it("an inline source whose URL is vault ciphertext matches by the API's address projection", () => {
    const plan = planJobStudioRoute(
      job({
        kind: "database", connection_string: "enc:v1:gAAAA-opaque", table: "qa_orders",
        address: { type: "postgresql", host: "db.internal", port: 5432, database: "shop" },
      }, { kind: "file_export", format: "csv" }),
      [conn({ id: "other", database: "analytics" }), conn({ id: "pg1" })],
    );
    assert.equal(plan.source.kind === "connector" && plan.source.connectorId, "pg1");
    assert.equal(plan.source.kind === "connector" && plan.source.table, "qa_orders");
  });

  it("an unmatched ciphertext source names its host and database in the warning", () => {
    const plan = planJobStudioRoute(
      job({
        kind: "database", connection_string: "enc:v1:x",
        address: { type: "postgresql", host: "db.internal", port: 5432, database: "nope" },
      }, { kind: "file_export" }),
      [conn({})],
    );
    assert.deepEqual(plan.source, { kind: "missing", reason: "unsaved_connection", label: "postgresql at db.internal/nope" });
  });

  it("a ciphertext destination URL is never replayed into the connection field", () => {
    const plan = planJobStudioRoute(
      job({ kind: "file", extra: { file_id: "f1" } }, {
        kind: "database", connection_string: "enc:v1:x",
        address: { type: "mysql", host: "m", port: 3306, database: "crm" },
      }),
      [],
    );
    assert.equal(plan.destination.kind === "database" && plan.destination.connectionString, "");
    assert.equal(plan.destination.kind === "database" && plan.destination.missingLabel, "mysql at m/crm");
  });
});

describe("schedule recovery route (QA 2026-11-06)", () => {
  const scheduled = (srcExtra: Record<string, unknown>, dstExtra: Record<string, unknown> = {}) => ({
    _id: "job-sched-1",
    transfer_request: {
      source: { kind: "database", format: "postgresql", connector_id: "pg-reporting", table: "orders", extra: srcExtra },
      destination: { kind: "database", format: "mysql", connector_id: "mysql-inventory", table: "orders_copy", extra: dstExtra },
    },
  });

  it("a schedule-fired job reopens its own route and names its schedule", () => {
    assert.deepEqual(scheduleRecoveryRoute(scheduled({ schedule_id: "sch-42" }, { schedule_id: "sch-42" })), {
      scheduleId: "sch-42",
      sourceConnectorId: "pg-reporting",
      destConnectorId: "mysql-inventory",
      sourceTable: "orders",
      destTable: "orders_copy",
    });
  });

  it("the schedule id on the destination alone is enough", () => {
    assert.equal(scheduleRecoveryRoute(scheduled({}, { schedule_id: "sch-7" }))?.scheduleId, "sch-7");
  });

  it("a one-shot job is not a schedule recovery: Studio keeps the plain job path", () => {
    assert.equal(scheduleRecoveryRoute(scheduled({})), null);
    assert.equal(scheduleRecoveryRoute(null), null);
    assert.equal(scheduleRecoveryRoute({}), null);
  });

  it("a schedule job without both saved connectors is not reopened by guesswork", () => {
    const job = scheduled({ schedule_id: "sch-42" });
    delete (job.transfer_request.destination as Record<string, unknown>).connector_id;
    assert.equal(scheduleRecoveryRoute(job), null);
  });

  it("the job-root connector ids stand in when the request copy lost them", () => {
    const job = {
      source_connector_id: "pg-reporting",
      dest_connector_id: "mysql-inventory",
      transfer_request: { source: { extra: { schedule_id: "sch-9" } }, destination: {} },
    };
    const route = scheduleRecoveryRoute(job);
    assert.equal(route?.sourceConnectorId, "pg-reporting");
    assert.equal(route?.destConnectorId, "mysql-inventory");
    assert.equal(route && "destTable" in route, false);
  });
});
