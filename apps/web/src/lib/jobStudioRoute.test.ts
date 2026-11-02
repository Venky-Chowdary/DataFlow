/**
 * Run: npx --yes tsx --test apps/web/src/lib/jobStudioRoute.test.ts
 */
import assert from "node:assert/strict";
import { describe, it } from "node:test";
import type { Connector } from "./types.js";
import { endpointAddress, matchSavedConnector, planJobStudioRoute } from "./jobStudioRoute.js";

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
