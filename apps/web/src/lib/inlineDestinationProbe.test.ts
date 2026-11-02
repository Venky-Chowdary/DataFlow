import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";
import { describe, it } from "node:test";
import {
  createDestinationSchemaProbe,
  destinationSchemaProbeKey,
  INLINE_DESTINATION_PROBE_DEBOUNCE_MS,
  isInlineDestinationConfigured,
  type DestinationProbeEndpoint,
} from "./destinationSchemaProbe.js";

const webRoot = join(dirname(fileURLToPath(import.meta.url)), "..");

const sqliteEndpoint: DestinationProbeEndpoint = {
  kind: "database",
  format: "sqlite",
  database: "/home/ubuntu/br_fixtures/dest.db",
  table: "customers",
  connection_string: "sqlite:////home/ubuntu/br_fixtures/dest.db",
};

const postgresEndpoint: DestinationProbeEndpoint = {
  kind: "database",
  format: "postgresql",
  host: "db.internal",
  port: 5432,
  database: "analytics",
  schema: "public",
  table: "customers",
  username: "transfer",
};

function delay(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

async function waitFor(predicate: () => boolean): Promise<void> {
  for (let attempt = 0; attempt < 40; attempt += 1) {
    if (predicate()) return;
    await delay(5);
  }
  assert.fail("Timed out waiting for destination probe");
}

describe("inline destination schema probing", () => {
  it("allows configured inline destinations through the loader and auto-probe", () => {
    const page = readFileSync(join(webRoot, "pages/TransferPage.tsx"), "utf8");
    assert.match(
      page,
      /const endpoint = opts\?\.\s*endpoint \?\? buildDestinationEndpoint\(\);[\s\S]{0,260}!connectorId\s*&&\s*!isInlineDestinationConfigured\(endpoint\)/,
    );
    assert.match(
      page,
      /!connectorId\s*&&\s*!isInlineDestinationConfigured\(currentDestinationEndpoint\)/,
    );
    assert.match(
      page,
      /void inlineDestinationProbeRef\.current\?\.schedule\(currentDestinationEndpoint\)/,
    );
    assert.equal(isInlineDestinationConfigured(sqliteEndpoint), true);
    assert.equal(
      isInlineDestinationConfigured({ format: "sqlite", database: "/tmp/dest.db" }),
      true,
    );
    assert.equal(
      isInlineDestinationConfigured({ format: "postgresql", host: "db", database: "analytics" }),
      true,
    );
    assert.equal(isInlineDestinationConfigured({ format: "postgresql", host: "db" }), false);
  });

  it("loads inline SQLite once after the debounce and publishes its schema", async () => {
    assert.equal(INLINE_DESTINATION_PROBE_DEBOUNCE_MS, 600);
    const calls: DestinationProbeEndpoint[] = [];
    let schema: Record<string, string> = {};
    const probe = createDestinationSchemaProbe({
      delayMs: 25,
      load: async (endpoint) => {
        calls.push(endpoint);
        return { columns: ["id", "email"], schema: { id: "BIGINT", email: "VARCHAR" } };
      },
      onResult: (result) => { schema = result.schema; },
    });

    const request = probe.schedule(sqliteEndpoint);
    await delay(10);
    assert.equal(calls.length, 0);
    const outcome = await request;
    assert.equal(outcome.status, "loaded");
    assert.equal(calls.length, 1);
    assert.deepEqual(schema, { id: "BIGINT", email: "VARCHAR" });
    probe.cancel();
  });

  it("invalidates the probe cache when the connection string changes", async () => {
    const calls: DestinationProbeEndpoint[] = [];
    const probe = createDestinationSchemaProbe({
      delayMs: 1,
      load: async (endpoint) => {
        calls.push(endpoint);
        return endpoint.connection_string ?? "";
      },
    });

    await probe.schedule(sqliteEndpoint);
    await probe.schedule({
      ...sqliteEndpoint,
      connection_string: "sqlite:////home/ubuntu/br_fixtures/alternate.db",
    });
    assert.equal(calls.length, 2);
    probe.cancel();
  });

  it("does not introspect an unconfigured inline form", async () => {
    let calls = 0;
    const probe = createDestinationSchemaProbe({
      delayMs: 1,
      load: async () => { calls += 1; },
    });

    assert.equal(isInlineDestinationConfigured({ format: "sqlite" }), false);
    assert.equal((await probe.schedule({ format: "sqlite" })).status, "unconfigured");
    assert.equal(calls, 0);
  });

  it("ignores an in-flight response after the endpoint changes", async () => {
    let resolveSqlite: ((value: string) => void) | null = null;
    let resolvePostgres: ((value: string) => void) | null = null;
    const results: string[] = [];
    const probe = createDestinationSchemaProbe({
      delayMs: 1,
      load: (endpoint) => new Promise((resolve) => {
        if (endpoint.format === "sqlite") resolveSqlite = resolve;
        else resolvePostgres = resolve;
      }),
      onResult: (result) => { results.push(result); },
    });

    const staleRequest = probe.schedule(sqliteEndpoint);
    await waitFor(() => resolveSqlite !== null);
    const currentRequest = probe.schedule(postgresEndpoint);
    await waitFor(() => resolvePostgres !== null);
    resolvePostgres?.("postgres-schema");
    assert.equal((await currentRequest).status, "loaded");
    resolveSqlite?.("stale-sqlite-schema");
    assert.equal((await staleRequest).status, "stale");
    assert.deepEqual(results, ["postgres-schema"]);
    probe.cancel();
  });

  it("changes the safe probe identity for connection fields without exposing secrets", () => {
    const base = {
      ...postgresEndpoint,
      connection_string: "postgresql://transfer:do-not-key-this@db.internal/analytics",
      password: "also-do-not-key-this",
    };
    const baseKey = destinationSchemaProbeKey(base);
    const variations: DestinationProbeEndpoint[] = [
      { ...base, format: "postgres" },
      { ...base, host: "db-next.internal" },
      { ...base, port: 5433 },
      { ...base, database: "reporting" },
      { ...base, schema: "archive" },
      { ...base, table: "orders" },
      { ...base, connection_string: "postgresql://transfer:other-secret@db-next.internal/analytics" },
      { ...base, username: "reader" },
    ];

    for (const variation of variations) {
      assert.notEqual(destinationSchemaProbeKey(variation), baseKey);
    }
    assert.match(baseKey, /"passwordPresent":true/);
    assert.doesNotMatch(baseKey, /do-not-key-this|also-do-not-key-this/);
    assert.equal(isInlineDestinationConfigured(postgresEndpoint), true);
  });
});
