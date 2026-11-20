import assert from "node:assert/strict";
import { describe, it } from "node:test";
import { destinationSchemaProbeKey } from "./destinationSchemaProbe.js";
import { NO_DEST_KEYS, destKeyColumnsFor, destKeyColumnsFromProbe } from "./destinationKeyColumns.js";

/** D369 / review minor 2: table A's key must never reach Map for table B. */
const ep = (table: string, extra: Record<string, unknown> = {}) =>
  destinationSchemaProbeKey({ format: "postgresql", host: "h", database: "d", schema: "public", table, ...extra } as never, "c1");

describe("destination key columns follow the probed table", () => {
  it("returns the keys only for the table they were read from", () => {
    const a = ep("orders");
    const keys = destKeyColumnsFromProbe(a, { tableExists: true, primaryKey: ["id"], identity: ["id", "seq"] });
    assert.deepEqual(destKeyColumnsFor(keys, a), ["id", "seq"]);
    // Another table typed (its probe failed or has not answered): no key.
    assert.deepEqual(destKeyColumnsFor(keys, ep("customers")), []);
    // Same table on another connector / database: no key either.
    assert.deepEqual(destKeyColumnsFor(keys, ep("orders", { database: "other" })), []);
    // Destination cleared.
    assert.deepEqual(destKeyColumnsFor(keys, ep("")), []);
  });

  it("a missing table has no key, and nothing read means no key", () => {
    const b = ep("new_table");
    assert.deepEqual(destKeyColumnsFor(destKeyColumnsFromProbe(b, { tableExists: false, primaryKey: ["id"] }), b), []);
    assert.deepEqual(destKeyColumnsFor(NO_DEST_KEYS, b), []);
    assert.deepEqual(destKeyColumnsFor(NO_DEST_KEYS, ""), []);
  });
});
