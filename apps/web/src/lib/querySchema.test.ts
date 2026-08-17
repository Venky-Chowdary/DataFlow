import assert from "node:assert/strict";
import { describe, it } from "node:test";
import type { QuerySchemaObjectInfo } from "./api";
import {
  firstExpanded,
  matchesObjectName,
  mergeExpandedObject,
  toSchemaObject,
} from "./querySchema";
import type { SchemaObject } from "./sqlIntel";

const obj = (over: Partial<QuerySchemaObjectInfo> = {}): QuerySchemaObjectInfo => ({
  name: "public.orders",
  type: "table",
  schema_name: "public",
  columns: [],
  row_estimate: 0,
  ...over,
});

describe("toSchemaObject", () => {
  it("keeps an unreported type empty instead of guessing one", () => {
    const o = toSchemaObject(
      obj({ columns: [{ name: "id", type: "", nullable: null, primary_key: false }] }),
    );
    assert.equal(o.columns?.[0].type, "");
  });

  it("maps nullability as tri-state so only explicit false means NOT NULL", () => {
    const o = toSchemaObject(
      obj({
        columns: [
          { name: "a", type: "BIGINT", nullable: false, primary_key: true },
          { name: "b", type: "TEXT", nullable: true, primary_key: false },
          { name: "c", type: "TEXT", nullable: null, primary_key: false },
        ],
      }),
    );
    assert.equal(o.columns?.[0].nullable, false);
    assert.equal(o.columns?.[1].nullable, true);
    assert.equal(o.columns?.[2].nullable, undefined);
  });

  it("preserves declared width verbatim — never narrows for display", () => {
    const o = toSchemaObject(
      obj({
        columns: [
          { name: "id", type: "DECIMAL(20,0)", nullable: false, primary_key: true },
        ],
      }),
    );
    assert.equal(o.columns?.[0].type, "DECIMAL(20,0)");
  });

  it("carries primary key flags and defaults them to false", () => {
    const o = toSchemaObject(
      obj({
        columns: [
          { name: "id", type: "INT", nullable: false, primary_key: true },
          { name: "nm", type: "TEXT" } as QuerySchemaObjectInfo["columns"][number],
        ],
      }),
    );
    assert.equal(o.columns?.[0].primaryKey, true);
    assert.equal(o.columns?.[1].primaryKey, false);
  });

  it("tolerates a response with no columns array at all", () => {
    const o = toSchemaObject({
      name: "c1",
      type: "collection",
    } as QuerySchemaObjectInfo);
    assert.deepEqual(o.columns, []);
    assert.equal(o.schema, undefined);
    assert.equal(o.rowEstimate, undefined);
  });

  it("drops a zero row estimate rather than displaying a fake zero", () => {
    assert.equal(toSchemaObject(obj({ row_estimate: 0 })).rowEstimate, undefined);
    assert.equal(toSchemaObject(obj({ row_estimate: 42 })).rowEstimate, 42);
  });
});

describe("matchesObjectName", () => {
  it("matches identical names", () => {
    assert.equal(matchesObjectName("orders", "orders"), true);
  });

  it("matches across schema qualification", () => {
    assert.equal(matchesObjectName("public.orders", "orders"), true);
    assert.equal(matchesObjectName("orders", "public.orders"), true);
  });

  it("does not match two distinct unqualified names that merely differ", () => {
    assert.equal(matchesObjectName("orders", "order_items"), false);
  });
});

describe("mergeExpandedObject", () => {
  const prev: SchemaObject[] = [
    { name: "public.orders", type: "table", columns: [] },
    { name: "public.customers", type: "table", columns: [] },
  ];

  it("fills columns for the expanded object only", () => {
    const next = mergeExpandedObject(
      prev,
      "orders",
      obj({
        columns: [{ name: "id", type: "BIGINT", nullable: false, primary_key: true }],
        row_estimate: 10,
      }),
    );
    assert.equal(next[0].columns?.length, 1);
    assert.equal(next[0].rowEstimate, 10);
    assert.equal(next[1].columns?.length, 0);
  });

  it("never drops the objects already listed", () => {
    const next = mergeExpandedObject(
      prev,
      "orders",
      obj({ columns: [{ name: "id", type: "INT", nullable: true, primary_key: false }] }),
    );
    assert.deepEqual(
      next.map((o) => o.name),
      prev.map((o) => o.name),
    );
  });

  it("leaves the tree untouched when the connector returned no columns", () => {
    assert.equal(mergeExpandedObject(prev, "orders", obj()), prev);
    assert.equal(mergeExpandedObject(prev, "orders", undefined), prev);
  });

  it("leaves the tree untouched when the expanded object is not listed", () => {
    const next = mergeExpandedObject(
      prev,
      "audit_log",
      obj({ columns: [{ name: "id", type: "INT", nullable: true, primary_key: false }] }),
    );
    assert.equal(next, prev);
  });

  it("keeps the existing row estimate when the expansion reports none", () => {
    const withEstimate: SchemaObject[] = [
      { name: "orders", type: "table", columns: [], rowEstimate: 99 },
    ];
    const next = mergeExpandedObject(
      withEstimate,
      "orders",
      obj({
        name: "orders",
        row_estimate: 0,
        columns: [{ name: "id", type: "INT", nullable: true, primary_key: false }],
      }),
    );
    assert.equal(next[0].rowEstimate, 99);
  });
});

describe("firstExpanded", () => {
  it("picks the first object carrying columns", () => {
    const found = firstExpanded([
      obj({ name: "a" }),
      obj({
        name: "b",
        columns: [{ name: "id", type: "INT", nullable: true, primary_key: false }],
      }),
    ]);
    assert.equal(found?.name, "b");
  });

  it("returns undefined when nothing was introspected", () => {
    assert.equal(firstExpanded([obj({ name: "a" })]), undefined);
    assert.equal(firstExpanded([]), undefined);
  });
});
