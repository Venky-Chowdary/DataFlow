/**
 * Scenario tests for multi-stream Advanced contracts.
 * Run: npx --yes tsx --test apps/web/src/lib/streamContracts.test.ts
 */
import assert from "node:assert/strict";
import { describe, it } from "node:test";
import {
  buildStreamContracts,
  seedStreamFieldsFromCandidates,
  streamContractsNeedReview,
} from "./streamContracts.js";

describe("buildStreamContracts", () => {
  it("emits per-stream cursor and primary key", () => {
    const contracts = buildStreamContracts({
      streamNames: ["orders", "items"],
      syncMode: "incremental_deduped",
      schemaPolicy: "manual_review",
      validationMode: "strict",
      fieldCount: 5,
      requiresCursor: true,
      requiresPrimaryKey: true,
      defaultCursor: "updated_at",
      defaultPrimaryKey: "id",
      streamFields: {
        orders: { cursorField: "updated_at", primaryKeyField: "order_id" },
        items: { cursorField: "modified_ts", primaryKeyField: "item_id" },
      },
    });
    assert.equal(contracts.length, 2);
    assert.equal(contracts[0].cursor_field, "updated_at");
    assert.equal(contracts[0].primary_key, "order_id");
    assert.equal(contracts[1].cursor_field, "modified_ts");
    assert.equal(contracts[1].primary_key, "item_id");
  });

  it("falls back to shared defaults when a stream has no override", () => {
    const contracts = buildStreamContracts({
      streamNames: ["a"],
      syncMode: "cdc",
      schemaPolicy: "propagate_columns",
      validationMode: "balanced",
      fieldCount: 2,
      requiresCursor: true,
      requiresPrimaryKey: true,
      defaultCursor: "ts",
      defaultPrimaryKey: "pk",
      streamFields: {},
    });
    assert.equal(contracts[0].cursor_field, "ts");
    assert.equal(contracts[0].primary_key, "pk");
  });

  it("stamps snapshot_mode on CDC contracts", () => {
    const contracts = buildStreamContracts({
      streamNames: ["orders"],
      syncMode: "cdc",
      schemaPolicy: "manual_review",
      validationMode: "strict",
      fieldCount: 3,
      requiresCursor: true,
      requiresPrimaryKey: true,
      defaultCursor: "updated_at",
      defaultPrimaryKey: "id",
      streamFields: {},
      snapshotMode: "never",
    });
    assert.equal(contracts[0].snapshot_mode, "never");
  });
});

describe("streamContractsNeedReview", () => {
  it("flags missing cursor on any stream", () => {
    const needs = streamContractsNeedReview({
      streamNames: ["a", "b"],
      sourceColumns: ["id", "ts"],
      requiresCursor: true,
      requiresPrimaryKey: false,
      defaultCursor: "ts",
      defaultPrimaryKey: "",
      streamFields: {
        a: { cursorField: "ts", primaryKeyField: "" },
        b: { cursorField: "", primaryKeyField: "" },
      },
    });
    assert.equal(needs, true);
  });

  it("passes when every stream has required fields", () => {
    const needs = streamContractsNeedReview({
      streamNames: ["a", "b"],
      sourceColumns: ["id", "ts"],
      requiresCursor: true,
      requiresPrimaryKey: true,
      defaultCursor: "",
      defaultPrimaryKey: "",
      streamFields: {
        a: { cursorField: "ts", primaryKeyField: "id" },
        b: { cursorField: "ts", primaryKeyField: "id" },
      },
    });
    assert.equal(needs, false);
  });
});

describe("seedStreamFieldsFromCandidates", () => {
  it("seeds missing streams and drops stale keys", () => {
    const next = seedStreamFieldsFromCandidates(
      ["orders", "items"],
      { stale: { cursorField: "x", primaryKeyField: "y" } },
      "updated_at",
      "id",
      ["id", "updated_at"],
    );
    assert.ok(next.orders);
    assert.ok(next.items);
    assert.equal(next.orders.cursorField, "updated_at");
    assert.equal(next.stale, undefined);
  });
});
