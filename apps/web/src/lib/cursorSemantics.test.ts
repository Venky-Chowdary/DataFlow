/**
 * Cursor-meaning verdicts and their serialization to the engine.
 * Run: npx --yes tsx --test apps/web/src/lib/cursorSemantics.test.ts
 */
import assert from "node:assert/strict";
import { describe, it } from "node:test";
import { evaluateCursorSemantics } from "./cursorSemantics.js";
import { buildStreamContracts, streamContractsNeedReview } from "./streamContracts.js";

describe("cursor semantics verdicts", () => {
  it("refuses an undeclared cursor for a sync that promises update capture", () => {
    const verdict = evaluateCursorSemantics({
      syncMode: "incremental_deduped",
      cursorField: "created_at",
      declared: "",
      validationMode: "strict",
    });
    assert.equal(verdict.status, "block");
    assert.ok(verdict.primaryAction.includes("created_at"));
    assert.ok(verdict.alternatives.length > 0);
  });

  it("accepts a cursor the source maintains on every change", () => {
    const verdict = evaluateCursorSemantics({
      syncMode: "incremental_deduped",
      cursorField: "updated_at",
      declared: "modification_timestamp",
    });
    assert.equal(verdict.status, "ok");
    assert.equal(verdict.capturesUpdates, true);
  });

  it("accepts insert-only but does not claim it captures updates", () => {
    const verdict = evaluateCursorSemantics({
      syncMode: "incremental_deduped",
      cursorField: "created_at",
      declared: "insert_only",
    });
    assert.equal(verdict.status, "ok");
    assert.equal(verdict.capturesUpdates, false);
    assert.ok(verdict.reason.includes("does not move when a row is updated"));
  });

  it("refuses a business date, which a backdated insert falls behind", () => {
    const verdict = evaluateCursorSemantics({
      syncMode: "incremental_append",
      cursorField: "order_date",
      declared: "business_date",
    });
    assert.equal(verdict.status, "block");
    assert.ok(verdict.reason.includes("never read"));
  });

  it("refuses a declaration it does not understand", () => {
    const verdict = evaluateCursorSemantics({
      syncMode: "incremental_append",
      cursorField: "updated_at",
      declared: "probably_fine",
    });
    assert.equal(verdict.status, "block");
  });

  it("has no opinion on a full refresh", () => {
    const verdict = evaluateCursorSemantics({
      syncMode: "full_refresh_overwrite",
      cursorField: "updated_at",
      declared: "",
    });
    assert.equal(verdict.status, "not_applicable");
  });
});

describe("cursor semantics reach the engine", () => {
  const base = {
    streamNames: ["orders"],
    syncMode: "incremental_append",
    schemaPolicy: "manual_review",
    validationMode: "strict",
    fieldCount: 3,
    requiresCursor: true,
    requiresPrimaryKey: false,
    defaultCursor: "updated_at",
    defaultPrimaryKey: "id",
    streamFields: {},
  };

  it("sends the declared meaning", () => {
    const [contract] = buildStreamContracts({
      ...base,
      defaultCursorSemantics: "modification_timestamp",
    });
    assert.equal(contract.cursor_semantics, "modification_timestamp");
  });

  it("sends no declaration rather than inventing one", () => {
    const [contract] = buildStreamContracts(base);
    assert.equal(contract.cursor_semantics, "");
  });

  it("carries a per-stream declaration over the shared default", () => {
    const [orders, items] = buildStreamContracts({
      ...base,
      streamNames: ["orders", "items"],
      defaultCursorSemantics: "modification_timestamp",
      streamFields: {
        items: {
          cursorField: "created_at",
          primaryKeyField: "id",
          cursorSemantics: "insert_only",
        },
      },
    });
    assert.equal(orders.cursor_semantics, "modification_timestamp");
    assert.equal(items.cursor_semantics, "insert_only");
  });

  it("flags a selected-but-undeclared cursor as needing review", () => {
    const needsReview = streamContractsNeedReview({
      streamNames: ["orders"],
      sourceColumns: ["id", "updated_at"],
      requiresCursor: true,
      requiresPrimaryKey: false,
      defaultCursor: "updated_at",
      defaultPrimaryKey: "id",
      streamFields: {},
      syncMode: "incremental_append",
      validationMode: "strict",
    });
    assert.equal(needsReview, true);
  });

  it("clears review once the cursor's meaning is declared", () => {
    const needsReview = streamContractsNeedReview({
      streamNames: ["orders"],
      sourceColumns: ["id", "updated_at"],
      requiresCursor: true,
      requiresPrimaryKey: false,
      defaultCursor: "updated_at",
      defaultPrimaryKey: "id",
      defaultCursorSemantics: "modification_timestamp",
      streamFields: {},
      syncMode: "incremental_append",
      validationMode: "strict",
    });
    assert.equal(needsReview, false);
  });

  it("does not ask for a declaration a full refresh will not use", () => {
    const needsReview = streamContractsNeedReview({
      streamNames: ["orders"],
      sourceColumns: ["id", "updated_at"],
      requiresCursor: false,
      requiresPrimaryKey: false,
      defaultCursor: "",
      defaultPrimaryKey: "",
      streamFields: {},
      syncMode: "full_refresh_overwrite",
      validationMode: "strict",
    });
    assert.equal(needsReview, false);
  });
});
