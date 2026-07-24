/**
 * Run: npx --yes tsx --test apps/web/src/lib/validateIssueGrouping.test.ts
 */
import assert from "node:assert/strict";
import { describe, it } from "node:test";
import type { PreflightResult, ValidationIssue } from "./types.js";
import {
  buildDisplayBlockers,
  buildExecutiveSummary,
  findDuplicateKeyRoot,
  groupIsoNormalizeIssues,
  isIsoNormalizeCoercion,
  partitionCoercionColumns,
  partitionExplainIssues,
} from "./validateIssueGrouping.js";

function basePreflight(over: Partial<PreflightResult> = {}): PreflightResult {
  return {
    passed: false,
    passed_count: 10,
    total_gates: 13,
    readiness_score: 76.9,
    gates: [],
    blockers: [],
    ...over,
  };
}

describe("findDuplicateKeyRoot", () => {
  it("collapses G9 + G6 + G8 duplicate blockers into one root", () => {
    const pf = basePreflight({
      gates: [
        {
          id: "g9_data_integrity",
          status: "block",
          message: "Data integrity failed: id: duplicate key values",
          duration_ms: 18,
          details: { issue_texts: ["id: duplicate key values (a×2)", "expect_column_unique:id: 12 failures"] },
        },
        {
          id: "g6_target_ddl",
          status: "block",
          message: "Primary key candidate 'id' has 12 duplicate value(s) in source sample",
          duration_ms: 5,
          details: { primary_key: { source: "id", target: "id" }, sample_duplicates: new Array(12).fill("x") },
        },
        {
          id: "g8_reconciliation",
          status: "block",
          message: "Dry-run reconciliation failed — 12 duplicate target key(s) on id",
          duration_ms: 4,
          details: { duplicate_keys: 12, primary_key: "id", target_rows: 25 },
        },
      ],
      blockers: [
        { id: "g9_data_integrity", message: "Data integrity failed: id: duplicate key values" },
        { id: "g6_target_ddl", message: "Primary key candidate 'id' has 12 duplicate value(s) in source sample" },
        { id: "g8_reconciliation", message: "Dry-run reconciliation failed — 12 duplicate target key(s) on id" },
      ],
    });

    const root = findDuplicateKeyRoot(pf);
    assert.ok(root);
    assert.equal(root!.title, "Duplicate identity keys");
    assert.equal(root!.primaryKey, "id");
    assert.equal(root!.duplicateCount, 12);
    assert.equal(root!.sampleRows, 25);
    assert.ok(root!.gateIds.includes("g9_data_integrity"));
    assert.ok(root!.gateIds.includes("g6_target_ddl"));
    assert.ok(root!.gateIds.includes("g8_reconciliation"));
    assert.match(root!.impact, /12 duplicate/);

    const display = buildDisplayBlockers(pf);
    assert.equal(display.length, 1);
    assert.equal(display[0].kind, "duplicate_root");
    assert.equal(display[0].gateChips?.length, 3);
  });

  it("leaves unrelated blockers separate", () => {
    const pf = basePreflight({
      gates: [
        {
          id: "g9_data_integrity",
          status: "block",
          message: "id: duplicate key values",
          duration_ms: 1,
          details: { duplicate_keys: 2, primary_key: "id" },
        },
      ],
      blockers: [
        { id: "g9_data_integrity", message: "id: duplicate key values", details: { duplicate_keys: 2, primary_key: "id" } },
        { id: "g4_mapping_confidence", message: "Mapping confidence below threshold" },
      ],
    });
    const display = buildDisplayBlockers(pf);
    assert.equal(display.length, 2);
    assert.equal(display[0].kind, "duplicate_root");
    assert.equal(display[1].kind, "blocker");
    assert.match(display[1].title, /mapping/i);
  });
});

describe("ISO normalize grouping", () => {
  it("collapses six Type normalize at write issues", () => {
    const cols = ["created_at", "last_updated", "posted_date", "scraped_at", "updated_at", "last_seen_at"];
    const issues: ValidationIssue[] = cols.map((col) => ({
      gate: "g3_schema_contract",
      title: "Type normalize at write",
      severity: "warning",
      what: `Column '${col}' → TIMESTAMP: 25 of 25 sampled value(s) use ISO timestamps`,
      why: "Converting the source type to the target type may lose precision",
      fix: `Column '${col}' → TIMESTAMP: will normalize`,
      examples: [],
      columns: [col],
      detail_messages: [],
    }));
    const { isoGroup, remaining } = groupIsoNormalizeIssues(issues);
    assert.ok(isoGroup);
    assert.equal(isoGroup!.columns.length, 6);
    assert.equal(remaining.length, 0);
    assert.match(isoGroup!.subtitle, /no data loss/i);

    const parts = partitionExplainIssues([
      ...issues,
      {
        gate: "g9_data_integrity",
        title: "Data integrity",
        severity: "block",
        what: "duplicate keys",
        why: "why",
        fix: "fix",
        examples: [],
        columns: ["id"],
        detail_messages: [],
      },
    ]);
    assert.equal(parts.blockers.length, 1);
    assert.equal(parts.warnings.length, 0);
    assert.ok(parts.isoGroup);
  });

  it("partitions coercion warn-normalize rows out of actionable drama", () => {
    const { isoNormalize, otherActionable, clean } = partitionCoercionColumns([
      {
        source: "created_at",
        target: "created_at",
        source_type: "TIMESTAMP",
        target_type: "TIMESTAMP",
        sampled: 25,
        ok: 25,
        nulls: 0,
        sentinel_nulls: 0,
        failed: 0,
        wire_normalize: 25,
        sample_failures: [],
        severity: "warn",
        suggested_fix: "ISO timestamps will normalize at write",
      },
      {
        source: "amount",
        target: "amount",
        source_type: "TEXT",
        target_type: "DECIMAL",
        sampled: 25,
        ok: 20,
        nulls: 0,
        sentinel_nulls: 0,
        failed: 5,
        sample_failures: [],
        severity: "block",
      },
      {
        source: "name",
        target: "name",
        source_type: "TEXT",
        target_type: "TEXT",
        sampled: 25,
        ok: 25,
        nulls: 0,
        sentinel_nulls: 0,
        failed: 0,
        sample_failures: [],
        severity: "ok",
      },
    ]);
    assert.equal(isoNormalize.length, 1);
    assert.equal(otherActionable.length, 1);
    assert.equal(clean.length, 1);
    assert.ok(isIsoNormalizeCoercion(isoNormalize[0]));
  });
});

describe("buildExecutiveSummary", () => {
  it("tells a blocked story with root-cause until lines", () => {
    const pf = basePreflight({
      gates: [
        {
          id: "g9_data_integrity",
          status: "block",
          message: "duplicate key values on id",
          duration_ms: 1,
          details: { duplicate_keys: 12, primary_key: "id", target_rows: 25 },
        },
        {
          id: "g6_target_ddl",
          status: "block",
          message: "Primary key candidate 'id' has 12 duplicate value(s)",
          duration_ms: 1,
          details: { primary_key: { target: "id" }, sample_duplicates: [1, 2] },
        },
        {
          id: "g8_reconciliation",
          status: "block",
          message: "12 duplicate target key(s) on id",
          duration_ms: 1,
          details: { duplicate_keys: 12, primary_key: "id" },
        },
      ],
      blockers: [
        { id: "g9_data_integrity", message: "duplicate key values on id" },
        { id: "g6_target_ddl", message: "Primary key candidate 'id' has 12 duplicate value(s)" },
        { id: "g8_reconciliation", message: "12 duplicate target key(s) on id" },
      ],
    });
    const summary = buildExecutiveSummary(pf);
    assert.ok(summary);
    assert.equal(summary!.title, "Validation blocked");
    assert.match(summary!.subtitle, /1 blocking issue/);
    assert.deepEqual(summary!.untilLines, ["Duplicate identity keys resolved"]);
    assert.match(summary!.railLine, /duplicate identity keys/i);
    assert.equal(summary!.aiPromptHint, "Why are duplicate IDs blocking this transfer?");
    assert.match(summary!.readinessCaption, /10\/13 gates/);
  });
});
