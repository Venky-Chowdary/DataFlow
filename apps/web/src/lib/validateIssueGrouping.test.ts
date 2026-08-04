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
  findFidelityCollapseRoot,
  groupIsoNormalizeIssues,
  isDeclaredFidelityCollapse,
  isEncodingIntegritySignal,
  isIsoNormalizeCoercion,
  partitionCoercionColumns,
  partitionExplainIssues,
  remapToTypeForMismatch,
  rankAndDedupeSuggestedActions,
} from "./validateIssueGrouping.js";

describe("remapToTypeForMismatch", () => {
  it("keeps UUID/ObjectId/DECIMAL/temporal families — never invents bare VARCHAR for them", () => {
    assert.equal(remapToTypeForMismatch("UUID", "VARCHAR"), "UUID");
    assert.equal(remapToTypeForMismatch("UUID", "CHAR(36)"), "UUID");
    assert.equal(remapToTypeForMismatch("OBJECTID", "TEXT"), "OBJECTID");
    assert.equal(remapToTypeForMismatch("OBJECTID", "CHAR(24)"), "OBJECTID");
    assert.equal(remapToTypeForMismatch("MACADDR", "TEXT"), "MACADDR");
    assert.equal(remapToTypeForMismatch("FLOAT", "DECIMAL(38,10)"), "DOUBLE");
    assert.equal(remapToTypeForMismatch("DECIMAL(38,10)", "INTEGER"), "DECIMAL(38,10)");
    assert.equal(remapToTypeForMismatch("TIMESTAMPTZ", "TIMESTAMP_NTZ"), "TIMESTAMPTZ");
    assert.equal(remapToTypeForMismatch("VARCHAR", "NUMBER(38,0)"), "VARCHAR");
    // Create-new dialect twins — keep destination text/json, never invent VARCHAR.
    assert.equal(remapToTypeForMismatch("TEXT COLLATE UTF8MB4_0900_AI_CI", "TEXT"), "TEXT");
    assert.equal(remapToTypeForMismatch("JSON", "JSONB"), "JSONB");
  });
});

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

  it("append sync hint points at PK clear / unique column — not false green", () => {
    const pf = basePreflight({
      gates: [
        {
          id: "g9_data_integrity",
          status: "block",
          message: "id: duplicate key values from source probe (a×4)",
          duration_ms: 1,
          details: { primary_key: "id", issue_texts: ["id: duplicate key values from source probe (a×4)"] },
        },
      ],
      blockers: [
        {
          id: "g9_data_integrity",
          message: "id: duplicate key values from source probe (a×4)",
          details: { primary_key: "id" },
        },
      ],
    });
    const root = findDuplicateKeyRoot(pf, "full_refresh_append");
    assert.ok(root);
    assert.match(root!.fixHint, /Primary key/i);
    assert.match(root!.fixHint, /unique column|dedupe/i);
    assert.doesNotMatch(root!.fixHint, /Re-run Validate after the API picks up/i);
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

  it("keeps declared fidelity collapse out of convert-cleanly bucket", () => {
    const collapse = {
      source: "amt",
      target: "amt",
      source_type: "DECIMAL(20,6)",
      target_type: "FLOAT",
      sampled: 10,
      ok: 10,
      nulls: 0,
      sentinel_nulls: 0,
      failed: 0,
      sample_failures: [],
      severity: "ok" as const, // stale client payload — still must not look clean
      fidelity_collapse: true,
      framing: {
        kind: "fidelity_collapse",
        label: "Sample coerces — declared type path collapses fidelity",
        sample_round_trip: true,
      },
    };
    const { otherActionable, clean } = partitionCoercionColumns([collapse]);
    assert.equal(clean.length, 0);
    assert.equal(otherActionable.length, 1);
    assert.ok(isDeclaredFidelityCollapse(otherActionable[0]));
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

  it("does not claim Execute unlocked when transfer_decision is missing", () => {
    const summary = buildExecutiveSummary({
      passed: true,
      passed_count: 8,
      total_gates: 8,
      gates: [],
      blockers: [],
      run_id: "pf_api_1",
    } as any);
    assert.ok(summary);
    assert.ok(!/Execute unlocked/i.test(summary!.subtitle));
    assert.match(summary!.title, /Review/i);
  });

  it("does not claim Execute unlocked for review-grade passed preflight", () => {
    const pf = basePreflight({
      passed: true,
      passed_count: 13,
      proof_bundle: {
        transfer_decision: {
          decision: "review",
          blockers: [],
          reason: "browser-local preflight",
        },
      } as never,
    });
    const summary = buildExecutiveSummary(pf);
    assert.ok(summary);
    assert.equal(summary!.title, "Review before Execute");
    assert.match(summary!.subtitle, /review-grade/i);
    assert.ok(!/Execute unlocked/i.test(summary!.subtitle));
  });
});

describe("isEncodingIntegritySignal", () => {
  it("matches format-control / U+200B integrity failures", () => {
    assert.equal(
      isEncodingIntegritySignal("format-control character detected (U+200B)"),
      true,
    );
    assert.equal(isEncodingIntegritySignal("zero-width space in description"), true);
  });

  it("does not steal type-mismatch CTAs for encoding_id columns", () => {
    assert.equal(
      isEncodingIntegritySignal(
        "Dry-run failed: encoding_id (VARCHAR) → encoding_id (NUMBER(38,0))",
      ),
      false,
    );
  });
});

describe("findFidelityCollapseRoot", () => {
  it("collapses multi-gate fidelity blockers into one root", () => {
    const pf = basePreflight({
      gates: [
        {
          id: "g3_type_compat",
          name: "Type",
          status: "block",
          message: "DECIMAL → FLOAT fidelity collapse on amt",
          details: { fidelity_collapse: true },
        },
        {
          id: "g5_sample",
          name: "Sample",
          status: "block",
          message: "Sample shows precision loss on amt",
          details: { framing: { kind: "fidelity_collapse" } },
        },
      ],
      blockers: [
        {
          id: "g3_type_compat",
          message: "DECIMAL → FLOAT fidelity collapse on amt",
          details: { fidelity_collapse: true },
        },
        {
          id: "g5_sample",
          message: "Sample shows precision loss on amt",
          details: { framing: { kind: "fidelity_collapse" } },
        },
      ],
    });
    const root = findFidelityCollapseRoot(pf);
    assert.ok(root);
    assert.match(root!.title, /fidelity/i);
    const display = buildDisplayBlockers(pf);
    assert.equal(display.filter((d) => d.kind === "fidelity_root").length, 1);
    assert.ok(display.every((d) => d.kind !== "blocker" || !/fidelity|precision loss/i.test(d.message)));
    // Root items must not invent a `source` payload — Validate UI reads
    // source.details only for kind === "blocker".
    const fidelity = display.find((d) => d.kind === "fidelity_root");
    assert.equal(fidelity?.source, undefined);
  });

  it("prefers engine root_causes over client collapse", () => {
    const pf = basePreflight({
      root_causes: [
        {
          root_id: "rc-fidelity-collapse-abc",
          kind: "fidelity_collapse",
          title: "Lossy / fidelity collapse across type path",
          summary: "2 column(s) collapse fidelity — impacts 3 gate check(s)",
          business_impact: "Execute stays locked until Risk Contract or remap.",
          affected_columns: ["country_auto_detected", "referral_credit_processed"],
          affected_rows_sample: 25,
          estimated_total_rows: 100000,
          recommended_fix: "Open Map · Accept · cast & continue",
          alternative_fixes: ["Remap to TEXT"],
          recovery_strategy: "Re-Validate after contract",
          quarantine_policy: "holdout_rejected_rows",
          rollback_policy: "not_productized_see_MIGRATION_ROLLBACK",
          impacted_gates: ["g3_schema_contract", "g4_mapping_confidence", "g9_data_integrity"],
          absorbed_blocker_ids: ["g3_schema_contract", "g4_mapping_confidence", "g9_data_integrity"],
        },
      ],
      blockers: [
        {
          id: "rc-fidelity-collapse-abc",
          message: "Lossy / fidelity collapse",
          details: { root_cause: true },
        },
      ],
      gates: [
        { id: "g3_schema_contract", status: "block", message: "lossy", details: {} },
        { id: "g4_mapping_confidence", status: "block", message: "lossy", details: {} },
        { id: "g9_data_integrity", status: "block", message: "lossy", details: {} },
      ],
    });
    const display = buildDisplayBlockers(pf);
    assert.equal(display.filter((d) => d.kind === "fidelity_root").length, 1);
    assert.ok(display[0].issues?.some((i) => /country_auto_detected/i.test(i)));
    assert.ok(display[0].issues?.some((i) => /Sample rows: 25/i.test(i)));
  });
});

describe("rankAndDedupeSuggestedActions", () => {
  it("dedupes encoding and map families and caps density", () => {
    const out = rankAndDedupeSuggestedActions([
      { kind: "normalize_control_chars", label: "Strip controls" },
      { kind: "quarantine_and_rerun", label: "Quarantine" },
      { kind: "open_bad_data_fix", label: "Fix bad data" },
      { kind: "map_column", column: "id", label: "Map id" },
      { kind: "review_mappings", column: "id", label: "Review id" },
      { kind: "change_target_type", column: "amt", to_type: "DECIMAL", label: "Widen amt" },
      { kind: "change_target_type", column: "amt", to_type: "DECIMAL", label: "Widen amt" },
      { kind: "add_transform", column: "x", label: "t1" },
      { kind: "add_transform", column: "y", label: "t2" },
      { kind: "add_transform", column: "z", label: "t3" },
      { kind: "check_connection", label: "Reconnect" },
    ], 6);
    assert.ok(out.length <= 6);
    assert.equal(out.filter((a) => a.kind === "open_bad_data_fix" || a.kind === "normalize_control_chars" || a.kind === "quarantine_and_rerun").length, 1);
    assert.equal(out.filter((a) => a.kind === "map_column" || a.kind === "review_mappings").length, 1);
    assert.equal(out.filter((a) => a.kind === "change_target_type").length, 1);
  });
});
