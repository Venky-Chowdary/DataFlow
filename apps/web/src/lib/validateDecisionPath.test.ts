/**
 * Module 10 — Validate decision path must follow charter order.
 * Run: npx --yes tsx --test apps/web/src/lib/validateDecisionPath.test.ts
 */
import assert from "node:assert/strict";
import { describe, it } from "node:test";
import {
  buildValidateDecisionPath,
  decisionPathStepLabels,
} from "./validateDecisionPath.ts";
import type { PreflightResult } from "./types.ts";

describe("validateDecisionPath", () => {
  it("exposes charter-ordered step labels", () => {
    assert.deepEqual(decisionPathStepLabels(), [
      "Root Cause",
      "Affected Gates",
      "Business Impact",
      "Recommended Actions",
      "Preview Changes",
      "Risk Contract",
      "Execute",
    ]);
  });

  it("builds path from engine root_causes without duplicating absorbed gates", () => {
    const preflight = {
      passed: false,
      passed_count: 6,
      total_gates: 9,
      readiness_score: 66,
      gates: [],
      blockers: [
        { id: "g3_schema_contract", message: "lossy TEXT→INTEGER" },
        { id: "g4_mapping_confidence", message: "below floor" },
        { id: "g9_data_integrity", message: "coercion" },
      ],
      root_causes: [
        {
          root_id: "rc1",
          kind: "fidelity_collapse",
          title: "TEXT → INTEGER",
          summary: "Declared lossy cast",
          business_impact: "Execute locked until Risk Contract or remap.",
          affected_columns: ["code"],
          recommended_fix: "Open Map · Accept · cast & continue",
          alternative_fixes: ["Remap to VARCHAR", "Quarantine row"],
          recovery_strategy: "Replay quarantine after fix",
          quarantine_policy: "holdout_rejected_rows",
          rollback_policy: "DOCUMENT_ONLY",
          impacted_gates: ["g3_schema_contract", "g4_mapping_confidence", "g9_data_integrity"],
          absorbed_blocker_ids: ["g3_schema_contract", "g4_mapping_confidence", "g9_data_integrity"],
        },
      ],
      proof_bundle: {
        risk_contracts: {
          incomplete: true,
          missing_columns: ["code"],
        },
        migration_proven: false,
      },
    } as unknown as PreflightResult;

    const path = buildValidateDecisionPath(preflight, { executeUnlocked: false });
    assert.equal(path.decisions.length, 1);
    assert.equal(path.steps.length, 7);
    assert.deepEqual(
      path.steps.map((s) => s.id),
      [
        "root_cause",
        "affected_gates",
        "business_impact",
        "recommended_actions",
        "preview_changes",
        "risk_contract",
        "execute",
      ],
    );
    assert.equal(path.steps[0].summary, "TEXT → INTEGER");
    assert.match(path.steps[1].summary, /Schema|Mapping|Integrity|g3|g4|g9/i);
    assert.match(path.steps[2].summary, /Execute locked/i);
    assert.match(path.steps[3].summary, /Accept|cast|Map/i);
    assert.equal(path.steps[5].status, "action");
    assert.equal(path.steps[6].status, "locked");
    assert.equal(path.migrationProven, false);
    assert.equal(path.riskContractIncomplete, true);
    assert.match(path.note, /population/i);
  });

  it("marks execute unlocked path as not migration proven", () => {
    const preflight = {
      passed: true,
      passed_count: 9,
      total_gates: 9,
      readiness_score: 100,
      gates: [],
      blockers: [],
      proof_bundle: { migration_proven: false, risk_contracts: { incomplete: false } },
    } as unknown as PreflightResult;

    const path = buildValidateDecisionPath(preflight, { executeUnlocked: true });
    assert.equal(path.decisions.length, 0);
    assert.equal(path.executeUnlocked, true);
    assert.equal(path.migrationProven, false);
    assert.equal(path.steps[6].status, "unlocked");
    assert.match(path.steps[6].summary, /migration_proven/i);
  });
});
