/**
 * Calm Validate — one studio primary, never a second teal Map CTA.
 * Run: npx --yes tsx --test apps/web/src/lib/validateStudioPrimary.test.ts
 */
import assert from "node:assert/strict";
import { describe, it } from "node:test";
import type { PreflightResult } from "./types.ts";
import type { DisplayBlocker } from "./validateIssueGrouping.ts";
import {
  actionFamilyFromLabel,
  dashboardCtaVariant,
  promoteBlockedPrimaryFix,
  resolveValidateStudioPrimary,
  studioPrimaryIsSingular,
  transferDecisionOf,
} from "./validateStudioPrimary.ts";

function blockedPreflight(over: Partial<PreflightResult> = {}): PreflightResult {
  return {
    passed: false,
    passed_count: 10,
    total_gates: 13,
    readiness_score: 76,
    gates: [],
    blockers: [
      {
        id: "g6_target_ddl",
        message: "Target DDL cannot accept DECIMAL(12,1) → INTEGER",
      },
    ],
    ...over,
  };
}

function approvedPreflight(): PreflightResult {
  return {
    passed: true,
    passed_count: 13,
    total_gates: 13,
    readiness_score: 100,
    gates: [],
    blockers: [],
    proof_bundle: {
      transfer_decision: { decision: "approve" },
    },
  } as unknown as PreflightResult;
}

describe("validateStudioPrimary", () => {
  it("Run preflight is the only primary before a verdict", () => {
    const primary = resolveValidateStudioPrimary({ preflight: null });
    assert.equal(primary.kind, "run_preflight");
    assert.equal(primary.label, "Run preflight");
    assert.equal(primary.enabled, true);
    assert.equal(primary.executeLabel, null);
    assert.equal(studioPrimaryIsSingular(primary), true);
  });

  it("Open live progress replaces Execute after launch", () => {
    const primary = resolveValidateStudioPrimary({
      preflight: approvedPreflight(),
      transferLaunch: { jobId: "job-1", rows: 3 },
    });
    assert.equal(primary.kind, "open_live");
    assert.equal(primary.label, "Open live progress");
    assert.equal(primary.executeIsPrimary, false);
    assert.equal(studioPrimaryIsSingular(primary), true);
  });

  it("Case A Target DDL — rail owns Open Map; Execute is ghost blocked", () => {
    const primary = resolveValidateStudioPrimary({
      preflight: blockedPreflight(),
      hasPrimaryFix: true,
      primaryFixLabel: "Open Map to fix target DDL",
    });
    assert.equal(primary.kind, "primary_fix");
    assert.equal(primary.family, "map_open");
    assert.equal(primary.label, "Open Map to fix target DDL");
    assert.equal(primary.enabled, true);
    assert.equal(primary.executeLabel, "Execute (blocked)");
    assert.equal(primary.executeIsPrimary, false);
    assert.equal(primary.executeEnabled, false);
    assert.equal(studioPrimaryIsSingular(primary), true);
    assert.equal(dashboardCtaVariant("map_open"), "ghost");
    assert.equal(actionFamilyFromLabel("Open Map to fix"), "map_open");
    assert.equal(actionFamilyFromLabel("Open Map to fix breaking change"), "map_open");
  });

  it("Execute unlocks only on passed + approve, not review-grade", () => {
    const ready = resolveValidateStudioPrimary({ preflight: approvedPreflight() });
    assert.equal(ready.kind, "execute");
    assert.equal(ready.label, "Execute");
    assert.equal(ready.enabled, true);
    assert.equal(ready.executeIsPrimary, true);
    assert.equal(studioPrimaryIsSingular(ready), true);

    const review = resolveValidateStudioPrimary({
      preflight: {
        ...approvedPreflight(),
        proof_bundle: { transfer_decision: { decision: "review" } },
      } as unknown as PreflightResult,
    });
    assert.equal(review.label, "Execute (review)");
    assert.equal(review.enabled, false);
    assert.equal(review.executeIsPrimary, true);
    assert.equal(studioPrimaryIsSingular(review), true);
    assert.equal(transferDecisionOf(approvedPreflight()), "approve");
  });

  it("Hold-out is the primary when risk is pending and no other fix", () => {
    const primary = resolveValidateStudioPrimary({
      preflight: blockedPreflight(),
      riskAckPendingCount: 2,
      hasPrimaryFix: false,
      hasHoldOut: true,
    });
    assert.equal(primary.kind, "hold_out");
    assert.equal(primary.label, "Run with rows held out");
    assert.equal(primary.executeIsPrimary, false);
    assert.equal(studioPrimaryIsSingular(primary), true);
  });

  it("Approve mappings is the primary for mapping review with no other fix", () => {
    const primary = resolveValidateStudioPrimary({
      preflight: blockedPreflight({
        blockers: [{ id: "g4_mapping_confidence", message: "below floor" }],
      }),
      mappingReviewCount: 3,
      hasPrimaryFix: false,
    });
    assert.equal(primary.kind, "approve_mappings");
    assert.equal(primary.family, "approve_mappings");
    assert.equal(studioPrimaryIsSingular(primary), true);
  });

  it("Dashboard never paints a teal Map / Execute / Run-preflight copy", () => {
    assert.equal(dashboardCtaVariant("map_open"), "ghost");
    assert.equal(dashboardCtaVariant("run_preflight"), "ghost");
    assert.equal(dashboardCtaVariant("execute"), "ghost");
    assert.equal(dashboardCtaVariant("acknowledge_pii"), "secondary");
    assert.equal(dashboardCtaVariant("identity"), "secondary");
    assert.equal(dashboardCtaVariant("remap_column"), "secondary");
  });

  it("promotes hard-breaking drift to Open Map on the rail", () => {
    const blocker: DisplayBlocker = {
      key: "schema_drift",
      kind: "blocker",
      title: "Schema drift",
      message: "Hard-breaking narrow_type",
      source: {
        id: "schema_drift",
        message: "narrow_type",
        details: {
          schema_evolution: {
            compatibility: "none",
            hard_breaking: [{ column: "arr_time", kind: "narrow_type" }],
          },
        },
      },
    };
    const promoted = promoteBlockedPrimaryFix(blocker);
    assert.equal(promoted?.destination, "map");
    assert.equal(promoted?.label, "Open Map to fix breaking change");
  });

  it("promotes acknowledge-able soft drift to the rail, not a Map teal", () => {
    const blocker: DisplayBlocker = {
      key: "schema_drift",
      kind: "blocker",
      title: "Schema drift",
      message: "Additive column",
      source: {
        id: "schema_drift",
        message: "additive",
        details: {
          remediation_kind: "acknowledge_schema_drift",
          schema_evolution: {
            compatibility: "backward",
            hard_breaking: [],
            should_pause: false,
          },
        },
      },
    };
    const promoted = promoteBlockedPrimaryFix(blocker);
    assert.equal(promoted?.destination, "ack_drift");
    assert.equal(promoted?.label, "Acknowledge drift for this run");
  });

  it("promotes PII and FK acknowledgments to the rail", () => {
    const pii = promoteBlockedPrimaryFix({
      key: "compliance",
      kind: "blocker",
      title: "PII",
      message: "ssn detected",
      source: {
        id: "g12_compliance",
        message: "PII",
        details: { compliance_ack_required: true },
      },
    });
    assert.equal(pii?.destination, "ack_pii");
    assert.equal(pii?.label, "Approve PII for this transfer");

    const fk = promoteBlockedPrimaryFix({
      key: "fk",
      kind: "blocker",
      title: "FK",
      message: "foreign key unmapped",
      source: {
        id: "constraint_fk",
        message: "fk_column_unmapped",
        details: { remediation_kind: "acknowledge_fk_risk" },
      },
    });
    assert.equal(fk?.destination, "ack_fk");
  });
});
