/**
 * Map gating contracts for a lossy type change on an existing destination column.
 * Run: npx --yes tsx --test apps/web/src/lib/mapBlockers.test.ts
 */
import assert from "node:assert/strict";
import { describe, it } from "node:test";
import {
  acknowledgeMappingRisk,
  applyDestTypeChange,
  clearExistingDestTypeOverride,
  isExistingDestTypeOverride,
  mappingRequiresRiskAck,
  mappingRiskChipState,
  type EditableMapping,
} from "./mapping";
import { mapBlockerSummary, mappingBlocker } from "./mapBlockers";
import { isMappingReady, needsMappingReview } from "./columnWorkbench";
import { isApiPreflight, isLocalPreflight } from "./localPreflight";

const THRESHOLD = 0.85;

/** Existing physical INTEGER column receiving a DECIMAL(10,4) source. */
function lossyExistingDest(): EditableMapping {
  return {
    source: "order_amt",
    target: "order_amount",
    confidence: 0.93,
    inferredType: "DECIMAL(10,4)",
    destType: "INTEGER",
    sample: "12.3456",
    approved: false,
    existsInDestination: true,
    transform: "cast_integer",
    fidelity: "lossy_cast",
    fidelityReason: "DECIMAL(10,4) → INTEGER drops scale 4",
    typeNarrowing: true,
    requiresReview: true,
    reason: "Type path narrows: DECIMAL(10,4) → INTEGER",
  };
}

describe("lossy existing-destination mapping is not a dead end", () => {
  it("requires a risk contract and names the loss in the blocker", () => {
    const m = lossyExistingDest();
    assert.equal(mappingRequiresRiskAck(m), true);
    const blocker = mappingBlocker(m, THRESHOLD);
    assert.ok(blocker);
    assert.equal(blocker.code, "risk_ack_required");
    assert.match(blocker.title, /order_amt/);
    assert.match(blocker.title, /DECIMAL\(10,4\)/);
    assert.equal(blocker.clearableFromMap, true);
  });

  it("unlocks Validate when signed with a continuing policy", () => {
    const signed = acknowledgeMappingRisk(lossyExistingDest(), {
      executionPolicy: "QUARANTINE_ROW",
    });
    assert.equal(signed.riskAcknowledged, true);
    assert.equal(signed.approved, true);
    assert.equal(signed.requiresReview, false);
    assert.equal(isMappingReady(signed, THRESHOLD), true);
    assert.equal(needsMappingReview(signed, THRESHOLD), false);
    assert.equal(mappingRiskChipState(signed), "accepted");
    assert.equal(mappingBlocker(signed, THRESHOLD), null);
  });

  it("keeps Validate shut for a fail-closed policy and says why", () => {
    const signed = acknowledgeMappingRisk(lossyExistingDest(), {
      executionPolicy: "FAIL_JOB",
    });
    assert.equal(signed.riskAcknowledged, true);
    assert.equal(isMappingReady(signed, THRESHOLD), false);
    assert.equal(mappingRiskChipState(signed), "fail_closed");
    const blocker = mappingBlocker(signed, THRESHOLD);
    assert.ok(blocker);
    assert.equal(blocker.code, "fail_closed_contract");
    assert.match(blocker.title, /FAIL_JOB/);
  });

  it("re-signing a fail-closed row with a continuing policy clears it", () => {
    const failClosed = acknowledgeMappingRisk(lossyExistingDest(), {
      executionPolicy: "STOP_TABLE",
    });
    const resigned = acknowledgeMappingRisk(failClosed, {
      executionPolicy: "CAST_AND_CONTINUE",
    });
    assert.equal(isMappingReady(resigned, THRESHOLD), true);
    assert.equal(mappingBlocker(resigned, THRESHOLD), null);
  });
});

describe("an ALTER request on an existing physical column stays truthful", () => {
  it("does not change the physical type and cannot be signed away", () => {
    const requested = applyDestTypeChange(lossyExistingDest(), "DECIMAL(10,4)");
    assert.equal(requested.destType, "INTEGER");
    assert.equal(isExistingDestTypeOverride(requested), true);

    const signed = acknowledgeMappingRisk(requested, { executionPolicy: "QUARANTINE_ROW" });
    assert.equal(signed.riskAcknowledged, false);
    assert.equal(signed.approved, false);
    assert.match(signed.reason || "", /Risk Contract cannot cover an ALTER/);

    const blocker = mappingBlocker(requested, THRESHOLD);
    assert.ok(blocker);
    assert.equal(blocker.code, "existing_dest_type_override");
    assert.equal(blocker.clearableFromMap, false);
    assert.match(blocker.action, /ALTER|remap/i);
  });

  it("restoring the live physical type withdraws the ALTER request", () => {
    const requested = applyDestTypeChange(lossyExistingDest(), "DECIMAL(10,4)");
    const withdrawn = clearExistingDestTypeOverride(requested);
    assert.equal(isExistingDestTypeOverride(withdrawn), false);
    assert.doesNotMatch(withdrawn.reason || "", /cannot be changed from Map/);
    assert.doesNotMatch(withdrawn.reason || "", /requires ALTER or remap/);

    const signed = acknowledgeMappingRisk(withdrawn, { executionPolicy: "QUARANTINE_ROW" });
    assert.equal(signed.riskAcknowledged, true);
    assert.equal(isMappingReady(signed, THRESHOLD), true);
  });

  it("selecting the live type again is the same withdrawal", () => {
    const requested = applyDestTypeChange(lossyExistingDest(), "DECIMAL(10,4)");
    const withdrawn = applyDestTypeChange(requested, "INTEGER");
    assert.equal(isExistingDestTypeOverride(withdrawn), false);
    assert.equal(withdrawn.destType, "INTEGER");
  });
});

describe("mapBlockerSummary explains exactly what holds Continue", () => {
  it("is empty when nothing blocks", () => {
    const signed = acknowledgeMappingRisk(lossyExistingDest(), {
      executionPolicy: "QUARANTINE_ROW",
    });
    const summary = mapBlockerSummary([signed], THRESHOLD);
    assert.equal(summary.blockers.length, 0);
    assert.equal(summary.clearableFromMap, 0);
  });

  it("counts rows that cannot be cleared from Map separately", () => {
    const requested = applyDestTypeChange(lossyExistingDest(), "DECIMAL(10,4)");
    const missingTarget: EditableMapping = {
      ...lossyExistingDest(),
      source: "ship_city",
      target: "",
      fidelity: undefined,
      fidelityReason: undefined,
      typeNarrowing: undefined,
      reason: "",
    };
    const summary = mapBlockerSummary([requested, missingTarget], THRESHOLD);
    assert.equal(summary.blockers.length, 2);
    assert.equal(summary.clearableFromMap, 1);
    assert.match(summary.headline, /2/);
    assert.match(summary.detail, /order_amt/);
    assert.match(summary.detail, /ship_city/);
  });
});

describe("preflight identity", () => {
  it("treats a missing run id as neither local nor API", () => {
    assert.equal(isApiPreflight({}), false);
    assert.equal(isApiPreflight(null), false);
    assert.equal(isLocalPreflight({}), false);
  });

  it("separates browser preflight from API preflight", () => {
    assert.equal(isApiPreflight({ run_id: "pf_local_abc" }), false);
    assert.equal(isLocalPreflight({ run_id: "pf_local_abc" }), true);
    assert.equal(isApiPreflight({ run_id: "pf_9f2c1ab44de1" }), true);
    assert.equal(isLocalPreflight({ run_id: "pf_9f2c1ab44de1" }), false);
  });
});
