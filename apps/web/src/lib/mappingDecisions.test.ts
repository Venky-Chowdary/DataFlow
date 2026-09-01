/**
 * Run: npx --yes tsx --test apps/web/src/lib/mappingDecisions.test.ts
 */
import assert from "node:assert/strict";
import { describe, it } from "node:test";
import {
  acknowledgeMappingRisk,
  mappingHasClearingRiskContract,
  mappingRequiresRiskAck,
  type EditableMapping,
} from "./mapping";
import {
  carryOperatorDecisions,
  holdOutRowsAndContinue,
  holdoutPolicyFor,
  mappingDecisionFingerprint,
} from "./mappingDecisions";

function lossy(over: Partial<EditableMapping> = {}): EditableMapping {
  return {
    source: "amount",
    target: "amount",
    confidence: 0.93,
    approved: false,
    inferredType: "DECIMAL(18,6)",
    destType: "NUMERIC(8,2)",
    fidelity: "lossy_cast",
    typeNarrowing: true,
    ...over,
  } as EditableMapping;
}

function clean(over: Partial<EditableMapping> = {}): EditableMapping {
  return {
    source: "name",
    target: "name",
    confidence: 0.99,
    approved: false,
    inferredType: "VARCHAR(64)",
    destType: "VARCHAR(64)",
    fidelity: "exact",
    ...over,
  } as EditableMapping;
}

describe("carryOperatorDecisions", () => {
  it("replays approvals and signed contracts onto regenerated mappings", () => {
    const signed = acknowledgeMappingRisk(lossy(), { executionPolicy: "QUARANTINE_ROW" });
    assert.equal(mappingHasClearingRiskContract(signed), true);
    // Map regenerates from scratch: same facts, no decisions.
    const regenerated = [lossy(), clean()];
    const carried = carryOperatorDecisions(regenerated, [signed, clean()]);
    assert.equal(carried[0].riskAcknowledged, true);
    assert.equal(carried[0].approved, true);
    assert.equal(carried[0].riskContract?.execution_policy, "QUARANTINE_ROW");
    assert.equal(mappingHasClearingRiskContract(carried[0]), true);
  });

  it("does not carry a decision when the type path changed", () => {
    const signed = acknowledgeMappingRisk(lossy(), { executionPolicy: "QUARANTINE_ROW" });
    const regenerated = [lossy({ destType: "VARCHAR(32)" })];
    const carried = carryOperatorDecisions(regenerated, [signed]);
    assert.equal(carried[0].riskAcknowledged, undefined);
    assert.equal(carried[0].approved, false);
  });

  it("does not carry across a fidelity, transform or create-new change", () => {
    const signed = acknowledgeMappingRisk(lossy(), { executionPolicy: "QUARANTINE_ROW" });
    for (const drift of [
      lossy({ fidelity: "mutate" }),
      lossy({ transform: "cast_number" }),
      lossy({ createNew: true }),
    ]) {
      const carried = carryOperatorDecisions([drift], [signed]);
      assert.equal(carried[0].approved, false, `carried a stale ack across ${JSON.stringify(drift)}`);
    }
  });

  it("is a no-op when nothing was decided", () => {
    const regenerated = [lossy(), clean()];
    assert.deepEqual(carryOperatorDecisions(regenerated, [lossy(), clean()]), regenerated);
    assert.deepEqual(carryOperatorDecisions(regenerated, []), regenerated);
  });

  it("keeps a declared reduction and its G16 evidence across regeneration", () => {
    // Regeneration proposes a target for the column again, which moves the
    // decision fingerprint — the omission must survive that anyway.
    const dropped: EditableMapping = {
      ...clean({ source: "old_audit_trail", target: "", transform: "omit", approved: true }),
      omitReason: "archive_only",
      omitReasonText: "Retired 2019 audit trail",
      archiveReference: "mainframe-vault://audit/2019",
      retentionUntil: "2031-12-31",
      omitApprovedBy: "Priya Raman",
    };

    const carried = carryOperatorDecisions(
      [clean({ source: "old_audit_trail", target: "old_audit_trail" }), lossy()],
      [dropped, lossy()],
    );

    assert.equal(carried[0].transform, "omit");
    assert.equal(carried[0].target, "");
    assert.equal(carried[0].omitReason, "archive_only");
    assert.equal(carried[0].omitReasonText, "Retired 2019 audit trail");
    assert.equal(carried[0].archiveReference, "mainframe-vault://audit/2019");
    assert.equal(carried[0].retentionUntil, "2031-12-31");
    assert.equal(carried[0].omitApprovedBy, "Priya Raman");
    // Other rows are untouched by the reduction replay.
    assert.equal(carried[1].transform, undefined);
  });

  it("keeps a declared code crosswalk across regeneration", () => {
    const prior: EditableMapping = {
      ...clean({ source: "status", target: "status", approved: true }),
      codeCrosswalk: { A: "active", B: "blocked" },
      codeCrosswalkSystem: "legacy_status→v2",
    };
    const carried = carryOperatorDecisions(
      [clean({ source: "status", target: "status_v2" })],
      [prior],
    );
    assert.deepEqual(carried[0].codeCrosswalk, { A: "active", B: "blocked" });
    assert.equal(carried[0].codeCrosswalkSystem, "legacy_status→v2");
  });

  it("does not omit a column the operator never dropped", () => {
    const carried = carryOperatorDecisions(
      [clean({ source: "email", target: "email" })],
      [clean({ source: "old_audit_trail", target: "", transform: "omit", approved: true })],
    );

    assert.equal(carried[0].transform, undefined);
    assert.equal(carried[0].target, "email");
  });

  it("fingerprints differ per column so decisions never cross rows", () => {
    assert.notEqual(
      mappingDecisionFingerprint(lossy()),
      mappingDecisionFingerprint(lossy({ source: "total", target: "total" })),
    );
  });
});

describe("holdOutRowsAndContinue", () => {
  it("signs a continue-policy contract for every blocking row", () => {
    const { mappings, signed } = holdOutRowsAndContinue([lossy(), clean()]);
    assert.deepEqual(signed, ["amount → amount"]);
    assert.equal(mappingHasClearingRiskContract(mappings[0]), true);
    assert.equal(mappings[0].riskContract?.quarantine_policy, "QUARANTINE_ROW_on_failure");
    // Clean rows are untouched — no invented risk.
    assert.equal(mappings[1].riskAcknowledged, undefined);
  });

  it("leaves an already-signed continue contract alone", () => {
    const signedRow = acknowledgeMappingRisk(lossy(), { executionPolicy: "SKIP_ROW" });
    const { mappings, signed } = holdOutRowsAndContinue([signedRow]);
    assert.deepEqual(signed, []);
    assert.equal(mappings[0].riskContract?.execution_policy, "SKIP_ROW");
  });

  it("re-signs a fail-closed contract that would still block Execute", () => {
    const failClosed = acknowledgeMappingRisk(lossy(), { executionPolicy: "FAIL_JOB" });
    assert.equal(mappingHasClearingRiskContract(failClosed), false);
    const { mappings, signed } = holdOutRowsAndContinue([failClosed]);
    assert.equal(signed.length, 1);
    assert.equal(mappingHasClearingRiskContract(mappings[0]), true);
  });

  it("holds out the row for value-dependent risk, quarantines on cast failure otherwise", () => {
    assert.equal(holdoutPolicyFor(lossy()), "QUARANTINE_ROW");
    const domainRisk = clean({
      inferredType: "OBJECTID",
      destType: "TEXT",
      fidelity: "mutate",
      transform: "identity_specialty",
    });
    assert.equal(mappingRequiresRiskAck(domainRisk), true);
    // Values survive ObjectId→TEXT, so quarantining every row would write nothing.
    assert.equal(holdoutPolicyFor(domainRisk), "CAST_AND_CONTINUE");
    const { mappings } = holdOutRowsAndContinue([domainRisk]);
    assert.equal(mappings[0].riskContract?.quarantine_policy, "QUARANTINE_ROW_on_failure");
    assert.equal(mappingHasClearingRiskContract(mappings[0]), true);
  });

  it("survives a Map regeneration round-trip", () => {
    const { mappings } = holdOutRowsAndContinue([lossy(), clean()]);
    const carried = carryOperatorDecisions([lossy(), clean()], mappings);
    assert.equal(mappingHasClearingRiskContract(carried[0]), true);
    assert.equal(
      carried.filter((m) => mappingRequiresRiskAck(m) && !m.riskAcknowledged).length,
      0,
      "regeneration must not reopen the quarantine loop",
    );
  });

  it("carries a control-total declaration across regeneration", () => {
    const prior: EditableMapping[] = [
      { ...clean(), source: "amount", target: "amount", controlTotal: true },
    ];
    const regenerated: EditableMapping[] = [
      { ...clean(), source: "amount", target: "amount" },
    ];
    const carried = carryOperatorDecisions(regenerated, prior);
    assert.equal(carried[0].controlTotal, true);
  });
});
