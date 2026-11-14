/**
 * UX-4 / UX-26 / UX-32: a plain PG → MySQL copy on the Map step.
 *
 * The types are the ones the audit's live MySQL destination reported
 * (`ui/map_export.json`): MySQL spells column attributes onto the type, so a
 * CHAR(2) column reads back as `CHAR(2) COLLATE utf8mb4_bin` and the key as
 * `BIGINT AUTO_INCREMENT`. Neither attribute changes how a value is stored.
 */
import assert from "node:assert/strict";
import { describe, it } from "node:test";
import { declaredCarrierFidelityRisk } from "./typeCarrierFidelity";
import {
  EXECUTION_POLICY_OPTIONS,
  mappingAckLabel,
  mappingRequiresRiskAck,
  recommendedExecutionPolicy,
  type EditableMapping,
} from "./mapping";
import { mapBlockerSummary, mappingBlocker } from "./mapBlockers";
import { remapToDestination, type DestColumnCatalog } from "./destColumnCatalog";

const THRESHOLD = 0.85;

function row(partial: Partial<EditableMapping> & Pick<EditableMapping, "source">): EditableMapping {
  return {
    target: partial.source,
    confidence: 0.97,
    approved: false,
    existsInDestination: true,
    ...partial,
  };
}

const CATALOG: DestColumnCatalog = {
  version: "dest_column_catalog.v1",
  connected: true,
  tableExists: true,
  destType: "mysql",
  nullabilityMeasured: true,
  canAddColumns: true,
  addColumnsReason: "",
  message: "",
  origin: "api",
  columns: [
    {
      name: "id", ordinal: 0, type: "BIGINT AUTO_INCREMENT", nullable: false, default: null, hasDefault: false,
      primaryKey: true, identity: true, generated: false, required: false, writable: true, fill: "identity", matchKey: "id",
    },
    {
      name: "country_code", ordinal: 1, type: "CHAR(2) COLLATE utf8mb4_bin", nullable: true, default: null,
      hasDefault: false, primaryKey: false, identity: false, generated: false, required: false, writable: true,
      fill: null, matchKey: "country_code",
    },
  ],
};

describe("UX-4: a column the engine calls lossless never holds behind a Risk Contract", () => {
  it("CHAR(2) → CHAR(2) COLLATE utf8mb4_bin keeps its blank padding: no fidelity risk", () => {
    assert.equal(declaredCarrierFidelityRisk("CHAR(2)", "CHAR(2) COLLATE utf8mb4_bin"), false);
    assert.equal(declaredCarrierFidelityRisk("CHAR(2)", "CHAR(2) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin"), false);
    // A real crossing still holds: CHAR(2) → VARCHAR(2) changes the padding.
    assert.equal(declaredCarrierFidelityRisk("CHAR(2)", "VARCHAR(2) COLLATE utf8mb4_bin"), true);
  });

  it("the country_code row asks for Approve, not a signed contract", () => {
    const m = row({ source: "country_code", inferredType: "CHAR(2)", destType: "CHAR(2) COLLATE utf8mb4_bin", fidelity: "preserve" });
    assert.equal(mappingRequiresRiskAck(m), false);
    assert.equal(mappingAckLabel(m), "Approve");
    // Not a hold: a preserve row only waits for the bulk Approve.
    assert.equal(mappingBlocker(m, THRESHOLD), null);
  });

  it("the instruction names the button the row really shows, and the policy in plain words", () => {
    const m = row({ source: "signup_ts", inferredType: "TIMESTAMPTZ", destType: "DATETIME(6)", fidelity: "lossy_cast" });
    const b = mappingBlocker(m, THRESHOLD);
    assert.equal(b?.code, "risk_ack_required");
    assert.ok(b!.action.includes(`“${mappingAckLabel(m)}”`), b!.action);
    assert.match(b!.action, /execution policy/i);
    assert.doesNotMatch(b!.action, /FAIL_JOB|STOP_TABLE|QUARANTINE_ROW|CAST_AND_CONTINUE/);
    const summary = mapBlockerSummary([m, row({ source: "x", inferredType: "TIMESTAMPTZ", destType: "DATETIME(6)", fidelity: "lossy_cast" })], THRESHOLD);
    for (const g of summary.groups) assert.doesNotMatch(`${g.title} ${g.action}`, /FAIL_JOB|STOP_TABLE|_ROW\b|_CONTINUE/);
  });

  it("offers plain-language policy labels with no raw codes", () => {
    for (const p of EXECUTION_POLICY_OPTIONS) {
      assert.doesNotMatch(p.label, /[A-Z]{3,}_[A-Z]+/, `${p.id} label shows a raw code: ${p.label}`);
    }
  });

  it("preselects a recommended policy that lets the run continue with the loss recorded", () => {
    const lossy = row({ source: "signup_ts", inferredType: "TIMESTAMPTZ", destType: "DATETIME(6)", fidelity: "lossy_cast" });
    const policy = recommendedExecutionPolicy(lossy);
    const option = EXECUTION_POLICY_OPTIONS.find((p) => p.id === policy);
    assert.ok(option, `recommended policy ${policy} is offered`);
    assert.equal(option!.continueUnlock, true);
    assert.equal(option!.recommended, true);
    assert.equal(recommendedExecutionPolicy(row({ source: "a", fidelity: "cast", inferredType: "TEXT", destType: "INT" })), "CAST_AND_CONTINUE");
  });

  it("fail-closed release names continue policies in words", () => {
    const m = row({
      source: "signup_ts", inferredType: "TIMESTAMPTZ", destType: "DATETIME(6)", fidelity: "lossy_cast",
      riskAcknowledged: true,
      riskContract: { column: "signup_ts", source_type: "TIMESTAMPTZ", destination_type: "DATETIME(6)", execution_policy: "FAIL_JOB" },
    });
    const b = mappingBlocker(m, THRESHOLD);
    assert.equal(b?.code, "fail_closed_contract");
    assert.match(b!.action, /continue policy/i);
    assert.doesNotMatch(`${b!.title} ${b!.action}`, /FAIL_JOB|QUARANTINE_ROW|CAST_AND_CONTINUE|STOP_COLUMN/);
  });
});

describe("UX-32: the cast policy says what CAST_AND_CONTINUE does", () => {
  it("is labelled as cast-and-continue, not CAST_FAIL_QUARANTINE", () => {
    const cast = EXECUTION_POLICY_OPTIONS.find((p) => p.id === "CAST_AND_CONTINUE");
    assert.ok(cast);
    assert.doesNotMatch(cast!.label, /CAST_FAIL_QUARANTINE/);
    assert.match(cast!.label, /^Cast and continue/);
  });
});

describe("UX-26: Auto-map does not raise holds on rows it leaves where they are", () => {
  it("re-pointing a row at the column it already writes keeps its approval", () => {
    const approved = row({ source: "id", inferredType: "INT4", destType: "BIGINT AUTO_INCREMENT", approved: true, fidelity: "preserve" });
    const next = remapToDestination(approved, "id", CATALOG);
    assert.equal(next.approved, true);
    assert.equal(next.fidelity, "preserve");
    assert.equal(mappingBlocker(next, THRESHOLD), null);
  });

  it("INT4 → BIGINT AUTO_INCREMENT is a widening, not a lossy cast", () => {
    assert.equal(declaredCarrierFidelityRisk("INT4", "BIGINT AUTO_INCREMENT"), false);
    const fresh = remapToDestination(row({ source: "id", target: "", inferredType: "INT4" }), "id", CATALOG);
    assert.equal(fresh.fidelity, undefined);
    assert.equal(mappingRequiresRiskAck(fresh), false);
  });
});
