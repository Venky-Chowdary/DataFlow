import assert from "node:assert/strict";
import { describe, it } from "node:test";
import {
  carrierLogicalDomain,
  crossDomainCoercionRisk,
  declaredCarrierFidelityRisk,
} from "./typeCarrierFidelity";
import { acknowledgeMappingRisk, mappingRequiresRiskAck } from "./mapping";
import { mappingBlocker } from "./mapBlockers";
import type { EditableMapping } from "./mapping";

const THRESHOLD = 0.9;

function mapping(
  partial: Partial<EditableMapping> & Pick<EditableMapping, "source" | "target">,
): EditableMapping {
  return {
    confidence: 0.95,
    approved: true,
    inferredType: "VARCHAR",
    destType: "VARCHAR",
    ...partial,
  };
}

/**
 * Verdicts read from ``services.type_system.is_lossy_coercion`` via
 * ``scripts/carrier_parity_audit.py``. Map may be stricter than the engine, but
 * never looser: a looser verdict is Approve here and refusal at Validate.
 */
const ENGINE_LOSSY: [string, string][] = [
  ["TEXT", "DECIMAL(38,15)"],
  ["VARCHAR(255)", "DECIMAL(10,2)"],
  ["DECIMAL(12,2)", "DATE"],
  ["TEXT", "DATE"],
  ["TEXT", "INTEGER"],
  ["TEXT", "BOOLEAN"],
  ["TEXT", "TIMESTAMP"],
  ["TIMESTAMP", "DATE"],
  ["INTEGER", "BOOLEAN"],
  ["JSON", "TEXT"],
  ["DATE", "INTEGER"],
  ["DECIMAL(10,2)", "INTEGER"],
  // Engine-lossy crossings Map used to offer Approve on (the D40 dead ends).
  ["TEXT", "JSON"],
  ["TEXT", "VARCHAR(255)"],
  ["CHAR(10)", "TEXT"],
  ["CHAR(10)", "VARCHAR(255)"],
  ["INTEGER", "VARCHAR(255)"],
  ["INTEGER", "JSONB"],
  ["BIGINT", "CHAR(10)"],
  ["DECIMAL(10,2)", "TEXT"],
  ["DECIMAL(38,15)", "VARCHAR(255)"],
  ["DOUBLE PRECISION", "CHAR(10)"],
  ["BOOLEAN", "JSONB"],
  ["DATE", "VARCHAR(255)"],
  ["TIMESTAMPTZ", "TEXT"],
  ["TIME", "VARCHAR(255)"],
  ["BYTEA", "TEXT"],
  ["BYTEA", "JSONB"],
  // Carriers Map did not classify at all, so every crossing read as safe.
  ["VARCHAR2(30)", "DATE"],
  ["VARCHAR2(30)", "INTEGER"],
  ["NCHAR(10)", "DECIMAL(10,2)"],
  ["NVARCHAR(100)", "LONGTEXT"],
];

const ENGINE_PRESERVING: [string, string][] = [
  ["INTEGER", "TEXT"],
  ["DATE", "TEXT"],
  ["DATE", "TIMESTAMP"],
  ["INTEGER", "BIGINT"],
  ["VARCHAR(50)", "VARCHAR(255)"],
  ["BOOLEAN", "INTEGER"],
  ["BOOLEAN", "TEXT"],
  ["INTEGER", "DECIMAL(10,2)"],
  ["DECIMAL(10,0)", "TEXT"],
  ["TIMESTAMP", "TEXT"],
  ["DATE", "JSON"],
  ["TIMESTAMP", "JSONB"],
  ["NVARCHAR(100)", "NVARCHAR(200)"],
];

describe("cross-domain coercion is a Risk Contract, not an Approve (D40)", () => {
  it("places the carriers the domain rule depends on", () => {
    assert.equal(carrierLogicalDomain("VARCHAR2(30)"), "string");
    assert.equal(carrierLogicalDomain("NCHAR(10)"), "string");
    assert.equal(carrierLogicalDomain("LONGTEXT"), "text");
    assert.equal(carrierLogicalDomain("TEXT"), "text");
    assert.equal(carrierLogicalDomain("VARCHAR(50)"), "string");
    assert.equal(carrierLogicalDomain("DECIMAL(38,15)"), "decimal");
    assert.equal(carrierLogicalDomain("DATE"), "date");
    assert.equal(carrierLogicalDomain("TIMESTAMP(6)"), "datetime");
    assert.equal(carrierLogicalDomain("BOOLEAN"), "boolean");
    assert.equal(carrierLogicalDomain("JSONB"), "json");
    assert.equal(carrierLogicalDomain("BIGINT"), "integer");
    assert.equal(carrierLogicalDomain("DOUBLE PRECISION"), "float");
    assert.equal(carrierLogicalDomain("BYTEA"), "binary");
    // Domains the specific rules above own — the crossing rule must not guess.
    assert.equal(carrierLogicalDomain("UUID"), null);
    assert.equal(carrierLogicalDomain("INTERVAL YEAR TO MONTH"), null);
    assert.equal(carrierLogicalDomain("GEOGRAPHY"), null);
    assert.equal(carrierLogicalDomain(""), null);
  });

  it("owns the domain crossings the specific width/scale rules do not see", () => {
    assert.equal(crossDomainCoercionRisk("TEXT", "DECIMAL(38,15)"), true);
    assert.equal(crossDomainCoercionRisk("DECIMAL(12,2)", "DATE"), true);
    assert.equal(crossDomainCoercionRisk("BYTEA", "TEXT"), true);
    assert.equal(crossDomainCoercionRisk("INTEGER", "JSONB"), true);
    assert.equal(crossDomainCoercionRisk("CHAR(10)", "TEXT"), true);
    // Same domain, and crossings the engine preserves, stay approvable.
    assert.equal(crossDomainCoercionRisk("VARCHAR(50)", "VARCHAR(255)"), false);
    assert.equal(crossDomainCoercionRisk("INTEGER", "TEXT"), false);
    assert.equal(crossDomainCoercionRisk("DATE", "TIMESTAMP"), false);
    // An unplaceable specialty carrier is left to the rules that know it.
    assert.equal(crossDomainCoercionRisk("UUID", "TEXT"), false);
  });

  for (const [src, tgt] of ENGINE_LOSSY) {
    it(`asks for a contract on ${src} → ${tgt}`, () => {
      // Which rule fires is an implementation detail; the Map verdict is not.
      assert.equal(declaredCarrierFidelityRisk(src, tgt), true);
      assert.equal(
        mappingRequiresRiskAck(
          mapping({ source: "c", target: "c", inferredType: src, destType: tgt }),
        ),
        true,
      );
    });
  }

  for (const [src, tgt] of ENGINE_PRESERVING) {
    it(`leaves ${src} → ${tgt} approvable`, () => {
      assert.equal(crossDomainCoercionRisk(src, tgt), false);
      assert.equal(declaredCarrierFidelityRisk(src, tgt), false);
    });
  }

  it("names the row-level release path on the blocked pairs Validate reported", () => {
    for (const [src, tgt] of [
      ["TEXT", "DECIMAL(38,15)"],
      ["DECIMAL(12,2)", "DATE"],
    ] as [string, string][]) {
      const blocker = mappingBlocker(
        mapping({ source: "amount", target: "amount", inferredType: src, destType: tgt }),
        THRESHOLD,
      );
      assert.ok(blocker, `${src} → ${tgt} must raise a Map blocker`);
      assert.equal(blocker!.code, "risk_ack_required");
      assert.equal(blocker!.clearableFromMap, true);
      assert.match(blocker!.action, /execution policy/i);
    }
  });

  it("clears once a continue-policy contract is signed on the row", () => {
    const row = mapping({
      source: "amount",
      target: "amount",
      inferredType: "TEXT",
      destType: "DECIMAL(38,15)",
      approved: false,
    });
    const signed = acknowledgeMappingRisk(row, { executionPolicy: "QUARANTINE_ROW" });
    assert.equal(signed.riskAcknowledged, true);
    assert.equal(signed.riskContract?.execution_policy, "QUARANTINE_ROW");
    assert.equal(mappingBlocker(signed, THRESHOLD), null);
  });

  it("keeps a fail-closed policy blocked, and says how to release it", () => {
    const row = mapping({
      source: "amount",
      target: "amount",
      inferredType: "TEXT",
      destType: "DECIMAL(38,15)",
      approved: false,
    });
    const signed = acknowledgeMappingRisk(row, { executionPolicy: "FAIL_JOB" });
    const blocker = mappingBlocker(signed, THRESHOLD);
    assert.ok(blocker);
    assert.equal(blocker!.code, "fail_closed_contract");
    assert.match(blocker!.action, /continue policy/i);
  });

  it("refuses to sign without a policy — no hidden default", () => {
    const row = mapping({
      source: "amount",
      target: "amount",
      inferredType: "TEXT",
      destType: "DECIMAL(38,15)",
      approved: false,
    });
    const unsigned = acknowledgeMappingRisk(row, {});
    assert.notEqual(unsigned.riskAcknowledged, true);
    assert.equal(mappingBlocker(unsigned, THRESHOLD)!.code, "risk_ack_required");
  });
});
