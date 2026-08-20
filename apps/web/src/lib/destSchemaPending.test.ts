/**
 * Unread destination schema is a catalog gap, not a fidelity verdict.
 *
 * Regression: a Snowflake → MySQL transfer into a table whose schema never
 * loaded showed 10 rows of `VARCHAR(16777216) → VARCHAR(16777216) loses
 * fidelity` behind a Risk Contract that no signature could clear, and the
 * unbounded warning list pushed the mapping grid off screen.
 *
 * Run: npx --yes tsx --test apps/web/src/lib/destSchemaPending.test.ts
 */
import assert from "node:assert/strict";
import { describe, it } from "node:test";
import {
  acknowledgeMappingRisk,
  approveMappingsHonestly,
  countApproveEligible,
  isDestSchemaPending,
  mappingRequiresRiskAck,
  type EditableMapping,
} from "./mapping";
import { groupMapBlockers, mapBlockerSummary, mappingBlocker } from "./mapBlockers";
import { isMappingReady, mappingTier, needsMappingReview } from "./columnWorkbench";

const THRESHOLD = 0.85;

/** Snowflake column mapped into a MySQL table whose catalog never answered. */
function pendingDestColumn(source: string): EditableMapping {
  return {
    source,
    target: source,
    confidence: 0.55,
    inferredType: "VARCHAR(16777216)",
    destType: "",
    sample: "EMP0000001",
    approved: false,
    assignmentStrategy: "pending_dest_schema",
    requiresReview: true,
    // The engine's fail-closed marker: no destination type was read.
    fidelity: "dest_type_unread",
    fidelityReason: "Destination column type has not been read from the destination yet",
  };
}

/** A real narrowing on a column whose destination type was actually read. */
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
  };
}

describe("pending destination schema is not a signable risk", () => {
  it("never asks for a Risk Contract", () => {
    const m = pendingDestColumn("employee_id");
    assert.equal(isDestSchemaPending(m), true);
    assert.equal(mappingRequiresRiskAck(m), false);
  });

  it("stays blocked even if Approve or a signature is attempted", () => {
    const m = pendingDestColumn("employee_id");
    assert.equal(mappingTier(m, THRESHOLD), "block");
    assert.equal(isMappingReady({ ...m, approved: true }, THRESHOLD), false);
    const signed = acknowledgeMappingRisk(m, { executionPolicy: "CAST_AND_CONTINUE" });
    assert.equal(isMappingReady({ ...signed, approved: true }, THRESHOLD), false);
    assert.equal(needsMappingReview(m, THRESHOLD), true);
  });

  it("blocks as an unloaded destination schema, never as a type path", () => {
    const blocker = mappingBlocker(pendingDestColumn("employee_id"), THRESHOLD);
    assert.ok(blocker);
    assert.equal(blocker.code, "dest_schema_unloaded");
    assert.equal(blocker.clearableFromMap, false);
    assert.doesNotMatch(blocker.title, /loses fidelity/i);
    // The invented fact: source type printed as the destination type.
    assert.doesNotMatch(blocker.title, /VARCHAR\(16777216\).*VARCHAR\(16777216\)/);
    assert.match(blocker.action, /Reload the destination schema/i);
  });

  it("is never cleared by Approve all", () => {
    // Regression: the Map fallback (mapping engine unreachable) produced rows the
    // bulk Approve path marked ready, unlocking Validate for a destination that
    // had never been read.
    const columns = ["employee_id", "age"].map(pendingDestColumn);
    assert.equal(countApproveEligible(columns), 0);
    for (const m of approveMappingsHonestly(columns)) {
      assert.equal(m.approved, false);
      assert.equal(m.requiresReview, true);
      assert.equal(isMappingReady(m, THRESHOLD), false);
    }
  });
});

describe("the Map warning area stays bounded", () => {
  it("collapses ten pending columns into one cause with one headline", () => {
    const columns = [
      "employee_id",
      "first_name",
      "last_name",
      "department",
      "job_title",
      "age",
      "salary_amount",
      "hire_date",
      "employment_status",
      "work_location",
    ].map(pendingDestColumn);
    const summary = mapBlockerSummary(columns, THRESHOLD);
    assert.equal(summary.blockers.length, 10);
    assert.equal(summary.destSchemaUnloadedOnly, true);
    assert.equal(summary.groups.length, 1);
    assert.equal(summary.groups[0].code, "dest_schema_unloaded");
    assert.equal(summary.groups[0].columns.length, 10);
    assert.match(summary.headline, /Destination schema not loaded/i);
    assert.doesNotMatch(summary.headline, /Risk Contract/i);
  });

  it("keeps distinct causes on distinct lines", () => {
    const groups = groupMapBlockers(
      mapBlockerSummary(
        [pendingDestColumn("employee_id"), pendingDestColumn("age"), lossyExistingDest()],
        THRESHOLD,
      ).blockers,
    );
    const codes = groups.map((g) => g.code).sort();
    assert.deepEqual(codes, ["dest_schema_unloaded", "risk_ack_required"]);
  });
});

describe("a measured lossy path still needs a signed contract", () => {
  it("is unaffected by the pending-schema exemption", () => {
    const m = lossyExistingDest();
    assert.equal(isDestSchemaPending(m), false);
    assert.equal(mappingRequiresRiskAck(m), true);
    assert.equal(isMappingReady({ ...m, approved: true }, THRESHOLD), false);
    const signed = acknowledgeMappingRisk(m, { executionPolicy: "QUARANTINE_ROW" });
    assert.equal(isMappingReady(signed, THRESHOLD), true);
    assert.equal(mappingBlocker(signed, THRESHOLD), null);
  });
});
