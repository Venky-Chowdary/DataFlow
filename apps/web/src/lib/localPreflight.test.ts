/**
 * Scenario tests for honest local / file-export preflight.
 * Run: npx --yes tsx --test apps/web/src/lib/localPreflight.test.ts
 */
import assert from "node:assert/strict";
import { describe, it } from "node:test";
import { applyLocalTransform } from "./localTransform.js";
import { isLocalPreflight, runLocalPreflight } from "./localPreflight.js";

describe("runLocalPreflight file export honesty", () => {
  it("skips remote destination / DDL / reconcile gates", () => {
    const pf = runLocalPreflight({
      columns: ["id", "name"],
      rowCount: 10,
      mappings: [
        { source: "id", target: "id", confidence: 0.95, transform: "none", approved: true, requiresReview: false, isPii: false },
        { source: "name", target: "name", confidence: 0.9, transform: "none", approved: true, requiresReview: false, isPii: false },
      ],
      sampleRows: [{ id: 1, name: "a" }],
      destKind: "file_export",
    });

    assert.ok(isLocalPreflight(pf));
    assert.ok(pf.run_id?.startsWith("pf_local_"));
    const byId = Object.fromEntries(pf.gates.map((g) => [g.id, g]));
    assert.equal(byId.g2_destination?.status, "skip");
    assert.equal(byId.g6_target_ddl?.status, "skip");
    assert.equal(byId.g8_reconciliation?.status, "skip");
    assert.equal(pf.proof_bundle?.quality_grade, "not_profiled");
    assert.equal(pf.proof_bundle?.quality_score, null);
    assert.equal(pf.proof_bundle?.transfer_decision?.decision, "review");
    assert.equal(pf.proof_bundle?.reconciliation?.passed, false);
    assert.ok((pf.proof_bundle?.transfer_decision?.warnings?.length ?? 0) >= 1);
    assert.ok((pf.readiness_score ?? 100) <= 72);
    assert.ok((pf.proof_bundle?.compliance?.tags ?? []).includes("local_preflight"));
    assert.ok(byId.g8_reconciliation?.details?.evidence_scope);
    assert.equal(
      (byId.g8_reconciliation?.details?.evidence_scope as { coverage?: string })?.coverage,
      "pending",
    );
    const ids = pf.gates.map((g) => g.id);
    assert.equal(new Set(ids).size, ids.length, "gate ids must be unique");
    assert.ok(ids.indexOf("g6_target_ddl") < ids.indexOf("g9_data_integrity"));
    assert.equal(byId.g13_source_coverage?.status, "skip");
    assert.equal(byId.g14_destination_requirements?.status, "skip");
    assert.equal(byId.constraint_fk?.status, "skip");
    assert.equal(byId.g15_dest_exists_shape?.status, "skip");
  });

  it("blocks SCD2 on a stored-procedure extract", () => {
    const pf = runLocalPreflight({
      columns: ["id"],
      rowCount: 1,
      mappings: [
        { source: "id", target: "id", confidence: 0.99, transform: "none", approved: true, requiresReview: false, isPii: false },
      ],
      destKind: "file_export",
      sourceReadMode: "procedure",
      syncMode: "scd2",
    });
    const byId = Object.fromEntries(pf.gates.map((g) => [g.id, g]));
    assert.equal(byId.g9_sync_contract?.status, "block");
    assert.equal(pf.passed, false);
  });

  it("blocks CDC on a dest stored-procedure write", () => {
    const pf = runLocalPreflight({
      columns: ["id"],
      rowCount: 1,
      mappings: [
        { source: "id", target: "id", confidence: 0.99, transform: "none", approved: true, requiresReview: false, isPii: false },
      ],
      destKind: "file_export",
      destWriteMode: "procedure",
      syncMode: "cdc",
    });
    const byId = Object.fromEntries(pf.gates.map((g) => [g.id, g]));
    assert.equal(byId.g9_sync_contract?.status, "block");
    assert.equal(pf.passed, false);
  });

  it("blocks CDC on a stored-procedure extract", () => {
    const pf = runLocalPreflight({
      columns: ["id"],
      rowCount: 1,
      mappings: [
        { source: "id", target: "id", confidence: 0.99, transform: "none", approved: true, requiresReview: false, isPii: false },
      ],
      destKind: "file_export",
      sourceReadMode: "procedure",
      syncMode: "cdc",
    });
    const byId = Object.fromEntries(pf.gates.map((g) => [g.id, g]));
    assert.equal(byId.g9_sync_contract?.status, "block");
    assert.equal(pf.passed, false);
  });

  it("does not invent approve decision when local gates pass", () => {
    const pf = runLocalPreflight({
      columns: ["id"],
      rowCount: 2,
      mappings: [
        { source: "id", target: "id", confidence: 0.99, transform: "none", approved: true, requiresReview: false, isPii: false },
      ],
      destKind: "file_export",
    });
    assert.equal(pf.passed, true);
    assert.equal(pf.proof_bundle?.transfer_decision?.decision, "review");
  });

  it("blocks database destinations that require API preflight", () => {
    const pf = runLocalPreflight({
      columns: ["id"],
      rowCount: 1,
      mappings: [
        { source: "id", target: "id", confidence: 0.99, transform: "none", approved: true, requiresReview: false, isPii: false },
      ],
      destKind: "database",
    });
    assert.equal(pf.passed, false);
    assert.ok(pf.blockers.some((b) => b.id === "g2_destination"));
  });

  it("treats operator-approved low-confidence mappings as passing G4", () => {
    const pf = runLocalPreflight({
      columns: ["id", "name"],
      rowCount: 2,
      mappings: [
        { source: "id", target: "id", confidence: 0.55, transform: "none", approved: true, requiresReview: false, isPii: false },
        { source: "name", target: "name", confidence: 0.55, transform: "none", approved: true, requiresReview: false, isPii: false },
      ],
      destKind: "file_export",
    });
    assert.equal(pf.passed, true);
    assert.ok(!pf.blockers.some((b) => b.id === "g4_mapping_confidence"));
  });

  it("blocks G4 when high-confidence mappings are not operator-approved", () => {
    const pf = runLocalPreflight({
      columns: ["id"],
      rowCount: 1,
      mappings: [
        { source: "id", target: "id", confidence: 0.99, transform: "none", approved: false, requiresReview: false, isPii: false },
      ],
      destKind: "file_export",
    });
    assert.equal(pf.passed, false);
    assert.ok(pf.blockers.some((b) => b.id === "g4_mapping_confidence"));
  });

  it("does not invent 1234 from 1,234 on a local decimal transform", () => {
    assert.equal(applyLocalTransform("1,234", "decimal"), "1,234");
    assert.equal(applyLocalTransform("1,234", "cast_number"), "1,234");
    assert.equal(applyLocalTransform("$1,000.00", "decimal"), 1000);
    assert.equal(applyLocalTransform("1,234", "decimal", "US"), 1234);
    assert.equal(applyLocalTransform("1,234", "decimal", "EU"), 1.234);
  });

  it("stamps date_locale_report set_locale when Auto cannot parse 01/02/2024", () => {
    const pf = runLocalPreflight({
      columns: ["event_date"],
      rowCount: 2,
      mappings: [
        { source: "event_date", target: "event_date", confidence: 0.9, transform: "none", approved: true, requiresReview: false, isPii: false },
      ],
      sampleRows: [{ event_date: "01/02/2024" }, { event_date: "03/04/2024" }],
      destKind: "file_export",
    });
    assert.equal(pf.date_locale_report?.decision, "set_locale");
    assert.deepEqual(
      (pf.date_locale_report?.ambiguous_columns || []).map((c) => c.column),
      ["event_date"],
    );
  });

  it("does not invent set_locale when date locale is MDY", () => {
    const pf = runLocalPreflight({
      columns: ["event_date"],
      rowCount: 1,
      mappings: [
        { source: "event_date", target: "event_date", confidence: 0.9, transform: "none", approved: true, requiresReview: false, isPii: false },
      ],
      sampleRows: [{ event_date: "01/02/2024" }],
      destKind: "file_export",
      dateLocale: "MDY",
    });
    assert.equal(pf.date_locale_report?.decision, "ok");
  });
});
