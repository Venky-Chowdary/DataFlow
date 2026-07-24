/**
 * Scenario tests for honest local / file-export preflight.
 * Run: npx --yes tsx --test apps/web/src/lib/localPreflight.test.ts
 */
import assert from "node:assert/strict";
import { describe, it } from "node:test";
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
    assert.equal(pf.proof_bundle?.quality_grade, "review");
    assert.equal(pf.proof_bundle?.transfer_decision?.decision, "review");
    assert.equal(pf.proof_bundle?.reconciliation?.passed, false);
    assert.ok((pf.proof_bundle?.transfer_decision?.warnings?.length ?? 0) >= 1);
    assert.ok((pf.readiness_score ?? 100) <= 72);
    assert.ok((pf.proof_bundle?.compliance?.tags ?? []).includes("local_preflight"));
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
});
