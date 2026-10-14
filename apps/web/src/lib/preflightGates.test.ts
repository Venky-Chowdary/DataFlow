/**
 * Run: npx --yes tsx --test apps/web/src/lib/preflightGates.test.ts
 */
import assert from "node:assert/strict";
import { describe, it } from "node:test";
import {
  compareGateDisplayOrder,
  countGateStatuses,
  gateDisplayCode,
} from "./preflightGates.ts";

describe("gate display codes", () => {
  it("gives data integrity and sync contract different codes", () => {
    assert.equal(gateDisplayCode("g9_data_integrity"), "G9");
    assert.equal(gateDisplayCode("g9_sync_contract"), "G12");
    assert.notEqual(gateDisplayCode("g9_data_integrity"), gateDisplayCode("g9_sync_contract"));
  });

  it("sorts G3F with G3, ahead of G4", () => {
    const ids = [
      "g19_dest_schema_replacement",
      "g3f_population_fit",
      "g4_mapping_confidence",
      "g3_schema_contract",
      "g9_sync_contract",
      "g9_data_integrity",
      "g18_cdc_snapshot_mode",
    ];
    const ordered = [...ids].sort(compareGateDisplayOrder).map(gateDisplayCode);
    assert.deepEqual(ordered, ["G3", "G3F", "G4", "G9", "G12", "G18", "G19"]);
  });

  it("counts every gate once so the hero adds up", () => {
    const counts = countGateStatuses([
      ...Array.from({ length: 17 }, () => ({ status: "pass" })),
      { status: "block" },
      ...Array.from({ length: 4 }, () => ({ status: "skip" })),
      { status: "warn" },
    ]);
    assert.equal(counts.total, 23);
    assert.equal(counts.passed, 17);
    assert.equal(counts.blocked, 1);
    assert.equal(counts.skipped, 4);
    assert.equal(counts.review, 1);
    assert.equal(
      counts.passed + counts.blocked + counts.skipped + counts.review + counts.pending,
      counts.total,
    );
  });
});
