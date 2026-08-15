/**
 * Run: npx --yes tsx --test apps/web/src/lib/destExistsShape.test.ts
 */
import assert from "node:assert/strict";
import { describe, it } from "node:test";
import {
  callableExtractNote,
  destExistsPrimaryCta,
  destOnlyPreserveColumns,
  extraSourceColumnsFromContract,
  shapeContractFromPreflight,
} from "./destExistsShape.js";

describe("destExistsShape", () => {
  it("lists extras and dest-only preserve from the contract", () => {
    const contract = {
      shape: "overlap",
      primary_action: "review_map",
      extra_source_columns: ["loyalty_tier"],
      dest_only: [
        { target: "updated_at", kind: "dest_only_preserve" },
        { target: "tenant_id", kind: "dest_only_required" },
      ],
    };
    assert.deepEqual(extraSourceColumnsFromContract(contract), ["loyalty_tier"]);
    assert.deepEqual(destOnlyPreserveColumns(contract), ["updated_at"]);
  });

  it("maps G15 primary_action to one Validate button", () => {
    const cta = destExistsPrimaryCta({
      primary_action: "reload_dest_schema",
      extra_source_columns: [],
    });
    assert.equal(cta?.kind, "reload_dest_schema");
    assert.match(cta?.label || "", /Reload destination schema/);
    assert.equal(destExistsPrimaryCta({ primary_action: "continue_validate" }), null);
  });

  it("reads shape_contract from source_coverage or the G15 gate", () => {
    const fromCoverage = shapeContractFromPreflight({
      source_coverage: { shape_contract: { shape: "equal", primary_action: "continue_validate" } },
    });
    assert.equal(fromCoverage?.shape, "equal");
    const fromGate = shapeContractFromPreflight({
      gates: [{
        id: "g15_dest_exists_shape",
        details: { shape: "source_superset", primary_action: "review_map", extra_source_columns: ["x"] },
      }],
    });
    assert.equal(fromGate?.shape, "source_superset");
    assert.equal(destExistsPrimaryCta(fromGate)?.kind, "review_mappings");
  });

  it("surfaces callable extract honesty from the Validate stamp", () => {
    assert.equal(
      callableExtractNote({
        callable_extract: { note: "Result-set snapshot — FK catalog skipped." },
      }),
      "Result-set snapshot — FK catalog skipped.",
    );
    assert.equal(callableExtractNote({}), "");
  });
});
