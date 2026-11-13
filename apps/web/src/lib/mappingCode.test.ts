import assert from "node:assert/strict";
import { describe, it } from "node:test";
import { catalogFromNames } from "./destColumnCatalog";
import type { EditableMapping } from "./mapping";
import {
  applyImportedMappings,
  computedStep,
  documentRows,
  loadTimeAdvice,
  mergeComputedSteps,
  templateUnboundNote,
} from "./mappingCode";

const row = (source: string, target: string, extra: Partial<EditableMapping> = {}): EditableMapping => ({
  source, target, confidence: 0.9, inferredType: "VARCHAR(40)", destType: "VARCHAR(40)",
  approved: true, transform: "none", ...extra,
});

describe("mapping-as-code rows", () => {
  it("exports what the document schema carries, omits with their reason", () => {
    const rows = documentRows([
      row("order_id", "legacy_ref", { inferredType: "BIGINT", destType: "BIGINT", existsInDestination: true }),
      row("first_name", "", { transform: "omit", omitReason: "dropped_redundant", omitReasonText: "In full_name" }),
      row("note", "note_text", { existsInDestination: false, createNew: true, inferredType: "TEXT", destType: "TEXT" }),
    ]);
    assert.deepEqual(rows, [
      { source: "order_id", target: "legacy_ref", source_type: "BIGINT", target_type: "BIGINT", transform: "none" },
      { source: "first_name", target: "", transform: "omit", intentional_omit: true,
        omit_reason: "dropped_redundant", omit_reason_text: "In full_name" },
      { source: "note", target: "note_text", source_type: "TEXT", target_type: "TEXT", transform: "none",
        create_new: true },
    ]);
  });

  it("import remaps matching rows onto the catalog, omits, and names what did not match", () => {
    const catalog = catalogFromNames(["legacy_ref", "given_name"], { legacy_ref: "BIGINT", given_name: "VARCHAR(40)" }, true);
    const current = [row("order_id", "order_id"), row("first_name", "first_name"), row("last_name", "last_name")];
    const out = applyImportedMappings(current, [
      { source: "order_id", target: "legacy_ref" },
      { source: "first_name", target: "given_name" },
      { source: "last_name", target: "", transform: "omit", intentional_omit: true, omit_reason: "dropped_redundant", omit_reason_text: "x" },
      { source: "ghost", target: "nowhere" },
    ], catalog);
    assert.deepEqual(out.unmatched, ["ghost"]);
    assert.equal(out.applied, 3);
    assert.equal(out.rows[0].target, "legacy_ref");
    assert.equal(out.rows[0].destType, "BIGINT");
    assert.equal(out.rows[0].approved, false, "an imported remap still needs Approve");
    assert.equal(out.rows[1].target, "given_name");
    assert.equal(out.rows[2].transform, "omit");
    assert.equal(out.rows[2].omitReason, "dropped_redundant");
  });
});

describe("computed columns are Transform steps", () => {
  it("an expression becomes derive_column named after its destination", () => {
    assert.deepEqual(computedStep({ kind: "expression", target: "full_name", expression: "CONCAT(first_name,' ',last_name)" }), {
      op: "derive_column", options: { to: "full_name", expression: "CONCAT(first_name,' ',last_name)" },
    });
  });

  it("a literal becomes constant_column", () => {
    assert.deepEqual(computedStep({ kind: "constant", target: "source_system", value: "mysql" }), {
      op: "constant_column", options: { to: "source_system", value: "mysql" },
    });
  });

  it("load time is a destination default, never a computed value", () => {
    const withDefault = loadTimeAdvice({ name: "loaded_at", hasDefault: true, default: "now()" });
    assert.equal(withDefault.ok, true);
    assert.match(withDefault.message, /Leave loaded_at unmapped: the destination stamps now\(\)/);
    const without = loadTimeAdvice({ name: "loaded_at", hasDefault: false, default: null });
    assert.equal(without.ok, false);
    assert.match(without.message, /preview, Validate, Execute and a resume/);
  });
});

describe("template re-binding notes", () => {
  it("says why each template pair did not bind", () => {
    assert.equal(
      templateUnboundNote({ source: "total", target: "total_cents", missing: "target_unwritable" }),
      "total → total_cents: total_cents is filled by the database (generated or identity), so it is left alone.",
    );
    assert.equal(
      templateUnboundNote({ source: "a", target: "b", missing: "both" }),
      "a → b: no matching source or destination column here.",
    );
    assert.equal(
      templateUnboundNote({ source: "a", target: "b", missing: "source" }),
      "a → b: no matching source column here (or it is already bound).",
    );
  });
});

describe("adding computed columns to Transform", () => {
  const derive = (to: string, expression: string, extra: Record<string, unknown> = {}) => ({
    op: "derive_column", options: { to, expression }, ...extra,
  });

  it("stamps the Map step's stream so a two-stream run applies it to that table only", () => {
    const out = mergeComputedSteps([], [derive("loaded_by", "'etl'")], { table: "orders", tableCount: 2 });
    assert.deepEqual(out, [{ ...derive("loaded_by", "'etl'"), source_table: "orders" }]);
  });

  it("replaces only the same column on the same stream", () => {
    const prev = [
      derive("loaded_by", "'old'", { source_table: "orders" }),
      derive("loaded_by", "'cust'", { source_table: "customers" }),
    ];
    const out = mergeComputedSteps(prev, [derive("loaded_by", "'new'")], { table: "orders", tableCount: 2 });
    assert.deepEqual(out, [
      derive("loaded_by", "'cust'", { source_table: "customers" }),
      derive("loaded_by", "'new'", { source_table: "orders" }),
    ]);
  });

  it("leaves a single-stream step unstamped", () => {
    const out = mergeComputedSteps([derive("x", "1")], [derive("x", "2")], { table: "orders", tableCount: 1 });
    assert.deepEqual(out, [derive("x", "2")]);
  });
});
