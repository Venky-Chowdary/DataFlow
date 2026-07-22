/**
 * Run: npx --yes tsx --test apps/web/src/lib/mapping.test.ts
 */
import assert from "node:assert/strict";
import { describe, it } from "node:test";
import {
  applyDestTypeChange,
  applyStructPolicyChange,
  applyTransformChange,
  buildPreflightMappings,
  canWidenMapping,
  editableFromPipelineMappings,
  engineTransformToUi,
  mappingHealthSummary,
  uiTransformToEngine,
  widenMappingToVarchar,
  type EditableMapping,
} from "./mapping.js";

describe("transform SSOT round-trip", () => {
  it("preserves phone/currency/integer engine transforms through Map edit", () => {
    const editable = editableFromPipelineMappings(
      [
        { source: "amt", target: "amount", confidence: 0.92, transform: "currency", source_type: "VARCHAR", target_type: "DECIMAL" },
        { source: "qty", target: "qty", confidence: 0.95, transform: "integer", source_type: "VARCHAR", target_type: "INTEGER" },
        { source: "phone", target: "phone", confidence: 0.9, transform: "phone", source_type: "VARCHAR", target_type: "VARCHAR" },
        { source: "blob", target: "blob", confidence: 0.9, transform: "binary", source_type: "BINARY", target_type: "BYTEA" },
        { source: "doc", target: "doc", confidence: 0.9, transform: "json", source_type: "JSON", target_type: "JSONB" },
      ],
      [],
      ["amount", "qty", "phone", "blob", "doc"],
      0.75,
      { amount: "DECIMAL", qty: "INTEGER", phone: "VARCHAR", blob: "BYTEA", doc: "JSONB" },
    );
    assert.equal(editable[0].transform, "currency");
    assert.equal(editable[0].engineTransform, "currency");
    assert.equal(editable[1].transform, "cast_integer");
    assert.equal(editable[1].engineTransform, "integer");
    assert.equal(editable[2].transform, "phone");
    assert.equal(editable[3].transform, "binary");
    assert.equal(editable[4].transform, "parse_json");

    const pf = buildPreflightMappings([], editable);
    assert.equal(pf[0].transform, "currency");
    assert.equal(pf[1].transform, "integer");
    assert.equal(pf[2].transform, "phone");
    assert.equal(pf[3].transform, "binary");
    assert.equal(pf[4].transform, "json");
  });

  it("maps engine json → parse_json (not none)", () => {
    assert.equal(engineTransformToUi("json"), "parse_json");
    assert.equal(uiTransformToEngine("parse_json"), "json");
    assert.equal(uiTransformToEngine("cast_integer"), "integer");
  });

  it("operator transform change updates engineTransform", () => {
    const m: EditableMapping = {
      source: "a",
      target: "a",
      confidence: 0.9,
      approved: true,
      transform: "phone",
      engineTransform: "phone",
    };
    const next = applyTransformChange(m, "cast_number");
    assert.equal(next.transform, "cast_number");
    assert.equal(next.engineTransform, "decimal");
    assert.equal(next.approved, false);
  });
});

describe("existing DDL honesty", () => {
  it("forbids Widen on existing destination columns", () => {
    const m: EditableMapping = {
      source: "status",
      target: "status",
      confidence: 0.9,
      approved: false,
      existsInDestination: true,
      destType: "BOOLEAN",
      inferredType: "VARCHAR",
      sample: "active",
      semanticRole: "string_enum",
      transform: "cast_boolean",
    };
    assert.equal(canWidenMapping(m), false);
    const widened = widenMappingToVarchar(m);
    assert.equal(widened.destType, "BOOLEAN");
    assert.equal(widened.requiresReview, true);
    assert.match(widened.reason || "", /ALTER|remap/i);
  });

  it("flags dest type change on existing columns without rewriting physical type", () => {
    const m: EditableMapping = {
      source: "id",
      target: "id",
      confidence: 0.99,
      approved: true,
      existsInDestination: true,
      destType: "INTEGER",
    };
    const next = applyDestTypeChange(m, "VARCHAR");
    assert.equal(next.destType, "INTEGER");
    assert.equal(next.requiresReview, true);
    assert.match(next.reason || "", /Desired type VARCHAR/);
  });
});

describe("specialty + health banner", () => {
  it("marks VECTOR as identity specialty and needs review", () => {
    const editable = editableFromPipelineMappings(
      [{ source: "emb", target: "emb", confidence: 0.99, transform: "none", source_type: "VECTOR(768)", target_type: "VECTOR(768)" }],
      [],
      [],
      0.75,
    );
    assert.equal(editable[0].transform, "identity_specialty");
    assert.equal(editable[0].requiresReview, true);
    assert.equal(editable[0].approved, false);
  });

  it("reports empty and conflict health", () => {
    assert.equal(mappingHealthSummary([]).total, 0);
    assert.equal(mappingHealthSummary([]).weak, true);
    const bad: EditableMapping[] = [{
      source: "status",
      target: "status",
      confidence: 0.9,
      approved: false,
      requiresReview: true,
      existsInDestination: true,
      destType: "BOOLEAN",
      sample: "active",
      semanticRole: "string_enum",
      reason: "Existing BOOLEAN column cannot be changed from Map",
    }];
    const h = mappingHealthSummary(bad, 0.85);
    assert.ok(h.existingTypeConflict >= 1 || h.needsReview >= 1);
    assert.equal(h.weak, true);
  });
});

describe("STRUCT Map policy", () => {
  it("defaults JSON to store_as_json and round-trips struct_policy", () => {
    const editable = editableFromPipelineMappings(
      [{ source: "addr", target: "addr", confidence: 0.9, transform: "json", source_type: "JSON", target_type: "JSONB" }],
      [{ addr: '{"city":"Austin","zip":"78701","geo":{"lat":30}}' }],
      [],
      0.75,
    );
    assert.equal(editable[0].structPolicy, "store_as_json");
    assert.equal(editable[0].requiresReview, true);
    const pf = buildPreflightMappings([], editable);
    assert.equal(pf[0].struct_policy, "store_as_json");
  });

  it("flatten synthesizes parent_key children and drops nested objects", () => {
    const base: EditableMapping[] = [{
      source: "addr",
      target: "addr",
      confidence: 0.9,
      approved: false,
      inferredType: "JSON",
      destType: "JSONB",
      sample: '{"city":"Austin","zip":"78701","geo":{"lat":30},"tags":["a"]}',
      structPolicy: "store_as_json",
      transform: "parse_json",
      engineTransform: "json",
    }];
    const next = applyStructPolicyChange(base, 0, "flatten_top_level_keys");
    assert.equal(next[0].structPolicy, "flatten_top_level_keys");
    const sources = next.map((m) => m.source);
    assert.ok(sources.includes("addr_city"));
    assert.ok(sources.includes("addr_zip"));
    assert.ok(sources.includes("addr_tags"));
    assert.ok(!sources.includes("addr_geo"), "nested object stays on parent blob");
    const child = next.find((m) => m.source === "addr_city");
    assert.equal(child?.structDerived, true);
    assert.equal(child?.structParent, "addr");
    assert.equal(child?.sample, "Austin");

    const back = applyStructPolicyChange(next, 0, "store_as_json");
    assert.equal(back.length, 1);
    assert.equal(back[0].structPolicy, "store_as_json");
  });
});
