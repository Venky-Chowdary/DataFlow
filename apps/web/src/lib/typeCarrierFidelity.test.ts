import assert from "node:assert/strict";
import { describe, it } from "node:test";
import {
  decimalWouldCollapse,
  effectiveDestCarrier,
  parseStringCarrierWidth,
  saasDefaultCarrier,
  sampleExceedsStringWidth,
  stringWidthWouldNarrow,
} from "./typeCarrierFidelity";
import {
  fidelityChipLabel,
  fidelityRiskForMapping,
} from "./schemaIntelligence";
import type { EditableMapping } from "./mapping";

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

describe("typeCarrierFidelity", () => {
  it("parses VARCHAR widths and detects narrowing", () => {
    assert.equal(parseStringCarrierWidth("VARCHAR(255)"), 255);
    assert.equal(stringWidthWouldNarrow("VARCHAR(2000)", "VARCHAR(512)"), true);
    assert.equal(stringWidthWouldNarrow("VARCHAR(80)", "VARCHAR(512)"), false);
    assert.equal(stringWidthWouldNarrow("TEXT", "VARCHAR(80)"), true);
    assert.equal(stringWidthWouldNarrow("VARCHAR", "VARCHAR(80)"), false);
  });

  it("detects decimal scale collapse", () => {
    assert.equal(decimalWouldCollapse("DECIMAL(20,6)", "DECIMAL(38,2)"), true);
    assert.equal(decimalWouldCollapse("DECIMAL(10,2)", "DECIMAL(18,4)"), false);
    assert.equal(decimalWouldCollapse("DECIMAL(38,0)", "DECIMAL(10,0)"), true);
  });

  it("applies Stripe SaaS defaults when destType is bare string", () => {
    assert.equal(saasDefaultCarrier("stripe", "email"), "VARCHAR(512)");
    assert.equal(saasDefaultCarrier("stripe", "phone"), "VARCHAR(20)");
    assert.equal(
      effectiveDestCarrier("string", "stripe", "email"),
      "VARCHAR(512)",
    );
    assert.equal(
      effectiveDestCarrier("VARCHAR(80)", "stripe", "email"),
      "VARCHAR(80)",
    );
  });

  it("flags sample longer than destination width", () => {
    assert.equal(sampleExceedsStringWidth("x".repeat(21), "VARCHAR(20)"), true);
    assert.equal(sampleExceedsStringWidth("ok", "VARCHAR(20)"), false);
  });
});

describe("Map fidelity chips — width/scale/SaaS", () => {
  it("flags VARCHAR width narrow as width chip", () => {
    const risk = fidelityRiskForMapping(
      mapping({
        source: "notes",
        target: "notes",
        inferredType: "VARCHAR(2000)",
        destType: "VARCHAR(512)",
      }),
    );
    assert.ok(risk);
    assert.equal(risk!.id.startsWith("width-"), true);
    assert.equal(fidelityChipLabel(risk!), "width");
    assert.match(risk!.title, /width/i);
  });

  it("uses Stripe catalog when destType lacks width", () => {
    const risk = fidelityRiskForMapping(
      mapping({
        source: "cust_email",
        target: "email",
        inferredType: "VARCHAR(2000)",
        destType: "string",
        sample: "a".repeat(10),
      }),
      { destConnector: "stripe" },
    );
    assert.ok(risk);
    assert.equal(risk!.id.startsWith("width-"), true);
    assert.match(risk!.detail, /512/);
  });

  it("flags Airtable-class decimal scale collapse", () => {
    const risk = fidelityRiskForMapping(
      mapping({
        source: "amount",
        target: "Amount",
        inferredType: "DECIMAL(20,6)",
        destType: "DECIMAL(38,2)",
      }),
    );
    assert.ok(risk);
    assert.equal(risk!.id.startsWith("scale-"), true);
    assert.equal(fidelityChipLabel(risk!), "scale");
  });

  it("flags sample overflow against Notion email width via catalog", () => {
    const risk = fidelityRiskForMapping(
      mapping({
        source: "e",
        target: "email",
        inferredType: "VARCHAR",
        destType: "string",
        sample: "a".repeat(201) + "@example.com",
      }),
      { destConnector: "notion" },
    );
    assert.ok(risk);
    assert.equal(risk!.id.startsWith("sample-width-"), true);
    assert.equal(fidelityChipLabel(risk!), "width");
  });
});
