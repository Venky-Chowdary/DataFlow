import assert from "node:assert/strict";
import { describe, it } from "node:test";
import { detectTypeRisks, fidelityRiskForMapping } from "./schemaIntelligence";
import type { EditableMapping } from "./mapping";

function mapping(partial: Partial<EditableMapping> & Pick<EditableMapping, "source" | "target">): EditableMapping {
  return {
    confidence: 0.95,
    approved: true,
    inferredType: "VARCHAR",
    destType: "VARCHAR",
    ...partial,
  };
}

describe("fidelityRiskForMapping", () => {
  it("flags decimal → float using live Map destType", () => {
    const risk = fidelityRiskForMapping(
      mapping({
        source: "amount",
        target: "amount",
        inferredType: "DECIMAL(20,6)",
        destType: "FLOAT",
      }),
    );
    assert.ok(risk);
    assert.match(risk!.title, /IEEE float|Fixed-point → IEEE/i);
  });

  it("flags float → decimal", () => {
    const risk = fidelityRiskForMapping(
      mapping({
        source: "score",
        target: "score",
        inferredType: "DOUBLE",
        destType: "DECIMAL(12,4)",
      }),
    );
    assert.ok(risk);
    assert.match(risk!.title, /IEEE float → fixed-point/i);
  });

  it("flags timestamptz → ntz polarity drop", () => {
    const risk = fidelityRiskForMapping(
      mapping({
        source: "ts",
        target: "ts",
        inferredType: "TIMESTAMPTZ",
        destType: "TIMESTAMP_NTZ",
      }),
    );
    assert.ok(risk);
    assert.match(risk!.title, /timezone|NTZ/i);
  });

  it("does not invent fidelity risk for aligned types", () => {
    const risk = fidelityRiskForMapping(
      mapping({
        source: "id",
        target: "id",
        inferredType: "BIGINT",
        destType: "BIGINT",
      }),
    );
    assert.equal(risk, null);
  });

  it("detectTypeRisks prefers mapping.destType over empty plan", () => {
    const risks = detectTypeRisks([
      mapping({
        source: "qty",
        target: "qty",
        inferredType: "DECIMAL(10,2)",
        destType: "INTEGER",
        approved: true,
        confidence: 0.99,
      }),
    ]);
    assert.ok(risks.some((r) => r.id.startsWith("lossy-")));
  });
});
