import assert from "node:assert/strict";
import { describe, it } from "node:test";

import type { EditableMapping } from "../../lib/mapping";
import { STEP_DESTINATION, STEP_MAP, STEP_SOURCE, STEP_VALIDATE } from "./studioConstants";
import {
  analysisFromPipeline,
  destinationCatalogLoadsOnStep,
  destinationCatalogRetriesOnStep,
  formatFileSize,
  sealRemediationApproval,
  studioSourceLabel,
} from "./studioHelpers";

describe("transfer studioHelpers (Phase F9)", () => {
  it("formats file sizes", () => {
    assert.equal(formatFileSize(500), "500 B");
    assert.match(formatFileSize(2048), /KB/);
  });

  it("builds analysis from pipeline mappings", () => {
    const a = analysisFromPipeline(
      ["id", "email"],
      { id: "integer", email: "string" },
      [{ source: "id", target: "id", confidence: 0.99, reasoning: "exact" }],
    );
    assert.equal(a.columns.length, 2);
    assert.equal(a.columns[0]?.confidence, 0.99);
  });

  it("seals remediation when risk ack missing", () => {
    const m = sealRemediationApproval({
      source: "amt",
      target: "amt",
      destType: "float",
      inferredType: "decimal",
      approved: true,
      riskAcknowledged: false,
      fidelity: "lossy_cast",
    } as EditableMapping);
    assert.equal(m.approved, false);
    assert.equal(m.requiresReview, true);
  });

  it("never prefers a leftover file name on a database route", () => {
    assert.equal(
      studioSourceLabel({
        sourceKind: "database",
        fileName: "qa_amb.csv",
        sourceConnectorName: "Local Postgres",
      }),
      "Local Postgres",
    );
    assert.equal(
      studioSourceLabel({ sourceKind: "file", fileName: "qa_amb.csv" }),
      "qa_amb.csv",
    );
  });

  it("loads the destination catalog on flagged connect and on the later steps", () => {
    assert.equal(destinationCatalogLoadsOnStep(STEP_SOURCE, true), true);
    assert.equal(destinationCatalogLoadsOnStep(STEP_SOURCE, false), false);
    assert.equal(destinationCatalogRetriesOnStep(STEP_SOURCE, true), true);
    assert.equal(destinationCatalogRetriesOnStep(STEP_SOURCE, false), false);
    for (const step of [STEP_DESTINATION, STEP_MAP, STEP_VALIDATE]) {
      assert.equal(destinationCatalogLoadsOnStep(step, false), true);
    }
    assert.equal(destinationCatalogRetriesOnStep(STEP_VALIDATE, true), false);
    assert.equal(destinationCatalogRetriesOnStep(STEP_MAP, false), true);
  });
});
