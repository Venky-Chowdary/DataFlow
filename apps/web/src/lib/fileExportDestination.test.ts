import assert from "node:assert/strict";
import { describe, it } from "node:test";
import { fileExportDestExtra, fileExportEndpoint, fileExportFormulaProtection } from "./fileExportDestination";
import { planJobStudioRoute } from "./jobStudioRoute";
import type { TransferJob } from "./types";

describe("Studio file_export formula_protection (G7-F2)", () => {
  it("sends formula_protection prefix for CSV/TSV only when the option is on", () => {
    assert.deepEqual(fileExportDestExtra({ format: "csv", formulaProtection: true }), { formula_protection: "prefix" });
    assert.deepEqual(fileExportDestExtra({ format: "TSV", formulaProtection: true }), { formula_protection: "prefix" });
    assert.deepEqual(fileExportDestExtra({ format: "excel", formulaProtection: true }), {});
    assert.deepEqual(fileExportDestExtra({ format: "csv", formulaProtection: false }), {});
  });

  it("builds the destination endpoint with the option in extra", () => {
    assert.deepEqual(
      fileExportEndpoint({ format: "csv", database: "", outputPath: "o.csv", formulaProtection: true }),
      { kind: "file_export", format: "csv", database: "", output_path: "o.csv", extra: { formula_protection: "prefix" } },
    );
    // Off: the same endpoint as before the option existed (the API default "off").
    assert.deepEqual(
      fileExportEndpoint({ format: "csv", database: "", outputPath: "", formulaProtection: false }),
      { kind: "file_export", format: "csv", database: "", output_path: "" },
    );
  });

  it("reads the option back from a saved destination", () => {
    assert.equal(fileExportFormulaProtection({ extra: { formula_protection: "prefix" } }), true);
    assert.equal(fileExportFormulaProtection({ formula_protection: " PREFIX " }), true);
    assert.equal(fileExportFormulaProtection({ extra: { formula_protection: "off" } }), false);
    assert.equal(fileExportFormulaProtection({}), false);
  });

  it("re-opening a job in Studio keeps its formula protection", () => {
    const job = {
      job_id: "j1",
      transfer_request: {
        source: { kind: "file", extra: { file_id: "f1" } },
        destination: { kind: "file_export", format: "csv", extra: { formula_protection: "prefix" } },
      },
    } as unknown as TransferJob;
    const plan = planJobStudioRoute(job, []);
    assert.equal(plan.destination.kind, "file_export");
    if (plan.destination.kind === "file_export") assert.equal(plan.destination.formulaProtection, true);
  });
});
