import assert from "node:assert/strict";
import { describe, it } from "node:test";
import {
  FILE_EXPORT_FORMULA_PROTECTION_DEFAULT,
  FILE_EXPORT_FORMULA_PROTECTION_HELP,
  QUERY_EXPORT_FORMULA_PROTECTION_HELP,
  fileExportDestExtra,
  fileExportEndpoint,
  fileExportFormulaProtection,
} from "./fileExportDestination";
import { runLocalFileExport } from "./localFileExport";
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

  it("a NEW job protects by default; a saved job without the field stays off", () => {
    assert.equal(FILE_EXPORT_FORMULA_PROTECTION_DEFAULT, true);
    const saved = {
      transfer_request: {
        source: { kind: "file", extra: { file_id: "f1" } },
        destination: { kind: "file_export", format: "csv" },
      },
    };
    const plan = planJobStudioRoute(saved, []);
    if (plan.destination.kind !== "file_export") throw new Error("expected file_export");
    assert.equal(plan.destination.formulaProtection, false);
  });

  it("the help text states the trade-off and that the prefix is reversible", () => {
    for (const text of [FILE_EXPORT_FORMULA_PROTECTION_HELP, QUERY_EXPORT_FORMULA_PROTECTION_HELP]) {
      assert.match(text, /= \+ - @/);
      assert.match(text, /' prefix/);
      assert.match(text, /don't execute/);
      assert.match(text, /reversible/);
    }
    assert.match(FILE_EXPORT_FORMULA_PROTECTION_HELP, /Gate-8/);
  });

  it("the offline browser export follows the same default", async () => {
    const created: Blob[] = [];
    const g = globalThis as unknown as { URL: typeof URL; document: unknown };
    const origCreate = g.URL.createObjectURL;
    const origDoc = g.document;
    g.URL.createObjectURL = ((blob: Blob) => {
      created.push(blob);
      return "blob:x";
    }) as typeof URL.createObjectURL;
    g.document = { createElement: () => ({ click: () => undefined }) };
    try {
      const base = {
        sourceFilename: "a.csv",
        rows: [{ v: "=1+1" }, { v: "-2" }, { v: "@x" }],
        mappings: [{ source: "v", target: "v", transform: "none" }] as never,
        format: "csv",
      };
      runLocalFileExport(base);
      runLocalFileExport({ ...base, formulaProtection: false });
    } finally {
      g.URL.createObjectURL = origCreate;
      g.document = origDoc;
    }
    assert.equal(await created[0].text(), "v\n'=1+1\n-2\n'@x");
    assert.equal(await created[1].text(), "v\n=1+1\n-2\n@x");
  });
});
