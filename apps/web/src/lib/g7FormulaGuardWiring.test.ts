/**
 * G7-F2: every CSV the browser builds goes through the one formula guard
 * (csvSafety.ts, the API's rule), and the export dialogs offer the option.
 */
import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { describe, it } from "node:test";
import { downloadJobQuarantineCsv } from "./api";

const src = (rel: string) => readFileSync(new URL(`../${rel}`, import.meta.url), "utf8");

describe("quarantine CSV fallback", () => {
  it("defuses formula cells like the server's quarantine CSV, every cell still quoted", async () => {
    const { blob } = await downloadJobQuarantineCsv("j1", [
      {
        row: 3,
        column: "note",
        target: "note",
        value: '=HYPERLINK("http://evil.example")',
        reason: "-1 too small",
        policy: "quarantine",
        suggested_transform: "",
        suggested_fix: "@fix",
        suggested_target_type: "",
      } as never,
    ]);
    const lines = (await blob.text()).split("\n");
    assert.equal(
      lines[1],
      '"3","note","note","\'=HYPERLINK(""http://evil.example"")","\'-1 too small","quarantine","","\'@fix",""',
    );
  });
});

describe("export dialogs offer 'Protect against spreadsheet formulas'", () => {
  it("Query console: on by default and sent to the API as formula_protection", () => {
    const page = src("pages/QueryPage.tsx");
    assert.match(page, /useState\(true\)/);
    assert.match(page, /\{QUERY_EXPORT_FORMULA_PROTECTION_HELP\}/);
    assert.match(page, /Protect against spreadsheet formulas/);
    assert.match(page, /formula_protection: formulaProtection \? "prefix" : "off"/);
    assert.match(page, /formulaGuardApplies\(exportFormat\)/);
  });

  it("Studio file_export: the option rides the run, plan, route analysis, contract and offline export", () => {
    const page = src("pages/TransferPage.tsx");
    assert.match(page, /Protect against spreadsheet formulas/);
    assert.match(page, /fileExportDestExtra\(\{ format: exportFormat, formulaProtection: exportFormulaProtection,/);
    assert.equal((page.match(/fileExportEndpoint\(\{/g) || []).length, 3);
    assert.doesNotMatch(page, /\{ kind: "file_export", format: exportFormat, database: targetDb/);
    assert.match(page, /formulaProtection: exportFormulaProtection,\n\s*tsvNull: tsvNullMarker,\n\s*\}\);/);
    assert.match(page, /setExportFormulaProtection\(dst\.formulaProtection\)/);
    // New job: on; reset (New transfer): back to on; reopened job: what it stored.
    assert.match(page, /useState\(FILE_EXPORT_FORMULA_PROTECTION_DEFAULT\)/);
    assert.match(page, /setExportFormulaProtection\(FILE_EXPORT_FORMULA_PROTECTION_DEFAULT\)/);
    assert.match(page, /\{FILE_EXPORT_FORMULA_PROTECTION_HELP\}/);
  });

  it("step-preview.csv and copy-as-CSV use the shared guard", () => {
    const canvas = src("components/transfer/TransferTransformCanvas.tsx");
    assert.match(canvas, /toDelimitedText\(columns, rows, \{ formulaProtection: true, cell: cellText \}\)/);
    assert.match(src("lib/queryResults.ts"), /toDelimitedText/);
    assert.match(src("lib/jobGrid.ts"), /delimitedCell\(value, \{ formulaProtection: true \}\)/);
  });
});
