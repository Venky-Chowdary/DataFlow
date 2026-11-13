import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { describe, it } from "node:test";
import {
  delimitedCell,
  neutralizeExportCell,
  neutralizeFormulaCell,
  restoreExportCell,
  toDelimitedText,
} from "./csvSafety";

const HYPERLINK = '=HYPERLINK("http://evil.example/x","Click")';

/** The exact vectors of apps/api/tests/test_g7_csv_export_formula_protection.py::test_neutralize_export_cell. */
const PYTHON_TEST_VECTORS: Array<[unknown, string]> = [
  [HYPERLINK, `'${HYPERLINK}`],
  ["=== VIP ===", "'=== VIP ==="],
  ["@SUM(A1)", "'@SUM(A1)"],
  ["+cmd", "'+cmd"],
  ["-2+3", "'-2+3"],
  ["\tx", "'\tx"],
  ["\rx", "'\rx"],
  ["'=x", "''=x"],
  ["plain", "plain"],
  ["", ""],
  [null, ""],
  // Numbers are data, not formulas — never mangled.
  ["-5", "-5"],
  ["-1.5", "-1.5"],
  ["+3", "+3"],
  ["-1e-3", "-1e-3"],
  [-5, "-5"],
  [-1.5, "-1.5"],
];

interface Vector {
  input: string | number | null;
  export: string;
  operator: string;
  restored: string;
}

/** One vector file for both rules: apps/api/tests/test_g7_f2_formula_guard_vectors.py reads it too. */
const SHARED = JSON.parse(
  readFileSync(new URL("../../../api/tests/fixtures/formula_guard_vectors.json", import.meta.url), "utf8"),
) as { vectors: Vector[] };

describe("csvSafety mirrors services/csv_safety.py", () => {
  for (const [raw, guarded] of PYTHON_TEST_VECTORS) {
    it(`neutralizeExportCell(${JSON.stringify(raw)})`, () => {
      assert.equal(neutralizeExportCell(raw), guarded);
      // Reversible: the exact value that was exported.
      assert.equal(restoreExportCell(guarded), raw == null ? "" : String(raw));
    });
  }

  it("agrees with the Python rule on every shared vector", () => {
    assert.ok(SHARED.vectors.length >= 30);
    for (const v of SHARED.vectors) {
      const label = JSON.stringify(v.input);
      assert.equal(neutralizeExportCell(v.input), v.export, `export ${label}`);
      assert.equal(neutralizeFormulaCell(v.input), v.operator, `operator ${label}`);
      assert.equal(restoreExportCell(v.export), v.restored, `restore ${label}`);
    }
  });
});

describe("delimitedCell", () => {
  it("quotes the delimiter, quotes, CR and LF for any delimiter", () => {
    assert.equal(delimitedCell("a,b"), '"a,b"');
    assert.equal(delimitedCell("a\tb", { delimiter: "\t" }), '"a\tb"');
    assert.equal(delimitedCell("a\tb"), "a\tb");
    assert.equal(delimitedCell("x\ry"), '"x\ry"');
    assert.equal(delimitedCell('say "hi"'), '"say ""hi"""');
    assert.equal(delimitedCell("plain", { quoteAll: true }), '"plain"');
  });

  it("guards only when asked, and quotes after guarding", () => {
    assert.equal(delimitedCell("=SUM(A1,B1)"), '"=SUM(A1,B1)"');
    assert.equal(delimitedCell("=SUM(A1,B1)", { formulaProtection: true }), "\"'=SUM(A1,B1)\"");
    assert.equal(delimitedCell("-5", { formulaProtection: true }), "-5");
  });

  it("guards header names too", () => {
    assert.equal(
      toDelimitedText(["=hdr", "id"], [{ "=hdr": "@x", id: -5 }], { formulaProtection: true }),
      "'=hdr,id\n'@x,-5",
    );
  });
});
