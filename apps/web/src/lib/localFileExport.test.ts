import assert from "node:assert/strict";
import { describe, it } from "node:test";
import { serializeRows } from "./localFileExport";

/** Customer data-plane output is byte-exact — no formula-prefix mangling. */
describe("localFileExport byte fidelity", () => {
  const values = ["-5", "+442071234567", "=A1", "@x", "a\tb", '"q"', "line1\nline2"];
  const rows = values.map((v) => ({ cell: v }));

  it("csv round-trips every value verbatim", async () => {
    const { blob } = serializeRows(rows, "csv");
    const text = await blob.text();
    for (const v of values) {
      assert.ok(text.includes(v), `csv lost ${JSON.stringify(v)}: ${text}`);
      assert.ok(!text.includes(`'${v}`), `csv prefixed ${JSON.stringify(v)}`);
    }
  });

  it("tsv round-trips every value verbatim", async () => {
    const { blob } = serializeRows(rows, "tsv");
    const text = await blob.text();
    for (const v of values) {
      if (v.includes("\t") || v.includes("\n") || v.includes('"')) {
        assert.ok(text.includes(v), `tsv lost ${JSON.stringify(v)}`);
      } else {
        assert.ok(text.includes(v), `tsv lost ${JSON.stringify(v)}`);
        assert.ok(!text.includes(`'${v}`), `tsv prefixed ${JSON.stringify(v)}`);
      }
    }
  });

  it("json preserves the values verbatim", async () => {
    const { blob } = serializeRows(rows, "json");
    const parsed = JSON.parse(await blob.text()) as { cell: string }[];
    assert.deepEqual(parsed.map((r) => r.cell), values);
  });
});

/** G7-F2: the Studio destination's "Protect against spreadsheet formulas", offline too. */
describe("localFileExport formula protection", () => {
  const rows = [{ "=h": '=HYPERLINK("x")', n: "-5" }, { "=h": "@SUM(A1)", n: "-2+3" }];

  it("guards CSV cells and header names with the API rule when the destination opts in", async () => {
    const { blob } = serializeRows(rows, "csv", { formulaProtection: true });
    assert.equal(await blob.text(), "'=h,n\n\"'=HYPERLINK(\"\"x\"\")\",-5\n'@SUM(A1),'-2+3");
  });

  it("guards TSV and quotes a tab, CR or LF inside a value", async () => {
    const { blob } = serializeRows([{ a: "=1\t2", b: "x\ry" }], "tsv", { formulaProtection: true });
    assert.equal(await blob.text(), 'a\tb\n"\'=1\t2"\t"x\ry"');
  });

  it("stays byte-exact when the destination leaves protection off (the API default)", async () => {
    const { blob } = serializeRows(rows, "csv");
    assert.equal(await blob.text(), '=h,n\n"=HYPERLINK(""x"")",-5\n@SUM(A1),-2+3');
  });
});
