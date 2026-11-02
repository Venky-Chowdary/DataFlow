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
