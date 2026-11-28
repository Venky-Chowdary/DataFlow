/**
 * Lead item 3: one compact "How this was checked" under the result header —
 * sentence case, no raw labels ("Dest COUNT", "Proof scope unmeasured · —").
 */
import assert from "node:assert/strict";
import { describe, it } from "node:test";
import { runCheckRows } from "./RunCheckSummary";
import { readGate8Population } from "../../lib/gate8Population";

describe("How this was checked", () => {
  it("a full-checksum run names the comparison, the counted destination and a match", () => {
    const reconciliation = { passed: true, coverage: "full_checksum", source_rows: 1200000, target_rows: 1199660, source_checksum: "sha256:9f2c1e0a77b4", target_checksum: "sha256:9f2c1e0a77b4" };
    const population = readGate8Population({ reconciliation, row_accounting: { dest_count: 1199660 }, preflight: { run_id: "pf_1" } as never });
    const rows = runCheckRows({ reconciliation, population });
    assert.deepEqual(rows.map((r) => [r.label, r.value]), [
      ["Comparison", "Full checksum of every row, after the write"],
      ["Destination count", "1,199,660 rows, counted in the destination"],
      ["Checksum", "Source and destination match"],
      ["Validate run", ""],
    ]);
    for (const r of rows) assert.doesNotMatch(`${r.label} ${r.value}`, /COUNT|Proof scope|unmeasured|—/);
  });

  it("no reconciliation: says no comparison ran and what the writer reported", () => {
    const population = readGate8Population({ row_accounting: { writer_ack: 1200000 } });
    const rows = runCheckRows({ reconciliation: null, population, writerRows: 1200000 });
    assert.equal(rows[0].value, "No comparison ran");
    assert.equal(rows[1].value, "Not counted. The writer reported 1,200,000 rows.");
    assert.equal(rows[2].value, "Not computed");
  });
});
