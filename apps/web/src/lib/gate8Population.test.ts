/**
 * Gate-8 population helper — dest COUNT never closes with writer ack.
 * run: npx --yes tsx --test apps/web/src/lib/gate8Population.test.ts
 */
import assert from "node:assert/strict";
import { describe, it } from "node:test";
import { formatProofScope, readGate8Population, readJobLineage } from "./gate8Population";

describe("readGate8Population", () => {
  it("prefers dest-engine ledger COUNT over dest_readback", () => {
    const view = readGate8Population({
      row_accounting: { dest_count: 40, dest_count_before: 10 },
      reconciliation: {
        target_rows: 99,
        dest_readback: { dest_count: 88, dest_checksum: "deadbeef", coverage: "full_checksum" },
        coverage: "full_checksum",
        source_checksum_provenance: "independent_source_reread",
      },
      preflight: { run_id: "pf-1" },
    });
    assert.equal(view.destCount, 40);
    assert.equal(view.destCountBefore, 10);
    assert.equal(view.destChecksum, "deadbeef");
    assert.equal(view.validateRunId, "pf-1");
    assert.equal(formatProofScope(view), "full_checksum · independent_source_reread");
  });

  it("falls back to dest_readback when ledger dest_count is missing", () => {
    const view = readGate8Population({
      reconciliation: {
        dest_readback: { dest_count: 7, dest_checksum: "abc", source: "gate8_dest_readback" },
      },
    });
    assert.equal(view.destCount, 7);
    assert.equal(view.source, "gate8_dest_readback");
  });

  it("does not invent a dest count from writer ack", () => {
    const view = readGate8Population({
      row_accounting: { dest_count: null, rows_written: 12, writer_ack: 12 },
      reconciliation: { coverage: "writer_ack" },
    });
    assert.equal(view.destCount, null);
    assert.equal(view.coverage, "writer_ack");
  });
});

describe("readJobLineage", () => {
  it("summarizes reconcile and quarantine without dumping mappings", () => {
    const rows = readJobLineage([
      {
        event_type: "reconciliation",
        timestamp: "2026-08-29T00:00:00Z",
        payload: { source_count: 10, target_count: 10, checksum_ok: true },
      },
      {
        event_type: "quarantine",
        payload: { quarantine_count: 2 },
      },
    ]);
    assert.equal(rows.length, 2);
    assert.match(rows[0].summary, /src 10/);
    assert.match(rows[1].summary, /q 2/);
  });
});
