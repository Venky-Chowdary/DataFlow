/**
 * Gate-8 population helper — dest COUNT never closes with writer ack.
 * run: npx --yes tsx --test apps/web/src/lib/gate8Population.test.ts
 */
import assert from "node:assert/strict";
import { describe, it } from "node:test";
import {
  formatProofScope,
  controlTotalEvidenceLabel,
  controlTotalEvidenceTitle,
  readControlTotals,
  readGate8Population,
  readJobLineage,
} from "./gate8Population";

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

describe("readControlTotals", () => {
  it("shows both independent SUMs as exact strings when proven", () => {
    const view = readControlTotals({
      control_totals: {
        declared: true,
        evidence: "exact",
        columns: [
          {
            source: "amount",
            target: "amount",
            source_sum: "618.75",
            dest_sum: "618.75",
            matched: true,
            proven: true,
            reason: "independent source SUM equals destination SUM",
          },
        ],
      },
    });
    assert.equal(view.declared, true);
    assert.equal(view.proven, true);
    assert.equal(view.mismatch, false);
    // Strings, not numbers — a float SUM is the evidence G21 refuses.
    assert.equal(view.rows[0].sourceSum, "618.75");
    assert.equal(view.rows[0].destSum, "618.75");
  });

  it("never renders a sampled or unread SUM as proof", () => {
    const sampled = readControlTotals({
      control_totals: {
        declared: true,
        evidence: "sampled",
        columns: [
          { source: "amount", source_sum: "10.00", dest_sum: "10.00", matched: true, proven: false, reason: "a sample SUM is not a population control total" },
        ],
      },
    });
    assert.equal(sampled.declared, true);
    assert.equal(sampled.proven, false);
    assert.equal(sampled.evidence, "sampled");

    const unread = readControlTotals({
      control_totals: {
        declared: true,
        evidence: "unmeasured",
        any_unproven: true,
        columns: [
          { source: "amount", source_sum: null, dest_sum: null, proven: false, reason: "sum failed: cannot connect" },
        ],
      },
    });
    assert.equal(unread.proven, false);
    // No sum is an em-dash at render time, never a zero balance.
    assert.equal(unread.rows[0].destSum, "");
    assert.match(unread.rows[0].reason, /sum failed/);
  });

  it("reports a mismatch and stays hidden when nothing was declared", () => {
    const mismatch = readControlTotals({
      control_totals: {
        declared: true,
        evidence: "exact",
        any_mismatch: true,
        columns: [
          { source: "amount", source_sum: "618.75", dest_sum: "618.74", matched: false, proven: false, reason: "control total mismatch" },
        ],
      },
    });
    assert.equal(mismatch.mismatch, true);
    assert.equal(mismatch.proven, false);

    assert.equal(readControlTotals({ control_totals: { declared: false, columns: [] } }).declared, false);
    assert.equal(readControlTotals(null).declared, false);
  });
});

describe("controlTotalEvidenceLabel", () => {
  const view = (control_totals: Record<string, unknown>) =>
    readControlTotals({ control_totals } as never);

  it("names the population scan when the totals are exact", () => {
    assert.equal(
      controlTotalEvidenceLabel(
        view({
          declared: true,
          evidence: "exact",
          columns: [{ source: "amount", source_sum: "618.75", dest_sum: "618.75", proven: true, matched: true }],
        }),
      ),
      "population SUM, exact",
    );
  });

  it("never calls two disagreeing exact sums unmeasured", () => {
    // The engine writes `unmeasured` for any unproven run, mismatches
    // included — two cent-exact sums on screen were plainly measured.
    const label = controlTotalEvidenceLabel(
      view({
        declared: true,
        evidence: "unmeasured",
        any_mismatch: true,
        columns: [{ source: "amount", source_sum: "618.75", dest_sum: "618.76", proven: false, matched: false }],
      }),
    );
    assert.equal(label, "measured, sums disagree");
    assert.doesNotMatch(label, /unmeasured/);
    // The auditor tooltip keeps the engine token, and explains it rather
    // than contradicting the two measured sums beside it.
    const title = controlTotalEvidenceTitle(
      view({
        declared: true,
        evidence: "unmeasured",
        any_mismatch: true,
        columns: [{ source: "amount", source_sum: "618.75", dest_sum: "618.76", proven: false }],
      }),
    );
    assert.match(title, /^engine evidence token: unmeasured — /);
    assert.match(title, /both sums were measured and they disagree/);
  });

  it("says a sample is not proof, and says when no population sum ran", () => {
    const columns = [{ source: "amount", source_sum: "618.75", dest_sum: "618.75", proven: false }];
    assert.equal(
      controlTotalEvidenceLabel(view({ declared: true, evidence: "sampled", columns })),
      "sample SUM — not proof",
    );
    assert.equal(
      controlTotalEvidenceLabel(view({ declared: true, evidence: "unmeasured", columns })),
      "no exact population SUM",
    );
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
