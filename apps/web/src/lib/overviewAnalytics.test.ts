/**
 * Run: npx --yes tsx --test apps/web/src/lib/overviewAnalytics.test.ts
 */
import assert from "node:assert/strict";
import { describe, it } from "node:test";
import { destProvenCount } from "./conservationLedger.js";
import { buildThroughputSeries } from "./overviewAnalytics.js";

function job(partial: Record<string, unknown>) {
  return {
    _id: String(partial._id || "j1"),
    status: String(partial.status || "completed"),
    created_at: String(partial.created_at || new Date().toISOString()),
    records_processed: Number(partial.records_processed ?? 0),
    ...partial,
  };
}

describe("Overview throughput uses conservation identity, not dest after", () => {
  it("counts append dest Δ, not dest COUNT(*) after", () => {
    const append = job({
      records_processed: 200,
      row_accounting: {
        rows_read: 200,
        rows_written: 200,
        rows_quarantined: 0,
        rows_skipped: 0,
        rows_coerced_null: 0,
        writer_ack: 200,
        dest_count: 300,
        dest_count_before: 100,
        dest_delta: 200,
        unaccounted: 0,
        balanced: true,
        rows_read_source: "gate8_source_count",
        rows_written_source: "gate8_dest_readback",
        conservation_kind: "append_delta",
        note: "dest Δ",
      },
    });
    assert.equal(destProvenCount(append), 200);
    const series = buildThroughputSeries([append as never], 1);
    assert.equal(series[0]?.rows, 200);
  });

  it("counts keyed dest Δ, not dest after", () => {
    const keyed = job({
      records_processed: 10,
      row_accounting: {
        rows_read: 10,
        rows_written: 1,
        rows_quarantined: 0,
        rows_skipped: 0,
        rows_coerced_null: 0,
        writer_ack: 10,
        dest_count: 31,
        dest_count_before: 30,
        dest_delta: 1,
        unaccounted: 0,
        balanced: true,
        rows_read_source: "gate8_source_count",
        rows_written_source: "gate8_dest_readback",
        conservation_kind: "keyed",
        note: "keyed Δ",
        inserts: 1,
        updates: 9,
        deletes: 0,
      },
    });
    assert.equal(destProvenCount(keyed), 1);
    const series = buildThroughputSeries([keyed as never], 1);
    assert.equal(series[0]?.rows, 1);
  });

  it("omits keyed dest after when dest Δ was not stamped", () => {
    const keyed = job({
      records_processed: 10,
      row_accounting: {
        rows_read: 10,
        rows_written: null,
        rows_quarantined: 0,
        rows_skipped: 0,
        rows_coerced_null: 0,
        writer_ack: 10,
        dest_count: 35,
        dest_count_before: 30,
        dest_delta: null,
        unaccounted: null,
        balanced: false,
        rows_read_source: "gate8_source_count",
        rows_written_source: "gate8_dest_readback",
        conservation_kind: "keyed",
        note: "no count identity",
      },
    });
    assert.equal(destProvenCount(keyed), null);
    const series = buildThroughputSeries([keyed as never], 1);
    assert.equal(series[0]?.rows, 0);
  });
});
