/**
 * Run: npx --yes tsx --test apps/web/src/lib/conservationLedger.test.ts
 */
import assert from "node:assert/strict";
import { describe, it } from "node:test";
import {
  destHeadline,
  destProvenCount,
  destMetricCompact,
  formatJobRowMetric,
  isDestMeasured,
  ledgerEquation,
  ledgerIdentityCells,
  conservationCompleteCopy,
  readConservationLedger,
  writerAckDisagrees,
  writerHeadline,
} from "./conservationLedger.js";

const overwriteLedger = {
  rows_read: 4,
  rows_written: 4,
  rows_quarantined: 0,
  rows_skipped: 0,
  rows_coerced_null: 0,
  writer_ack: 10_000,
  dest_count: 4,
  dest_count_before: 0,
  unaccounted: 0,
  balanced: true,
  rows_read_source: "gate8_source_count",
  rows_written_source: "gate8_dest_readback",
  conservation_kind: "overwrite",
  note: "Dest COUNT(*) closes the identity. Writer ack is diagnostic.",
  writer_ack_delta: -9996,
  inserts: null,
  updates: null,
  deletes: null,
  dest_delta: null,
  unique_batch_keys: null,
  dest_preexisting: null,
  active_count: null,
  inferred_deletes: null,
  reactivated: null,
  events_read: null,
};

describe("readConservationLedger", () => {
  it("returns null when the server did not stamp a ledger", () => {
    assert.equal(readConservationLedger({ records_processed: 1000 }), null);
    assert.equal(readConservationLedger({ row_accounting: {} }), null);
    assert.equal(readConservationLedger(null), null);
  });

  it("reads dest COUNT independently of writer ack", () => {
    const ledger = readConservationLedger({ row_accounting: overwriteLedger });
    assert.ok(ledger);
    assert.equal(ledger.dest_count, 4);
    assert.equal(ledger.writer_ack, 10_000);
    assert.equal(isDestMeasured(ledger), true);
    assert.equal(writerAckDisagrees(ledger), true);
  });
});

describe("destHeadline never falls back to writer ack", () => {
  it("shows dest COUNT when measured even if writer claimed 10,000", () => {
    const h = destHeadline({
      status: "completed",
      records_processed: 10_000,
      row_accounting: overwriteLedger,
    });
    assert.equal(h.value, "4");
    assert.equal(h.measured, true);
    assert.equal(h.label, "At destination");
  });

  it("shows em dash when dest is unmeasured — never invents dest = writer ack", () => {
    const h = destHeadline({
      status: "completed",
      records_processed: 10_000,
    });
    assert.equal(h.value, "—");
    assert.equal(h.measured, false);
    assert.equal(h.label, "Dest unmeasured");
    assert.equal(destProvenCount({ records_processed: 10_000 }), null);
  });

  it("treats measured empty pass zero as dest 0, not unmeasured", () => {
    const h = destHeadline({
      status: "completed",
      records_processed: 0,
      row_accounting: {
        ...overwriteLedger,
        rows_read: 0,
        rows_written: 0,
        writer_ack: 0,
        dest_count: 0,
        writer_ack_delta: 0,
        conservation_kind: "empty_pass",
        rows_written_source: "empty_pass",
        note: "Measured empty pass",
      },
    });
    assert.equal(h.value, "0");
    assert.equal(h.measured, true);
  });

  it("does not treat unmeasured kind as dest proof even if dest_count is stuffed", () => {
    const h = destHeadline({
      status: "completed",
      records_processed: 10_000,
      row_accounting: {
        ...overwriteLedger,
        dest_count: 10_000,
        conservation_kind: "unmeasured",
        rows_written_source: "unmeasured",
      },
    });
    assert.equal(h.measured, false);
    assert.equal(h.value, "—");
  });
});

describe("writerHeadline is diagnostic", () => {
  it("surfaces writer ack separately from dest", () => {
    const w = writerHeadline({
      status: "completed",
      records_processed: 10_000,
      row_accounting: overwriteLedger,
    });
    assert.equal(w.value, "10,000");
    assert.equal(w.label, "Writer ack");
    assert.equal(w.tone, "warn");
  });

  it("labels in-flight jobs as written so far, not at destination", () => {
    const w = writerHeadline({ status: "running", records_processed: 12 });
    assert.equal(w.value, "12");
    assert.equal(w.label, "Written so far");
    const d = destHeadline({ status: "running", records_processed: 12 });
    assert.equal(d.value, "—");
  });
});

describe("formatJobRowMetric", () => {
  it("prefers dest COUNT on the jobs list", () => {
    const m = formatJobRowMetric({
      status: "completed",
      records_processed: 10_000,
      row_accounting: overwriteLedger,
    });
    assert.equal(m.value, "4");
    assert.equal(m.measured, true);
  });
});

describe("ledgerEquation is display-only", () => {
  it("renders overwrite identity from engine fields", () => {
    const eq = ledgerEquation(overwriteLedger);
    assert.match(eq, /read 4/);
    assert.match(eq, /dest 4/);
  });
});

describe("conservationCompleteCopy", () => {
  it("names dest COUNT, not writer ack, on success", () => {
    const copy = conservationCompleteCopy({
      status: "completed",
      records_processed: 10_000,
      row_accounting: overwriteLedger,
    });
    assert.match(copy, /4 at destination/);
    assert.doesNotMatch(copy, /10,000/);
    assert.doesNotMatch(copy, /transferred/);
  });

  it("refuses to say transferred when dest is unmeasured", () => {
    const copy = conservationCompleteCopy({
      status: "completed",
      records_processed: 10_000,
    });
    assert.match(copy, /unmeasured/);
    assert.doesNotMatch(copy, /transferred/);
  });
});

describe("ledgerIdentityCells", () => {
  it("shows dest COUNT as its own cell, not writer ack", () => {
    const cells = ledgerIdentityCells(overwriteLedger);
    const dest = cells.find((c) => c.label.includes("Dest"));
    assert.equal(dest?.value, "4");
  });
});

const mirrorLedger = {
  ...overwriteLedger,
  rows_read: 3,
  rows_written: 3,
  writer_ack: 10_000,
  dest_count: 4,
  dest_count_before: 3,
  unaccounted: 0,
  balanced: true,
  rows_written_source: "gate8_dest_active_readback",
  conservation_kind: "mirror",
  note: "Mirror active population closed. Physical COUNT(*) does not drop.",
  writer_ack_delta: -9997,
  active_count: 3,
  inferred_deletes: 1,
  reactivated: 0,
  deletes: 1,
};

describe("mirror active population is dest headline, not physical COUNT(*)", () => {
  it("shows active 3 when physical is 4 and writer claimed 10,000", () => {
    const job = {
      status: "completed",
      records_processed: 10_000,
      row_accounting: mirrorLedger,
    };
    const h = destHeadline(job);
    assert.equal(h.value, "3");
    assert.equal(h.measured, true);
    assert.equal(h.label, "Active at dest");
    assert.equal(destProvenCount(job), 3);
    assert.equal(formatJobRowMetric(job).value, "3");
    assert.equal(destMetricCompact(destHeadline(job)), "3 active");
    const copy = conservationCompleteCopy(job);
    assert.match(copy, /3 active at destination/);
    assert.doesNotMatch(copy, /10,000/);
    const cells = ledgerIdentityCells(mirrorLedger);
    assert.equal(cells.find((c) => c.label === "Active")?.value, "3");
    assert.equal(cells.find((c) => c.label === "Physical COUNT(*)")?.value, "4");
    assert.equal(cells.find((c) => c.label === "Inferred deletes")?.value, "1");
    assert.match(ledgerEquation(mirrorLedger), /active 3/);
  });

  it("treats stream-path active census as measured even when physical COUNT is unknown", () => {
    const h = destHeadline({
      status: "completed",
      records_processed: 10_000,
      row_accounting: {
        ...mirrorLedger,
        dest_count: null,
        inferred_deletes: null,
        reactivated: null,
      },
    });
    assert.equal(h.value, "3");
    assert.equal(h.measured, true);
  });
});

describe("keyed ledger shows events vs keys, never closes on event count", () => {
  it("surfaces 10 events and 3 keys from the engine census", () => {
    const cells = ledgerIdentityCells({
      ...overwriteLedger,
      conservation_kind: "keyed",
      dest_delta: 0,
      inserts: 0,
      updates: 3,
      deletes: 0,
      unique_batch_keys: 3,
      dest_preexisting: 3,
      events_read: 10,
      writer_ack: 10_000,
    });
    assert.equal(cells.find((c) => c.label === "Events")?.value, "10");
    assert.equal(cells.find((c) => c.label === "Keys")?.value, "3");
    assert.equal(cells.find((c) => c.label === "Dest Δ")?.value, "0");
  });
});
