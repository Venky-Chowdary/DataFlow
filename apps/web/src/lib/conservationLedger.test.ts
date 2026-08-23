/**
 * Run: npx --yes tsx --test apps/web/src/lib/conservationLedger.test.ts
 */
import assert from "node:assert/strict";
import { describe, it } from "node:test";
import {
  destHeadline,
  destProvenCount,
  destMetricCompact,
  destMetricToneClass,
  formatJobRowMetric,
  isDestMeasured,
  ledgerEquation,
  ledgerIdentityCells,
  conservationCompleteCopy,
  readConservationLedger,
  removedOnRead,
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
    assert.equal(destMetricToneClass(h), "is-dest");
  });

  it("shows artifact record count, never 'at destination table', when dest is a file", () => {
    const artifactLedger = {
      ...overwriteLedger,
      dest_count: 3,
      rows_written: 3,
      rows_read: 3,
      writer_ack: 10_000,
      writer_ack_delta: -9997,
      rows_written_source: "artifact_readback",
      note: "Every source row is in the export artifact (independent record count).",
    };
    const h = destHeadline({
      status: "completed",
      records_processed: 10_000,
      row_accounting: artifactLedger,
    });
    assert.equal(h.value, "3");
    assert.equal(h.measured, true);
    assert.equal(h.label, "In export artifact");
    assert.equal(destMetricCompact(h), "3 in artifact");
    const copy = conservationCompleteCopy({
      status: "completed",
      records_processed: 10_000,
      row_accounting: artifactLedger,
    });
    assert.match(copy, /3 in export artifact/);
    assert.doesNotMatch(copy, /at destination/);
    const cells = ledgerIdentityCells(artifactLedger);
    assert.equal(cells.find((c) => c.label === "Artifact records")?.value, "3");
    assert.equal(cells.find((c) => c.label === "Dest COUNT(*)"), undefined);
    assert.match(ledgerEquation(artifactLedger), /artifact 3/);
  });

  it("shows identity COUNT, never vector COUNT(*) or 'at destination table', for RAG dest", () => {
    const vectorLedger = {
      ...overwriteLedger,
      dest_count: 2,
      rows_written: 2,
      rows_read: 2,
      writer_ack: 10_000,
      writer_ack_delta: -9998,
      rows_written_source: "identity_readback",
      conservation_kind: "vector",
      identity_count: 2,
      vector_rows: 5,
      note: "Vector identity closed: dest-engine COUNT(DISTINCT source_id) = 2.",
    };
    const h = destHeadline({
      status: "completed",
      records_processed: 10_000,
      row_accounting: vectorLedger,
    });
    assert.equal(h.value, "2");
    assert.equal(h.measured, true);
    assert.equal(h.label, "Identities at dest");
    assert.equal(destMetricCompact(h), "2 identities");
    const copy = conservationCompleteCopy({
      status: "completed",
      records_processed: 10_000,
      row_accounting: vectorLedger,
    });
    assert.match(copy, /2 identities at dest/);
    assert.doesNotMatch(copy, /at destination/);
    assert.doesNotMatch(copy, /10,000/);
    const cells = ledgerIdentityCells(vectorLedger);
    assert.equal(cells.find((c) => c.label === "Identities")?.value, "2");
    assert.equal(cells.find((c) => c.label === "Vectors")?.value, "5");
    assert.equal(cells.find((c) => c.label === "Writer ack")?.value, "10,000");
    assert.equal(cells.find((c) => c.label === "Dest COUNT(*)"), undefined);
    assert.match(ledgerEquation(vectorLedger), /identities 2/);
    assert.equal(destProvenCount({ row_accounting: vectorLedger }), 2);
  });

  it("shows current COUNT, never history COUNT(*) or 'Active at dest', for SCD2 dest", () => {
    const scd2Ledger = {
      ...overwriteLedger,
      dest_count: 2,
      rows_written: 2,
      rows_read: 2,
      writer_ack: 10_000,
      writer_ack_delta: -9998,
      rows_written_source: "current_readback",
      conservation_kind: "scd2",
      current_count: 2,
      history_rows: 3,
      active_count: null,
      note: "SCD2 current-row identity closed: dest-engine COUNT(*) WHERE is_current = 2.",
    };
    const h = destHeadline({
      status: "completed",
      records_processed: 10_000,
      row_accounting: scd2Ledger,
    });
    assert.equal(h.value, "2");
    assert.equal(h.measured, true);
    assert.equal(h.label, "Current at dest");
    assert.equal(destMetricCompact(h), "2 current");
    const copy = conservationCompleteCopy({
      status: "completed",
      records_processed: 10_000,
      row_accounting: scd2Ledger,
    });
    assert.match(copy, /2 current at dest/);
    assert.doesNotMatch(copy, /at destination/);
    assert.doesNotMatch(copy, /Active at dest/);
    assert.doesNotMatch(copy, /10,000/);
    const cells = ledgerIdentityCells(scd2Ledger);
    assert.equal(cells.find((c) => c.label === "Current")?.value, "2");
    assert.equal(cells.find((c) => c.label === "History")?.value, "3");
    assert.equal(cells.find((c) => c.label === "Writer ack")?.value, "10,000");
    assert.equal(cells.find((c) => c.label === "Dest COUNT(*)"), undefined);
    assert.equal(cells.find((c) => c.label === "Active"), undefined);
    assert.match(ledgerEquation(scd2Ledger), /current 2/);
    assert.equal(destProvenCount({ row_accounting: scd2Ledger }), 2);
    assert.equal(isDestMeasured(scd2Ledger), true);
  });

  it("surfaces MISSING_TARGET and EXTRA_TARGET keys when COUNT(*) would net them", () => {
    const keysetLedger = {
      ...overwriteLedger,
      dest_count: 3,
      rows_read: 3,
      rows_written: 3,
      unaccounted: 0,
      balanced: false,
      missing_keys: 1,
      extra_keys: 1,
      writer_ack: 10_000,
      writer_ack_delta: -9997,
      note: "Dest-engine keyset: 1 MISSING_TARGET key(s), 1 EXTRA_TARGET leftover dest key(s).",
    };
    const job = {
      status: "completed",
      records_processed: 10_000,
      row_accounting: keysetLedger,
    };
    const h = destHeadline(job);
    assert.equal(h.value, "3");
    assert.equal(h.measured, true);
    assert.equal(h.tone, "danger");
    assert.equal(isDestMeasured(keysetLedger), true);
    const cells = ledgerIdentityCells(keysetLedger);
    assert.equal(cells.find((c) => c.label === "Missing keys")?.value, "1");
    assert.equal(cells.find((c) => c.label === "Extra dest keys")?.value, "1");
    assert.equal(cells.find((c) => c.label === "Dest COUNT(*)")?.value, "3");
    assert.doesNotMatch(conservationCompleteCopy(job), /10,000/);
  });

  it("surfaces leftover deleted after complete-snapshot MERGE, never writer ack", () => {
    const mergeLedger = {
      ...overwriteLedger,
      dest_count: 3,
      rows_read: 3,
      rows_written: 3,
      unaccounted: 0,
      balanced: true,
      missing_keys: 0,
      extra_keys: 0,
      leftover_deleted: 1,
      writer_ack: 10_000,
      writer_ack_delta: -9997,
      note: "Dest-engine MERGE deleted 1 leftover dest key(s) not in the complete source snapshot.",
    };
    const job = {
      status: "completed",
      records_processed: 10_000,
      row_accounting: mergeLedger,
    };
    const h = destHeadline(job);
    assert.equal(h.value, "3");
    assert.equal(h.measured, true);
    assert.doesNotMatch(h.label, /Active at dest/);
    const cells = ledgerIdentityCells(mergeLedger);
    assert.equal(cells.find((c) => c.label === "Extra dest keys")?.value, "0");
    assert.equal(cells.find((c) => c.label === "Leftover deleted")?.value, "1");
    assert.doesNotMatch(conservationCompleteCopy(job), /10,000/);
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
    const keyedPayload = {
      ...overwriteLedger,
      conservation_kind: "keyed",
      dest_delta: 0,
      dest_count_before: 3,
      dest_count: 3,
      inserts: 0,
      updates: 3,
      deletes: 0,
      unique_batch_keys: 3,
      dest_preexisting: 3,
      events_read: 10,
      writer_ack: 10_000,
    };
    const cells = ledgerIdentityCells(keyedPayload);
    assert.equal(cells.find((c) => c.label === "Events")?.value, "10");
    assert.equal(cells.find((c) => c.label === "Keys")?.value, "3");
    assert.equal(cells.find((c) => c.label === "Dest Δ")?.value, "0");
    assert.equal(cells.find((c) => c.label === "Dest before")?.value, "3");
    assert.equal(cells.find((c) => c.label === "Dest after")?.value, "3");
    const h = destHeadline({ status: "completed", records_processed: 10, row_accounting: keyedPayload });
    assert.equal(h.value, "0");
    assert.equal(h.label, "Dest Δ this run");
    assert.equal(destMetricCompact(h), "0 dest Δ");
    assert.equal(destProvenCount({
      status: "completed",
      records_processed: 10,
      row_accounting: {
        ...overwriteLedger,
        conservation_kind: "keyed",
        dest_delta: 1,
        dest_count_before: 30,
        dest_count: 31,
        inserts: 1,
        updates: 2,
        deletes: 0,
        unique_batch_keys: 3,
        dest_preexisting: 30,
        events_read: 10,
        writer_ack: 10,
      },
    }), 1);
  });
});

describe("job rollup never takes last-stream dest COUNT(*)", () => {
  const jobLedger = {
    ...overwriteLedger,
    conservation_kind: "job_rollup",
    dest_count: 5,
    rows_read: 5,
    rows_written: 5,
    writer_ack: 10_000,
    writer_ack_delta: -9995,
    stream_count: 2,
    measured_streams: 2,
    summable: true,
    per_stream: [
      { stream: "customers", measured: true, balanced: true, conservation_kind: "overwrite", dest_count: 2, active_count: null, rows_read: 2 },
      { stream: "orders", measured: true, balanced: true, conservation_kind: "overwrite", dest_count: 3, active_count: null, rows_read: 3 },
    ],
    note: "Job conservation closed across 2 overwrite stream(s).",
  };

  it("headlines 5 at dest when last table held 3 and writer claimed 10,000", () => {
    const job = { status: "completed", records_processed: 10_000, row_accounting: jobLedger };
    const h = destHeadline(job);
    assert.equal(h.value, "5");
    assert.equal(h.measured, true);
    assert.equal(destProvenCount(job), 5);
    assert.equal(destMetricCompact(h), "5 at dest");
    assert.match(conservationCompleteCopy(job), /5 at destination/);
    assert.doesNotMatch(conservationCompleteCopy(job), /10,000/);
    const cells = ledgerIdentityCells(jobLedger);
    assert.equal(cells.find((c) => c.label === "customers")?.value, "2");
    assert.equal(cells.find((c) => c.label === "orders")?.value, "3");
  });

  it("does not invent a dest number for mixed kinds", () => {
    const mixed = {
      ...jobLedger,
      dest_count: null,
      rows_written: null,
      active_count: null,
      summable: false,
      rows_written_source: "per_stream",
      balanced: true,
    };
    const h = destHeadline({ status: "completed", records_processed: 10_000, row_accounting: mixed });
    assert.equal(h.value, "—");
    assert.equal(h.measured, true);
    assert.equal(destMetricCompact(h), "per-stream dest");
    assert.equal(destProvenCount({ row_accounting: mixed }), null);
  });
});

const appendLedger = {
  ...overwriteLedger,
  rows_read: 200,
  rows_written: 200,
  writer_ack: 200,
  dest_count: 300,
  dest_count_before: 100,
  dest_delta: 200,
  unaccounted: 0,
  balanced: true,
  conservation_kind: "append_delta",
  note: "This run's dest COUNT(*) growth is 200 (100 → 300). Pre-existing dest rows remain.",
  writer_ack_delta: 0,
};

describe("append dest headline is dest Δ, not dest after", () => {
  it("shows this run's growth, not dest COUNT(*) after", () => {
    const job = {
      status: "completed",
      records_processed: 200,
      row_accounting: appendLedger,
    };
    const h = destHeadline(job);
    assert.equal(h.value, "200");
    assert.equal(h.label, "Appended this run");
    assert.equal(h.tone, "warn");
    assert.equal(destMetricCompact(h), "200 appended");
    assert.equal(destMetricToneClass(h), "is-dest is-warn");
    assert.equal(destProvenCount(job), 200);
    assert.equal(destMetricToneClass(h), "is-dest is-warn");
    assert.match(conservationCompleteCopy(job), /200 appended this run/);
    assert.match(conservationCompleteCopy(job), /100 → 300/);
    assert.doesNotMatch(conservationCompleteCopy(job), /300 at destination/);
    assert.equal(writerAckDisagrees(job), false);
    const cells = ledgerIdentityCells(appendLedger);
    assert.equal(cells.find((c) => c.label === "Dest Δ")?.value, "200");
    assert.equal(cells.find((c) => c.label === "Dest before")?.value, "100");
    assert.equal(cells.find((c) => c.label === "Dest after")?.value, "300");
    assert.match(ledgerEquation(appendLedger), /dest Δ 200/);
  });

  it("does not treat dest after as writer disagreement when dest Δ matches ack", () => {
    const noDeltaField = {
      ...appendLedger,
      dest_delta: null,
      writer_ack_delta: null,
    };
    assert.equal(writerAckDisagrees({ row_accounting: noDeltaField }), false);
    const h = destHeadline({ status: "completed", row_accounting: noDeltaField });
    assert.equal(h.value, "200");
  });
});

describe("shaped rows are named, never drawn as silent loss", () => {
  const shapedLedger = {
    ...overwriteLedger,
    rows_read: 5,
    rows_written: 2,
    dest_count: 2,
    unaccounted: 0,
    balanced: true,
    writer_ack: 2,
    writer_ack_delta: 0,
    rows_shaped_out: 2,
    rows_source_filtered: 1,
    shape_recipe_hash: "abc123def4567890",
  };

  it("parses the engine shape terms without inventing them", () => {
    const ledger = readConservationLedger({ row_accounting: shapedLedger });
    assert.ok(ledger);
    assert.equal(ledger.rows_shaped_out, 2);
    assert.equal(ledger.rows_source_filtered, 1);
    assert.equal(ledger.shape_recipe_hash, "abc123def4567890");
    const plain = readConservationLedger({ row_accounting: overwriteLedger });
    assert.ok(plain);
    assert.equal(plain.rows_shaped_out, 0);
    assert.equal(plain.rows_source_filtered, 0);
    assert.equal(plain.shape_recipe_hash, "");
  });

  it("states the removed rows inside the identity", () => {
    const ledger = readConservationLedger({ row_accounting: shapedLedger });
    assert.ok(ledger);
    assert.equal(removedOnRead(ledger), 3);
    assert.match(ledgerEquation(ledger), /removed on read 3/);
  });

  it("leaves a plain transfer identity untouched", () => {
    const ledger = readConservationLedger({ row_accounting: overwriteLedger });
    assert.ok(ledger);
    assert.equal(removedOnRead(ledger), 0);
    assert.doesNotMatch(ledgerEquation(ledger), /removed on read/);
  });

  it("names each removal authority and the recipe that ran", () => {
    const ledger = readConservationLedger({ row_accounting: shapedLedger });
    assert.ok(ledger);
    const cells = ledgerIdentityCells(ledger);
    const labels = cells.map((c) => c.label);
    assert.ok(labels.includes("Filtered on read"));
    assert.ok(labels.includes("Removed by transform"));
    assert.equal(cells.find((c) => c.label === "Removed by transform")?.value, "2");
    assert.equal(cells.find((c) => c.label === "Transform recipe")?.value, "abc123def4567890");
  });

  it("draws no transform row when nothing was removed", () => {
    const ledger = readConservationLedger({ row_accounting: overwriteLedger });
    assert.ok(ledger);
    const labels = ledgerIdentityCells(ledger).map((c) => c.label);
    assert.ok(!labels.includes("Removed by transform"));
    assert.ok(!labels.includes("Filtered on read"));
    assert.ok(!labels.includes("Transform recipe"));
  });

  it("keeps a shaped run measured and balanced", () => {
    const metric = destHeadline({ status: "completed", row_accounting: shapedLedger });
    assert.equal(metric.measured, true);
    assert.equal(metric.value, "2");
  });
});
