/**
 * Run: npx --yes tsx --test apps/web/src/lib/jobEvidence.test.ts
 */
import assert from "node:assert/strict";
import { describe, it } from "node:test";
import {
  readCoercedNullRows,
  readJobStreams,
  readRejectedDetails,
  readRejectedRows,
} from "./jobEvidence.js";

const orders = {
  name: "orders",
  status: "completed",
  records_processed: 50,
  watermark: "50",
  row_accounting: { conservation_kind: "overwrite", dest_count: 50, balanced: true },
};
const users = {
  name: "users",
  status: "completed",
  records_processed: 3,
  cdc_lag_seconds: 1.2,
};

describe("readJobStreams", () => {
  it("reads stream health that lives only on the destination summary", () => {
    const streams = readJobStreams({
      destination_summary: { streams: [orders, users] },
    });
    assert.deepEqual(streams.map((s) => s.name), ["orders", "users"]);
    assert.equal(streams[0].records_processed, 50);
    assert.equal(streams[0].watermark, "50");
    assert.equal(streams[1].cdc_lag_seconds, 1.2);
    assert.equal(streams[0].row_accounting?.conservation_kind, "overwrite");
  });

  it("prefers the summary list when the top-level list is a different copy", () => {
    const streams = readJobStreams({
      streams: [{ name: "stale", status: "running" }],
      destination_summary: { streams: [orders] },
    });
    assert.deepEqual(streams.map((s) => s.name), ["orders"]);
  });

  it("uses the top-level list when the summary has no streams", () => {
    const streams = readJobStreams({
      streams: [users],
      destination_summary: { rejected_rows: 1 },
    });
    assert.deepEqual(streams.map((s) => s.name), ["users"]);
  });

  it("keeps a single named stream", () => {
    const streams = readJobStreams({ destination_summary: { streams: [orders] } });
    assert.equal(streams.length, 1);
    assert.equal(streams[0].name, "orders");
  });

  it("drops unnamed rows and empty payloads", () => {
    assert.deepEqual(readJobStreams(null), []);
    assert.deepEqual(readJobStreams({ destination_summary: { streams: [] } }), []);
    assert.deepEqual(
      readJobStreams({ destination_summary: { streams: [{ status: "completed" }, { name: "  " }] } }),
      [],
    );
  });

  it("accepts a stream key when name is absent", () => {
    const streams = readJobStreams({
      destination_summary: { streams: [{ stream: "payments", status: "failed", error: "timeout" }] },
    });
    assert.equal(streams[0].name, "payments");
    assert.equal(streams[0].error, "timeout");
  });
});

describe("quarantine evidence", () => {
  it("reads rejected and coerced counts from the summary when the job root is empty", () => {
    const job = { destination_summary: { rejected_rows: 4, coerced_null_rows: 1 } };
    assert.equal(readRejectedRows(job), 4);
    assert.equal(readCoercedNullRows(job), 1);
  });

  it("keeps a stamped top-level count", () => {
    const job = {
      rejected_rows: 2,
      destination_summary: { rejected_rows: 9 },
    };
    assert.equal(readRejectedRows(job), 2);
  });

  it("shows summary findings when the job root has no sample", () => {
    const details = readRejectedDetails({
      destination_summary: {
        rejected_details: [{ column: "qty", reason: "not a number", value: "x" }],
      },
    });
    assert.equal(details?.length, 1);
    assert.equal(details?.[0].column, "qty");
  });
});
