import assert from "node:assert/strict";
import { test } from "node:test";
import { journalRowsLabel, journalSourceLabel, journalStorageBadge } from "./JournalsDrawer";

test("a mirrored journal is labelled shared storage", () => {
  const badge = journalStorageBadge({ durable: true, backend: "s3", uri: "s3://b/journals/ws/j/" });
  assert.equal(badge.label, "Shared storage");
  assert.match(badge.detail, /s3/);
});

test("a host-local journal says why it is not durable — never green by default", () => {
  const reason = "no shared object store is configured (S3_BUCKET or BLOB_ROOT)";
  assert.equal(journalStorageBadge({ durable: false, reason }).label, "This host only");
  assert.equal(journalStorageBadge({ durable: false, reason }).detail, reason);
  assert.equal(journalStorageBadge(undefined).label, "This host only");
});

test("source label names the engine and object", () => {
  assert.equal(journalSourceLabel({ journal_id: "j", source: { format: "sqlite", object: "orders" } }), "sqlite · orders");
  assert.equal(journalSourceLabel({ journal_id: "j", source: { kind: "file" } }), "file");
});

test("a file journal is not shown as 0 rows", () => {
  assert.equal(journalRowsLabel({ journal_id: "j", streams: [{ name: "a.csv", format: "csv", rows: 0 }] }), "Source file");
  assert.equal(journalRowsLabel({ journal_id: "j", streams: [{ name: "t", format: "jsonl", rows: 1200 }] }), (1200).toLocaleString());
  assert.equal(journalRowsLabel({ journal_id: "j", streams: [{ name: "t", format: "jsonl", rows: 0 }] }), "0");
});
