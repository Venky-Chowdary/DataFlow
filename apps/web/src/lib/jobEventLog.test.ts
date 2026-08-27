/**
 * Run: npx --yes tsx --test apps/web/src/lib/jobEventLog.test.ts
 */
import assert from "node:assert/strict";
import { describe, it } from "node:test";
import {
  eventLogMessageBody,
  keepEventLogStartAndTail,
  mergeEventLogLines,
} from "./jobEventLog.js";

describe("mergeEventLogLines", () => {
  it("keeps the start of the run when the server only sends a tail", () => {
    const local = [
      "10:00:01 — Connecting to live job stream…",
      "10:00:02 — Entered extract phase",
      "10:00:03 — 10,000 rows processed",
    ];
    const incoming = [
      "10:01:00 — 10,000 rows processed",
      "10:01:01 — Entered write phase",
    ];
    const merged = mergeEventLogLines(local, incoming);
    assert.equal(eventLogMessageBody(merged[0]), "Connecting to live job stream…");
    assert.ok(merged.some((l) => eventLogMessageBody(l) === "Entered extract phase"));
    assert.ok(merged.some((l) => eventLogMessageBody(l) === "Entered write phase"));
  });

  it("hydrates from a longer server history when the client only has boot", () => {
    const local = ["10:00:00 — Connecting to live job stream…"];
    const incoming = [
      "09:59:00 — Entered extract phase",
      "09:59:10 — Batch 1/50 written",
    ];
    const merged = mergeEventLogLines(local, incoming);
    assert.equal(eventLogMessageBody(merged[0]), "Entered extract phase");
    assert.ok(merged.some((l) => l.includes("Connecting")));
  });
});

describe("keepEventLogStartAndTail", () => {
  it("never drops the first lines when clipping", () => {
    const lines = Array.from({ length: 100 }, (_, i) => `t — line ${i}`);
    const kept = keepEventLogStartAndTail(lines, 20);
    assert.equal(kept[0], "t — line 0");
    assert.ok(kept.some((l) => l.includes("clipped")));
    assert.equal(kept[kept.length - 1], "t — line 99");
  });
});
