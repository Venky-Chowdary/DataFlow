/**
 * Run: npx --yes tsx --test apps/web/src/lib/jobEventLog.test.ts
 */
import assert from "node:assert/strict";
import { describe, it } from "node:test";
import {
  eventLogMessageBody,
  isTerminalJobLogLine,
  keepEventLogStartAndTail,
  localizeServerEventLine,
  localizeServerEventLog,
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

describe("isTerminalJobLogLine", () => {
  it("matches a client-stamped completion once", () => {
    assert.equal(
      isTerminalJobLogLine("10:10:09 PM — Job completed — 5 writer-acked — dest COUNT unmeasured"),
      true,
    );
    assert.equal(isTerminalJobLogLine("10:10:04 PM — Batch 1/1 written"), false);
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

describe("localizeServerEventLine (QA 2026-11-07)", () => {
  const local = (y: number, mo: number, d: number, h: number, mi: number, se: number) =>
    new Date(Date.UTC(y, mo, d, h, mi, se)).toLocaleTimeString();

  it("shows a server UTC stamp in the viewer's local time, the theater's own format", () => {
    const now = new Date(Date.UTC(2026, 10, 7, 12, 5, 0));
    const line = "12:00:04 — Error: Destination name '1M Records' is not a plain SQL identifier.";
    assert.equal(
      localizeServerEventLine(line, now),
      `${local(2026, 10, 7, 12, 0, 4)} — Error: Destination name '1M Records' is not a plain SQL identifier.`,
    );
  });

  it("keeps the message body, so merging still de-duplicates against local lines", () => {
    const now = new Date(Date.UTC(2026, 10, 7, 12, 5, 0));
    const incoming = localizeServerEventLog(["12:00:01 — Entered queued phase"], now);
    assert.equal(eventLogMessageBody(incoming[0]), "Entered queued phase");
    const merged = mergeEventLogLines(["7:00:01 AM — Entered queued phase"], incoming);
    assert.equal(merged.length, 1);
  });

  it("puts a line from just before UTC midnight on the previous day", () => {
    const now = new Date(Date.UTC(2026, 10, 8, 0, 1, 0));
    assert.equal(
      localizeServerEventLine("23:59:30 — Batch 9/9 written", now),
      `${local(2026, 10, 7, 23, 59, 30)} — Batch 9/9 written`,
    );
  });

  it("leaves anything that is not a server stamp exactly as it is", () => {
    const now = new Date(Date.UTC(2026, 10, 7, 12, 5, 0));
    for (const line of [
      "7:00:03 AM — Entered validate phase", // the theater's own en-US stamp
      "no stamp at all",
      "25:00:00 — not a time of day",
      "t — line 1",
    ]) {
      assert.equal(localizeServerEventLine(line, now), line);
    }
    assert.deepEqual(localizeServerEventLog(undefined, now), []);
  });
});
