/**
 * Run: npx --yes tsx --test apps/web/src/lib/jobTheaterProgress.test.ts
 */
import assert from "node:assert/strict";
import { describe, it } from "node:test";
import {
  earliestJobStartMs,
  jobAverageRowsPerSecond,
  theaterProgressPct,
} from "./jobTheaterProgress.js";

describe("theaterProgressPct", () => {
  it("does not floor 0/1M during preflight to 1% (the 5%↔1% bounce)", () => {
    const pct = theaterProgressPct({
      phase: "preflight",
      progress_pct: 5,
      total_rows: 1_000_000,
      records_processed: 0,
      isRunning: true,
    });
    assert.equal(pct, 5);
  });

  it("keeps reading phase % before total is known", () => {
    const pct = theaterProgressPct({
      phase: "reading",
      progress_pct: 2,
      total_rows: 0,
      records_processed: 0,
      isRunning: true,
    });
    assert.equal(pct, 2);
  });

  it("uses written/total once the load is moving", () => {
    const pct = theaterProgressPct({
      phase: "writing",
      progress_pct: 15,
      total_rows: 1_000_000,
      records_processed: 40_000,
      isRunning: true,
    });
    assert.equal(pct, 4);
  });

  it("44,000 of 1,000,000 is 4% — never 44%", () => {
    const pct = theaterProgressPct({
      phase: "writing",
      progress_pct: 15,
      total_rows: 1_000_000,
      records_processed: 44_000,
      isRunning: true,
    });
    assert.equal(pct, 4);
    assert.notEqual(pct, 44);
  });

  it("460,000 of 1,000,000 is 46% — the live jurty job shape", () => {
    const pct = theaterProgressPct({
      phase: "writing",
      progress_pct: 50,
      total_rows: 1_000_000,
      records_processed: 460_000,
      isRunning: true,
    });
    assert.equal(pct, 46);
  });

  it("44% of 1,000,000 is 440,000 rows, not 44,000", () => {
    const pct = theaterProgressPct({
      phase: "writing",
      progress_pct: 50,
      total_rows: 1_000_000,
      records_processed: 440_000,
      isRunning: true,
    });
    assert.equal(pct, 44);
  });

  it("holds 99% in reconcile — never 100% before terminal success", () => {
    const pct = theaterProgressPct({
      phase: "reconcile",
      progress_pct: 99,
      total_rows: 1_000_000,
      records_processed: 1_000_000,
      reconciling: true,
      isRunning: true,
    });
    assert.equal(pct, 99);
  });
});

describe("earliestJobStartMs", () => {
  it("uses created_at when started_at was reset to now (Elapsed 0s on reconnect)", () => {
    const created = Date.parse("2026-08-27T12:07:56.000Z");
    const resetStarted = Date.parse("2026-08-27T12:37:41.000Z");
    const start = earliestJobStartMs({
      startedAt: "2026-08-27T12:37:41.000Z",
      createdAt: "2026-08-27T12:07:56.000Z",
      nowMs: resetStarted,
    });
    assert.equal(start, created);
    const elapsedMin = (resetStarted - start) / 60_000;
    assert.ok(elapsedMin > 25 && elapsedMin < 35);
  });
});

describe("jobAverageRowsPerSecond", () => {
  it("does not invent hundreds of thousands of rows/s on a 0.5s reconnect", () => {
    assert.equal(jobAverageRowsPerSecond(460_000, 500), 0);
  });

  it("reports job-average rps for 460k over ~27 minutes", () => {
    const rps = jobAverageRowsPerSecond(460_000, 27 * 60 * 1000);
    assert.ok(rps >= 270 && rps <= 300);
  });
});
