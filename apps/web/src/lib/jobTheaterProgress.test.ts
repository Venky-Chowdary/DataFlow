/**
 * Run: npx --yes tsx --test apps/web/src/lib/jobTheaterProgress.test.ts
 */
import assert from "node:assert/strict";
import { describe, it } from "node:test";
import {
  earliestJobStartMs,
  jobAverageRowsPerSecond,
  publishedEngineElapsedSeconds,
  theaterElapsedMs,
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

  it("paints checksum tenths during reconcile and never reaches 100", () => {
    const pct = theaterProgressPct({
      phase: "reconcile",
      progress_pct: 99,
      reconcile_pct: 40,
      total_rows: 1_000_000,
      records_processed: 1_000_000,
      reconciling: true,
      isRunning: true,
    });
    assert.equal(pct, 98.4);
    const done = theaterProgressPct({
      phase: "reconcile",
      progress_pct: 98.99,
      reconcile_pct: 99,
      reconciling: true,
      isComplete: true,
    });
    assert.equal(done, 100);
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

describe("theaterElapsedMs", () => {
  it("freezes after complete when completed_at is missing", () => {
    const start = Date.parse("2026-09-25T02:09:51.000Z");
    const done = start + 18_000;
    const later = done + 239 * 60_000;
    const frozen = theaterElapsedMs({
      createdAt: "2026-09-25T02:09:51.000Z",
      terminal: true,
      frozenEndMs: done,
      nowMs: later,
    });
    assert.equal(frozen, 18_000);
    const live = theaterElapsedMs({
      createdAt: "2026-09-25T02:09:51.000Z",
      terminal: false,
      nowMs: later,
    });
    assert.ok(live > 200 * 60_000);
  });
});

describe("publishedEngineElapsedSeconds", () => {
  it("keeps a measured sub-second execute", () => {
    assert.equal(publishedEngineElapsedSeconds(1.25), 1.25);
    assert.equal(publishedEngineElapsedSeconds("0.4"), 0.4);
  });

  it("refuses a missing or non-numeric clock", () => {
    assert.equal(publishedEngineElapsedSeconds(undefined), null);
    assert.equal(publishedEngineElapsedSeconds(null), null);
    assert.equal(publishedEngineElapsedSeconds(""), null);
    assert.equal(publishedEngineElapsedSeconds(false), null);
    assert.equal(publishedEngineElapsedSeconds("n/a"), null);
    assert.equal(publishedEngineElapsedSeconds(-1), null);
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

describe("theaterElapsedMs stops at the terminal or parked time (QA: '240m 55s')", () => {
  const withTz = <T,>(tz: string, fn: () => T): T => {
    const prior = process.env.TZ;
    process.env.TZ = tz;
    try {
      return fn();
    } finally {
      if (prior === undefined) delete process.env.TZ;
      else process.env.TZ = prior;
    }
  };

  it("reads the job store's offset-less UTC timestamps as UTC west of UTC", () => {
    // A 55 s run, viewed 10 minutes later from UTC-4 (New York, late October).
    const startMs = Date.parse("2026-10-30T15:00:00Z");
    const ms = withTz("America/New_York", () =>
      theaterElapsedMs({
        startedAt: "2026-10-30T15:00:00.120000",
        createdAt: "2026-10-30T14:59:59.900000",
        completedAt: "2026-10-30T15:00:55.120000",
        fallbackStartMs: startMs,
        nowMs: startMs + 10 * 60_000,
        terminal: true,
      }),
    );
    assert.ok(ms < 60_000, `expected ~55s, got ${Math.round(ms / 1000)}s`);
    assert.ok(ms >= 55_000 - 500, `expected ~55s, got ${Math.round(ms / 1000)}s`);
  });

  it("freezes a run parked on a decision at its parked time", () => {
    const created = "2026-10-30T15:00:00Z";
    const parkedAt = "2026-10-30T15:02:10Z";
    const later = Date.parse("2026-10-30T19:00:55Z");
    const ms = theaterElapsedMs({
      createdAt: created,
      startedAt: created,
      updatedAt: parkedAt,
      nowMs: later,
      terminal: false,
      parked: true,
    });
    assert.equal(ms, 130_000);
  });

  it("stops a finished run without completed_at at its last update, not now", () => {
    const ms = theaterElapsedMs({
      createdAt: "2026-10-30T15:00:00Z",
      updatedAt: "2026-10-30T15:00:55Z",
      nowMs: Date.parse("2026-10-30T19:00:55Z"),
      terminal: true,
    });
    assert.equal(ms, 55_000);
  });

  it("keeps ticking a live run", () => {
    const now = Date.parse("2026-10-30T15:05:00Z");
    const ms = theaterElapsedMs({
      createdAt: "2026-10-30T15:00:00Z",
      updatedAt: "2026-10-30T15:04:59Z",
      nowMs: now,
      terminal: false,
    });
    assert.equal(ms, 300_000);
  });
});
