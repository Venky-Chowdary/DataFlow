/**
 * Run: npx --yes tsx --test apps/web/src/lib/jobTheaterProgress.test.ts
 */
import assert from "node:assert/strict";
import { describe, it } from "node:test";
import { theaterProgressPct } from "./jobTheaterProgress.js";

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
