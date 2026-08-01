/**
 * Run: npx --yes tsx --test apps/web/src/lib/phaseProfile.test.ts
 */
import assert from "node:assert/strict";
import { describe, it } from "node:test";

import {
  buildPhaseProfileView,
  formatSeconds,
  formatThroughput,
} from "./phaseProfile.js";
import type { PhaseProfileReport } from "./types.js";

function report(overrides: Partial<PhaseProfileReport> = {}): PhaseProfileReport {
  return {
    phases: [
      {
        phase: "read",
        label: "Reading source",
        seconds: 12,
        calls: 40,
        rows: 120_000,
        share_of_busy: 0.6,
        rows_per_second: 10_000,
      },
      {
        phase: "transform_write",
        label: "Transforming and writing",
        seconds: 6,
        calls: 40,
        rows: 120_000,
        share_of_busy: 0.3,
        rows_per_second: 20_000,
      },
      {
        phase: "checksum",
        label: "Verifying checksum",
        seconds: 2,
        calls: 1,
        rows: 120_000,
        share_of_busy: 0.1,
        rows_per_second: 60_000,
      },
    ],
    busy_seconds: 20,
    elapsed_seconds: 20,
    dominant_phase: "read",
    overlap_factor: 1.0,
    ...overrides,
  };
}

describe("formatSeconds", () => {
  it("renders sub-millisecond work without claiming zero", () => {
    assert.equal(formatSeconds(0.0004), "<1ms");
  });

  it("renders milliseconds, seconds, minutes and hours", () => {
    assert.equal(formatSeconds(0.25), "250ms");
    assert.equal(formatSeconds(3.5), "3.50s");
    assert.equal(formatSeconds(42.5), "42.5s");
    assert.equal(formatSeconds(125), "2m 5s");
    assert.equal(formatSeconds(7500), "2h 5m");
  });

  it("returns a dash rather than NaN for missing values", () => {
    assert.equal(formatSeconds(Number.NaN), "—");
    assert.equal(formatSeconds(-1), "—");
  });
});

describe("formatThroughput", () => {
  it("scales to K and M", () => {
    assert.equal(formatThroughput(850), "850 rows/s");
    assert.equal(formatThroughput(12_400), "12.4K rows/s");
    assert.equal(formatThroughput(2_500_000), "2.5M rows/s");
  });

  it("returns a dash for zero or invalid throughput", () => {
    assert.equal(formatThroughput(0), "—");
    assert.equal(formatThroughput(Number.NaN), "—");
  });
});

describe("buildPhaseProfileView", () => {
  it("returns null when there is nothing to show", () => {
    assert.equal(buildPhaseProfileView(null), null);
    assert.equal(buildPhaseProfileView(undefined), null);
    assert.equal(buildPhaseProfileView(report({ phases: [] })), null);
    assert.equal(
      buildPhaseProfileView(report({ phases: [], busy_seconds: 0 })),
      null
    );
  });

  it("orders phases slowest first", () => {
    const view = buildPhaseProfileView(report())!;
    assert.deepEqual(
      view.rows.map((r) => r.phase),
      ["read", "transform_write", "checksum"]
    );
  });

  it("computes percentages that sum to 100", () => {
    const view = buildPhaseProfileView(report())!;
    assert.deepEqual(
      view.rows.map((r) => r.percent),
      [60, 30, 10]
    );
  });

  it("recomputes the share rather than trusting a stale share_of_busy", () => {
    // An older engine build could send a share that contradicts the seconds
    // displayed next to it.
    const stale = report();
    stale.phases[0].share_of_busy = 0.99;
    const view = buildPhaseProfileView(stale)!;
    assert.equal(view.rows[0].percent, 60);
  });

  it("marks and names the dominant phase", () => {
    const view = buildPhaseProfileView(report())!;
    assert.equal(view.rows[0].dominant, true);
    assert.equal(view.rows[1].dominant, false);
    assert.equal(view.dominantLabel, "Reading source");
    assert.match(view.headline, /Reading source took 12\.0s, 60% of engine time/);
  });

  it("falls back to the slowest phase when none is named dominant", () => {
    const view = buildPhaseProfileView(report({ dominant_phase: "" }))!;
    assert.equal(view.dominantLabel, "Reading source");
    assert.ok(view.headline.length > 0);
  });

  it("stays silent about overlap on a serial run", () => {
    const view = buildPhaseProfileView(report())!;
    assert.equal(view.overlapNote, "");
  });

  it("explains that shares are of engine time when phases overlapped", () => {
    // busy 20s inside a 7s wall clock means real concurrency; without the note
    // the percentages look like they should sum against the wall clock.
    const view = buildPhaseProfileView(
      report({ elapsed_seconds: 7, overlap_factor: 2.9 })
    )!;
    assert.match(view.overlapNote, /2\.9× overlap/);
    assert.match(view.overlapNote, /7\.00s wall clock/);
  });

  it("derives busy seconds when the engine omitted the total", () => {
    const view = buildPhaseProfileView(report({ busy_seconds: 0 }))!;
    assert.equal(view.busySeconds, 20);
    assert.equal(view.rows[0].percent, 60);
  });

  it("derives throughput when the engine omitted rows_per_second", () => {
    const raw = report();
    raw.phases[0].rows_per_second = 0;
    const view = buildPhaseProfileView(raw)!;
    assert.equal(view.rows[0].throughputLabel, "10.0K rows/s");
  });

  it("shows a dash for a phase that processed no rows", () => {
    const raw = report();
    raw.phases[2].rows = 0;
    raw.phases[2].rows_per_second = 0;
    const view = buildPhaseProfileView(raw)!;
    const checksum = view.rows.find((r) => r.phase === "checksum")!;
    assert.equal(checksum.throughputLabel, "—");
  });

  it("falls back to the phase id when the engine sent no label", () => {
    const raw = report();
    raw.phases[0].label = "";
    const view = buildPhaseProfileView(raw)!;
    assert.equal(view.rows[0].label, "read");
  });
});
