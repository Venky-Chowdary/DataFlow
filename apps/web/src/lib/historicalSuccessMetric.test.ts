/**
 * Historical success process metric — never invent a percent.
 * Run: npx --yes tsx --test apps/web/src/lib/historicalSuccessMetric.test.ts
 */
import assert from "node:assert/strict";
import { describe, it } from "node:test";
import {
  buildHistoricalSuccessMetric,
  historicalSuccessHeadlineHasPercent,
} from "./historicalSuccessMetric.ts";

describe("historicalSuccessMetric", () => {
  it("unmeasured headline and badge contain no percent, even if a rate is present", () => {
    const metric = buildHistoricalSuccessMetric({
      measured: false,
      success_rate: 0.99,
      runs_observed: 0,
      rows_written_total: 0,
      rows_rejected_total: 0,
    });
    assert.equal(metric.measured, false);
    assert.equal(metric.successRate, null);
    assert.equal(metric.hasPercent, false);
    assert.equal(historicalSuccessHeadlineHasPercent(metric.headline), false);
    assert.equal(metric.headline.includes("%"), false);
    assert.equal(metric.badge.includes("%"), false);
    assert.match(metric.headline, /unmeasured/i);
    assert.equal(metric.badge, "Unmeasured");
  });

  it("absent evidence is unmeasured with no invented rate", () => {
    const metric = buildHistoricalSuccessMetric(undefined);
    assert.equal(metric.measured, false);
    assert.equal(metric.successRate, null);
    assert.equal(metric.hasPercent, false);
    assert.equal(metric.headline.includes("%"), false);
  });

  it("measured publishes rate, runs, and kept/rejected", () => {
    const metric = buildHistoricalSuccessMetric({
      measured: true,
      success_rate: 1,
      runs_observed: 1,
      rows_written_total: 1_000_000,
      rows_rejected_total: 0,
    });
    assert.equal(metric.measured, true);
    assert.equal(metric.successRate, 1);
    assert.equal(metric.runsObserved, 1);
    assert.equal(metric.rowsKept, 1_000_000);
    assert.equal(metric.rowsRejected, 0);
    assert.equal(metric.hasPercent, true);
    assert.match(metric.headline, /100\.0%/);
    assert.match(metric.keptLabel, /1,000,000 kept/);
    assert.equal(metric.rejectedLabel, "0 rejected");
  });

  it("measured=true without a numeric rate stays unmeasured", () => {
    const metric = buildHistoricalSuccessMetric({
      measured: true,
      success_rate: null,
      runs_observed: 2,
    });
    assert.equal(metric.measured, false);
    assert.equal(metric.hasPercent, false);
    assert.equal(metric.headline.includes("%"), false);
  });
});
