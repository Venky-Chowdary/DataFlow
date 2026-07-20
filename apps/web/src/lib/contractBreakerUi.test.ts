/**
 * Run: npx --yes tsx --test apps/web/src/lib/contractBreakerUi.test.ts
 */
import assert from "node:assert/strict";
import { describe, it } from "node:test";
import {
  breakerBadgeClass,
  breakerBlocksRuns,
  breakerLabel,
  breakerWarnLabel,
} from "./contractBreakerUi.js";

describe("contractBreakerUi", () => {
  it("labels and classes by state", () => {
    assert.equal(breakerLabel("closed"), "Breaker closed");
    assert.equal(breakerLabel("half_open"), "Breaker half-open");
    assert.equal(breakerBadgeClass("closed"), "df2-badge-live");
    assert.equal(breakerBadgeClass("open"), "df2-badge-warn");
  });

  it("blocks runs when open or half-open", () => {
    assert.equal(breakerBlocksRuns("closed"), false);
    assert.equal(breakerBlocksRuns("open"), true);
    assert.equal(breakerBlocksRuns("half_open"), true);
  });

  it("warn label only for blocking states", () => {
    assert.equal(breakerWarnLabel("closed"), "");
    assert.equal(breakerWarnLabel("open"), "Breaker open");
  });
});
