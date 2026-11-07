/**
 * Run: npx --yes tsx --test apps/web/src/lib/engineProgress.test.ts
 */
import assert from "node:assert/strict";
import { describe, it } from "node:test";
import { engineProgressCopy } from "./engineProgress.js";

describe("engine progress copy", () => {
  it("never wraps a 1/9 stage counter — rows scanned are the live signal", () => {
    const copy = engineProgressCopy(
      { rows_scanned: 412_000, rows_estimate: 1_000_000, phase: "scanning_population_fit" },
      48_000,
    );
    assert.match(copy.count, /412/);
    assert.match(copy.count, /1,000,000|1000000/);
    assert.match(copy.name, /Scanning population fit/);
    assert.match(copy.count, /48s/);
    assert.doesNotMatch(copy.count, /\/\s*9\b/);
    assert.doesNotMatch(copy.name, /1\/9|9\/9/);
  });

  it("stays on one honest line when the worker has not heartbeated yet", () => {
    const copy = engineProgressCopy(null, 320_000);
    assert.equal(copy.count, "320s");
    assert.match(copy.name, /Engine running/);
    assert.doesNotMatch(copy.name, /Reading source catalog/);
  });

  it("does not invent a later stage after many seconds", () => {
    const early = engineProgressCopy({ phase: "running_gates" }, 1_100);
    const late = engineProgressCopy({ phase: "running_gates" }, 330_000);
    assert.equal(early.name, late.name);
  });
});
