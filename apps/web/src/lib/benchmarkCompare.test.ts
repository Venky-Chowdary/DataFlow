/**
 * QA (557f556c): Proofs → Scale said competitors were "5.6x slower" while
 * listing 4,000 rows/s against Datawrap's 717, and the page claimed "~10k rows
 * in under a second" next to a 13.9 s measurement.
 */
import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { describe, it } from "node:test";
import { compareThroughput } from "./benchmarkCompare.js";

describe("compareThroughput", () => {
  it("names Datawrap as the slower side when the other number is higher (QA's case)", () => {
    assert.deepEqual(compareThroughput(717.6, 4000), { verdict: "slower", label: "Datawrap 5.6× slower" });
  });
  it("names Datawrap as faster only when its number is higher", () => {
    assert.equal(compareThroughput(5000, 2500).label, "Datawrap 2.0× faster");
  });
  it("calls numbers within 10% about the same, and a missing run unknown", () => {
    assert.equal(compareThroughput(1000, 1050).label, "About the same");
    assert.equal(compareThroughput(0, 4000).verdict, "unknown");
  });
  it("uses the same 10% band on both sides, like the API's comparison()", () => {
    // 4,000 rows/s is 1.103x our 3,626.7: slower, not "about the same".
    assert.equal(compareThroughput(3626.7, 4000).verdict, "slower");
    assert.equal(compareThroughput(4000, 3626.7).verdict, "faster");
    for (const [ours, theirs] of [[1000, 1100], [1100, 1000], [1000, 1099], [1099, 1000]]) {
      assert.equal(compareThroughput(ours, theirs).verdict, "similar", `${ours} vs ${theirs}`);
    }
    for (const [ours, theirs] of [[1000, 1101], [1101, 1000], [1000, 1110], [1110, 1000]]) {
      assert.notEqual(compareThroughput(ours, theirs).verdict, "similar", `${ours} vs ${theirs}`);
    }
  });
});

describe("Scale page claims", () => {
  const page = readFileSync(new URL("../pages/BenchmarksPage.tsx", import.meta.url), "utf8");
  it("makes no static speed claim the measurement can contradict", () => {
    assert.doesNotMatch(page, /under a second/i);
  });
  it("runs the benchmark as a background run, not one long request", () => {
    assert.match(page, /startBenchmarkRun/);
    assert.doesNotMatch(page, /await runBenchmark\(/);
  });
});
