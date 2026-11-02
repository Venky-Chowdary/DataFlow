/**
 * Run: npx --yes tsx --test apps/web/src/lib/slimPreflightForExplain.test.ts
 */
import assert from "node:assert/strict";
import { describe, it } from "node:test";
import { slimPreflightForExplain } from "./slimPreflightForExplain.js";

describe("slimPreflightForExplain", () => {
  it("drops gates and caps example values so nginx does not spill to disk", () => {
    const slim = slimPreflightForExplain({
      passed: false,
      run_id: "pf_1",
      gates: [{ id: "g1", sample_rows: Array.from({ length: 200 }, () => ["x"]) }],
      blockers: [{ id: "g3f_population_fit", message: "overflow", details: { issues: Array.from({ length: 40 }, () => "x") } }],
      population_fit: {
        findings: [{ source: "DEP_TIME", example_values: Array.from({ length: 200 }, (_, i) => String(i)) }],
      },
    });
    assert.equal(slim.run_id, "pf_1");
    assert.equal("gates" in slim, false);
    const findings = (slim.population_fit as { findings: Array<{ example_values: string[] }> }).findings;
    assert.equal(findings[0].example_values.length, 3);
    const blockers = slim.blockers as Array<{ details: { issues: string[] } }>;
    assert.equal(blockers[0].details.issues.length, 10);
  });
});
