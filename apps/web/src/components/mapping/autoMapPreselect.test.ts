/**
 * P1-19: Auto-map never pre-selects a lossy or low-confidence suggestion.
 * The operator chooses those; safe matches at or above the mode's floor start selected.
 */
import assert from "node:assert/strict";
import { describe, it } from "node:test";
import { autoMapPreselected } from "./AutoMapDialog";
import type { AutoMapSuggestion } from "../../lib/mappingStudioApi";

const s = (over: Partial<AutoMapSuggestion>): AutoMapSuggestion => ({
  source: "status",
  target: "order_status",
  confidence: 0.9,
  kind: "synonym",
  reason: "Synonyms",
  selected: true,
  alternatives: [],
  ...over,
} as AutoMapSuggestion);

describe("auto-map pre-selection", () => {
  it("selects a safe match at or above the floor", () => {
    assert.equal(autoMapPreselected(s({ compat: { verdict: "safe", reasons: [] } as never }), 0.85), true);
    assert.equal(autoMapPreselected(s({ confidence: 0.85 }), 0.85), true);
  });

  it("leaves lossy, incompatible and unread type checks for the operator", () => {
    for (const verdict of ["lossy", "incompatible", "unknown"]) {
      assert.equal(autoMapPreselected(s({ compat: { verdict, reasons: [] } as never }), 0.85), false, verdict);
    }
  });

  it("leaves a match below the floor unselected, and never selects what the matcher did not", () => {
    assert.equal(autoMapPreselected(s({ confidence: 0.78 }), 0.85), false);
    assert.equal(autoMapPreselected(s({ confidence: 0.78 }), 0.75), true);
    assert.equal(autoMapPreselected(s({ selected: false }), 0.85), false);
  });
});
