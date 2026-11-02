/**
 * Run: npx --yes tsx --test apps/web/src/lib/mapSafeBand.test.ts
 */
import assert from "node:assert/strict";
import { describe, it } from "node:test";
import type { EditableMapping } from "./mapping.js";
import {
  isSafeBandMapping,
  mappingMapBand,
  partitionMapBands,
  shouldCollapseSafeBand,
  shouldCollapseReadyBand,
} from "./mapSafeBand.js";

function m(partial: Partial<EditableMapping> & Pick<EditableMapping, "source">): EditableMapping {
  return {
    target: partial.target ?? partial.source,
    confidence: 0.95,
    approved: false,
    transform: "none",
    fidelity: "preserve",
    ...partial,
  };
}

describe("mapSafeBand — fail-closed classification", () => {
  it("treats preserve Approve-tier as safe", () => {
    assert.equal(isSafeBandMapping(m({ source: "id", fidelity: "preserve" })), true);
    assert.equal(mappingMapBand(m({ source: "id", fidelity: "preserve" })), "safe");
  });

  it("treats approved as ready, omit as omitted", () => {
    assert.equal(mappingMapBand(m({ source: "a", approved: true })), "ready");
    assert.equal(
      mappingMapBand(m({ source: "b", target: "", transform: "omit" as EditableMapping["transform"] })),
      "omitted",
    );
  });

  it("never invents safe for risk, cast, specialty, PII, pending schema, struct", () => {
    assert.equal(
      isSafeBandMapping(m({ source: "x", fidelity: "lossy_cast" })),
      false,
    );
    assert.equal(
      isSafeBandMapping(m({ source: "d", fidelity: "preserve", isPii: true })),
      false,
    );
    assert.equal(
      isSafeBandMapping(
        m({ source: "e", fidelity: "preserve", assignmentStrategy: "pending_dest_schema" }),
      ),
      false,
    );
    assert.equal(
      isSafeBandMapping(
        m({ source: "f", fidelity: "preserve", inferredType: "VECTOR", destType: "VECTOR" }),
      ),
      false,
    );
    assert.equal(
      isSafeBandMapping(
        m({
          source: "g",
          fidelity: "preserve",
          structPolicy: "flatten_top_level_keys",
        }),
      ),
      false,
    );
  });

  it("partitions and collapses only when safe ≥ 2", () => {
    const items = [
      { index: 0, mapping: m({ source: "a", fidelity: "preserve" }) },
      { index: 1, mapping: m({ source: "b", fidelity: "preserve" }) },
      { index: 2, mapping: m({ source: "c", fidelity: "lossy_cast", riskAckRequired: true }) },
      { index: 3, mapping: m({ source: "d", approved: true }) },
    ];
    const bands = partitionMapBands(items);
    assert.equal(bands.safe.length, 2);
    assert.equal(bands.attention.length, 1);
    assert.equal(bands.ready.length, 1);
    assert.equal(shouldCollapseSafeBand(bands), true);
    assert.equal(shouldCollapseSafeBand({ ...bands, safe: bands.safe.slice(0, 1) }), false);
    assert.equal(shouldCollapseReadyBand(bands), false);
  });

  it("keeps an all-approved map on screen and folds ready rows only beside other work", () => {
    const approved = [0, 1, 2].map((index) => ({
      index,
      mapping: m({ source: `c${index}`, approved: true }),
    }));
    const onlyReady = partitionMapBands(approved);
    assert.equal(shouldCollapseReadyBand(onlyReady), false);

    const withIssue = partitionMapBands([
      ...approved,
      { index: 3, mapping: m({ source: "risk", fidelity: "lossy_cast", riskAckRequired: true }) },
    ]);
    assert.equal(shouldCollapseReadyBand(withIssue), true);
  });
});
