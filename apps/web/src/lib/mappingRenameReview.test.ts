/**
 * QA browser 12: a source column renamed amount -> total_amount was
 * auto-matched onto the existing `amount` column at 82% with no warning.
 * The API marks such a row ``review_kind: "rename"`` (services/
 * schema_drift_map_review.flag_rename_suggestions) and the drift gate holds
 * until Map approves it. Studio did not know the kind: the row read as a
 * generic "below the auto-approve floor" review. It now reads as a proposed
 * rename that needs confirmation.
 */
import assert from "node:assert/strict";
import { describe, it } from "node:test";
import { classifyMappingReview, editableFromPipelineMappings, mappingReviewKindMeta } from "./mapping.js";

describe("possible rename in Map (QA browser 12)", () => {
  it("keeps the engine's rename kind and says it is a proposed match to confirm", () => {
    const [row] = editableFromPipelineMappings(
      [{
        source: "total_amount",
        target: "amount",
        confidence: 0.82,
        reasoning: "Possible rename: 'amount' is no longer in the source and 'total_amount' is new — approve this mapping to accept the rename",
        requires_review: true,
        review_kind: "rename",
        source_type: "DECIMAL",
        target_type: "DECIMAL",
      }],
      [],
      ["amount"],
      0.7,
    );
    assert.equal(row.reviewKind, "rename");
    assert.equal(classifyMappingReview(row), "rename");
    const meta = mappingReviewKindMeta("rename");
    assert.equal(meta.chip, "possible rename");
    assert.match(meta.detail, /Proposed match/);
    assert.match(meta.confirmLabel, /Approve rename/);
    assert.notEqual(row.approved, true, "a below-exact rename is never pre-approved");
  });
});
