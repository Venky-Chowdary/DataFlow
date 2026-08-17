/**
 * Run: npx --yes tsx --test apps/web/src/lib/mappingReviewKind.test.ts
 *
 * Airbyte schema review is all-or-nothing. DataFlow must name qty≠amt /
 * user≠customer / dest collision and keep them off Approve eligible.
 */
import assert from "node:assert/strict";
import { describe, it } from "node:test";
import {
  applyOperatorRemapDest,
  approveMappingHonestly,
  approveMappingsHonestly,
  buildPreflightMappings,
  classifyMappingReview,
  confirmFalseFriendMapping,
  confirmFalseFriendsBySource,
  countApproveEligible,
  editableFromPipelineMappings,
  mappingHealthSummary,
  mappingReviewKindMeta,
  type EditableMapping,
} from "./mapping.js";

function qtyAmt(): EditableMapping {
  return {
    source: "order_qty",
    target: "order_amt",
    confidence: 0.78,
    approved: false,
    requiresReview: true,
    reviewKind: "measure_kind",
    reason: "Schematic index match · measure-kind mismatch — review required",
    existsInDestination: true,
  };
}

function userCustomer(): EditableMapping {
  return {
    source: "user_id",
    target: "customer_id",
    confidence: 0.78,
    approved: false,
    requiresReview: true,
    reviewKind: "entity_identity",
    reason: "entity qualifier conflict — review required",
    existsInDestination: true,
  };
}

describe("Map review kind — false-friend operator surface", () => {
  it("classifies engine stamps and reason fallback", () => {
    assert.equal(classifyMappingReview(qtyAmt()), "measure_kind");
    assert.equal(classifyMappingReview(userCustomer()), "entity_identity");
    assert.equal(
      classifyMappingReview({
        source: "user_id",
        target: "UserID",
        confidence: 0.78,
        approved: false,
        requiresReview: true,
        reason: "Exact name match · destination identifier collision — review required",
      }),
      "dest_collision",
    );
    assert.equal(mappingReviewKindMeta("measure_kind").chip, "qty≠amt");
    assert.equal(mappingReviewKindMeta("entity_identity").chip, "user≠customer");
  });

  it("pipeline review_kind survives editableFromPipelineMappings", () => {
    const editable = editableFromPipelineMappings(
      [
        {
          source: "order_qty",
          target: "order_amt",
          confidence: 0.78,
          reasoning: "measure-kind mismatch — review required",
          requires_review: true,
          review_kind: "measure_kind",
          source_type: "INTEGER",
          target_type: "DECIMAL",
        },
        {
          source: "user_id",
          target: "customer_id",
          confidence: 0.78,
          reasoning: "entity qualifier conflict — review required",
          requires_review: true,
          review_kind: "entity_identity",
          source_type: "INTEGER",
          target_type: "INTEGER",
        },
      ],
      [],
      ["order_amt", "customer_id"],
      0.85,
      { order_amt: "DECIMAL", customer_id: "INTEGER" },
    );
    assert.equal(editable[0].reviewKind, "measure_kind");
    assert.equal(editable[1].reviewKind, "entity_identity");
    assert.equal(classifyMappingReview(editable[0]), "measure_kind");
    assert.equal(classifyMappingReview(editable[1]), "entity_identity");
  });

  it("health headline names the kind — not generic need review", () => {
    const health = mappingHealthSummary([qtyAmt(), userCustomer()], 0.85);
    assert.equal(health.falseFriendCount, 2);
    assert.equal(health.falseFriendKinds.measure_kind, 1);
    assert.equal(health.falseFriendKinds.entity_identity, 1);
    assert.match(health.headline, /quantity≠amount/);
    assert.match(health.headline, /user≠customer/);
    assert.match(health.detail, /Approve eligible will not clear/);
    assert.equal(health.weak, true);
  });

  it("Approve eligible and Approve-all leave false-friends", () => {
    const rows = [
      qtyAmt(),
      userCustomer(),
      {
        source: "order_id",
        target: "order_id",
        confidence: 0.99,
        approved: false,
        requiresReview: false,
      },
    ];
    assert.equal(countApproveEligible(rows), 1);
    const next = approveMappingsHonestly(rows);
    assert.equal(next[0].approved, false);
    assert.equal(next[1].approved, false);
    assert.equal(next[2].approved, true);
    assert.equal(approveMappingHonestly(qtyAmt()).approved, false);
  });

  it("Confirm this pair clears one false-friend; remap drops the kind", () => {
    const confirmed = confirmFalseFriendMapping(qtyAmt());
    assert.equal(confirmed.approved, true);
    assert.equal(confirmed.falseFriendConfirmed, true);
    assert.equal(classifyMappingReview(confirmed), null);
    const remapped = applyOperatorRemapDest(qtyAmt(), "order_quantity");
    assert.equal(remapped.target, "order_quantity");
    assert.equal(remapped.reviewKind, "generic");
    assert.equal(remapped.approved, false);
    assert.equal(classifyMappingReview(remapped), "generic");
    const wire = buildPreflightMappings([], [confirmed]);
    assert.equal(wire[0].false_friend_confirmed, true);
    assert.equal(wire[0].review_kind, "measure_kind");
    assert.equal(wire[0].user_override, true);
    const unconfirmed = buildPreflightMappings([], [qtyAmt()]);
    assert.equal(unconfirmed[0].false_friend_confirmed, undefined);
    const restored = editableFromPipelineMappings(wire);
    assert.equal(restored[0].falseFriendConfirmed, true);
    assert.equal(restored[0].approved, true);
  });

  it("Validate confirm_or_remap stamps only the named false-friend", () => {
    const rows = [qtyAmt(), userCustomer(), {
      source: "order_id",
      target: "order_id",
      confidence: 0.99,
      approved: false,
      requiresReview: false,
    }];
    const named = confirmFalseFriendsBySource(rows, ["order_qty"]);
    assert.deepEqual(named.confirmed, ["order_qty"]);
    assert.equal(named.mappings[0].falseFriendConfirmed, true);
    assert.equal(named.mappings[1].falseFriendConfirmed, undefined);
    assert.equal(named.unmatched.length, 0);
    const all = confirmFalseFriendsBySource(rows);
    assert.deepEqual(all.confirmed, ["order_qty", "user_id"]);
    const miss = confirmFalseFriendsBySource(rows, ["loyalty_tier"]);
    assert.deepEqual(miss.confirmed, []);
    assert.deepEqual(miss.unmatched, ["loyalty_tier"]);
  });
});
