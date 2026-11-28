/**
 * P1-20 / P1-23: the Validate footer puts secondaries first and the one
 * primary rightmost, uses the product's word (Run, never Execute), keeps its
 * label while validating, and says why Run is disabled in visible text.
 */
import assert from "node:assert/strict";
import { describe, it } from "node:test";
import { createElement } from "react";
import { renderToStaticMarkup } from "react-dom/server";
import { ValidateActionsRail } from "./ValidateActionsRail";
import type { PreflightResult } from "../../lib/types";

const approved = {
  passed: true,
  passed_count: 13,
  total_gates: 13,
  readiness_score: 100,
  gates: [],
  blockers: [],
  proof_bundle: { transfer_decision: { decision: "approve" } },
} as unknown as PreflightResult;

const review = {
  ...approved,
  proof_bundle: { transfer_decision: { decision: "review" } },
} as unknown as PreflightResult;

const noop = () => {};
const rail = (over: Record<string, unknown>) => renderToStaticMarkup(createElement(ValidateActionsRail, {
  preflight: null,
  preflighting: false,
  transferring: false,
  mappingReviewCount: 0,
  onBack: noop,
  onRunPreflight: noop,
  onApproveMappings: noop,
  onExecute: noop,
  onOpenJobTheater: noop,
  onSaveAsContract: noop,
  ...over,
}));

/** Button labels in document order. */
function buttons(html: string): string[] {
  return Array.from(html.matchAll(/<button[^>]*>([\s\S]*?)<\/button>/g))
    .map((m) => m[1].replace(/<[^>]+>/g, "").trim());
}

describe("Validate footer", () => {
  it("approved: Back, Save contract, Re-run Validate, then Run transfer rightmost", () => {
    const labels = buttons(rail({ preflight: approved }));
    assert.deepEqual(labels, ["Back", "Save contract", "Re-run Validate", "Run transfer"]);
    assert.doesNotMatch(rail({ preflight: approved }), /Execute/);
  });

  it("validating keeps a label on the busy button", () => {
    const labels = buttons(rail({ preflighting: true }));
    assert.equal(labels.at(-1), "Validating…");
  });

  it("review-grade: Run transfer is disabled and the reason is visible beside it", () => {
    const html = rail({ preflight: review });
    assert.match(html, /Validate returned review: resolve it, then re-run/);
    assert.match(html, /<button[^>]*disabled=""[^>]*>[\s\S]*?Run transfer<\/button>/);
  });
});
