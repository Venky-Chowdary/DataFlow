/**
 * FK orphan CTAs must drive the honesty scan flag or Map — never invent parents.
 * Run: npx --yes tsx --test apps/web/src/lib/fkOrphanCta.test.ts
 */
import assert from "node:assert/strict";
import { describe, it } from "node:test";
import {
  isFkOrphanBlockerText,
  isFkOrphanCtaKind,
  planFkOrphanSuggestedAction,
  resolvePopulationOrphanScanFlag,
} from "./fkOrphanCta.ts";

describe("fkOrphanCta", () => {
  it("treats only engine orphan kinds as orphan CTAs", () => {
    assert.equal(isFkOrphanCtaKind("run_population_orphan_scan"), true);
    assert.equal(isFkOrphanCtaKind("fix_orphans"), true);
    assert.equal(isFkOrphanCtaKind("review_mappings"), false);
    assert.equal(isFkOrphanCtaKind("confirm_or_remap"), false);
  });

  it("runs the population scan on the first click without waiting on setState", () => {
    const plan = planFkOrphanSuggestedAction({ kind: "run_population_orphan_scan" });
    assert.ok(plan);
    assert.equal(plan.enablePopulationScan, true);
    assert.equal(plan.rerunValidateWithPopulationScan, true);
    assert.equal(plan.goToMap, false);
    assert.match(plan.toastMessage, /RI proven|referential integrity/i);
    assert.equal(resolvePopulationOrphanScanFlag(true, false), true);
    assert.equal(resolvePopulationOrphanScanFlag(undefined, false), false);
    assert.equal(resolvePopulationOrphanScanFlag(undefined, true), true);
  });

  it("does not invent parent rows — Map focus is the in-product door", () => {
    const plan = planFkOrphanSuggestedAction({
      kind: "fix_orphans",
      column: "customer_id",
    });
    assert.ok(plan);
    assert.equal(plan.enablePopulationScan, false);
    assert.equal(plan.rerunValidateWithPopulationScan, false);
    assert.equal(plan.goToMap, true);
    assert.equal(plan.focusSource, "customer_id");
    assert.match(plan.toastMessage, /cannot invent/i);
    assert.match(plan.toastMessage, /customer_id/);
    assert.doesNotMatch(plan.toastMessage, /RI proven(?!.)/i);
    assert.match(plan.toastMessage, /never claims RI proven/i);
  });

  it("detects sample/population orphan copy and not unmapped-FK metadata", () => {
    assert.equal(
      isFkOrphanBlockerText(
        "Sample orphan probe: 2/10 customer_id values missing. Coverage=sample_orphan_probe — population RI not proven.",
      ),
      true,
    );
    assert.equal(
      isFkOrphanBlockerText(
        "Population orphan scan: 12 rows in orders.customer_id missing from customers.id.",
      ),
      true,
    );
    assert.equal(
      isFkOrphanBlockerText("Destination FK columns unmapped — transfer blocked"),
      false,
    );
  });

  it("ignores unknown kinds so Map / G15 CTAs stay on their owners", () => {
    assert.equal(planFkOrphanSuggestedAction({ kind: "review_mappings" }), null);
    assert.equal(planFkOrphanSuggestedAction({ kind: "confirm_or_remap" }), null);
  });
});
