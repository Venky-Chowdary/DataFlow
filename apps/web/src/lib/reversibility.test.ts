/**
 * Reversibility (Page C) wording: verdict and column pills, reason codes, the
 * data check (a fit is never proof), the gate line and review order.
 */
import assert from "node:assert/strict";
import { describe, it } from "node:test";
import {
  checkText,
  columnLabel,
  columnPillPresentation,
  columnsForReview,
  countsText,
  dataCheckedText,
  gatePresentation,
  invertibilityPresentation,
  reasonCodeLabel,
  reversibilityFromPreflight,
  reversibilityVerdictPresentation,
  rollbackLabel,
  tableColumnCounts,
} from "./reversibility.js";
import { blockedReversibilityFixture, planReversibilityFixture } from "../components/exit/exitFixtures.js";
import { realPlanReversibility } from "../components/exit/realExitPayloads.js";

describe("verdict pills", () => {
  it("maps reversible / partly / not reversible to words and tones", () => {
    assert.deepEqual(reversibilityVerdictPresentation("reversible"), { label: "Reversible", short: "Reversible", tone: "ok" });
    assert.deepEqual(reversibilityVerdictPresentation("partly_reversible"), { label: "Partly reversible", short: "Partly", tone: "warn" });
    assert.deepEqual(reversibilityVerdictPresentation("not_reversible"), { label: "Not reversible", short: "Not reversible", tone: "danger" });
    assert.equal(reversibilityVerdictPresentation("unknown_thing").tone, "muted");
  });

  it("maps invertibility, and a conditional column by its data check", () => {
    assert.equal(invertibilityPresentation("lossy").tone, "danger");
    assert.equal(invertibilityPresentation("invertible").tone, "ok");
    // A fit on a sample is not green: it is invertible for the data seen only.
    assert.deepEqual(columnPillPresentation({ invertibility: "conditional", check: { status: "fits", rows_checked: 25, rows_not_fitting: 0 } }), { label: "Fits sample", tone: "muted" });
    assert.deepEqual(columnPillPresentation({ invertibility: "conditional", check: { status: "does_not_fit", rows_checked: 25, rows_not_fitting: 4 } }), { label: "Does not fit", tone: "danger" });
    assert.deepEqual(columnPillPresentation({ invertibility: "conditional", check: null }), { label: "Not checked", tone: "warn" });
    assert.equal(columnPillPresentation({ invertibility: "lossy", check: null }).label, "Lossy");
  });
});

describe("reasons", () => {
  it("names every lossy reason code, and an unknown one in words", () => {
    assert.equal(reasonCodeLabel("hash_pii"), "Hashed (hash_pii)");
    assert.equal(reasonCodeLabel("mask_pii"), "Masked (mask_pii)");
    assert.equal(reasonCodeLabel("dropped"), "Dropped");
    assert.equal(reasonCodeLabel("narrowing"), "Narrowed");
    assert.equal(reasonCodeLabel("row_change"), "Changes rows");
    assert.equal(reasonCodeLabel("new_code_x"), "New code x");
  });

  it("states a fit with its sample size and that unsampled rows are not proven", () => {
    assert.equal(checkText({ status: "fits", rows_checked: 25, rows_not_fitting: 0 }), "Fits the 25 observed rows; not proven for rows not sampled.");
    assert.equal(checkText({ status: "does_not_fit", rows_checked: 25, rows_not_fitting: 4 }), "4 of 25 observed rows would not round-trip.");
    assert.match(checkText({ status: "not_checked", rows_checked: 0, rows_not_fitting: 0 }), /^Not checked/);
    assert.equal(checkText(null), "");
  });

  it("says what the conditional columns were checked against, never the population", () => {
    assert.equal(dataCheckedText({ rows: 25, source: "plan_sample" }), "Conditional columns were checked against 25 observed rows from the plan sample. A sample never proves the full table.");
    assert.match(dataCheckedText({ rows: 0, source: "none" }), /No observed rows/);
  });
});

describe("the gate line", () => {
  it("is not required when the switch is off, and blocks Execute when on and not reversible", () => {
    assert.equal(gatePresentation(planReversibilityFixture()).label, "Not required");
    const blocked = gatePresentation(blockedReversibilityFixture());
    assert.equal(blocked.label, "Blocks Execute");
    assert.equal(blocked.tone, "danger");
    assert.match(blocked.detail, /customers\.email/);
    assert.equal(gatePresentation({ require_reversibility: true, gate: { id: "g24_reversibility", status: "pass", message: "" } }).label, "Gate passes");
  });
});

describe("review order", () => {
  it("puts lossy columns first, then conditional that do not fit or were not checked", () => {
    const payments = planReversibilityFixture().tables[2];
    assert.deepEqual(columnsForReview(payments.columns).map((c) => c.source), ["amount", "ref", "payment_id"]);
    assert.deepEqual(tableColumnCounts(payments), { invertible: 1, conditional: 1, lossy: 1 });
    assert.equal(countsText({ invertible: 1, conditional: 0, lossy: 3 }), "3 lossy · 1 invertible");
  });

  it("labels renamed, dropped and destination-only columns and recipe steps", () => {
    assert.equal(columnLabel({ source: "cust", target: "customer_name", transforms: [] }), "cust → customer_name");
    assert.equal(columnLabel({ source: "note", target: null, transforms: [] }), "note (not carried)");
    assert.equal(columnLabel({ source: null, target: "region", transforms: ["default:EU"] }), "region (destination only)");
    assert.equal(columnLabel({ source: null, target: null, transforms: ["filter_rows"] }), "filter_rows step");
    assert.equal(rollbackLabel("keep_source_as_fallback"), "Keep source as fallback");
  });
});

describe("preflight wiring", () => {
  it("reads the report plan preflight attaches, and nothing else", () => {
    assert.equal(reversibilityFromPreflight({ passed: true, reversibility: realPlanReversibility }), realPlanReversibility);
    assert.equal(reversibilityFromPreflight({ passed: true }), null);
    assert.equal(reversibilityFromPreflight({ reversibility: { verdict: "x" } }), null);
    assert.equal(reversibilityFromPreflight(null), null);
  });
});
