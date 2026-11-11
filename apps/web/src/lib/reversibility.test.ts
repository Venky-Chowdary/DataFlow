/**
 * Reversibility (Page C) wording: verdict and column pills, reason codes, the
 * data check (a fit is never proof), the gate line and review order.
 */
import assert from "node:assert/strict";
import { describe, it } from "node:test";
import {
  checkText,
  columnLabel,
  columnReasonLabel,
  lossyColumnBadge,
  lossyColumnName,
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
import { realPlanReversibility, realReversibilityCodes, realReversibilityEnum } from "../components/exit/realExitPayloads.js";

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

describe("contract 10 reason codes", () => {
  it("names the new lossy and conditional codes in words", () => {
    assert.equal(reasonCodeLabel("timezone"), "Time zone dropped");
    assert.equal(reasonCodeLabel("json_normalize"), "JSON normalised (json → jsonb)");
    assert.equal(reasonCodeLabel("charset"), "Character set");
    assert.equal(reasonCodeLabel("enum"), "Into an enum");
    assert.equal(reasonCodeLabel("blank_pad"), "Blank-padded (CHAR)");
    assert.equal(reasonCodeLabel("text_width"), "Text width");
  });

  it("says which rows a row change keeps: row cap, source filter, top N", () => {
    const rows = realReversibilityCodes.tables[0].columns.filter((c) => c.reason_code === "row_change");
    assert.deepEqual(rows.map((c) => c.transforms[0]).sort(), ["limit", "rank_rows", "source_filter"]);
    const byStep = Object.fromEntries(rows.map((c) => [c.transforms[0], c]));
    assert.equal(columnReasonLabel(byStep.limit), "Row cap (limit)");
    assert.equal(columnReasonLabel(byStep.source_filter), "Source filter");
    assert.equal(columnReasonLabel(byStep.rank_rows), "Top rows only (rank_rows)");
    assert.equal(columnLabel(byStep.limit), "All rows (row cap)");
    assert.equal(columnLabel(byStep.source_filter), "All rows (source filter)");
    assert.equal(columnLabel(byStep.rank_rows), "All rows (rank_rows step)");
    // Any other row-changing step still reads as a step; a column code reads as its code.
    assert.equal(columnReasonLabel({ reason_code: "row_change", transforms: ["filter_rows"] }), "Changes rows (filter_rows)");
    assert.equal(columnReasonLabel({ reason_code: "timezone", transforms: [] }), "Time zone dropped");
  });

  it("lists every lossy column by name, and a row change as the table's rows with its own badge", () => {
    const report = realReversibilityCodes;
    const names = report.lossy_columns.map((c) => lossyColumnName(c));
    assert.ok(names.includes("events.created_at"));
    assert.equal(names.filter((n) => n === "events (rows)").length, 3);
    assert.deepEqual(report.lossy_columns.map((c) => lossyColumnBadge(c, report)), [
      "Time zone dropped", "JSON normalised (json → jsonb)", "Top rows only (rank_rows)", "Row cap (limit)", "Source filter",
    ]);
  });

  it("the real enum report: a conditional column that fits its sample is never proof", () => {
    const state = realReversibilityEnum.tables[0].columns.find((c) => c.reason_code === "enum")!;
    assert.equal(columnPillPresentation(state).label, "Fits sample");
    assert.equal(columnReasonLabel(state), "Into an enum");
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
