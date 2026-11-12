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
    // A fit on a sample is not green: Execute blocks it unless the whole table was checked (contract 11.1.2).
    assert.deepEqual(columnPillPresentation({ invertibility: "conditional", check: { status: "fits", rows_checked: 25, rows_not_fitting: 0 } }), { label: "Fits sample only", tone: "warn" });
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
    assert.equal(dataCheckedText({ rows: 25, source: "plan_sample" }), "Conditional columns were checked against 25 observed rows from the plan sample. A sample never proves the full table: under Require reversibility a conditional column is checked on the sample here and enforced on every row while Execute writes.");
    assert.match(dataCheckedText({ rows: 0, source: "none" }), /No observed rows/);
    // Review 3, M4: only the server's covers_population says the sample is the table, never a count alone.
    assert.doesNotMatch(dataCheckedText({ rows: 25, source: "plan_sample", population_rows: 10 }), /holds every row/);
    assert.match(dataCheckedText({ rows: 10, source: "preflight_sample", population_rows: 10, covers_population: true }), /all 10 rows of the table \(the Validate sample holds every row\)/);
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

  it("never shows a status it does not know as a pass (review 3)", () => {
    for (const status of ["error", "", "pending"]) {
      const shown = gatePresentation({ require_reversibility: true, gate: { id: "g24_reversibility", status: status as "pass", message: "" } });
      assert.deepEqual([shown.label, shown.tone], ["Not evaluated", "muted"]);
    }
    assert.equal(gatePresentation({ require_reversibility: true, gate: undefined as never }).label, "Not evaluated");
  });

  it("words a conditional column enforced while writing or at Execute (review 3, M3)", () => {
    const writing = { status: "fits" as const, rows_checked: 100, rows_not_fitting: 0, covers_population: false, enforcement: "while_writing" as const };
    assert.deepEqual(columnPillPresentation({ invertibility: "conditional", check: writing }), { label: "Enforced while writing", tone: "ok" });
    assert.match(checkText(writing), /enforced while writing: every row is checked and the run stops on the first value that would change/);
    const validate = { ...writing, enforcement: "at_execute" as const };
    assert.deepEqual(columnPillPresentation({ invertibility: "conditional", check: validate }), { label: "Enforced at Execute", tone: "ok" });
    assert.equal(checkText(validate), "Checked on the sample (100 observed rows); enforced on every row at Execute.");
    const inherited = { ...writing, enforcement: "unavailable" as const };
    assert.deepEqual(columnPillPresentation({ invertibility: "conditional", check: inherited }), { label: "Cannot be enforced", tone: "warn" });
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
    assert.equal(columnPillPresentation(state).label, "Fits sample only");
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

describe("contract 11: a fit counts only over the whole table; new reason codes", () => {
  it("a conditional column that fits only the sample reads 'Fits sample only' and says Execute blocks it", async () => {
    const { realReversibilityReview2 } = await import("../components/exit/realExitPayloads.js");
    const cols = realReversibilityReview2.tables[0].columns;
    const email = cols.find((c) => c.source === "email")!;
    assert.equal(email.check?.covers_population, false);
    // Review 3 (M3): a run that must be reversible enforces it on every row at Execute.
    assert.deepEqual(columnPillPresentation(email), { label: "Enforced at Execute", tone: "ok" });
    const sampleOnly = { ...email, check: { ...email.check!, enforcement: undefined } };
    assert.deepEqual(columnPillPresentation(sampleOnly), { label: "Fits sample only", tone: "warn" });
    assert.match(checkText(sampleOnly.check), /Fits the sample \(2 observed rows\); rows not sampled are not proven/);
    const whole = { ...email, check: { ...email.check!, covers_population: true } };
    assert.deepEqual(columnPillPresentation(whole), { label: "Fits whole table", tone: "ok" });
    assert.match(checkText(whole.check), /the check covered the whole table/);
    // Review 3, M4: the plan's row_count_estimate is the client's, so no population is claimed.
    assert.match(dataCheckedText(realReversibilityReview2.data_checked), /2 observed rows from the plan sample/);
    assert.match(dataCheckedText({ rows: 5, source: "preflight_sample", population_rows: 5, covers_population: true }), /all 5 rows of the table/);
    assert.match(realReversibilityReview2.gate.message, /CONTACTS: source query/);
    assert.doesNotMatch(realReversibilityReview2.gate.message, /\(rows\)/);
  });

  it("labels the new reason codes and row changes", async () => {
    const { realReversibilityReview2 } = await import("../components/exit/realExitPayloads.js");
    assert.equal(reasonCodeLabel("byte_length"), "Byte length (UTF-8)");
    assert.equal(reasonCodeLabel("boolean_domain"), "0/1 into boolean");
    assert.equal(reasonCodeLabel("timestamp_range"), "TIMESTAMP range (1970–2038)");
    const rows = realReversibilityReview2.tables[0].columns.find((c) => c.reason_code === "row_change")!;
    assert.equal(columnReasonLabel(rows), "Source query");
    assert.equal(columnLabel(rows), "The table (read through a query)");
    assert.equal(columnReasonLabel({ reason_code: "row_change", transforms: ["skip_footer"] }), "Skipped trailing rows");
    assert.equal(realReversibilityReview2.tables[0].columns.find((c) => c.source === "name")!.reason_code, "byte_length");
  });
});

describe("review 3: a real Validate report with a conditional column enforced at Execute", () => {
  it("reads Enforced at Execute and says it is checked on the sample", async () => {
    const { realReversibilityReview3 } = await import("../components/exit/realExitPayloads.js");
    const email = realReversibilityReview3.tables[0].columns.find((c) => c.source === "email")!;
    assert.equal(email.check?.enforcement, "at_execute");
    assert.deepEqual(columnPillPresentation(email), { label: "Enforced at Execute", tone: "ok" });
    assert.equal(checkText(email.check), "Checked on the sample (2 observed rows); enforced on every row at Execute.");
    assert.equal(realReversibilityReview3.data_checked.covers_population, false);
    assert.deepEqual(gatePresentation(realReversibilityReview3).label, "Gate passes");
  });
});
