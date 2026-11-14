/**
 * W6 round 2, item 1: every count on the Map step comes from one selector.
 *
 * The fixture is the screenshot's (w6/after/map-light-1440.png): five sample
 * columns written to new columns of an existing table, none approved yet, and
 * two NOT NULL destination columns nothing maps to. The screen said "2 need
 * your review" and Review 2, while the tabs said "Needs attention (0)",
 * "Ready (0)" and the button "Approve eligible (5)".
 */
import assert from "node:assert/strict";
import { describe, it } from "node:test";
import { countByFilter, filterMappings, mapRowState, mapStepCounts } from "./columnWorkbench";
import { mapStatusLine } from "../components/mapping/MapIssueList";
import type { EditableMapping } from "./mapping";

const row = (source: string, inferredType: string, destType: string): EditableMapping => ({
  source,
  target: source,
  confidence: 0.99,
  inferredType,
  destType,
  existsInDestination: false,
  createNew: true,
  assignmentStrategy: "create_compatible_new",
  fidelity: "preserve",
  transform: "none",
  approved: false,
});

export const SCREENSHOT_ROWS = (): EditableMapping[] => [
  row("order_id", "INTEGER", "BIGINT"),
  row("customer_email", "VARCHAR", "VARCHAR"),
  row("order_amt", "DECIMAL(6,2)", "DECIMAL(38,2)"),
  row("order_date", "DATE", "DATE"),
  row("status", "VARCHAR", "VARCHAR"),
];

describe("Map step counts come from one selector", () => {
  it("the screenshot's rows: nothing needs attention, all five are ready to approve", () => {
    const rows = SCREENSHOT_ROWS();
    const counts = mapStepCounts(rows, 2);
    assert.deepEqual(
      { total: counts.total, mapped: counts.mapped, attention: counts.attention, ready: counts.ready, approvable: counts.approvable },
      { total: 5, mapped: 5, attention: 0, ready: 5, approvable: 5 },
    );
    assert.equal(counts.destinationIssues, 2);
  });

  it("the tabs reconcile: All = Needs attention + Ready, and they match the header and the button", () => {
    const rows = SCREENSHOT_ROWS();
    const counts = mapStepCounts(rows, 2);
    const tabs = countByFilter(rows, 0.85);
    assert.equal(tabs.all, counts.total);
    assert.equal(tabs.attention, counts.attention);
    assert.equal(tabs.clear, counts.ready);
    assert.equal(tabs.attention + tabs.clear, tabs.all);
    assert.equal(filterMappings(rows, { search: "", filter: "attention", sort: "name-asc", threshold: 0.85 }).length, counts.attention);
    assert.equal(filterMappings(rows, { search: "", filter: "clear", sort: "name-asc", threshold: 0.85 }).length, counts.ready);
    // The header says the same numbers in words; the destination issues are named, not counted as rows to review.
    assert.equal(mapStatusLine(counts), "5 of 5 mapped · 2 destination column issues · 5 ready to approve");
  });

  it("a row that needs a decision moves from Ready to Needs attention, and only Ready rows are approvable", () => {
    const rows = SCREENSHOT_ROWS();
    rows[1] = { ...rows[1], fidelity: "lossy_cast", destType: "VARCHAR(20)", inferredType: "VARCHAR(120)" };
    rows[3] = { ...rows[3], target: "" };
    rows[4] = { ...rows[4], approved: true };
    const counts = mapStepCounts(rows, 0);
    assert.equal(mapRowState(rows[1]), "attention");
    assert.equal(mapRowState(rows[3]), "attention");
    assert.equal(mapRowState(rows[4]), "approved");
    assert.deepEqual([counts.attention, counts.approvable, counts.approved, counts.ready], [2, 2, 1, 3]);
    assert.equal(counts.attention + counts.ready, counts.total);
    assert.equal(mapStatusLine(counts), "4 of 5 mapped · 2 need your review");
  });
});
