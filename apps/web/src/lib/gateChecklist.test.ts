/**
 * Validate as a ranked checklist (UI pass 2, item 2).
 * Run: npx tsx --test apps/web/src/lib/gateChecklist.test.ts
 */
import assert from "node:assert/strict";
import { describe, it } from "node:test";
import { buildChecklist, checklistSummary, findingGroupLine, gateOutcome, groupFindingsByCause, scopeLine } from "./gateChecklist.ts";
import { explainGate, explainVerdict } from "./explainCatalog.ts";
import { compareGateDisplayOrder } from "./preflightGates.ts";

const gates = [
  { id: "g1_source", status: "pass", message: "Source readable — 5 columns", details: { evidence_scope: { coverage: "sample", sample_rows: 5 } } },
  { id: "g3_schema_contract", status: "block", message: "3 columns narrow", details: { issue_texts: ["email: value too long for VARCHAR(120)", "name: value too long for VARCHAR(120)", "city: Value too long for VARCHAR(120).", "zip: not a number"] } },
  { id: "g13_source_coverage", status: "skip", message: "Source coverage check could not run: the service that runs it did not answer.", details: { unmeasured: true } },
  { id: "g14_destination_requirements", status: "skip", message: "Destination requirements do not apply to this route.", details: { applicable: false } },
  { id: "g22_dest_referential_integrity", status: "skip", message: "No foreign keys were declared on this route — destination referential integrity was not asked.", details: { declared: false } },
  { id: "g15_dest_exists_shape", status: "warn", message: "Destination existence unproven" },
  { id: "g2_destination", status: "pass", message: "ok" },
];

describe("gateOutcome: structured fields only (review UI2 #4)", () => {
  it("reads unmeasured / retry_validate / declared / applicable, never the message", () => {
    assert.equal(gateOutcome(gates[2]), "could_not_run");
    assert.equal(gateOutcome(gates[3]), "not_applicable");
    assert.equal(gateOutcome(gates[4]), "not_applicable");
    // G14 when the destination's nullability catalog is unreadable (destination_requirements_gate.py).
    assert.equal(gateOutcome({ id: "g14_destination_requirements", status: "skip",
      message: "Destination nullability catalog unreadable — required-column coverage is unmeasured (not proven). The NOT NULL contract is still enforced at write.",
      details: { reason: "nullability_metadata_unavailable", unmeasured: true, remediation_kind: "retry_validate" } }), "could_not_run");
    assert.equal(gateOutcome({ id: "g2", status: "skip", message: "not applicable", details: { error: "boom" } }), "could_not_run", "words do not decide");
    assert.equal(gateOutcome({ id: "g21_control_totals", status: "skip", details: { declared: true, evidence: "unmeasured", rule_id: "g21_control_totals.post_write" } }), "after_run");
    assert.equal(gateOutcome({ id: "g19_dest_schema_replacement", status: "skip", details: { dest_recreated: false, destination_table_exists: null } }), "not_applicable");
  });
  it("an unknown skip or status is Could not run, never passed or hidden", () => {
    assert.equal(gateOutcome({ id: "g99", status: "skip", message: "Nothing to see here" }), "could_not_run");
    assert.equal(gateOutcome({ id: "g99", status: "errored" }), "could_not_run");
    assert.equal(gateOutcome(gates[1]), "blocking");
    assert.equal(gateOutcome(gates[5]), "review");
    assert.equal(gateOutcome(gates[0]), "passed");
  });
});

describe("buildChecklist", () => {
  const issues = (g: { details?: Record<string, unknown> | null }) => (g.details?.issue_texts as string[] | undefined) ?? [];
  const list = buildChecklist(gates, issues, compareGateDisplayOrder);

  it("ranks Blocking, Could not run, Needs review, Passed; not-applicable apart", () => {
    assert.deepEqual(list.groups.map((g) => g.title), ["Blocking", "Could not run", "Needs review", "Passed"]);
    assert.deepEqual(list.groups.map((g) => g.collapsedByDefault), [false, false, false, true]);
    assert.deepEqual(list.notApplicable.map((r) => r.id), ["g14_destination_requirements", "g22_dest_referential_integrity"]);
    assert.deepEqual(list.groups[3].rows.map((r) => r.code), ["G1", "G2"], "gate order inside a group");
  });

  it("never counts a check that could not run as passed", () => {
    assert.equal(list.counts.passed, 2);
    assert.equal(list.counts.could_not_run, 1);
    assert.equal(checklistSummary(list.counts), "1 blocking · 1 could not run · 1 to review · 2 passed · 2 not applicable");
    const withPending = buildChecklist([{ id: "g1_source", status: "running" }]);
    assert.equal(withPending.groups[0].title, "Running", "a running check is shown and counted");
  });

  it("groups same-cause findings with a count", () => {
    const blocking = list.groups[0].rows[0];
    assert.deepEqual(blocking.findings.map((g) => findingGroupLine(g)), [
      "Value too long for VARCHAR(120): email, name, city",
      "Not a number: zip",
    ]);
  });

  it("says how much a check looked at", () => {
    assert.equal(list.groups[3].rows[0].scope, "Checked on a sample of 5 rows");
    assert.equal(scopeLine({ evidence_scope: { coverage: "full_schema", columns: 1 } }), "Checked against the full schema (1 column)");
    assert.equal(scopeLine(null), "");
  });
});

describe("groupFindingsByCause (review UI2 #8)", () => {
  it("keeps causes without an object as their own line, and counts repeats", () => {
    const groups = groupFindingsByCause(["Duplicate key 7", "Duplicate key 7", "column id: null in a required column"]);
    assert.deepEqual(groups.map((g) => findingGroupLine(g)), ["Duplicate key 7 (2 times)", "Null in a required column: id"]);
  });
  it("never reads a label as an object", () => {
    const groups = groupFindingsByCause(["Probe: information_schema", "Error: connection refused", "Privilege status: unavailable"]);
    assert.deepEqual(groups.map((g) => findingGroupLine(g)), ["Probe: information_schema", "Error: connection refused", "Privilege status: unavailable"]);
  });
  it("names every object, cutting a long list behind 'and N more'", () => {
    const lines = ["a", "b", "c", "d", "e", "f"].map((c) => `${c}: value too long`);
    const [group] = groupFindingsByCause(lines);
    assert.equal(findingGroupLine(group), "Value too long: a, b, c, d and 2 more");
    assert.equal(findingGroupLine(group, true), "Value too long: a, b, c, d, e, f");
  });
});

describe("explain catalog (deterministic, never a model)", () => {
  it("answers what, this result, what to do for a check", () => {
    const failed = explainGate("g3_schema_contract", "blocking", "3 columns narrow");
    assert.equal(failed.title, "Schema contract");
    assert.match(failed.result, /^This check failed, so Run stays locked until it passes\. The engine said: "3 columns narrow"\.$/);
    assert.match(failed.next, /Widen the destination column/);
    assert.match(failed.source, /not by AI/);
    assert.match(explainGate("g13_source_coverage", "could_not_run").result, /nothing was proven either way\. It is not counted as passed/);
    // Review UI2 #12: a review finding never promises Run is open.
    const review = explainGate("g15_dest_exists_shape", "review");
    assert.doesNotMatch(`${review.result} ${review.next}`, /does not lock Run/);
    assert.match(review.next, /while it reads Review, Run stays locked/);
    assert.equal(explainGate("g22_dest_referential_integrity", "not_applicable").next, "Nothing to do.");
  });
  it("explains a verdict in the Trust Report's words", () => {
    const v = explainVerdict("no_difference_found");
    assert.equal(v.title, "Verdict: No difference found");
    assert.match(v.result, /sample or a read-back, not every row/);
    assert.match(explainVerdict("unexplained").next, /Open the Trust Report/);
  });
});
