/**
 * Validate as a ranked checklist (UI pass 2, item 2).
 * Run: npx tsx --test apps/web/src/lib/gateChecklist.test.ts
 */
import assert from "node:assert/strict";
import { describe, it } from "node:test";
import { buildChecklist, checklistSummary, findingGroupLine, findingsFromDetails, gateOutcome, groupFindingsByCause, scopeLine } from "./gateChecklist.ts";
import { explainGate, explainVerdict } from "./explainCatalog.ts";
import { compareGateDisplayOrder } from "./preflightGates.ts";

const gates = [
  { id: "g1_source", status: "pass", message: "Source readable — 5 columns", details: { evidence_scope: { coverage: "sample", sample_rows: 5 } } },
  { id: "g3_schema_contract", status: "block", message: "3 columns narrow", details: { errors: [
    { column: "email", message: "Value too long for VARCHAR(120)" }, { column: "name", message: "Value too long for VARCHAR(120)" },
    { column: "city", message: "Value too long for VARCHAR(120)" }, { column: "zip", message: "Not a number" }] } },
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
  const list = buildChecklist(gates, undefined, compareGateDisplayOrder);

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

describe("findings come from structured fields only (re-review G)", () => {
  it("counts repeats of the same text", () => {
    const groups = groupFindingsByCause(["Duplicate key 7", "Duplicate key 7"]);
    assert.deepEqual(groups.map((g) => findingGroupLine(g)), ["Duplicate key 7 (2 times)"]);
  });
  it("shows a message exactly as the engine wrote it, never parsing an object out of it", () => {
    const groups = groupFindingsByCause(findingsFromDetails({ issue_texts: ["Table: orders missing", "Probe: information_schema", "Error: connection refused"] }));
    assert.deepEqual(groups.map((g) => findingGroupLine(g)), ["Table: orders missing", "Probe: information_schema", "Error: connection refused"]);
  });
  it("takes objects from the engine's fields", () => {
    const found = findingsFromDetails({
      issues_detail: [{ column: "email", message: "Value too long" }],
      low_confidence: [{ source: "cust_nm", reason: "Low confidence" }],
      unmeasured_columns: ["created_at"],
    });
    assert.deepEqual(found, [
      { cause: "Value too long", object: "email" },
      { cause: "Low confidence", object: "cust_nm" },
      { cause: "Required-column coverage not measured", object: "created_at" },
    ]);
  });
  it("names every object, cutting a long list behind 'and N more'", () => {
    const [group] = groupFindingsByCause(["a", "b", "c", "d", "e", "f"].map((c) => ({ cause: "Value too long", object: c })));
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

describe("every gate skip site is classified by its structured field (re-review B)", () => {
  it("matches what each site's field means; none falls to the default", async () => {
    const { readFileSync } = await import("node:fs");
    const { sites } = JSON.parse(readFileSync(new URL("./__fixtures__/gate_skip_sites.json", import.meta.url), "utf8")) as {
      sites: { site: string; fields: Record<string, unknown> }[];
    };
    assert.ok(sites.length >= 15);
    for (const { site, fields } of sites) {
      const meant = fields.unmeasured === true || fields.remediation_kind === "retry_validate"
        ? "could_not_run"
        : String(fields.rule_id || "").endsWith(".post_write") ? "after_run" : "not_applicable";
      const details = { ...fields, ...(typeof fields.rule_id === "string" ? { rule_id: `gx${fields.rule_id}` } : {}) };
      assert.equal(gateOutcome({ id: "gx", status: "skip", details }), meant, `${site} ${JSON.stringify(fields)}`);
    }
  });

  it("G15 on a create-new table is Not applicable, not Could not run", () => {
    assert.equal(gateOutcome({ id: "g15_dest_exists_shape", status: "skip",
      details: { shape: "create_new", applicable: false, rule_id: "g15_dest_exists_shape.create_new_table", remediation_kind: "review_map" } }), "not_applicable");
  });

  it("the browser's own local skips say why too", async () => {
    const { runLocalPreflight } = await import("./localPreflight.ts");
    const pf = runLocalPreflight({
      columns: ["id", "name"],
      rowCount: 10,
      mappings: [
        { source: "id", target: "id", confidence: 0.95, transform: "none", approved: true, requiresReview: false, isPii: false },
        { source: "name", target: "name", confidence: 0.9, transform: "none", approved: true, requiresReview: false, isPii: false },
      ],
      sampleRows: [{ id: 1, name: "a" }],
      destKind: "file_export",
    } as Parameters<typeof runLocalPreflight>[0]);
    const skips = pf.gates.filter((g) => g.status === "skip");
    assert.ok(skips.length > 0);
    const outcomes = Object.fromEntries(skips.map((g) => [g.id, gateOutcome(g)]));
    assert.equal(outcomes.g2_destination, "not_applicable", "file export needs no remote destination");
    assert.equal(outcomes.g8_reconciliation, "after_run");
    assert.equal(outcomes.g13_source_coverage, "could_not_run", "needs the API");
  });
});
