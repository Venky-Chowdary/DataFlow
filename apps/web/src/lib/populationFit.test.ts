import test from "node:test";
import assert from "node:assert/strict";

import { createNewFitWidenActions, createNewFitWidenLabel, populationFitSummary } from "./populationFit";

test("a clean preview never claims population fit", () => {
  const s = populationFitSummary({
    evidence: "sampled",
    rows_scanned: 25,
    rows_total: 1000000,
    scanned_population: false,
    bounded_columns: [{ source: "arr_time", target_type: "NUMBER(11,8)" }],
    findings: [],
  });
  assert.ok(s);
  assert.equal(s.proven, false);
  assert.match(s.headline, /unproven/);
  assert.doesNotMatch(s.headline, /Every value/);
});

test("an exact clean scan is allowed to say every row", () => {
  const s = populationFitSummary({
    evidence: "exact",
    rows_scanned: 1000000,
    rows_total: 1000000,
    scanned_population: true,
    bounded_columns: [{ source: "arr_time", target_type: "NUMBER(11,8)" }],
    findings: [],
  });
  assert.ok(s);
  assert.equal(s.proven, true);
  assert.match(s.headline, /Every value in 1,000,000 source row\(s\)/);
});

test("findings name the column, the carrier and the first offending rows", () => {
  const s = populationFitSummary({
    evidence: "exact",
    rows_scanned: 1000000,
    rows_total: 1000000,
    bounded_columns: [{ source: "arr_time", target_type: "NUMBER(11,8)" }],
    findings: [
      {
        source: "arr_time",
        target: "arr_time",
        target_type: "NUMBER(11,8)",
        unfit_rows: 21,
        example_rows: [431, 433, 3596],
        example_values: ["9999.99999999"],
        aborts_job: true,
        suggested_target_type: "NUMBER(12,9)",
        suggested_fix: "Open Map → widen arr_time to NUMBER(12,9)",
      },
    ],
  });
  assert.ok(s);
  assert.equal(s.proven, false);
  assert.match(s.headline, /21 value\(s\) in all 1,000,000 row\(s\)/);
  assert.match(s.headline, /arr_time → NUMBER\(11,8\)/);
  assert.deepEqual(s.offenders[0].exampleRows, [431, 433, 3596]);
  assert.equal(s.offenders[0].abortsJob, true);
  assert.equal(s.offenders[0].suggestedTargetType, "NUMBER(12,9)");
  assert.match(s.offenders[0].suggestedFix, /NUMBER\(12,9\)/);
});

test("float32 clock residue names the dest widen, never a blank fix", () => {
  const s = populationFitSummary({
    evidence: "exact",
    rows_scanned: 293,
    rows_total: 293,
    bounded_columns: [{ source: "DEP_TIME", target_type: "NUMBER(9,6)" }],
    findings: [
      {
        source: "DEP_TIME",
        target: "DEP_TIME",
        target_type: "NUMBER(9,6)",
        unfit_rows: 1,
        example_rows: [293],
        example_values: ["7.9166665"],
        aborts_job: true,
        suggested_target_type: "NUMBER(10,7)",
        suggested_fix:
          "Open Map → widen DEP_TIME to NUMBER(10,7) (or ALTER the destination) → re-Validate. Do not silently truncate.",
      },
    ],
  });
  assert.ok(s);
  assert.equal(s.offenders[0].suggestedTargetType, "NUMBER(10,7)");
  assert.match(s.offenders[0].suggestedFix, /Do not silently truncate/);
});

test("a partial scan says how far it got", () => {
  const s = populationFitSummary({
    evidence: "partial",
    rows_scanned: 5000,
    rows_total: 1000000,
    bounded_columns: [{ source: "c", target_type: "VARCHAR(255)" }],
    findings: [],
  });
  assert.ok(s);
  assert.equal(s.proven, false);
  assert.match(s.headline, /5,000 of 1,000,000/);
});

test("nothing bounded is a pass, not an unmeasured warning", () => {
  const s = populationFitSummary({
    evidence: "unmeasured",
    rows_scanned: 0,
    bounded_columns: [],
    findings: [],
    safe_by_declaration: ["arr_time"],
  });
  assert.ok(s);
  assert.equal(s.proven, true);
  assert.match(s.headline, /no value scan needed/);
});

test("no payload renders nothing rather than a guess", () => {
  assert.equal(populationFitSummary(undefined), null);
  assert.equal(populationFitSummary(null), null);
});

test("flights create-new NUMBER(15,11) widens are one Apply, not Open Map", () => {
  const actions = createNewFitWidenActions({
    passed: false,
    gates: [
      {
        id: "g3f_population_fit",
        status: "block",
        message: "19564 value(s) in 91944 scanned row(s)",
        details: {
          create_new_table: true,
          suggested_actions: [
            {
              kind: "change_target_type",
              column: "DEP_TIME",
              to_type: "NUMBER(15,11)",
              label: "Widen 'DEP_TIME' CREATE type to NUMBER(15,11)",
              requires_ddl: false,
              mapping_applyable: true,
              apply_proven: true,
            },
            {
              kind: "change_target_type",
              column: "ARR_TIME",
              to_type: "NUMBER(15,11)",
              label: "Widen 'ARR_TIME' CREATE type to NUMBER(15,11)",
              requires_ddl: false,
              mapping_applyable: true,
              apply_proven: true,
            },
            { kind: "review_mappings", label: "Review mappings" },
          ],
        },
      },
    ],
    blockers: [],
  } as never);
  assert.equal(actions.length, 2);
  assert.deepEqual(actions.map((a) => a.column), ["DEP_TIME", "ARR_TIME"]);
  assert.equal(
    createNewFitWidenLabel(actions),
    "Widen CREATE types to fit this source (2 columns)",
  );
});

test("live DDL widens are not Apply", () => {
  const actions = createNewFitWidenActions({
    passed: false,
    gates: [
      {
        id: "g3f_population_fit",
        status: "block",
        details: {
          create_new_table: false,
          suggested_actions: [
            {
              kind: "change_target_type",
              column: "DEP_TIME",
              to_type: "NUMBER(15,11)",
              requires_ddl: true,
              mapping_applyable: false,
              apply_proven: true,
            },
          ],
        },
      },
    ],
    blockers: [],
  } as never);
  assert.equal(actions.length, 0);
});
