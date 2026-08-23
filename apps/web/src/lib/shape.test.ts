import assert from "node:assert/strict";
import { test } from "node:test";
import {
  changedCellIndex,
  describeStep,
  fieldsFor,
  linesToList,
  missingRequired,
  moveStep,
  recipePayload,
  removeStep,
  sameRecipe,
  sortSuggestions,
  summarizeEffect,
  toggleStep,
  type ShapeEffect,
  type ShapeOperation,
  type ShapeStepWire,
  type ShapeSuggestion,
} from "./shape";

const TRIM: ShapeOperation = {
  op: "trim",
  summary: "Remove leading and trailing whitespace",
  active: false,
  needs_column: true,
  options: [],
  required: [],
  expression_option: null,
};

const FILTER: ShapeOperation = {
  op: "filter_rows",
  summary: "Keep only rows the condition matches",
  active: true,
  needs_column: false,
  options: ["condition", "keep"],
  required: ["condition"],
  expression_option: "condition",
};

const ROUND: ShapeOperation = {
  op: "round_number",
  summary: "Round to N decimal places",
  active: false,
  needs_column: true,
  options: ["places"],
  required: ["places"],
  expression_option: null,
};

const steps: ShapeStepWire[] = [
  { op: "trim", column: "name" },
  { op: "round_number", column: "arr_time", options: { places: 8 } },
  { op: "filter_rows", options: { condition: "[status] <> 'void'" } },
];

test("reordering a step returns a new list and refuses to fall off either end", () => {
  const moved = moveStep(steps, 2, -1);
  assert.deepEqual(moved.map((s) => s.op), ["trim", "filter_rows", "round_number"]);
  // The original list is untouched: order is state the caller owns.
  assert.deepEqual(steps.map((s) => s.op), ["trim", "round_number", "filter_rows"]);
  assert.equal(moveStep(steps, 0, -1), steps);
  assert.equal(moveStep(steps, 2, 1), steps);
  assert.equal(moveStep(steps, 9, 1), steps);
});

test("a disabled step stays in the list, so its order survives being toggled off", () => {
  const off = toggleStep(steps, 1);
  assert.equal(off.length, 3);
  assert.equal(off[1].enabled, false);
  assert.equal(toggleStep(off, 1)[1].enabled, true);
});

test("removing a step drops exactly one position", () => {
  assert.deepEqual(removeStep(steps, 1).map((s) => s.op), ["trim", "filter_rows"]);
  assert.equal(removeStep(steps, 5), steps);
});

test("a step reads as its operation and target until the operator names it", () => {
  assert.equal(describeStep({ op: "trim", column: "name" }, TRIM), "Remove leading and trailing whitespace · name");
  assert.equal(describeStep({ op: "trim", column: "name", label: "Tidy names" }, TRIM), "Tidy names");
  assert.equal(describeStep({ op: "filter_rows", options: { condition: "1 = 1" } }, FILTER), "Keep only rows the condition matches");
  assert.equal(
    describeStep({ op: "derive_column", options: { to: "total" } }),
    "derive_column · total",
  );
});

test("an expression option renders as an expression field, not a text box", () => {
  const fields = fieldsFor(FILTER);
  assert.deepEqual(fields.map((f) => [f.name, f.kind]), [
    ["condition", "expression"],
    ["keep", "boolean"],
  ]);
  assert.equal(fields[0].required, true);
  assert.equal(fields[1].required, false);
  assert.equal(fieldsFor(ROUND)[0].kind, "number");
});

test("a missing required option is named before the step can be added", () => {
  assert.match(missingRequired(TRIM, "", {}), /Pick the column/);
  assert.equal(missingRequired(TRIM, "name", {}), "");
  assert.match(missingRequired(ROUND, "arr_time", {}), /Decimal places is required/);
  assert.equal(missingRequired(ROUND, "arr_time", { places: 8 }), "");
  // Zero is a real answer: round to no decimal places.
  assert.equal(missingRequired(ROUND, "arr_time", { places: 0 }), "");
  assert.match(missingRequired(FILTER, "", { condition: "" }), /Condition is required/);
});

test("a one-per-line list drops blank lines and surrounding space", () => {
  assert.deepEqual(linesToList(" N/A \n\nunknown\n  \n-\n"), ["N/A", "unknown", "-"]);
});

test("the effect sentence carries the ledger terms, not just a row count", () => {
  const effect: ShapeEffect = {
    rows_in: 100,
    rows_out: 97,
    rows_shaped_out: 2,
    rows_diverted: 1,
    cells_changed: 14,
    nulls_introduced: 3,
    balanced: true,
    steps: [],
  };
  const sentence = summarizeEffect(effect);
  assert.match(sentence, /100 row\(s\) in/);
  assert.match(sentence, /2 shaped out/);
  assert.match(sentence, /1 diverted/);
  assert.match(sentence, /14 cell\(s\) changed/);
  assert.match(sentence, /3 null\(s\) introduced/);
  assert.equal(summarizeEffect(null), "");
});

test("a removed cell is not highlighted as a change in the after grid", () => {
  const index = changedCellIndex([
    { row: 0, column: "name", kind: "changed" },
    { row: 1, column: "total", kind: "added" },
    { row: 1, column: "gone", kind: "removed" },
  ]);
  assert.ok(index.has("0:name"));
  assert.ok(index.has("1:total"));
  assert.ok(!index.has("1:gone"));
});

test("a recipe of only disabled steps sends nothing, so an untouched draft is untouched", () => {
  assert.equal(recipePayload([]), undefined);
  assert.equal(recipePayload([{ op: "trim", column: "name", enabled: false }]), undefined);
  assert.deepEqual(recipePayload([{ op: "trim", column: "name" }]), {
    steps: [{ op: "trim", column: "name" }],
  });
});

test("recipe identity compares the program, so an approval survives a re-render", () => {
  assert.ok(sameRecipe({ steps }, { steps: steps.slice() }));
  assert.ok(!sameRecipe({ steps }, { steps: moveStep(steps, 0, 1) }));
  assert.ok(sameRecipe(null, { steps: [] }));
});

test("a blocking suggestion outranks a decision, and a decision outranks hygiene", () => {
  const suggestions: ShapeSuggestion[] = [
    { id: "trim:name", title: "", reason: "", rows_affected: 40, severity: "hygiene", step: { op: "trim", column: "name" } },
    { id: "parse_date:d", title: "", reason: "", rows_affected: 3, severity: "decision", step: { op: "parse_date", column: "d" } },
    { id: "round_number:a", title: "", reason: "", rows_affected: 2, severity: "blocking", step: { op: "round_number", column: "a" } },
    { id: "trim:city", title: "", reason: "", rows_affected: 90, severity: "hygiene", step: { op: "trim", column: "city" } },
  ];
  assert.deepEqual(sortSuggestions(suggestions).map((s) => s.id), [
    "round_number:a",
    "parse_date:d",
    "trim:city",
    "trim:name",
  ]);
});
