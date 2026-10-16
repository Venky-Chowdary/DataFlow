import assert from "node:assert/strict";
import { test } from "node:test";
import {
  applyColumnLayout,
  columnsEnteringColumnLayout,
  isColumnLayoutStep,
  layoutRowsFromSteps,
  type ColumnLayoutRow,
} from "./columnLayout";
import type { ShapeStepWire } from "./shape";

const COLUMNS = ["id", "email", "amount"];

function rows(overrides: Partial<ColumnLayoutRow>[] = []): ColumnLayoutRow[] {
  return COLUMNS.map((source, index) => ({
    source,
    name: source,
    kept: true,
    ...overrides[index],
  }));
}

test("an unchanged board writes no steps", () => {
  const result = applyColumnLayout([{ op: "trim", column: "email" }], rows(), COLUMNS);
  assert.equal(result.error, "");
  assert.deepEqual(result.steps, [{ op: "trim", column: "email" }]);
});

test("rename, drop, and reorder become rename and keep steps", () => {
  const edited: ColumnLayoutRow[] = [
    { source: "amount", name: "amount", kept: true },
    { source: "email", name: "customer_email", kept: true },
    { source: "id", name: "id", kept: false },
  ];
  const result = applyColumnLayout([{ op: "trim", column: "email" }], edited, COLUMNS);
  assert.equal(result.error, "");
  assert.deepEqual(result.steps.map((step) => step.op), ["trim", "drop_column", "rename_column", "keep_columns"]);
  assert.equal(result.steps[1].column, "id");
  assert.equal(result.steps[2].column, "email");
  assert.deepEqual(result.steps[2].options, { to: "customer_email" });
  assert.deepEqual(result.steps[3].options, { columns: ["amount", "customer_email"] });
  assert.ok(result.steps.slice(1).every(isColumnLayoutStep));
});

test("applying again replaces the previous layout and keeps the other steps first", () => {
  const first = applyColumnLayout([], [
    { source: "id", name: "order_id", kept: true },
    { source: "email", name: "email", kept: true },
    { source: "amount", name: "amount", kept: false },
  ], COLUMNS);
  const second = applyColumnLayout(
    [...first.steps, { op: "trim", column: "email" }],
    rows(),
    COLUMNS,
  );
  assert.deepEqual(second.steps.map((step) => step.op), ["trim"]);
});

test("a rename onto a column that is still kept is refused", () => {
  const edited: ColumnLayoutRow[] = [
    { source: "id", name: "email", kept: true },
    { source: "email", name: "id", kept: true },
    { source: "amount", name: "amount", kept: true },
  ];
  const result = applyColumnLayout([], edited, COLUMNS);
  assert.match(result.error, /temporary name/);
  assert.deepEqual(result.steps, []);
});

test("dropping a column frees its name for a rename", () => {
  const edited: ColumnLayoutRow[] = [
    { source: "id", name: "id", kept: false },
    { source: "email", name: "id", kept: true },
    { source: "amount", name: "amount", kept: true },
  ];
  const result = applyColumnLayout([], edited, COLUMNS);
  assert.equal(result.error, "");
  assert.deepEqual(result.steps.map((step) => step.op), ["drop_column", "rename_column"]);
  assert.equal(result.steps[0].column, "id");
  assert.equal(result.steps[1].column, "email");
});

test("a repeated target name is refused before a step is written", () => {
  const edited = rows();
  edited[1] = { ...edited[1], name: "id" };
  const result = applyColumnLayout([], edited, COLUMNS);
  assert.match(result.error, /used more than once/);
  assert.deepEqual(result.steps, []);
});

test("the board reads back the layout it wrote", () => {
  const edited: ColumnLayoutRow[] = [
    { source: "email", name: "customer_email", kept: true },
    { source: "id", name: "id", kept: true },
    { source: "amount", name: "amount", kept: false },
  ];
  const applied = applyColumnLayout([], edited, COLUMNS);
  assert.deepEqual(layoutRowsFromSteps(applied.steps, COLUMNS), edited);
});

test("layout columns follow non-layout renames and ignore a disabled step", () => {
  const steps: ShapeStepWire[] = [
    { op: "rename_column", column: "email", options: { to: "customer_email" } },
    { op: "drop_column", column: "amount", enabled: false },
    { op: "keep_columns", options: { columns: ["id"] }, label: "Column layout: keep" },
  ];
  assert.deepEqual(columnsEnteringColumnLayout(steps, COLUMNS), ["id", "customer_email", "amount"]);
});
