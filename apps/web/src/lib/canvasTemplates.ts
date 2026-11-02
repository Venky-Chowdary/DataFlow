/**
 * Five local recipes. Each one is a list of shape steps or a refusal.
 * None of them emit hash_pii — that digest is the server's HMAC, not a template.
 */

import type { ShapeStepWire, ShapeSuggestion } from "./shape";

export interface TemplateColumn {
  name: string;
  type: string;
}

export interface TemplateResult {
  steps: ShapeStepWire[];
  error: string;
}

const TEXT_TYPE = /char|text|string|varchar|clob|nvarchar/i;
const DATE_TYPE = /date|time/i;

function stamp(step: ShapeStepWire, sourceTable: string): ShapeStepWire {
  const table = sourceTable.trim();
  return table ? { ...step, source_table: table } : step;
}

export function trimTextColumns(columns: TemplateColumn[], sourceTable = ""): TemplateResult {
  const steps = columns
    .filter((column) => column.name.trim() && TEXT_TYPE.test(column.type))
    .map((column) => stamp({ op: "trim", column: column.name }, sourceTable));
  if (steps.length === 0) {
    return { steps: [], error: "No text columns are declared, so trim-all has nothing to add." };
  }
  return { steps, error: "" };
}

export function dropEmptyColumns(suggestions: ShapeSuggestion[], sourceTable = ""): TemplateResult {
  const steps = suggestions
    .filter((suggestion) => suggestion.step?.op === "drop_column" && suggestion.step.column)
    .map((suggestion) => stamp({ ...suggestion.step }, sourceTable));
  if (steps.length === 0) {
    return { steps: [], error: "The profile did not suggest dropping any empty column." };
  }
  return { steps, error: "" };
}

export function parseDateColumns(
  columns: TemplateColumn[],
  format: string,
  sourceTable = "",
): TemplateResult {
  const explicit = format.trim();
  if (!explicit) {
    return {
      steps: [],
      error: "Parse dates needs an explicit format. The engine does not guess day and month order.",
    };
  }
  const steps = columns
    .filter((column) => column.name.trim() && DATE_TYPE.test(column.type))
    .map((column) => stamp({
      op: "parse_date",
      column: column.name,
      options: { format: explicit },
    }, sourceTable));
  if (steps.length === 0) {
    return { steps: [], error: "No date or time columns are declared." };
  }
  return { steps, error: "" };
}

export function divertMatchingRows(expression: string, sourceTable = ""): TemplateResult {
  const condition = expression.trim();
  if (!condition) {
    return { steps: [], error: "Divert rows needs a condition. Nothing was added." };
  }
  return {
    steps: [stamp({ op: "divert_rows", options: { condition } }, sourceTable)],
    error: "",
  };
}

export function hashIdentityKey(columns: string[], sourceTable = ""): TemplateResult {
  const names = columns.map((name) => name.trim()).filter(Boolean);
  if (names.length === 0) {
    return { steps: [], error: "Hash identity needs at least one column." };
  }
  return {
    steps: [stamp({
      op: "hash_identity",
      options: { columns: names, to: "_df_row_key" },
    }, sourceTable)],
    error: "",
  };
}

export function templateUsesHashPii(result: TemplateResult): boolean {
  return result.steps.some((step) => step.op === "hash_pii");
}
