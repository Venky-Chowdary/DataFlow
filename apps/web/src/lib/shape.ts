/**
 * What a pre-load transform recipe is on the client, and the pure decisions about it.
 *
 * The server owns the meaning of a recipe (which operations exist, what each
 * one does to a value, what its identity is). This module owns only what the
 * editor needs to build one: the wire shapes, the ordering of applied steps,
 * and the option fields to render for the operation the operator picked. Every
 * function here is pure so the editor's behaviour is testable without a
 * browser, and so no rule is duplicated in a component where it could drift
 * from the engine.
 */

/** One applied step, exactly as the API accepts it. */
export interface ShapeStepWire {
  op: string;
  column?: string;
  options?: Record<string, unknown>;
  enabled?: boolean;
  on_error?: string;
  label?: string;
}

export interface ShapeRecipeWire {
  steps: ShapeStepWire[];
}

/** One operation the engine will accept, as described by `/shape/catalog`. */
export interface ShapeOperation {
  op: string;
  summary: string;
  active: boolean;
  needs_column: boolean;
  options: string[];
  required: string[];
  expression_option: string | null;
}

export interface ShapeFunctionDoc {
  name: string;
  min_args: number;
  max_args: number;
  summary: string;
}

export interface ShapeErrorPolicy {
  value: string;
  label: string;
  detail: string;
}

export interface ShapeCatalog {
  operations: ShapeOperation[];
  functions: ShapeFunctionDoc[];
  max_steps: number;
  max_preview_rows: number;
  error_policies: ShapeErrorPolicy[];
  post_load_only: { operations: string[]; reason: string };
}

export interface ShapeColumnProfile {
  name: string;
  rows: number;
  blanks: number;
  non_blank: number;
  distinct: number;
  distinct_capped: boolean;
  samples: string[];
  logical_type: string;
  numeric_like: number;
  integer_like: number;
  max_scale: number;
  max_integer_digits: number;
  min: string;
  max: string;
  untrimmed: number;
  inner_whitespace: number;
  sentinels: Record<string, number>;
  non_printable: number;
  unnormalized_unicode: number;
  boolean_like: number;
  scale_counts: Record<string, number>;
  date_formats: string[];
  ambiguous_date_order: boolean;
  max_length: number;
}

export interface ShapeSuggestion {
  id: string;
  title: string;
  reason: string;
  rows_affected: number;
  severity: string;
  step: ShapeStepWire;
}

export interface ShapeProfileResponse {
  sampled_rows: number;
  columns: ShapeColumnProfile[];
  suggestions: ShapeSuggestion[];
  sample_notice: string;
}

export interface ShapeIdentity {
  valid: boolean;
  recipe_hash: string;
  step_count: number;
  has_active_step: boolean;
  input_columns: string[];
  output_columns: string[];
  summary: string;
  steps: ShapeStepWire[];
}

export interface ShapeStepEffect {
  step: number;
  op: string;
  label: string;
  active: boolean;
  rows_in: number;
  rows_out: number;
  rows_removed: number;
  rows_diverted: number;
  cells_changed: number;
  nulls_introduced: number;
  errors: number;
  error_samples?: Record<string, unknown>[];
}

export interface ShapeEffect {
  rows_in: number;
  rows_out: number;
  rows_shaped_out: number;
  rows_diverted: number;
  cells_changed: number;
  nulls_introduced: number;
  balanced: boolean;
  steps: ShapeStepEffect[];
  diverted_samples?: Record<string, unknown>[];
}

export interface ShapeChangedCell {
  row: number;
  column: string;
  kind: "changed" | "added" | "removed";
}

export interface ShapeRefusal {
  step: number;
  op: string;
  column: string;
  row: number;
  message: string;
}

export interface ShapePreviewResponse {
  recipe: ShapeIdentity;
  sampled_rows: number;
  before: Record<string, unknown>[];
  after: Record<string, unknown>[];
  effect: ShapeEffect;
  changed_cells: ShapeChangedCell[];
  refusal: ShapeRefusal | null;
  shaped_profile: ShapeColumnProfile[];
  suggestions: ShapeSuggestion[];
}

/** Move one applied step, returning a new list (out-of-range moves are no-ops). */
export function moveStep(steps: ShapeStepWire[], index: number, delta: number): ShapeStepWire[] {
  const target = index + delta;
  if (index < 0 || index >= steps.length) return steps;
  if (target < 0 || target >= steps.length) return steps;
  const next = steps.slice();
  const [moved] = next.splice(index, 1);
  next.splice(target, 0, moved);
  return next;
}

export function removeStep(steps: ShapeStepWire[], index: number): ShapeStepWire[] {
  if (index < 0 || index >= steps.length) return steps;
  return steps.filter((_, i) => i !== index);
}

export function toggleStep(steps: ShapeStepWire[], index: number): ShapeStepWire[] {
  if (index < 0 || index >= steps.length) return steps;
  return steps.map((step, i) =>
    i === index ? { ...step, enabled: step.enabled === false } : step,
  );
}

/** How a step reads in the applied list when the operator gave it no label. */
export function describeStep(step: ShapeStepWire, operation?: ShapeOperation): string {
  if (step.label) return step.label;
  const options = step.options ?? {};
  const produced = typeof options.to === "string" ? options.to : "";
  const target = step.column || produced;
  const summary = operation?.summary ?? step.op;
  return target ? `${summary} · ${target}` : summary;
}

/** The kind of input an option needs, so the editor is not a wall of text boxes. */
export type ShapeFieldKind = "text" | "number" | "boolean" | "columns" | "list" | "expression" | "choice";

export interface ShapeField {
  name: string;
  kind: ShapeFieldKind;
  label: string;
  hint: string;
  required: boolean;
  choices?: string[];
}

const FIELD_LABEL: Record<string, string> = {
  to: "New column name",
  to_type: "Type",
  value: "Value",
  values: "Values",
  columns: "Columns",
  separator: "Separator",
  into: "New column names",
  limit: "Maximum pieces",
  mode: "Case",
  characters: "Characters",
  width: "Width",
  fill: "Fill character",
  side: "Side",
  search: "Find",
  replacement: "Replace with",
  regex: "Treat as a pattern",
  places: "Decimal places",
  min: "Minimum",
  max: "Maximum",
  format: "Input format",
  output_format: "Output format",
  form: "Normal form",
  expression: "Expression",
  condition: "Condition",
  keep: "Keep matching rows",
  reason: "Quarantine reason",
};

const FIELD_HINT: Record<string, string> = {
  values: "One per line. Each is treated as null.",
  columns: "Pick the columns, in the order they apply.",
  into: "One name per piece, in order.",
  format: "Explicit, e.g. %d/%m/%Y — the engine never guesses day/month order.",
  places: "Digits kept after the decimal point.",
  condition: "A row-local expression, e.g. [status] <> 'void'.",
  expression: "A row-local expression over this row's columns.",
  keep: "Off keeps the rows the condition does not match.",
  reason: "Recorded on every diverted row.",
};

const FIELD_CHOICES: Record<string, string[]> = {
  to_type: ["text", "integer", "decimal", "boolean", "date", "timestamp"],
  mode: ["upper", "lower", "title"],
  side: ["left", "right"],
  form: ["NFC", "NFD", "NFKC", "NFKD"],
};

const NUMBER_FIELDS = new Set(["places", "width", "limit", "min", "max"]);
const BOOLEAN_FIELDS = new Set(["regex", "keep"]);
const COLUMN_LIST_FIELDS = new Set(["columns"]);
const LIST_FIELDS = new Set(["values", "into"]);

/** The fields to render for one operation, in the catalog's own order. */
export function fieldsFor(operation: ShapeOperation): ShapeField[] {
  return operation.options.map((name) => {
    const required = operation.required.includes(name);
    const kind: ShapeFieldKind = name === operation.expression_option
      ? "expression"
      : FIELD_CHOICES[name]
        ? "choice"
        : NUMBER_FIELDS.has(name)
          ? "number"
          : BOOLEAN_FIELDS.has(name)
            ? "boolean"
            : COLUMN_LIST_FIELDS.has(name)
              ? "columns"
              : LIST_FIELDS.has(name)
                ? "list"
                : "text";
    return {
      name,
      kind,
      label: FIELD_LABEL[name] ?? name,
      hint: FIELD_HINT[name] ?? "",
      required,
      choices: FIELD_CHOICES[name],
    };
  });
}

/** Which required option the draft is still missing, so Add can say why. */
export function missingRequired(
  operation: ShapeOperation,
  column: string,
  options: Record<string, unknown>,
): string {
  if (operation.needs_column && !column) return "Pick the column this step applies to.";
  for (const name of operation.required) {
    const value = options[name];
    const empty = value === undefined
      || value === null
      || value === ""
      || (Array.isArray(value) && value.length === 0);
    if (empty) return `${FIELD_LABEL[name] ?? name} is required for ${operation.op}.`;
  }
  return "";
}

/** Parse a textarea of one-per-line values into the list the API expects. */
export function linesToList(raw: string): string[] {
  return raw
    .split("\n")
    .map((line) => line.trim())
    .filter((line) => line.length > 0);
}

/** One sentence stating what the recipe did to the sample, ledger terms included. */
export function summarizeEffect(effect: ShapeEffect | null): string {
  if (!effect) return "";
  const parts = [`${effect.rows_in.toLocaleString()} row(s) in`, `${effect.rows_out.toLocaleString()} out`];
  if (effect.rows_shaped_out) parts.push(`${effect.rows_shaped_out.toLocaleString()} removed`);
  if (effect.rows_diverted) parts.push(`${effect.rows_diverted.toLocaleString()} diverted`);
  if (effect.cells_changed) parts.push(`${effect.cells_changed.toLocaleString()} cell(s) changed`);
  if (effect.nulls_introduced) parts.push(`${effect.nulls_introduced.toLocaleString()} null(s) introduced`);
  return parts.join(" · ");
}

/** Index of changed cells, keyed row:column, for highlighting the after grid. */
export function changedCellIndex(marks: ShapeChangedCell[]): Set<string> {
  return new Set(marks.filter((m) => m.kind !== "removed").map((m) => `${m.row}:${m.column}`));
}

/** Whether two recipes are the same program — used to spot an unapproved edit. */
export function sameRecipe(a: ShapeRecipeWire | null, b: ShapeRecipeWire | null): boolean {
  return JSON.stringify(a?.steps ?? []) === JSON.stringify(b?.steps ?? []);
}

/** The recipe payload to send with a plan or a run — empty when nothing is shaped. */
export function recipePayload(steps: ShapeStepWire[]): ShapeRecipeWire | undefined {
  const enabled = steps.filter((step) => step.enabled !== false);
  return enabled.length ? { steps } : undefined;
}

/** Severity ordering for the suggestion list: decisions before hygiene. */
export function suggestionRank(severity: string): number {
  if (severity === "blocking") return 0;
  if (severity === "decision") return 1;
  return 2;
}

export function sortSuggestions(suggestions: ShapeSuggestion[]): ShapeSuggestion[] {
  return suggestions.slice().sort((a, b) => {
    const bySeverity = suggestionRank(a.severity) - suggestionRank(b.severity);
    if (bySeverity !== 0) return bySeverity;
    return b.rows_affected - a.rows_affected;
  });
}
