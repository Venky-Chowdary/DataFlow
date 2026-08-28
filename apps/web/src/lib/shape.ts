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
  expands?: boolean;
  family?: string;
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
  json_array_like?: number;
  json_object_like?: number;
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

/**
 * The transformed image a declared recipe produces, as the studio carries it
 * from Transform (pre-load) into Map, Validate and the run request.
 *
 * `columnTypes` is what the columns hold *after* the recipe: declared carriers
 * where no step wrote the column, re-read carriers where one did. Map must
 * decide fidelity from these — a column rounded to whole numbers is no longer a
 * lossy decimal, and saying otherwise is an untrue refusal.
 */
export interface TransformImage {
  hash: string;
  columns: string[];
  columnTypes: Record<string, string>;
  retypedColumns: Record<string, string>;
  /** Transformed sample rows — the values Map is shown, never a population claim. */
  sampleRows: Record<string, unknown>[];
}

export interface ShapeStepEffect {
  step: number;
  op: string;
  label: string;
  active: boolean;
  rows_in: number;
  rows_out: number;
  rows_removed: number;
  rows_expanded?: number;
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
  rows_expanded?: number;
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
  /**
   * Carriers of the transformed image: declared where no step wrote the column,
   * re-read from the transformed values where one did. Map decides fidelity from
   * these, so a rounded decimal is not still described as a lossy decimal.
   */
  column_types?: Record<string, string>;
  /** Only the columns whose carrier the recipe changed. */
  retyped_columns?: Record<string, string>;
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
  index_to: "Index column",
  keep_parent: "Keep the original JSON column",
  depth: "Flatten depth",
  keys: "Object keys to promote",
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
  index_to: "Optional. 0-based position of the exploded element.",
  keep_parent: "On keeps the JSON array so nothing is silently dropped.",
  depth: "top promotes one level; deep walks further, still capped.",
  keys: "One key per line. Leave blank to promote every key the sample holds.",
};

const FIELD_CHOICES: Record<string, string[]> = {
  to_type: ["text", "integer", "decimal", "boolean", "date", "timestamp"],
  mode: ["upper", "lower", "title"],
  side: ["left", "right"],
  form: ["NFC", "NFD", "NFKC", "NFKD"],
  depth: ["top", "deep"],
};

const NUMBER_FIELDS = new Set(["places", "width", "limit", "min", "max"]);
const BOOLEAN_FIELDS = new Set(["regex", "keep", "keep_parent"]);
const COLUMN_LIST_FIELDS = new Set(["columns"]);
const LIST_FIELDS = new Set(["values", "into", "keys"]);

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

/**
 * Caption for the before/after grids. Source-row language is a lie after
 * unnest (2 parents become 3 children) or a filter; name both sides then.
 */
export function previewSampleNote(sourceRows: number, transformedRows: number, cap = 12): string {
  if (sourceRows <= 0) return "no sampled rows yet";
  if (transformedRows <= 0 || transformedRows === sourceRows) {
    return `first ${Math.min(cap, sourceRows)} sampled row(s) · changed cells are highlighted`;
  }
  return `${sourceRows} source row(s) · ${transformedRows} transformed row(s) · changed cells are highlighted`;
}

/** One sentence stating what the recipe did to the sample, ledger terms included. */
export function summarizeEffect(effect: ShapeEffect | null): string {
  if (!effect) return "";
  const parts = [`${effect.rows_in.toLocaleString()} row(s) in`, `${effect.rows_out.toLocaleString()} out`];
  if (effect.rows_shaped_out) parts.push(`${effect.rows_shaped_out.toLocaleString()} removed`);
  if (effect.rows_expanded) parts.push(`${effect.rows_expanded.toLocaleString()} added by unnest`);
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

const FAMILY_ORDER = ["nested", "rows", "structural", "cleanse"] as const;
const FAMILY_LABEL: Record<string, string> = {
  nested: "Nested JSON",
  rows: "Row count",
  structural: "Columns",
  cleanse: "Values",
};

/** Group the catalog for the operation picker — one list, four families, no fourth surface. */
export function operationsByFamily(operations: ShapeOperation[]): { family: string; label: string; operations: ShapeOperation[] }[] {
  const buckets = new Map<string, ShapeOperation[]>();
  for (const op of operations) {
    const family = op.family && FAMILY_LABEL[op.family] ? op.family : "cleanse";
    const list = buckets.get(family) ?? [];
    list.push(op);
    buckets.set(family, list);
  }
  return FAMILY_ORDER
    .filter((family) => (buckets.get(family) ?? []).length > 0)
    .map((family) => ({
      family,
      label: FAMILY_LABEL[family],
      operations: buckets.get(family) ?? [],
    }));
}

export function sortSuggestions(suggestions: ShapeSuggestion[]): ShapeSuggestion[] {
  return suggestions.slice().sort((a, b) => {
    const bySeverity = suggestionRank(a.severity) - suggestionRank(b.severity);
    if (bySeverity !== 0) return bySeverity;
    return b.rows_affected - a.rows_affected;
  });
}
