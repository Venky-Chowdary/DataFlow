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
  /** Compiler stamp — a customers step is not applied to orders. */
  source_table?: string;
  dest_table?: string;
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
  buffered?: boolean;
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

/** A set operation the catalog may publish once it can run. Empty until then. */
export interface SetOperation {
  op: string;
  summary: string;
  executable: boolean;
  execution: string[];
  family: string;
  inputs: string[];
  outputs: string[];
  options: string[];
  required: string[];
}

export interface SetEngineCatalog {
  available: boolean;
  reference: string;
  pushdown_dialects: string[];
  /** Empty-document hash from the canvas catalog. The engine omits it. */
  set_hash?: string;
}

export interface SetGraphError {
  node: string;
  op: string;
  message: string;
}

export interface ShapeCatalog {
  operations: ShapeOperation[];
  functions: ShapeFunctionDoc[];
  max_steps: number;
  max_preview_rows: number;
  error_policies: ShapeErrorPolicy[];
  post_load_only: { operations: string[]; reason: string };
  /**
   * Empty until a set node can run. The engine publishes a SetOperation.
   * The canvas also accepts the certified op name as a string.
   */
  set_operations?: Array<string | SetOperation>;
  set_engine?: SetEngineCatalog;
  /** Port contract for nodes the row catalog does not run. */
  canvas_nodes?: {
    op: string;
    summary: string;
    family: string;
    executable: boolean;
    placement: "palette" | "wizard" | "plan";
    inputs: { id: string; role: "in" | "out"; kind: "row" | "field"; label: string; visible: boolean }[];
    outputs: { id: string; role: "in" | "out"; kind: "row" | "field"; label: string; visible: boolean }[];
    field_ports: { id: string; role: "in" | "out"; kind: "row" | "field"; label: string; visible: boolean }[];
  }[];
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
  /** Empty when the recipe has no set nodes. Not part of recipe_hash. */
  set_hash?: string;
  set_errors?: SetGraphError[];
  set_graph?: { nodes: Record<string, unknown>[]; main_output?: string };
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
  /** Set when the sample is one stream of a multi-table recipe. */
  preview_note?: string;
}

/**
 * A new step among several tables belongs to the table the operator is
 * looking at. An existing stamp is kept. One table needs no stamp.
 */
export function stampStepSourceTable(
  step: ShapeStepWire,
  table: string,
  tableCount: number,
): ShapeStepWire {
  const name = table.trim();
  if (tableCount < 2 || !name || step.source_table) return step;
  return { ...step, source_table: name };
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
export type ShapeFieldKind = "text" | "number" | "boolean" | "columns" | "list" | "expression" | "choice" | "json";

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
  precision: "Precision",
  scale: "Scale",
  min: "Minimum",
  max: "Maximum",
  format: "Input format",
  timezone: "Time zone",
  output_format: "Output format",
  form: "Normal form",
  expression: "Expression",
  condition: "Condition",
  keep: "Keep matching rows",
  reason: "Quarantine reason",
  index_to: "Index column",
  keep_parent: "Keep the original JSON column (Validate then needs drop/flatten)",
  depth: "Flatten depth",
  keys: "Object keys to promote",
  lookup_key: "Reference key column",
  rows: "Inline reference rows",
  reference: "Connector reference",
  map: "Return columns map",
  unmatched: "No-match policy",
  match: "Duplicate-key policy",
  defaults: "Defaults for unmatched",
  type: "Join type",
  on: "Join keys",
  fanout: "Duplicate-key fan-out",
  extra: "Extra columns policy",
  routes: "Routes",
  targets: "Route destination tables",
};

const FIELD_HINT: Record<string, string> = {
  values: "One per line. Each is treated as null.",
  columns: "Pick the columns, in the order they apply.",
  into: "One name per piece, in order.",
  format: "Explicit, e.g. %d/%m/%Y — the engine never guesses day/month order.",
  places: "Digits kept after the decimal point.",
  precision: "Total digits. A value that needs more digits is refused, not truncated.",
  scale: "Digits after the decimal point. Half rounds away from zero, the same rule as round.",
  timezone: "Source zone for a naive clock. The value is stored in UTC. Blank keeps the source clock with no zone.",
  condition: "A row-local expression, e.g. [status] <> 'void'.",
  expression: "A row-local expression over this row's columns.",
  keep: "Off keeps the rows the condition does not match.",
  reason: "Recorded on every diverted row.",
  index_to: "Optional. 0-based position of the exploded element.",
  keep_parent: "Off drops the parent as a named step so Validate is not blocked on leftover ARRAY. On keeps the blob — drop or flatten it before Validate.",
  depth: "top promotes one level; deep walks further, still capped.",
  keys: "One key per line. Leave blank to promote every key the sample holds.",
  lookup_key: "The column in the reference data matched against this column's value.",
  rows: 'JSON array of objects, e.g. [{"code":"US","name":"United States"}] — baked into the recipe. Leave blank when a connector reference is set.',
  reference: '{"connector_id":"<saved id>","table":"<table>","columns":["key","name"]} — read live once at run start; re-run to pick up changed reference data.',
  map: 'JSON object: reference column → output column, e.g. {"name":"country_name"}. Multiple return columns supported.',
  unmatched: "null writes nulls · drop removes the row · divert quarantines with the reason · default writes the defaults map.",
  match: "first keeps the earliest row for a duplicate key · refuse fails the run on a duplicate key.",
  defaults: 'JSON object: output column → value, used when no reference row matches, e.g. {"country_name":"UNKNOWN"}.',
  type: "inner keeps only matching rows · left keeps every row, filling mapped columns with nulls.",
  on: 'JSON object: source column → reference column, e.g. {"cc":"code"}. Composite keys list several pairs.',
  fanout: "refuse fails the run when a key matches several reference rows (default — duplicate keys never multiply a row silently) · first keeps the earliest match · allow emits one row per match (fan-out).",
  extra: "refuse fails the run when an appended column is not a stream column and has no map entry (default) · drop omits it.",
  routes: 'JSON array: [{"name":"high","condition":"[risk] = \'high\'","map":{"name":"customer_name"}},{"name":"other","default":true}] — first matching condition wins; one route may declare default: true for unmatched rows (then unmatched stays blank); every other route needs a condition. Each route may carry its own map. Without a default route, unmatched is required: divert quarantines, fail stops the run.',
  targets: 'JSON object: route name → destination table, e.g. {"high":"customers_high"}. Routes without an entry write to <base>__<route>.',
};

const FIELD_CHOICES: Record<string, string[]> = {
  to_type: ["text", "integer", "decimal", "boolean", "date", "timestamp"],
  mode: ["upper", "lower", "title"],
  side: ["left", "right"],
  form: ["NFC", "NFD", "NFKC", "NFKD"],
  depth: ["top", "deep"],
  unmatched: ["null", "drop", "divert", "default"],
  match: ["first", "refuse"],
  type: ["inner", "left"],
  fanout: ["first", "refuse", "allow"],
  extra: ["refuse", "drop"],
};

/** Options whose allowed values differ per op — join's no-match policy has
 *  no null/default (a left join keeps the row with nulls already). */
const FIELD_CHOICES_BY_OP: Record<string, Record<string, string[]>> = {
  join: { unmatched: ["drop", "divert"] },
  router: { unmatched: ["divert", "fail"] },
};

const NUMBER_FIELDS = new Set(["places", "precision", "scale", "width", "limit", "min", "max"]);
const BOOLEAN_FIELDS = new Set(["regex", "keep", "keep_parent"]);
const COLUMN_LIST_FIELDS = new Set(["columns"]);
const LIST_FIELDS = new Set(["values", "into", "keys"]);
/** Options whose value must be a JSON object/array — typed text is parsed and
 *  refused with a named error at draft time, never silently coerced. */
const JSON_FIELDS = new Set(["rows", "reference", "map", "defaults", "on", "routes", "targets"]);

/** The fields to render for one operation, in the catalog's own order. */
export function fieldsFor(operation: ShapeOperation): ShapeField[] {
  return operation.options.map((name) => {
    const required = operation.required.includes(name);
    const choices = FIELD_CHOICES_BY_OP[operation.op]?.[name] ?? FIELD_CHOICES[name];
    const kind: ShapeFieldKind = name === operation.expression_option
      ? "expression"
      : choices
        ? "choice"
      : JSON_FIELDS.has(name)
        ? "json"
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
      choices,
    };
  });
}

/** Keep the typed text when it is not a finite number so the field does not snap to NaN. */
export function parseNumberOption(raw: string): number | string {
  const trimmed = raw.trim();
  if (trimmed === "") return "";
  const parsed = Number(trimmed);
  return Number.isFinite(parsed) ? parsed : raw;
}

/**
 * Blank for Add / required chrome: empty, empty list, or a number field that is
 * not a finite number (NaN, Infinity, or leftover text). Zero is a real answer.
 */
export function isBlankOption(name: string, value: unknown): boolean {
  if (value === undefined || value === null || value === "") return true;
  if (Array.isArray(value) && value.length === 0) return true;
  if (NUMBER_FIELDS.has(name)) {
    return typeof value !== "number" || !Number.isFinite(value);
  }
  return false;
}

/** Drop blank / non-finite number drafts so the recipe never carries NaN. */
export function draftOptionsForWire(options: Record<string, unknown>): Record<string, unknown> {
  const next: Record<string, unknown> = {};
  for (const [name, value] of Object.entries(options)) {
    if (isBlankOption(name, value)) continue;
    next[name] = value;
  }
  return next;
}

/**
 * Ignore a compile answer that is older than the text now in the box.
 * Undefined means the caller must not touch expressionError.
 */
export function settleExpressionCheck(
  requestId: number,
  latestId: number,
  result: { valid?: boolean; error?: string },
): string | undefined {
  if (requestId !== latestId) return undefined;
  if (result.valid) return "";
  return result.error || "Expression is not valid.";
}

/** Which required option the draft is still missing, so Add can say why. */
export function missingRequired(
  operation: ShapeOperation,
  column: string,
  options: Record<string, unknown>,
): string {
  if (operation.needs_column && !column) return "Pick the column this step applies to.";
  for (const name of operation.required) {
    if (isBlankOption(name, options[name])) {
      return `${FIELD_LABEL[name] ?? name} is required for ${operation.op}.`;
    }
  }
  return "";
}

/**
 * Insert a column or function into an expression.
 * A column token fills the empty call the operator just inserted, so
 * `upper()` then `[email]` becomes `upper([email])` rather than two fragments.
 */
export function insertExpressionToken(current: string, token: string): string {
  if (token.startsWith("[") && /\(\)\s*$/.test(current)) {
    return current.replace(/\(\)\s*$/, `(${token})`);
  }
  if (!current) return token;
  return /\s$/.test(current) ? `${current}${token}` : `${current} ${token}`;
}

/** Parse a textarea of one-per-line values into the list the API expects. */
export function linesToList(raw: string): string[] {
  return raw
    .split("\n")
    .map((line) => line.trim())
    .filter((line) => line.length > 0);
}

/**
 * Source columns to sit beside the after grid.
 *
 * A rename's new name is not on the source row. Pair it back to the column
 * the step read, so "What changed" cannot render an empty source bar.
 * When nothing pairs, the whole source row is shown.
 */
export function sourceColumnsBeside(
  before: readonly string[],
  shownAfter: readonly string[],
  steps: readonly ShapeStepWire[],
): string[] {
  const origin = new Map<string, string>();
  for (const step of steps) {
    if (step.enabled === false || step.op !== "rename_column" || !step.column) continue;
    const to = step.options?.to;
    if (typeof to === "string" && to) origin.set(to, step.column);
  }
  const picked: string[] = [];
  for (const name of shownAfter) {
    let source = name;
    const seen = new Set<string>();
    while (origin.has(source) && !seen.has(source)) {
      seen.add(source);
      source = origin.get(source) ?? source;
    }
    if (before.includes(source) && !picked.includes(source)) picked.push(source);
  }
  return picked.length > 0 ? picked : before.slice();
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

export function enabledShapeSteps(steps: ShapeStepWire[]): ShapeStepWire[] {
  return steps.filter((step) => step.enabled !== false);
}

/**
 * Column catalog source of truth after a recipe runs.
 *
 * The source profile describes the file/table. After unnest/flatten the
 * operator must see the transformed image — what Map will bind — not the
 * parent JSON they already exploded.
 */
export function kitchenCatalogColumns(
  sourceColumns: ShapeColumnProfile[],
  shapedColumns: ShapeColumnProfile[] | undefined,
  hasEnabledRecipe: boolean,
): { columns: ShapeColumnProfile[]; from: "source" | "shaped" } {
  if (hasEnabledRecipe && shapedColumns && shapedColumns.length > 0) {
    return { columns: shapedColumns, from: "shaped" };
  }
  return { columns: sourceColumns, from: "source" };
}

export function kitchenSampleValues(
  column: string,
  sourceRows: Record<string, unknown>[],
  afterRows: Record<string, unknown>[] | undefined,
  from: "source" | "shaped",
): unknown[] {
  const rows = from === "shaped" ? (afterRows ?? []) : sourceRows;
  return rows.map((row) => row[column]);
}

export interface ContinueTransformState {
  enabled: boolean;
  label: string;
  reason: string;
}

/**
 * Fail-closed Continue. A recipe without a matching, balanced preview has no
 * identity for Map / Validate / Execute to be held to — advancing anyway is
 * how a surplus or a stale hash reaches the writer.
 */
/** Sync modes whose dest history was not written by this recipe. */
const PRELOAD_REFUSED_SYNCS = new Set(["cdc", "scd2", "full_refresh_mirror", "mirror"]);

export function preloadTransformRefused(syncMode?: string): boolean {
  return PRELOAD_REFUSED_SYNCS.has((syncMode || "").trim().toLowerCase());
}

export function preloadTransformRefusalReason(syncMode?: string): string {
  const sync = (syncMode || "").trim().toLowerCase() || "this";
  return (
    `Transform (pre-load) is not applied on the ${sync} route: it merges each row ` +
    "against history already stored on the destination, which was not written by this recipe. " +
    "Remove the steps, or use full refresh / incremental append."
  );
}

export function continueTransformState(input: {
  steps: ShapeStepWire[];
  preview: ShapePreviewResponse | null;
  previewError: string;
  busy: boolean;
  previewMatchesSteps: boolean;
  syncMode?: string;
  /** Compiled business rules awaiting Map — CTA must not say "without transforming". */
  ruleCount?: number;
}): ContinueTransformState {
  const hasRecipe = enabledShapeSteps(input.steps).length > 0;
  const hasRules = (input.ruleCount ?? 0) > 0;
  const label = hasRecipe
    ? "Continue with this transform"
    : hasRules ? "Continue to Map" : "Continue without transforming";
  if (hasRecipe && preloadTransformRefused(input.syncMode)) {
    return {
      enabled: false,
      label,
      reason: preloadTransformRefusalReason(input.syncMode),
    };
  }
  if (hasRecipe && input.previewError) {
    return {
      enabled: false,
      label,
      reason: "Preview the recipe before Map — Retry, or remove the steps.",
    };
  }
  if (hasRecipe && (input.busy || !input.previewMatchesSteps || !input.preview)) {
    return {
      enabled: false,
      label,
      reason: "Wait for the recipe identity before Map. The preview is still catching up.",
    };
  }
  if (input.preview?.refusal) {
    return {
      enabled: false,
      label,
      reason: "A refused row must be decided before Map.",
    };
  }
  if (hasRecipe && input.preview && !input.preview.effect.balanced && !input.preview.refusal) {
    return {
      enabled: false,
      label,
      reason: "Ledger does not balance — this is a defect, do not approve.",
    };
  }
  if (hasRecipe && !input.preview?.recipe.recipe_hash) {
    return {
      enabled: false,
      label,
      reason: "The recipe has no identity yet.",
    };
  }
  return {
    enabled: true,
    label,
    reason: hasRecipe
      ? "Continue to Map with this recipe identity."
      : "Continue to Map. The source is unchanged.",
  };
}

/** Severity ordering for the suggestion list: decisions before hygiene. */
export function suggestionRank(severity: string): number {
  if (severity === "blocking") return 0;
  if (severity === "decision") return 1;
  return 2;
}

const FAMILY_ORDER = ["cleanse", "structural", "rows", "nested"] as const;
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

/**
 * The wire a completed draft becomes. The form and the canvas both call this
 * so a required field cannot be enforced in one and skipped in the other.
 */
export function stepFromDraft(
  operation: ShapeOperation,
  column: string,
  options: Record<string, unknown>,
  label: string,
  policy: string,
): { step: ShapeStepWire | null; error: string } {
  const missing = missingRequired(operation, column, options);
  if (missing) return { step: null, error: missing };
  const wire = draftOptionsForWire(options);
  // Object-typed options must parse as JSON before Add — a string the engine
  // would refuse at save is caught here naming the field instead.
  for (const name of JSON_FIELDS) {
    const raw = wire[name];
    if (typeof raw !== "string") continue;
    try {
      wire[name] = JSON.parse(raw);
    } catch {
      return {
        step: null,
        error: `${FIELD_LABEL[name] ?? name} must be valid JSON (object or array).`,
      };
    }
  }
  const step: ShapeStepWire = { op: operation.op, options: wire };
  if (operation.needs_column) step.column = column;
  if (label.trim()) step.label = label.trim();
  if (policy && policy !== "refuse") step.on_error = policy;
  return { step, error: "" };
}

/**
 * A bracketed column the expression names that this step cannot see.
 * Empty when every reference is still on the row.
 */
export function unknownExpressionColumn(expression: string, available: readonly string[]): string {
  const present = new Set(available);
  const missing = [...expression.matchAll(/\[([^\]\n]+)\]/g)]
    .map((match) => match[1].trim())
    .find((name) => name.length > 0 && !present.has(name));
  if (!missing) return "";
  const list = available.slice(0, 20).join(", ");
  return `refers to column '${missing}', which is not available at this step. Available: ${list}`;
}

/**
 * A preview refusal belongs to the recipe that produced it. A later edit
 * must not keep showing that sentence.
 */
export function previewErrorForSteps(error: string, errorKey: string, stepsKey: string): string {
  if (!error || errorKey !== stepsKey) return "";
  return error;
}

export interface CanvasValidationIssue {
  /** 0-based step index. -1 is the recipe, not one node. */
  index: number;
  message: string;
}

/**
 * Where a refusal belongs on the canvas. The step number in the engine
 * payload and in "step N" prose is 1-based.
 */
export function canvasValidationIssues(
  error: string,
  refusal: { step: number; message: string } | null | undefined,
): CanvasValidationIssue[] {
  const issues: CanvasValidationIssue[] = [];
  if (refusal?.message) {
    const index = Number.isFinite(refusal.step) && refusal.step > 0 ? refusal.step - 1 : -1;
    issues.push({ index, message: refusal.message });
  }
  const text = error.trim();
  if (text && !issues.some((issue) => issue.message === text)) {
    const match = text.match(/step\s+(\d+)/i);
    const parsed = match ? Number(match[1]) : 0;
    const index = match && parsed > 0 ? parsed - 1 : -1;
    issues.push({ index, message: text });
  }
  return issues;
}

/** Always-visible sample caption. The preview is not the population. */
export function samplePopulationBanner(shown: number, population?: number): string {
  const sample = Math.max(0, shown).toLocaleString();
  if (population == null || population <= shown) {
    return `${sample} sampled row(s). Validate checks the population.`;
  }
  return `${sample} of ${population.toLocaleString()} sampled rows. Validate checks the population.`;
}

export function sortSuggestions(suggestions: ShapeSuggestion[]): ShapeSuggestion[] {
  return suggestions.slice().sort((a, b) => {
    const bySeverity = suggestionRank(a.severity) - suggestionRank(b.severity);
    if (bySeverity !== 0) return bySeverity;
    return b.rows_affected - a.rows_affected;
  });
}
