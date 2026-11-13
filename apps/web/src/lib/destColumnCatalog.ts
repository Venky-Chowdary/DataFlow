/**
 * Destination column catalog for the Map step: what the operator can map onto.
 *
 * The API builds the catalog (`POST /transfer/mapping/destination-columns`)
 * from the destination's own catalog, with the same "required" predicate
 * Validate's G14 gate blocks on. This module only *reads* it: which columns
 * are mapped, which required ones are still empty, which targets are claimed
 * twice, and how a remap rewrites a mapping row.
 */
import { declaredCarrierFidelityRisk } from "./typeCarrierFidelity";
import { applyOperatorRemapDest, isIntentionalOmit, type EditableMapping } from "./mapping";

export interface DestColumn {
  name: string;
  ordinal: number;
  type: string;
  /** null = the destination did not report nullability (unmeasured, not "optional"). */
  nullable: boolean | null;
  default: string | null;
  hasDefault: boolean;
  primaryKey: boolean;
  identity: boolean;
  generated: boolean;
  /** NOT NULL, no default, not engine-filled. null = unmeasured. */
  required: boolean | null;
}

export interface DestColumnCatalog {
  version: string;
  connected: boolean;
  tableExists: boolean | null;
  destType: string;
  columns: DestColumn[];
  nullabilityMeasured: boolean;
  /** A target name the table lacks is acceptable (CREATE / ADD COLUMN / schemaless). */
  canAddColumns: boolean;
  addColumnsReason: string;
  message: string;
  /** "api" = read from the destination catalog; "names" = names/types only. */
  origin: "api" | "names";
}

type RawColumn = Record<string, unknown>;

function bool(v: unknown): boolean {
  return v === true;
}

function triBool(v: unknown): boolean | null {
  return v === true ? true : v === false ? false : null;
}

export function catalogFromApi(raw: Record<string, unknown>): DestColumnCatalog {
  const cols = Array.isArray(raw.columns) ? (raw.columns as RawColumn[]) : [];
  return {
    version: String(raw.version ?? ""),
    connected: raw.connected !== false,
    tableExists: triBool(raw.table_exists),
    destType: String(raw.dest_type ?? ""),
    columns: cols
      .filter((c) => typeof c.name === "string" && c.name)
      .map((c, i) => ({
        name: String(c.name),
        ordinal: typeof c.ordinal === "number" ? c.ordinal : i,
        type: String(c.type ?? ""),
        nullable: triBool(c.nullable),
        default: c.default == null ? null : String(c.default),
        hasDefault: bool(c.has_default) || c.default != null,
        primaryKey: bool(c.primary_key),
        identity: bool(c.identity),
        generated: bool(c.generated),
        required: triBool(c.required),
      })),
    nullabilityMeasured: raw.nullability_measured === true,
    canAddColumns: raw.can_add_columns === true,
    addColumnsReason: String(raw.add_columns_reason ?? ""),
    message: String(raw.message ?? ""),
    origin: "api",
  };
}

/**
 * Names + types Studio already holds, until the catalog read answers.
 * Nullability is unknown here, so nothing is called required or optional.
 */
export function catalogFromNames(
  names: string[],
  schema: Record<string, string>,
  tableExists: boolean | null,
  destType = "",
): DestColumnCatalog {
  return {
    version: "names",
    connected: true,
    tableExists,
    destType,
    columns: names.map((name, ordinal) => ({
      name,
      ordinal,
      type: schema[name] ?? "",
      nullable: null,
      default: null,
      hasDefault: false,
      primaryKey: false,
      identity: false,
      generated: false,
      required: null,
    })),
    nullabilityMeasured: false,
    // Only a confirmed-missing table may promise a CREATE before the read lands.
    canAddColumns: tableExists === false,
    addColumnsReason: tableExists === false
      ? "The table does not exist yet: every mapped column is created with it."
      : "Destination columns are still being read.",
    message: "",
    origin: "names",
  };
}

export function findDestColumn(
  catalog: DestColumnCatalog | null | undefined,
  name: string,
): DestColumn | undefined {
  if (!catalog || !name) return undefined;
  const want = name.trim().toLowerCase();
  return catalog.columns.find((c) => c.name.toLowerCase() === want);
}

/** Rows that write a destination column (omitted rows write nothing). */
function writingRows(mappings: EditableMapping[]): EditableMapping[] {
  return mappings.filter((m) => !isIntentionalOmit(m) && m.target.trim());
}

/** lower(target) → sources writing it, only where more than one source claims it. */
export function targetConflicts(mappings: EditableMapping[]): Map<string, string[]> {
  const by = new Map<string, string[]>();
  for (const m of writingRows(mappings)) {
    const key = m.target.trim().toLowerCase();
    by.set(key, [...(by.get(key) ?? []), m.source]);
  }
  for (const [key, sources] of by) if (sources.length < 2) by.delete(key);
  return by;
}

export type DestColumnStatus =
  | "mapped"
  | "conflict"
  | "required_unmapped"
  | "default_unmapped"
  | "engine_unmapped"
  | "null_unmapped"
  | "unmeasured_unmapped";

export interface DestColumnState {
  column: DestColumn;
  status: DestColumnStatus;
  sources: string[];
  /** One sentence the panel shows: what happens to this column on write. */
  explanation: string;
  /** True when Validate (G14) blocks on this column as it stands. */
  blocking: boolean;
}

export function destinationColumnStates(
  catalog: DestColumnCatalog | null | undefined,
  mappings: EditableMapping[],
): DestColumnState[] {
  if (!catalog) return [];
  const sourcesFor = new Map<string, string[]>();
  for (const m of writingRows(mappings)) {
    const key = m.target.trim().toLowerCase();
    sourcesFor.set(key, [...(sourcesFor.get(key) ?? []), m.source]);
  }
  return catalog.columns.map((column) => {
    const sources = sourcesFor.get(column.name.toLowerCase()) ?? [];
    if (sources.length > 1) {
      return {
        column, sources, status: "conflict", blocking: true,
        explanation: `Written by ${sources.join(" and ")}: keep one source.`,
      };
    }
    if (sources.length === 1) {
      return { column, sources, status: "mapped", blocking: false, explanation: `Written from ${sources[0]}.` };
    }
    if (column.identity || column.generated) {
      return {
        column, sources, status: "engine_unmapped", blocking: false,
        explanation: column.identity
          ? "Unmapped: the destination generates it (identity)."
          : "Unmapped: the destination computes it (generated).",
      };
    }
    if (column.required === true) {
      return {
        column, sources, status: "required_unmapped", blocking: true,
        explanation: "NOT NULL, no default: blocks Validate until mapped.",
      };
    }
    if (column.hasDefault) {
      return {
        column, sources, status: "default_unmapped", blocking: false,
        explanation: `Unmapped: new rows get the default (${column.default ?? "default"}).`,
      };
    }
    if (column.nullable === true) {
      return {
        column, sources, status: "null_unmapped", blocking: false,
        explanation: "Unmapped: new rows get NULL.",
      };
    }
    return {
      column, sources, status: "unmeasured_unmapped", blocking: false,
      explanation: "Unmapped. Nullability not read: Validate re-checks it.",
    };
  });
}

export interface MapTargetSummary {
  mapped: number;
  unmapped: number;
  requiredUnmapped: string[];
  conflicts: Map<string, string[]>;
  /** Rows whose target is not a destination column (CREATE / ADD COLUMN). */
  newTargets: string[];
  /** New targets the destination cannot accept (policy / unread table). */
  refusedNewTargets: string[];
}

export function summarizeMapTargets(
  catalog: DestColumnCatalog | null | undefined,
  mappings: EditableMapping[],
): MapTargetSummary {
  const states = destinationColumnStates(catalog, mappings);
  const conflicts = targetConflicts(mappings);
  const known = new Set((catalog?.columns ?? []).map((c) => c.name.toLowerCase()));
  const hasCatalog = Boolean(catalog && catalog.columns.length > 0);
  const newTargets = hasCatalog
    ? writingRows(mappings).filter((m) => !known.has(m.target.trim().toLowerCase())).map((m) => m.target)
    : [];
  return {
    mapped: states.filter((s) => s.sources.length > 0).length,
    unmapped: states.filter((s) => s.sources.length === 0).length,
    requiredUnmapped: states.filter((s) => s.status === "required_unmapped").map((s) => s.column.name),
    conflicts,
    newTargets,
    refusedNewTargets: catalog && !catalog.canAddColumns && catalog.origin === "api" ? newTargets : [],
  };
}

/**
 * Point a mapping row at ``target``. An existing column binds to its physical
 * type; a name the table lacks becomes a create-new column. Stale engine
 * fidelity never survives a target change: the carrier check runs again and
 * Validate re-grades the pair.
 */
export function remapToDestination(
  m: EditableMapping,
  target: string,
  catalog: DestColumnCatalog | null | undefined,
): EditableMapping {
  const base = applyOperatorRemapDest(m, target);
  if (!base.target) return base;
  const col = findDestColumn(catalog, base.target);
  const reset = {
    riskAcknowledged: false,
    riskContract: undefined,
    destTypeDeclared: undefined,
    falseFriendConfirmed: false,
  };
  if (col) {
    const destType = col.type || undefined;
    const narrowing = destType ? declaredCarrierFidelityRisk(m.inferredType, destType) : false;
    const strategy = m.assignmentStrategy === "create_compatible_new"
      || m.assignmentStrategy === "pending_dest_schema"
      || m.assignmentStrategy === "identity_passthrough"
      ? "bind_existing"
      : m.assignmentStrategy;
    return {
      ...base,
      ...reset,
      target: col.name,
      destType,
      existsInDestination: true,
      createNew: false,
      assignmentStrategy: strategy,
      fidelity: narrowing ? "lossy_cast" : undefined,
      fidelityReason: narrowing
        ? `${col.name} is ${destType}: may collapse fidelity from ${m.inferredType || "source"}`
        : undefined,
      typeNarrowing: narrowing || undefined,
    };
  }
  return {
    ...base,
    ...reset,
    destType: m.inferredType || m.destType,
    existsInDestination: false,
    createNew: true,
    assignmentStrategy: "create_compatible_new",
    fidelity: undefined,
    fidelityReason: undefined,
    typeNarrowing: undefined,
  };
}

/** Clear a row's target (the panel's "unmap"); the row stays for review. */
export function unmapTarget(m: EditableMapping): EditableMapping {
  return remapToDestination(m, "", null);
}

/**
 * Picker ranking: exact, then prefix, then substring, then word-normalized
 * substring (``givenname`` finds ``given_name``). Stable within a rank.
 */
export function filterDestColumns(columns: DestColumn[], query: string): DestColumn[] {
  const q = query.trim().toLowerCase();
  if (!q) return columns;
  const flat = (s: string) => s.toLowerCase().replace(/[^a-z0-9]+/g, "");
  const qf = flat(q);
  const ranked: Array<[number, DestColumn]> = [];
  for (const c of columns) {
    const n = c.name.toLowerCase();
    const rank = n === q ? 0 : n.startsWith(q) ? 1 : n.includes(q) ? 2 : qf && flat(n).includes(qf) ? 3 : -1;
    if (rank >= 0) ranked.push([rank, c]);
  }
  return ranked.sort((a, b) => a[0] - b[0] || a[1].ordinal - b[1].ordinal).map(([, c]) => c);
}

/** Short attribute line for a column: ``BIGINT · PK · NOT NULL · default 0``. */
export function destColumnFacts(c: DestColumn): string[] {
  const facts: string[] = [];
  if (c.type) facts.push(c.type);
  if (c.primaryKey) facts.push("PK");
  if (c.identity) facts.push("identity");
  if (c.generated) facts.push("generated");
  if (c.nullable === false) facts.push("NOT NULL");
  else if (c.nullable === true) facts.push("nullable");
  if (c.hasDefault) facts.push(`default ${c.default ?? ""}`.trim());
  return facts;
}

export type TargetIssueCode = "target_conflict" | "required_unmapped" | "new_target_refused" | "no_target";

export interface TargetIssue {
  code: TargetIssueCode;
  title: string;
  action: string;
}

/**
 * Destination-side reasons Map refuses to continue — the same conditions
 * Validate blocks on (duplicate target, G14 required column, a new name the
 * destination cannot add, a row with no destination), named per column.
 * ``tableRecreated``: an overwrite that drops and recreates the table makes
 * the live NOT NULL columns moot, exactly as G14 skips them.
 */
export function mapTargetIssues(
  summary: MapTargetSummary,
  mappings: EditableMapping[],
  opts: { tableRecreated?: boolean } = {},
): TargetIssue[] {
  const issues: TargetIssue[] = [];
  for (const [target, sources] of summary.conflicts) {
    issues.push({
      code: "target_conflict",
      title: `${sources.join(" and ")} all write ${target}`,
      action: "Keep one source per destination column: pick another column for the others, or omit them.",
    });
  }
  if (!opts.tableRecreated) {
    for (const name of summary.requiredUnmapped) {
      issues.push({
        code: "required_unmapped",
        title: `${name} is NOT NULL with no default and nothing maps to it`,
        action: `Drag a source onto ${name}, or pick ${name} in a row's destination column.`,
      });
    }
  }
  for (const name of summary.refusedNewTargets) {
    issues.push({
      code: "new_target_refused",
      title: `${name} is not a destination column`,
      action: "Pick an existing destination column: this destination cannot add one.",
    });
  }
  for (const m of mappings) {
    if (isIntentionalOmit(m) || m.target.trim()) continue;
    issues.push({
      code: "no_target",
      title: `${m.source} has no destination column`,
      action: "Pick a destination column, or omit the column with a reason.",
    });
  }
  return issues;
}

/** ``OrderID`` / ``order-id`` / ``Order Id`` → ``order_id`` (the API's normalize_name). */
export function normalizeColumnName(name: string): string {
  return String(name || "")
    .trim()
    .replace(/([A-Z]+)([A-Z][a-z])/g, "$1_$2")
    .replace(/([a-z0-9])([A-Z])/g, "$1_$2")
    .replace(/(?<=[A-Za-z])(?=[0-9])|(?<=[0-9])(?=[A-Za-z])/g, "_")
    .split(/[^0-9A-Za-z]+/)
    .filter(Boolean)
    .map((t) => t.toLowerCase())
    .join("_");
}
