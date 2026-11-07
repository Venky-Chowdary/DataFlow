/**
 * Jobs / Schedules → Transfer Studio.
 *
 * The studio page stays mounted, so "Open Validate" used to paint whatever
 * route was already on screen (QA 2026-11-06: a paused schedule's failed run
 * opened an unrelated 1,000,000-row transfer). Recovery replaces that route
 * outright: source, destination, SQL, mappings, keys, cursor, sync mode.
 * A missing connector or a wrong workspace clears the stale route instead of
 * keeping it. Connection strings and passwords are never copied out.
 */
import type { PipelineSchedule } from "./types";

export type SourceReadMode = "table" | "query" | "procedure";

export interface RecoveryMapping {
  source: string;
  destination: string;
  destination_type?: string;
  target_type?: string;
  transform?: string;
}

/** Absolute route identity. Empty strings clear the previous value. */
export interface StudioRouteSnapshot {
  sourceConnectorId: string;
  destConnectorId: string;
  sourceTable: string;
  destDatabase: string;
  destTable: string;
  syncMode: string;
  sourceReadMode: SourceReadMode;
  sourceSql: string;
  primaryKey: string;
  cursorColumn: string;
  rowLimit: number;
  mappings: RecoveryMapping[];
  scheduleId: string;
  /** Shape program for this route. Empty replaces a previous route's recipe. */
  shapeSteps: unknown[];
  /** Procedure binds. Empty replaces a previous call's parameters. */
  procedureParams: Record<string, string>;
}

export interface RecoveryFields extends StudioRouteSnapshot {
  workspaceId: string;
  /** List row: tables and connectors only. SQL/keys arrive with the full job. */
  partial: boolean;
}

export type RecoveryDecision =
  | { action: "refuse"; reason: "unauthenticated" | "wrong_workspace" }
  | {
    action: "apply";
    fields: RecoveryFields;
    missingSource: boolean;
    missingDest: boolean;
  };

export interface RecoveryInput {
  job?: unknown;
  schedule?: Partial<PipelineSchedule> | null;
  activeWorkspaceId: string;
  knownConnectorIds: readonly string[];
  /** False while the connector list is still loading — ids are not "missing" yet. */
  connectorsLoaded: boolean;
  authenticated: boolean;
}

const EMPTY_SNAPSHOT = (): StudioRouteSnapshot => ({
  sourceConnectorId: "",
  destConnectorId: "",
  sourceTable: "",
  destDatabase: "",
  destTable: "",
  syncMode: "full_refresh_append",
  sourceReadMode: "table",
  sourceSql: "",
  primaryKey: "",
  cursorColumn: "",
  rowLimit: 0,
  mappings: [],
  scheduleId: "",
  shapeSteps: [],
  procedureParams: {},
});

const text = (v: unknown): string => (typeof v === "string" ? v.trim() : "");

function asRec(v: unknown): Record<string, unknown> {
  return v && typeof v === "object" ? (v as Record<string, unknown>) : {};
}

function endpointOf(rec: Record<string, unknown>, key: "source" | "destination"): Record<string, unknown> {
  return asRec(rec[key]);
}

function extraOf(ep: Record<string, unknown>): Record<string, unknown> {
  return asRec(ep.extra);
}

function contractsOf(tr: Record<string, unknown>): Record<string, unknown>[] {
  const raw = tr.stream_contracts;
  if (!Array.isArray(raw)) return [];
  return raw.filter((row) => row && typeof row === "object") as Record<string, unknown>[];
}

export function scheduleIdFromJob(job: unknown): string {
  const rec = asRec(job);
  const tr = asRec(rec.transfer_request);
  const src = extraOf(endpointOf(tr, "source"));
  const dst = extraOf(endpointOf(tr, "destination"));
  return text(rec.schedule_id) || text(tr.schedule_id) || text(src.schedule_id) || text(dst.schedule_id);
}

function workspaceIdFromJob(job: unknown): string {
  const rec = asRec(job);
  const tr = asRec(rec.transfer_request);
  return text(rec.workspace_id) || text(tr.workspace_id);
}

function keyText(value: unknown): string {
  if (Array.isArray(value)) {
    return value.map((part) => text(part)).filter(Boolean).join(",");
  }
  return text(value);
}

function primaryKeyFrom(dstExtra: Record<string, unknown>, contracts: Record<string, unknown>[]): string {
  const direct = keyText(dstExtra.primary_key) || keyText(dstExtra.primary_keys);
  if (direct) return direct;
  for (const row of contracts) {
    const pk = keyText(row.primary_key) || keyText(row.primary_keys);
    if (pk) return pk;
  }
  return "";
}

function cursorFrom(contracts: Record<string, unknown>[], fallback: string): string {
  for (const row of contracts) {
    const cursor = text(row.cursor_field) || text(row.cursor_column);
    if (cursor) return cursor;
  }
  return fallback;
}

function normalizeMode(raw: string): SourceReadMode {
  const mode = raw.toLowerCase();
  if (mode === "query" || mode === "select" || mode === "sql") return "query";
  if (mode === "procedure" || mode === "call" || mode === "proc") return "procedure";
  return "table";
}

function readMode(ep: Record<string, unknown>, extra: Record<string, unknown>, contracts: Record<string, unknown>[]): {
  mode: SourceReadMode;
  sql: string;
} {
  const contract = contracts.find((row) => text(row.source_query) || text(row.procedure_call) || text(row.source_read_mode)) || {};
  const modeRaw = (
    text(ep.source_read_mode) || text(extra.source_read_mode) || text(contract.source_read_mode)
  ).toLowerCase();
  const query = text(ep.source_query) || text(extra.source_query) || text(contract.source_query);
  const proc = text(ep.procedure_call) || text(extra.procedure_call) || text(contract.procedure_call);
  if (modeRaw === "procedure" || modeRaw === "call" || modeRaw === "proc") {
    return { mode: "procedure", sql: proc };
  }
  if (modeRaw === "query" || modeRaw === "select" || modeRaw === "sql") {
    return { mode: "query", sql: query };
  }
  if (query && !proc) return { mode: "query", sql: query };
  if (proc && !query) return { mode: "procedure", sql: proc };
  return { mode: "table", sql: "" };
}

function mappingsFrom(raw: unknown): RecoveryMapping[] {
  if (!Array.isArray(raw)) return [];
  const out: RecoveryMapping[] = [];
  for (const item of raw) {
    if (!item || typeof item !== "object") continue;
    const row = item as Record<string, unknown>;
    const source = text(row.source) || text(row.source_column);
    const destination = text(row.target) || text(row.destination) || text(row.target_column) || source;
    if (!source && !destination) continue;
    const destinationType = text(row.destination_type) || text(row.target_type);
    out.push({
      source,
      destination,
      ...(destinationType ? { destination_type: destinationType, target_type: destinationType } : {}),
      ...(text(row.transform) ? { transform: text(row.transform) } : {}),
    });
  }
  return out;
}

function stepsFromRecipe(raw: unknown): unknown[] {
  if (Array.isArray(raw)) return raw.slice();
  if (raw && typeof raw === "object" && Array.isArray((raw as { steps?: unknown }).steps)) {
    return ((raw as { steps: unknown[] }).steps).slice();
  }
  return [];
}

function paramsFrom(raw: unknown): Record<string, string> {
  if (!raw || typeof raw !== "object" || Array.isArray(raw)) return {};
  const out: Record<string, string> = {};
  for (const [key, value] of Object.entries(raw as Record<string, unknown>)) {
    if (typeof value === "string") out[key] = value;
  }
  return out;
}

function rowLimitOf(value: unknown): number {
  const n = typeof value === "number" ? value : Number(text(value));
  if (!Number.isFinite(n) || n <= 0) return 0;
  return Math.floor(n);
}

function fieldsFromJob(job: unknown): RecoveryFields {
  const rec = asRec(job);
  const tr = asRec(rec.transfer_request);
  const hasRequest = Boolean(rec.transfer_request && typeof rec.transfer_request === "object");
  const src = endpointOf(tr, "source");
  const dst = endpointOf(tr, "destination");
  const srcExtra = extraOf(src);
  const dstExtra = extraOf(dst);
  const contracts = contractsOf(tr);
  const read = readMode(src, srcExtra, contracts);
  const sourceTable = text(src.table) || text(src.collection) || (hasRequest ? "" : text(rec.source_name));
  const destTable = text(dst.table) || text(dst.collection) || (hasRequest ? "" : text(rec.destination_collection));
  return {
    scheduleId: scheduleIdFromJob(job),
    workspaceId: workspaceIdFromJob(job),
    sourceConnectorId: text(src.connector_id) || text(rec.source_connector_id),
    destConnectorId: text(dst.connector_id) || text(rec.dest_connector_id),
    sourceTable,
    destDatabase: text(dst.database) || (hasRequest ? "" : text(rec.destination_database)),
    destTable,
    syncMode: text(tr.sync_mode) || text(rec.sync_mode),
    sourceReadMode: hasRequest ? read.mode : normalizeMode(text(rec.source_read_mode)),
    sourceSql: hasRequest ? read.sql : "",
    primaryKey: hasRequest ? primaryKeyFrom(dstExtra, contracts) : "",
    cursorColumn: hasRequest ? cursorFrom(contracts, "") : "",
    rowLimit: hasRequest ? rowLimitOf(tr.limit) : 0,
    mappings: hasRequest ? mappingsFrom(tr.mappings) : [],
    shapeSteps: hasRequest ? stepsFromRecipe(tr.shape_recipe) : [],
    procedureParams: hasRequest
      ? paramsFrom(src.procedure_params ?? srcExtra.procedure_params)
      : {},
    partial: !hasRequest,
  };
}

function fieldsFromSchedule(sched: Partial<PipelineSchedule>): RecoveryFields {
  const modeRaw = text(sched.source_read_mode).toLowerCase();
  const query = text(sched.source_query);
  const proc = text(sched.procedure_call);
  let sourceReadMode: SourceReadMode = "table";
  let sourceSql = "";
  if (modeRaw === "procedure" || modeRaw === "call" || (proc && modeRaw !== "query")) {
    sourceReadMode = "procedure";
    sourceSql = proc;
  } else if (modeRaw === "query" || modeRaw === "select" || query) {
    sourceReadMode = "query";
    sourceSql = query;
  }
  const contracts = Array.isArray(sched.stream_contracts) ? sched.stream_contracts as Record<string, unknown>[] : [];
  return {
    scheduleId: text(sched.id),
    workspaceId: text(sched.workspace_id),
    sourceConnectorId: text(sched.source_connector_id),
    destConnectorId: text(sched.dest_connector_id),
    sourceTable: text(sched.source_table),
    destDatabase: text((sched as { dest_database?: string }).dest_database),
    destTable: text(sched.dest_table),
    syncMode: text(sched.sync_mode),
    sourceReadMode,
    sourceSql,
    primaryKey: text(sched.primary_key) || primaryKeyFrom({}, contracts),
    cursorColumn: text(sched.cursor_column) || cursorFrom(contracts, ""),
    rowLimit: rowLimitOf(sched.row_limit),
    mappings: mappingsFrom(sched.mappings),
    shapeSteps: stepsFromRecipe(sched.shape_recipe),
    procedureParams: paramsFrom(sched.procedure_params),
    partial: false,
  };
}

function prefer(primary: string, fallback: string): string {
  return primary || fallback;
}

function mergeFields(jobFields: RecoveryFields | null, schedule: RecoveryFields | null): RecoveryFields {
  if (!jobFields && schedule) return schedule;
  if (!schedule || !jobFields) return jobFields || { ...EMPTY_SNAPSHOT(), workspaceId: "", partial: true };
  // The job is the run that failed. The schedule fills contract fields the
  // list row does not carry. A full job request already has its own SQL.
  const jobOwnsContract = !jobFields.partial;
  return {
    scheduleId: prefer(jobFields.scheduleId, schedule.scheduleId),
    workspaceId: prefer(jobFields.workspaceId, schedule.workspaceId),
    sourceConnectorId: prefer(jobFields.sourceConnectorId, schedule.sourceConnectorId),
    destConnectorId: prefer(jobFields.destConnectorId, schedule.destConnectorId),
    sourceTable: prefer(jobFields.sourceTable, schedule.sourceTable),
    destDatabase: prefer(jobFields.destDatabase, schedule.destDatabase),
    destTable: prefer(jobFields.destTable, schedule.destTable),
    syncMode: prefer(jobFields.syncMode, schedule.syncMode),
    sourceReadMode: jobOwnsContract ? jobFields.sourceReadMode : schedule.sourceReadMode,
    sourceSql: jobOwnsContract ? jobFields.sourceSql : schedule.sourceSql,
    primaryKey: jobOwnsContract ? jobFields.primaryKey : prefer(jobFields.primaryKey, schedule.primaryKey),
    cursorColumn: jobOwnsContract ? jobFields.cursorColumn : prefer(jobFields.cursorColumn, schedule.cursorColumn),
    rowLimit: jobOwnsContract ? jobFields.rowLimit : (jobFields.rowLimit || schedule.rowLimit),
    mappings: jobOwnsContract && jobFields.mappings.length ? jobFields.mappings : schedule.mappings,
    shapeSteps: jobOwnsContract && jobFields.shapeSteps.length ? jobFields.shapeSteps : schedule.shapeSteps,
    procedureParams: jobOwnsContract && Object.keys(jobFields.procedureParams).length
      ? jobFields.procedureParams
      : schedule.procedureParams,
    partial: false,
  };
}

/** Browser views, not tenant ids. Comparing them to a job workspace refused a legal open. */
const WORKSPACE_VIEWS = new Set(["__all__", "__unassigned__"]);

export function workspaceBlocksRecovery(active: string, ids: readonly string[]): boolean {
  const named = ids.map((id) => text(id)).filter(Boolean);
  if (new Set(named).size > 1) return true;
  const scope = text(active);
  if (WORKSPACE_VIEWS.has(scope)) return false;
  if (!scope) return named.length > 0;
  return named.some((id) => id !== scope);
}

/**
 * Decide how Studio should replace its current route.
 * ``previous`` is not an input: the caller must not merge with it.
 */
export function decideStudioRecovery(input: RecoveryInput): RecoveryDecision {
  if (!input.authenticated) return { action: "refuse", reason: "unauthenticated" };
  const jobFields = input.job ? fieldsFromJob(input.job) : null;
  const jobScheduleId = input.job ? scheduleIdFromJob(input.job) : "";
  const scheduleId = text(input.schedule?.id);
  const scheduleMatches = !input.schedule || !jobScheduleId || jobScheduleId === scheduleId;
  const scheduleFields = input.schedule && scheduleMatches ? fieldsFromSchedule(input.schedule) : null;
  const fields = mergeFields(jobFields, scheduleFields);
  const active = text(input.activeWorkspaceId);
  const workspaceIds = [jobFields?.workspaceId || "", scheduleFields?.workspaceId || ""];
  if (workspaceBlocksRecovery(active, workspaceIds)) {
    return { action: "refuse", reason: "wrong_workspace" };
  }
  const known = new Set(input.knownConnectorIds);
  const missing = (id: string) =>
    Boolean(input.connectorsLoaded && id && !known.has(id));
  return {
    action: "apply",
    fields,
    missingSource: missing(fields.sourceConnectorId),
    missingDest: missing(fields.destConnectorId),
  };
}

/**
 * Absolute replacement. ``previous`` is accepted so a test can prove a stale
 * route contributes nothing — spreading it back would keep the old table.
 */
export interface RecoveryIntentFields {
  workspaceId?: string;
  scheduleId?: string;
  sourceConnectorId?: string;
  destConnectorId?: string;
  sourceTable?: string;
  destDatabase?: string;
  destTable?: string;
  syncMode?: string;
  sourceReadMode?: string;
  sourceSql?: string;
  primaryKey?: string;
  cursorColumn?: string;
  rowLimit?: number;
  /** Job and schedule rows are not the same shape. Normalized inside. */
  mappings?: readonly unknown[];
  shapeSteps?: unknown[];
  procedureParams?: Record<string, string>;
}

/**
 * Apply an intent the Jobs or Schedules page already built.
 * Connector ids that are not in this workspace are dropped. The previous
 * snapshot is not merged in.
 */
export function applyRecoveryIntent(
  previous: StudioRouteSnapshot,
  intent: RecoveryIntentFields,
  opts: {
    activeWorkspaceId: string;
    knownConnectorIds: readonly string[];
    connectorsLoaded: boolean;
    authenticated: boolean;
  },
): { refused?: "unauthenticated" | "wrong_workspace"; state: StudioRouteSnapshot } {
  if (!opts.authenticated) {
    return {
      refused: "unauthenticated",
      state: applyStudioRecovery(previous, { action: "refuse", reason: "unauthenticated" }),
    };
  }
  const workspaceId = text(intent.workspaceId);
  if (workspaceBlocksRecovery(opts.activeWorkspaceId, [workspaceId])) {
    return {
      refused: "wrong_workspace",
      state: applyStudioRecovery(previous, { action: "refuse", reason: "wrong_workspace" }),
    };
  }
  const fields: RecoveryFields = {
    scheduleId: text(intent.scheduleId),
    workspaceId,
    sourceConnectorId: text(intent.sourceConnectorId),
    destConnectorId: text(intent.destConnectorId),
    sourceTable: text(intent.sourceTable),
    destDatabase: text(intent.destDatabase),
    destTable: text(intent.destTable),
    syncMode: text(intent.syncMode),
    sourceReadMode: normalizeMode(text(intent.sourceReadMode)),
    sourceSql: text(intent.sourceSql),
    primaryKey: text(intent.primaryKey),
    cursorColumn: text(intent.cursorColumn),
    rowLimit: rowLimitOf(intent.rowLimit),
    mappings: mappingsFrom(intent.mappings),
    shapeSteps: stepsFromRecipe(intent.shapeSteps),
    procedureParams: paramsFrom(intent.procedureParams),
    partial: false,
  };
  const known = new Set(opts.knownConnectorIds);
  const missing = (id: string) => Boolean(opts.connectorsLoaded && id && !known.has(id));
  return {
    state: applyStudioRecovery(previous, {
      action: "apply",
      fields,
      missingSource: missing(fields.sourceConnectorId),
      missingDest: missing(fields.destConnectorId),
    }),
  };
}

export function applyStudioRecovery(
  previous: StudioRouteSnapshot,
  decision: RecoveryDecision,
): StudioRouteSnapshot {
  if (decision.action === "refuse") {
    return { ...EMPTY_SNAPSHOT(), scheduleId: "" };
  }
  // `previous` is intentionally unused. Replacement is absolute so a stale
  // destination, query, key, or cursor cannot survive the click.
  void previous;
  const f = decision.fields;
  return {
    sourceConnectorId: decision.missingSource ? "" : f.sourceConnectorId,
    destConnectorId: decision.missingDest ? "" : f.destConnectorId,
    sourceTable: f.sourceTable,
    destDatabase: f.destDatabase,
    destTable: f.destTable,
    syncMode: f.syncMode || "full_refresh_append",
    sourceReadMode: f.sourceReadMode,
    sourceSql: f.sourceSql,
    primaryKey: f.primaryKey,
    cursorColumn: f.cursorColumn,
    rowLimit: f.rowLimit,
    mappings: f.mappings.map((row) => ({ ...row })),
    scheduleId: f.scheduleId,
    shapeSteps: f.shapeSteps.slice(),
    procedureParams: { ...f.procedureParams },
  };
}

/**
 * Column effect after a recovery paint.
 *
 * Heuristics fill only a field the schedule did not declare. A declared
 * cursor or key is kept while the mounted column list is still the previous
 * route, and is not replaced when the new columns include a tempting
 * `updated_at` or `id`.
 */
export function keepDeclaredCursorKey(input: {
  cursorField: string;
  primaryKeyField: string;
  columns: readonly string[];
  cursorCandidate: string;
  primaryKeyCandidate: string;
  declaredCursor: string;
  declaredPrimaryKey: string;
}): { cursorField: string; primaryKeyField: string } {
  const nextField = (
    current: string,
    candidate: string,
    declared: string,
  ): string => {
    if (declared) return declared;
    const known = input.columns.length > 0;
    if (candidate && (!current || (known && !input.columns.includes(current)))) return candidate;
    if (!candidate && current && known && !input.columns.includes(current)) return "";
    return current;
  };
  return {
    cursorField: nextField(input.cursorField, input.cursorCandidate, input.declaredCursor),
    primaryKeyField: nextField(input.primaryKeyField, input.primaryKeyCandidate, input.declaredPrimaryKey),
  };
}

/** Missing destination: drop the previous engine. Empty database clears the previous one. */
export function destinationEnginePaint(input: {
  destConnectorId: string;
  destTable: string;
  destDatabase: string;
  connectorFound: boolean;
}): { connectorId: string; table: string; database: string; clearEngine: boolean } {
  if (input.destConnectorId && input.connectorFound) {
    return {
      connectorId: input.destConnectorId,
      table: input.destTable,
      database: input.destDatabase,
      clearEngine: false,
    };
  }
  return {
    connectorId: "",
    table: input.destTable,
    database: input.destDatabase,
    clearEngine: true,
  };
}

const RECOVERY_STORAGE_KEY = "df2.studioRecovery.v1";

export interface StoredStudioRecovery {
  workspaceId: string;
  scheduleId: string;
  sourceConnectorId: string;
  destConnectorId: string;
  sourceTable: string;
  destDatabase: string;
  destTable: string;
  syncMode: string;
  sourceReadMode: string;
  sourceSql: string;
  primaryKey: string;
  cursorColumn: string;
  rowLimit: number;
  mappings: RecoveryMapping[];
  shapeSteps: unknown[];
  procedureParams: Record<string, string>;
  workspaceOfRoute: string;
}

function storage(): Storage | null {
  try {
    return globalThis.sessionStorage ?? null;
  } catch {
    return null;
  }
}

/** Persist the open route so a hard reload can paint it again. No secrets. */
export function persistStudioRecovery(activeWorkspaceId: string, snap: StudioRouteSnapshot, routeWorkspaceId = ""): void {
  const store = storage();
  if (!store) return;
  const record: StoredStudioRecovery = {
    workspaceId: text(activeWorkspaceId),
    scheduleId: snap.scheduleId,
    sourceConnectorId: snap.sourceConnectorId,
    destConnectorId: snap.destConnectorId,
    sourceTable: snap.sourceTable,
    destDatabase: snap.destDatabase,
    destTable: snap.destTable,
    syncMode: snap.syncMode,
    sourceReadMode: snap.sourceReadMode,
    sourceSql: snap.sourceSql,
    primaryKey: snap.primaryKey,
    cursorColumn: snap.cursorColumn,
    rowLimit: snap.rowLimit,
    mappings: snap.mappings.map((row) => ({ source: row.source, destination: row.destination })),
    shapeSteps: snap.shapeSteps.slice(),
    procedureParams: { ...snap.procedureParams },
    workspaceOfRoute: text(routeWorkspaceId),
  };
  try {
    store.setItem(RECOVERY_STORAGE_KEY, JSON.stringify(record));
  } catch {
    // A full session store must not block the paint.
  }
}

export function loadStudioRecovery(activeWorkspaceId: string): StoredStudioRecovery | null {
  const store = storage();
  if (!store) return null;
  let parsed: StoredStudioRecovery | null = null;
  try {
    const raw = store.getItem(RECOVERY_STORAGE_KEY);
    if (!raw) return null;
    parsed = JSON.parse(raw) as StoredStudioRecovery;
  } catch {
    return null;
  }
  if (!parsed || typeof parsed !== "object") return null;
  const scope = text(activeWorkspaceId);
  const owner = text(parsed.workspaceOfRoute) || text(parsed.workspaceId);
  if (owner && !WORKSPACE_VIEWS.has(scope) && scope && owner !== scope && text(parsed.workspaceId) !== scope) {
    return null;
  }
  return parsed;
}

export function clearStudioRecovery(): void {
  const store = storage();
  try {
    store?.removeItem(RECOVERY_STORAGE_KEY);
  } catch {
    // ignore
  }
}
