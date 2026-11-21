/**
 * Why the schedule form's Save button is disabled (QA D344, PR #435).
 *
 * The form disabled Save until every requirement was met and never said
 * which one was missing. One function answers "can this be saved?" and
 * "what is still missing?", so the button, the reason beside it and the
 * field markers cannot disagree. Each rule mirrors what the API enforces:
 *
 * - Primary key: the schedule store refuses ``duplicate_policy`` upsert or
 *   skip_duplicates without one (``validate_schedule_file_policy``, 400 at
 *   save). Append needs none. Other key needs (SCD2, mirror, CDC, deduped)
 *   are judged at Validate (g9), which also accepts the source table's own
 *   primary key, so the form does not refuse them.
 * - Cursor column: Validate (g9) needs one for incremental_append and
 *   incremental_deduped (not CDC: its position is the change log; not a Kafka
 *   source: the consumer offset; not a file on deduped: a keyed re-read).
 *   When the server plans the route (a new schedule, or one with no mapping
 *   contract yet: Save / Plan mappings), the plan binds the table's single
 *   change-time column or refuses with the reason, so the form does not
 *   demand it. An edit of a planned schedule replays without re-planning,
 *   so there the form requires it.
 */

export interface ScheduleFormState {
  name: string;
  sourceId: string;
  destId: string;
  sourceTable: string;
  destTable: string;
  callable: boolean;
  procedureText: string;
  cadenceMode: "preset" | "cron" | string;
  cron: string;
  syncMode: string;
  duplicatePolicy: string;
  primaryKey: string;
  contractBlock: string;
  /** Watermark column (only read when the mode advances one). */
  cursorColumn?: string;
  /** Source connector type (kafka / file families advance without a column). */
  sourceType?: string;
  /** The server plans this route on save (new schedule, or no mapping contract yet). */
  serverPlans?: boolean;
}

export interface ScheduleSaveBlocker {
  /** The input id the operator fixes (for focus and aria-invalid). */
  field: string;
  /** What is missing, as an instruction. */
  message: string;
}

/** The store's save rule: upsert and skip_duplicates need a key; append does not. */
export function schedulePrimaryKeyRequired(duplicatePolicy: string): boolean {
  return duplicatePolicy !== "append";
}

const CURSOR_COLUMN_MODES = new Set(["incremental_append", "incremental_deduped", "incremental"]);
const KAFKA_FAMILY = new Set([
  "kafka", "apache_kafka", "confluent_kafka", "confluent", "redpanda", "msk", "amazon_msk",
]);
const FILE_SOURCES = new Set([
  "file", "csv", "excel", "json", "parquet", "sftp", "s3", "gcs", "adls", "local", "upload",
]);

/** Validate (g9) needs a cursor column for this mode and source. */
export function scheduleModeNeedsCursorColumn(syncMode: string, sourceType = ""): boolean {
  if (!CURSOR_COLUMN_MODES.has(syncMode)) return false; // CDC: the change log is the position
  const kind = sourceType.trim().toLowerCase();
  if (KAFKA_FAMILY.has(kind)) return false;
  if (syncMode === "incremental_deduped" && FILE_SOURCES.has(kind)) return false;
  return true;
}

/** Save needs the cursor typed here: the mode needs one and no server plan will bind it. */
export function scheduleCursorRequired(
  state: Pick<ScheduleFormState, "syncMode" | "sourceType" | "serverPlans">,
): boolean {
  return !state.serverPlans && scheduleModeNeedsCursorColumn(state.syncMode, state.sourceType ?? "");
}

export function scheduleSaveBlockers(state: ScheduleFormState): ScheduleSaveBlocker[] {
  const out: ScheduleSaveBlocker[] = [];
  if (!state.name.trim()) out.push({ field: "sched-name", message: "name the schedule" });
  if (!state.sourceId) out.push({ field: "sched-src", message: "choose the source connection" });
  if (!state.sourceTable.trim()) {
    out.push({ field: "sched-src-table", message: "choose the source table" });
  }
  if (!state.destId) out.push({ field: "sched-dst", message: "choose the destination connection" });
  if (!state.destTable.trim()) {
    out.push({ field: "sched-dst-table", message: "name the destination table" });
  }
  if (state.callable && !state.procedureText.trim()) {
    out.push({ field: "sched-callable-sql", message: "enter the query or procedure call" });
  }
  if (state.cadenceMode === "cron" && !state.cron.trim()) {
    out.push({ field: "sched-cron", message: "enter the cron expression" });
  }
  if (scheduleCursorRequired(state) && !(state.cursorColumn ?? "").trim()) {
    out.push({
      field: "sched-cursor",
      message: `enter the cursor column (${state.syncMode} advances on it)`,
    });
  }
  if (schedulePrimaryKeyRequired(state.duplicatePolicy) && !state.primaryKey.trim()) {
    out.push({
      field: "sched-pk",
      message: "enter the primary key, or set Duplicate policy to Append",
    });
  }
  if (state.contractBlock) out.push({ field: "sched-contract", message: state.contractBlock });
  return out;
}

/** "To save: name the schedule; enter the primary key …" or "" when Save is open. */
export function scheduleSaveBlockedReason(blockers: ScheduleSaveBlocker[]): string {
  if (!blockers.length) return "";
  return `To save: ${blockers.map((b) => b.message).join("; ")}.`;
}

/**
 * QA batch 1: a new schedule without a mapping contract asks the API to plan
 * its route (mapping + Validate, saved only if it would run) instead of
 * saving a paused draft Run now could never start.
 */
export function scheduleCreatePlanFields(isEdit: boolean, mappingCount: number): { plan_mappings?: true } {
  return !isEdit && mappingCount <= 0 ? { plan_mappings: true } : {};
}
