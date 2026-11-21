/**
 * Why the schedule form's Save button is disabled (QA D344).
 *
 * The form disabled Save until every requirement was met and never said
 * which one was missing: an incremental schedule sat greyed out while the
 * primary-key field beside the cursor column looked optional. One function
 * answers "can this be saved?" and "what is still missing?", so the button,
 * the reason beside it and the field markers cannot disagree.
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
}

export interface ScheduleSaveBlocker {
  /** The input id the operator fixes (for focus and aria-invalid). */
  field: string;
  /** What is missing, as an instruction. */
  message: string;
}

/** The key is required: SCD2 / mirror always, and any policy except "append". */
export function schedulePrimaryKeyRequired(syncMode: string, duplicatePolicy: string): boolean {
  return syncMode === "scd2" || syncMode === "mirror" || duplicatePolicy !== "append";
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
  if (schedulePrimaryKeyRequired(state.syncMode, state.duplicatePolicy) && !state.primaryKey.trim()) {
    out.push({
      field: "sched-pk",
      message:
        state.syncMode === "scd2" || state.syncMode === "mirror"
          ? `enter the primary key (${state.syncMode} needs it)`
          : "enter the primary key, or set Duplicate policy to Append",
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
