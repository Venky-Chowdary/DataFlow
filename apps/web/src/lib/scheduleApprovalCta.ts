/**
 * What the operator can do about a parked schedule finding.
 *
 * Empty-mapping refusals are plan changes: a signature cannot invent column
 * names. The only honest next action is Transfer Studio (Map → Validate →
 * Schedule), not "Open Validate for this job" and not Decide.
 */

import type { PipelineSchedule, ScheduleApprovalInboxItem } from "./types";

export const EMPTY_MAPPING_STUDIO_CORRECTIVE =
  "Open Transfer Studio with this schedule's source and destination. Map the columns, run Validate, then Schedule from the Studio footer — that persists the mapping contract the beat can replay. A signature here cannot invent column names.";

export function isEmptyMappingFinding(code?: string, finding?: string): boolean {
  if ((code || "").toUpperCase() === "EMPTY_MAPPING_CONTRACT") return true;
  return /no persisted column mappings/i.test(finding || "");
}

export function inboxNeedsStudio(item: ScheduleApprovalInboxItem): boolean {
  return isEmptyMappingFinding(item.approval.code, item.approval.finding);
}

export const DEST_EXISTS_AFTER_CREATE_NEW_CORRECTIVE =
  "The destination table exists after the first write. The Map contract is unchanged — dest-exists is write-by-name, not a plan change. Run now, or wait for the next cadence after the park is released.";

export const ZERO_RETRY_BUDGET_CORRECTIVE =
  "The park named a retry budget, not a plan change. If Map is unchanged after the first write created the destination, Run now. A real Map or type-path refuse still needs Validate.";

export function isCreateNewDestExistsPark(code?: string, finding?: string): boolean {
  const text = `${finding || ""}`.toLowerCase();
  if (isEmptyMappingFinding(code, finding)) return false;
  return text.includes("decision artifact") && (
    text.includes("diverged from current map")
    || text.includes("dest schema drifted since validate")
    || text.includes("content_hash mismatch")
    || text.includes("content_hash does not match")
  );
}

export function isZeroRetryBudgetPark(code?: string, finding?: string): boolean {
  if (isEmptyMappingFinding(code, finding)) return false;
  return /retry budget exhausted after 0 attempt/i.test(finding || "");
}

export function inboxNeedsRunNow(item: ScheduleApprovalInboxItem): boolean {
  return (
    isCreateNewDestExistsPark(item.approval.code, item.approval.finding)
    || isZeroRetryBudgetPark(item.approval.code, item.approval.finding)
  );
}

/** Prefer Studio copy when a stored finding still says "Open Validate for this job". */
export function inboxCorrectiveAction(item: ScheduleApprovalInboxItem): string {
  if (inboxNeedsStudio(item)) return EMPTY_MAPPING_STUDIO_CORRECTIVE;
  if (isCreateNewDestExistsPark(item.approval.code, item.approval.finding)) {
    return DEST_EXISTS_AFTER_CREATE_NEW_CORRECTIVE;
  }
  if (isZeroRetryBudgetPark(item.approval.code, item.approval.finding)) {
    return ZERO_RETRY_BUDGET_CORRECTIVE;
  }
  return item.approval.corrective_action || "";
}

/** Rows a beat can replay — source or target present. Same rule as the engine. */
export function persistedMappingRows(raw: unknown): Array<Record<string, unknown>> {
  if (!Array.isArray(raw)) return [];
  const rows: Array<Record<string, unknown>> = [];
  for (const item of raw) {
    if (!item || typeof item !== "object") continue;
    const row = item as { source?: string; target?: string };
    if (String(row.source || "").trim() || String(row.target || "").trim()) {
      rows.push(item as Record<string, unknown>);
    }
  }
  return rows;
}

export function scheduleNeedsStudio(sched: PipelineSchedule): boolean {
  if (isEmptyMappingFinding(sched.approval_code, sched.approval_finding)) return true;
  const count = sched.mapping_count ?? (Array.isArray(sched.mappings) ? sched.mappings.length : undefined);
  if (count === 0 && (sched.last_status === "needs_approval" || !sched.enabled)) {
    return true;
  }
  return false;
}

/** Wait for the connector list before consuming a schedule→Studio token. */
export function studioIntentConnectorsReady(
  intent: { sourceConnectorId?: string; destConnectorId?: string },
  connectorIds: readonly string[],
): boolean {
  if (!intent.sourceConnectorId && !intent.destConnectorId) return true;
  return connectorIds.length > 0;
}

export { scheduleCreateOpensStudio } from "./sourceObjectPick";

export function studioIntentFromSchedule(sched: PipelineSchedule): {
  step: "source";
  scheduleId: string;
  sourceConnectorId: string;
  destConnectorId: string;
  sourceTable: string;
  destTable: string;
} {
  return {
    step: "source",
    scheduleId: sched.id,
    sourceConnectorId: sched.source_connector_id,
    destConnectorId: sched.dest_connector_id,
    sourceTable: sched.source_table,
    destTable: sched.dest_table,
  };
}
