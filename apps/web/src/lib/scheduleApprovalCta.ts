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

/** Prefer Studio copy when a stored finding still says "Open Validate for this job". */
export function inboxCorrectiveAction(item: ScheduleApprovalInboxItem): string {
  if (inboxNeedsStudio(item)) return EMPTY_MAPPING_STUDIO_CORRECTIVE;
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
