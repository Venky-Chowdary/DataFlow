/**
 * What the operator can do about a parked schedule finding.
 *
 * Empty-mapping refusals are plan changes: a signature cannot invent column
 * names. The only honest next action is Transfer Studio (Map → Validate →
 * Schedule), not "Open Validate for this job" and not Decide.
 */

import type { PipelineSchedule, ScheduleApprovalInboxItem } from "./types";

export function isEmptyMappingFinding(code?: string, finding?: string): boolean {
  if ((code || "").toUpperCase() === "EMPTY_MAPPING_CONTRACT") return true;
  return /no persisted column mappings/i.test(finding || "");
}

export function inboxNeedsStudio(item: ScheduleApprovalInboxItem): boolean {
  return isEmptyMappingFinding(item.approval.code, item.approval.finding);
}

export function scheduleNeedsStudio(sched: PipelineSchedule): boolean {
  if (isEmptyMappingFinding(sched.approval_code, sched.approval_finding)) return true;
  const count = sched.mapping_count ?? (Array.isArray(sched.mappings) ? sched.mappings.length : undefined);
  if (count === 0 && (sched.last_status === "needs_approval" || !sched.enabled)) {
    return true;
  }
  return false;
}

export function studioIntentFromSchedule(sched: PipelineSchedule): {
  step: "source";
  sourceConnectorId: string;
  destConnectorId: string;
  sourceTable: string;
  destTable: string;
} {
  return {
    step: "source",
    sourceConnectorId: sched.source_connector_id,
    destConnectorId: sched.dest_connector_id,
    sourceTable: sched.source_table,
    destTable: sched.dest_table,
  };
}
