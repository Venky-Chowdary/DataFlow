/** Honest GitOps toolbar copy — empty fleet is not an unexpected error. */

export function fleetExportBlockedReason(scheduleCount: number): string | null {
  if (scheduleCount > 0) return null;
  return "There are no scheduled jobs to export yet. Create a schedule first, or Import YAML to bring one in.";
}
