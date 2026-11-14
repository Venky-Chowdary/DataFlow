/**
 * "Schedule" from Transfer Studio (UX-3).
 *
 * One click used to create an *active daily* schedule. With Full append that
 * re-inserts the whole source table into the destination every day. Studio
 * now opens a prefilled schedule form that starts paused and says so plainly
 * when an append mode has no cursor to stop it re-reading every row.
 */
import { SYNC_MODES } from "./transferConstants";

function syncModeLabel(mode: string): string {
  return SYNC_MODES.find((m) => m.id === mode)?.label ?? mode;
}

export type StudioScheduleCadence = "hourly" | "daily" | "weekly";

export interface StudioScheduleDraft {
  name: string;
  interval: StudioScheduleCadence;
  /** False = created paused. The default: nothing runs until someone turns it on. */
  enabled: boolean;
}

export function defaultStudioScheduleDraft(sourceName: string, destTable: string): StudioScheduleDraft {
  return { name: `${sourceName || "Source"} → ${destTable}`.slice(0, 128), interval: "daily", enabled: false };
}

/** True when every run would append the full source again (no cursor bounds it). */
export function appendsWithoutCursor(syncMode: string, cursorColumn: string): boolean {
  if (syncMode === "full_refresh_append") return true;
  return syncMode === "incremental_append" && !(cursorColumn || "").trim();
}

export function appendWithoutCursorWarning(syncMode: string, interval: StudioScheduleCadence): string {
  const every = interval === "hourly" ? "every hour" : interval === "weekly" ? "every week" : "every day";
  return (
    `${syncModeLabel(syncMode)} has no cursor, so each run appends every source row again: the destination grows by ` +
    `the whole table ${every}. Choose an incremental mode with a cursor column, or Overwrite, before you turn this schedule on.`
  );
}

export function studioScheduleModeLabel(mode: string): string {
  return syncModeLabel(mode);
}

export function studioScheduleSubmitLabel(draft: StudioScheduleDraft): string {
  return draft.enabled ? "Create and start schedule" : "Create paused schedule";
}

export function studioScheduleCreatedToast(draft: StudioScheduleDraft): { title: string; message: string } {
  return draft.enabled
    ? { title: "Schedule created", message: `Runs ${draft.interval}. Manage it in Schedules.` }
    : { title: "Schedule created, paused", message: "Nothing runs until you turn it on in Schedules." };
}
