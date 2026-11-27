import type { Tenant } from "./api";

/** "default" follows the deployment's setting; "on"/"off" are this workspace's own. */
export type SnapshotMode = "default" | "on" | "off";

export function snapshotModeOf(t: Pick<Tenant, "verification_snapshot">): SnapshotMode {
  if (t.verification_snapshot === true) return "on";
  if (t.verification_snapshot === false) return "off";
  return "default";
}

/** The tenant fields a save sends: null puts a field back to the deployment's setting. */
export function snapshotPayload(mode: SnapshotMode, minutes: string): {
  verification_snapshot: boolean | null;
  verification_snapshot_max_minutes: number | null;
} {
  const n = Number(minutes);
  return {
    verification_snapshot: mode === "default" ? null : mode === "on",
    verification_snapshot_max_minutes: minutes.trim() && Number.isFinite(n) && n > 0
      ? Math.min(1440, Math.max(1, Math.round(n))) : null,
  };
}
