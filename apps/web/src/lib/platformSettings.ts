import type { PlatformEditability } from "./api";

/** The API's own words; used when an older API omits ``editable_reason``. */
export const PLATFORM_SETTINGS_REASON = "Only a platform admin can change deployment-wide settings";

export type PlatformLock = { locked: boolean; reason: string };

export const UNLOCKED: PlatformLock = { locked: false, reason: "" };

/**
 * Read-only state for a deployment-wide settings group from its read.
 *
 * Only an explicit ``editable: false`` locks: an API that predates the field
 * keeps the page editable, and the server still refuses the write.
 */
export function platformLock(read: PlatformEditability | null | undefined): PlatformLock {
  if (!read || read.editable !== false) return UNLOCKED;
  return { locked: true, reason: (read.editable_reason || "").trim() || PLATFORM_SETTINGS_REASON };
}
