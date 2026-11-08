/**
 * Settings → Team: how someone is brought into a workspace.
 *
 * "email" (default) sends a branded invitation the person accepts themselves —
 * nothing is granted until they do. "direct" adds the membership at once (for
 * deployments without mail, or someone who already has a login); minting a
 * login there puts a one-time password on screen, so it stays an explicit
 * choice.
 */
export type InviteMode = "email" | "direct";

export const INVITE_DEFAULTS = {
  mode: "email" as InviteMode,
  createLogin: false,
} as const;

/** "Expires in 6 days" / "Expires in 3 hours" / "Expired" for a pending invite. */
export function inviteExpiryLabel(expiresAt: string | undefined, now: number = Date.now()): string {
  if (!expiresAt) return "";
  const at = new Date(expiresAt).getTime();
  if (!Number.isFinite(at)) return "";
  const left = at - now;
  if (left <= 0) return "Expired";
  const hours = Math.floor(left / 3_600_000);
  if (hours >= 48) return `Expires in ${Math.floor(hours / 24)} days`;
  if (hours >= 1) return `Expires in ${hours} hour${hours === 1 ? "" : "s"}`;
  return "Expires within the hour";
}

/** Invitations whose link lapsed stay listed (resend re-opens them). */
export function isInviteExpired(expiresAt: string | undefined, now: number = Date.now()): boolean {
  return inviteExpiryLabel(expiresAt, now) === "Expired";
}
