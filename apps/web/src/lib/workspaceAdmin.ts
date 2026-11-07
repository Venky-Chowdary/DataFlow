/**
 * Last-admin protection — same predicate as ``team_store._assert_not_last_admin``.
 * The API refuses the write; Map/Settings must not offer a control that can
 * only toast that refusal.
 */

export function normalizeMemberEmail(email: string | null | undefined): string {
  return String(email || "").trim().toLowerCase();
}

export function workspaceAdmins<T extends { email?: string; role?: string }>(
  members: T[],
): T[] {
  return members.filter((m) => String(m.role || "").trim().toLowerCase() === "admin");
}

/** True when removing or demoting this email would leave no workspace admin. */
export function isLastWorkspaceAdmin<T extends { email?: string; role?: string }>(
  members: T[],
  email: string | null | undefined,
): boolean {
  const wanted = normalizeMemberEmail(email);
  if (!wanted) return false;
  const admins = workspaceAdmins(members);
  return admins.length <= 1 && admins.some((m) => normalizeMemberEmail(m.email) === wanted);
}

export const LAST_ADMIN_PROTECTED =
  "This is the only admin — grant the admin role to someone else first";
