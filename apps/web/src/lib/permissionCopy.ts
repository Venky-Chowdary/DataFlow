/**
 * One sentence explains a refusal, wherever the refusal is discovered.
 *
 * A disabled control and a 403 from the gate are the same fact seen at two
 * moments, so they must read alike. The gate's own wording,
 * `Permission denied: workspace.manage`, is a fact about the gate rather than
 * an explanation to a person; the permission name is kept — support needs it —
 * but after the action it withheld and the role that lacked it.
 */

const ROLE_PHRASE: Record<string, string> = {
  viewer: "a viewer",
  operator: "an operator",
  editor: "an editor",
  admin: "an administrator",
};

const PERMISSION_PHRASE: Record<string, string> = {
  "connector.write": "add or change connections",
  "connector.delete": "delete connections",
  "job.read": "read transfers",
  "job.run": "run transfers",
  "job.manage": "change transfers",
  "job.plan": "prepare a transfer plan",
  "schedule.read": "read schedules",
  "schedule.manage": "create or change schedules",
  "schedule.authorize": "approve a scheduled run",
  "workspace.read": "read workspace settings",
  "workspace.manage": "change workspace settings",
  "member.invite": "add or remove workspace members",
  "audit.read": "read the audit log",
};

/**
 * The lowest role that holds each permission, so a refusal names a role the
 * reader does not already have. Telling an editor to "ask for the editor role"
 * is not an instruction; it is a dead end.
 */
const PERMISSION_ROLE: Record<string, string> = {
  "connector.delete": "admin",
  "schedule.authorize": "admin",
  "workspace.manage": "admin",
};

/** The sentence a refused caller should read. */
export function refusalSentence(permission: string, role: string): string {
  const action = PERMISSION_PHRASE[permission] ?? "do this";
  const who = ROLE_PHRASE[role];
  const needed = PERMISSION_ROLE[permission] ?? "editor";
  return (
    `You don't have permission to ${action}` +
    (who ? ` — you are ${who} in this workspace` : "") +
    `. Ask a workspace admin for the ${needed} role` +
    (permission ? ` (needs ${permission})` : "") +
    `.`
  );
}

/** The permission a gate refusal names, taken from its body or its wording. */
export function permissionFromRefusal(detail: string, named: string): string {
  return named || /^Permission denied:\s*(\S+)/.exec(detail)?.[1] || "";
}
