/**
 * The audit actor cell: a person's email as-is; a machine identity as its name
 * and a short masked id (the API sends `actor_display`), never a raw key id.
 */
const MACHINE: Record<string, string> = {
  api_key: "API key",
  service_account: "Service account",
  scim: "SCIM token",
};

export function auditActorLabel(ev: { actor?: string | null; actor_display?: string | null }): string {
  const display = String(ev.actor_display || "").trim();
  if (display) return display;
  const actor = String(ev.actor || "").trim();
  if (!actor) return "system";
  const sep = actor.indexOf(":");
  const kind = sep > 0 ? actor.slice(0, sep) : "";
  const id = sep > 0 ? actor.slice(sep + 1) : "";
  if (MACHINE[kind] && id) return `${MACHINE[kind]} …${id.slice(-4)}`;
  return actor;
}
