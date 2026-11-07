/**
 * API-key ids on the Audit screen (QA browser 20).
 *
 * The API masks issued key ids to "…" + last four for readers who do not
 * manage keys (`key_ids_masked`, `X-Datawrap-Key-Ids-Masked`), and sends them
 * whole to workspace and platform admins for the export. On screen the id is
 * a handle, not something to read: every reader sees the same masked form the
 * actor column already uses (`actor_display`). The page never rebuilds a
 * full id from a masked one.
 */

const UUID = "[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}";
/** A key id where a key is named: key routes and machine actors. */
const KEY_CONTEXT = new RegExp(
  `((?:api-keys|service-accounts|scim-token|api_key|service_account|scim)[/:])(${UUID})`,
  "g",
);

export function maskedKeyId(id: string): string {
  const text = String(id || "");
  return text ? `…${text.slice(-4)}` : "…";
}

/** The audit resource / actor text with any key id shown as "…last4". */
export function maskKeyIdsForDisplay(text: string): string {
  return String(text ?? "").replace(KEY_CONTEXT, (_m, head: string, id: string) => `${head}${maskedKeyId(id)}`);
}

export const KEY_IDS_MASKED_HINT =
  "API-key ids are shown as their last four characters. Workspace admins get them in full in the export.";

export const KEY_IDS_ADMIN_HINT =
  "API-key ids are shown as their last four characters here; your export carries them in full.";

/** The note under the audit table: who sees full key ids. */
export function auditKeyIdHint(keyIdsMasked: boolean): string {
  return keyIdsMasked ? KEY_IDS_MASKED_HINT : KEY_IDS_ADMIN_HINT;
}
