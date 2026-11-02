import type { AuditExportFormat, WorkspaceApiKey, WorkspaceApiKeyLifetime } from "./api.js";

export const API_KEY_LIFETIME_OPTIONS: Array<{
  value: WorkspaceApiKeyLifetime;
  label: string;
}> = [
  { value: "7d", label: "7 days" },
  { value: "30d", label: "30 days" },
  { value: "60d", label: "60 days" },
  { value: "90d", label: "90 days" },
  { value: "365d", label: "1 year" },
  { value: "never", label: "No expiration" },
];

export function filterWorkspaceApiKeys(keys: WorkspaceApiKey[]): WorkspaceApiKey[] {
  return keys.filter((key) => key.kind !== "service_account" && key.kind !== "scim");
}

// Mirrors apps/api/services/team_store.py:ROLES.
export const SCIM_GROUP_MAPPING_ROLES = ["admin", "editor", "viewer"] as const;
export type ScimGroupMappingRole = (typeof SCIM_GROUP_MAPPING_ROLES)[number];

export function scopeOptions(actorPermissions: string[]): string[] {
  return [...new Set(actorPermissions.map((permission) => permission.trim()).filter(Boolean))].sort();
}

export type ScimGroupMappingRow = {
  group: string;
  workspace_id: string;
  role: string;
};

export type ScimGroupMappingsPayload =
  | { ok: true; mappings: Record<string, { workspace_id: string; role: ScimGroupMappingRole }> }
  | { ok: false; error: string };

export function buildScimGroupMappingsPayload(
  rows: ScimGroupMappingRow[],
): ScimGroupMappingsPayload {
  const entries: Array<[string, { workspace_id: string; role: ScimGroupMappingRole }]> = [];
  const seen = new Set<string>();
  for (const row of rows) {
    const group = row.group.trim();
    const workspaceId = row.workspace_id.trim();
    const role = row.role.trim().toLowerCase();
    if (!group) return { ok: false, error: "Enter an IdP group name." };
    const foldedGroup = group.toLowerCase();
    if (seen.has(foldedGroup)) return { ok: false, error: `Duplicate IdP group name: ${group}.` };
    seen.add(foldedGroup);
    if (!workspaceId) return { ok: false, error: `Select a workspace for ${group}.` };
    if (!role) return { ok: false, error: `Select a role for ${group}.` };
    if (!SCIM_GROUP_MAPPING_ROLES.includes(role as ScimGroupMappingRole)) {
      return { ok: false, error: `Unsupported role for ${group}: ${role}.` };
    }
    entries.push([group, { workspace_id: workspaceId, role: role as ScimGroupMappingRole }]);
  }
  return { ok: true, mappings: Object.fromEntries(entries) };
}

export function auditExportFilename(format: AuditExportFormat, now = new Date()): string {
  const timestamp = now.toISOString();
  const date = timestamp.slice(0, 10).replaceAll("-", "");
  const time = timestamp.slice(11, 19).replaceAll(":", "");
  return `datawrap-audit-${date}-${time}.${format}`;
}
