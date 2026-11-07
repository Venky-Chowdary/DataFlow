/**
 * Security → Connector egress, decided from the tenant the operator just saved.
 *
 * The posture flag is the same fact on the server. A read that is not for this
 * workspace must not be painted as "Disabled" — that is how a saved CIDR still
 * looked off.
 */

export type EgressTenant = {
  workspace_id?: string;
  egress_allowlist?: string[];
} | null;

export type EgressPosture = {
  egress_allowlist_enabled?: boolean;
  scope_workspace_id?: string | null;
} | null;

export type ConnectorEgressState = "on" | "off" | "unknown";

export function connectorEgressState(
  workspaceId: string,
  tenant: EgressTenant | undefined,
  posture: EgressPosture,
): ConnectorEgressState {
  const scope = (workspaceId || "").trim();
  if (tenant && (!scope || !tenant.workspace_id || tenant.workspace_id === scope)) {
    return (tenant.egress_allowlist?.length ?? 0) > 0 ? "on" : "off";
  }
  if (tenant === null && scope) return "off";
  if (posture) {
    const answered = (posture.scope_workspace_id || "").trim();
    if (scope && answered && answered !== scope) return "unknown";
    if (scope && !answered) return "unknown";
    return posture.egress_allowlist_enabled ? "on" : "off";
  }
  return "unknown";
}
