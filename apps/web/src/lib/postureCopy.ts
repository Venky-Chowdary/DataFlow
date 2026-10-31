/**
 * Settings -> Security posture rows and the Enterprise save feedback
 * (QA enterprise #6/#7/#8, round 8; QA round 11 S7).
 *
 * Badges and toasts say exactly what the API enforces, from the same
 * `enforcement` block the posture and the tenant save response carry. Copy is
 * for workspace admins: a control only the platform can switch on says "ask
 * your platform administrator", never a deployment variable name. MFA is not
 * enforced (design only).
 */
import type { SecurityPosture, TenantEnforcement } from "./api";

export type PostureRow = { title: string; desc: string; on: boolean; pending: boolean };

export function sessionTimeoutRow(p: SecurityPosture): PostureRow {
  const on = Boolean(p.session_timeout_enforced);
  return {
    title: `Session timeout (${p.session_timeout_hours}h idle)`,
    desc: on
      ? "Enforced: sign-in sessions idle this long are signed out. API keys are not sessions."
      : "Recorded only. Turn on \"Enforce as idle timeout\" under Enterprise to sign idle sessions out.",
    on,
    pending: false,
  };
}

export function ipAllowlistRow(p: SecurityPosture): PostureRow {
  const status = p.enforcement?.ip_allowlist?.status;
  let desc = "Not enforced. Save an IP allowlist under Enterprise to restrict this workspace.";
  if (p.ip_allowlist_enforced) {
    desc = "Enforced for every sign-in session and API key using this workspace, on any host.";
  } else if (status === "audit_only") {
    desc = "Saved, not enforced: requests from outside the list are only logged. Ask your platform administrator to enable IP allowlist enforcement.";
  } else if (status === "untrusted_client_ip") {
    desc = "Saved, not enforced: the platform's proxy hop count is not configured, so real client addresses are not visible and requests are only logged. Ask your platform administrator to set it.";
  }
  return { title: "IP allowlisting", desc, on: Boolean(p.ip_allowlist_enforced), pending: false };
}

export function mfaRow(): PostureRow {
  return {
    title: "MFA required for admins",
    desc: "Recorded policy. Login MFA is not enforced yet (design proposal in docs/MFA_ENFORCEMENT_DESIGN.md).",
    on: false,
    pending: false,
  };
}

export type SaveFeedback = {
  title: string;
  message: string;
  tone: "success" | "warning";
  /** One line per control, as the API reports it now. */
  lines: string[];
  warnings: string[];
};

/** What the Enterprise save toast and status panel say, from the save response. */
export function tenantSaveFeedback(e: TenantEnforcement | undefined): SaveFeedback {
  if (!e) {
    return {
      title: "Tenant saved",
      message: "Saved. Enforcement status is not available; check Security & compliance.",
      tone: "warning",
      lines: [],
      warnings: ["The server did not report what is enforced."],
    };
  }
  const lines: string[] = [];
  const ip = e.ip_allowlist;
  if (ip?.status === "enforced") lines.push("IP allowlist: enforced.");
  else if (ip?.status === "audit_only" || ip?.status === "untrusted_client_ip") {
    lines.push("IP allowlist: saved, not enforced (requests are only logged).");
  }
  else lines.push("IP allowlist: none saved.");
  const st = e.session_timeout;
  if (st?.enforced) lines.push(`Session timeout: enforced (${st.hours}h idle).`);
  else if (st?.status === "recorded_only") lines.push(`Session timeout: recorded, not enforced (${st.hours}h).`);
  lines.push("MFA: recorded only, not enforced yet.");
  const warnings = e.warnings ?? [];
  return warnings.length
    ? { title: "Saved — not everything is enforced", message: warnings[0], tone: "warning", lines, warnings }
    : { title: "Tenant saved", message: lines.slice(0, 2).join(" "), tone: "success", lines, warnings };
}
