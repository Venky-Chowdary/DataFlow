/**
 * Settings -> Security posture rows (QA enterprise #6/#7/#8, round 8).
 *
 * Badges must say exactly what the API enforces: session timeout is an idle
 * timeout only when the tenant turns enforcement on; the IP allowlist only on
 * the tenant custom domain with DATAFLOW_TENANT_IP_ALLOWLIST_ENFORCE on; MFA
 * is not enforced (design only).
 */
import type { SecurityPosture } from "./api";

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
  const missing = p.enforcement?.ip_allowlist?.missing ?? [];
  return {
    title: "IP allowlisting",
    desc: p.ip_allowlist_enforced
      ? "Enforced for requests to this tenant's custom domain. The shared app host is not covered."
      : missing.length
        ? `Not enforced. Missing: ${missing.join(", ")}.`
        : "Not enforced.",
    on: Boolean(p.ip_allowlist_enforced),
    pending: false,
  };
}

export function mfaRow(): PostureRow {
  return {
    title: "MFA required for admins",
    desc: "Recorded policy. Login MFA is not enforced yet (design proposal in docs/MFA_ENFORCEMENT_DESIGN.md).",
    on: false,
    pending: false,
  };
}
