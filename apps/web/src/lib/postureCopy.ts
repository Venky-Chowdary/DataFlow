/**
 * Settings -> Security posture rows (QA enterprise #6/#7/#8, round 8).
 *
 * Badges must say exactly what the API enforces: session timeout is an idle
 * timeout only when the tenant turns enforcement on; the IP allowlist only on
 * the tenant custom domain with DATAFLOW_TENANT_IP_ALLOWLIST_ENFORCE on; MFA
 * only while the tenant requires it (round 11).
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

export function mfaRow(p?: SecurityPosture | null): PostureRow {
  const block = p?.enforcement?.mfa;
  const on = Boolean(p?.mfa_enforced);
  const counts =
    typeof block?.members_total === "number" && typeof block?.members_enrolled === "number"
      ? ` ${block.members_enrolled} of ${block.members_total} members have set it up.`
      : "";
  const sso = block?.sso_exempt === false ? " SSO sessions are held too." : " SSO sessions are exempt (the identity provider does MFA).";
  return {
    title: "Two-factor authentication",
    desc: on
      ? `Enforced: password sign-ins need an authenticator-app code; members without one can only reach setup.${sso} API keys are not affected.${counts}`
      : "Available, not required. Turn on \"Require two-factor authentication\" under Enterprise to enforce it.",
    on,
    pending: false,
  };
}
