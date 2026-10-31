/**
 * Two-factor authentication (TOTP) client — QA S7, round 11.
 *
 * The API owns every decision: whether a code is right, whether a session is
 * held by the workspace "require MFA" policy, how long a lockout lasts. This
 * module only carries the calls and a few pure helpers for display/input.
 */
import { apiErrorFrom, apiFetch, ApiError, type LoginMfaChallenge, type LoginSession } from "./api";
import { API_BASE } from "./types";

export type MfaHold = "mfa_enrollment_required" | "mfa_verification_required" | null;

export interface MfaStatus {
  enrolled: boolean;
  method: "totp" | null;
  enrolled_at: string | null;
  pending_enrollment: boolean;
  recovery_codes_remaining: number;
  locked: boolean;
  locked_until: number | null;
  required: boolean;
  hold: MfaHold;
  session_verified: boolean;
  via_sso: boolean;
  sso_exempt: boolean;
}

export interface MfaEnrollment {
  secret: string;
  otpauth_uri: string;
  issuer: string;
  account: string;
  digits: string;
  period: string;
}

/** Proof for re-enrolment, new recovery codes, or turning two-factor off. */
export interface MfaReauth {
  code?: string;
  recoveryCode?: string;
  password?: string;
}

function reauthBody(proof: MfaReauth = {}): string {
  return JSON.stringify({
    code: proof.code ?? "",
    recovery_code: proof.recoveryCode ?? "",
    password: proof.password ?? "",
  });
}

async function postJson<T>(path: string, body: string, fallback: string): Promise<T> {
  const res = await apiFetch(`${API_BASE}${path}`, { method: "POST", body });
  if (!res.ok) throw await apiErrorFrom(res, fallback);
  return (await res.json()) as T;
}

export async function fetchMfaStatus(): Promise<MfaStatus> {
  const res = await apiFetch(`${API_BASE}/auth/mfa`);
  if (!res.ok) throw await apiErrorFrom(res, "Could not read your two-factor settings");
  return (await res.json()) as MfaStatus;
}

export function startMfaEnrollment(proof?: MfaReauth): Promise<MfaEnrollment> {
  return postJson("/auth/mfa/totp/enroll", reauthBody(proof), "Could not start two-factor setup");
}

export async function confirmMfaEnrollment(code: string): Promise<string[]> {
  const data = await postJson<{ recovery_codes: string[] }>(
    "/auth/mfa/totp/confirm",
    JSON.stringify({ code }),
    "That code did not confirm two-factor setup",
  );
  return data.recovery_codes ?? [];
}

export function verifyMfaSession(proof: { code?: string; recoveryCode?: string }): Promise<{ verified: boolean }> {
  return postJson(
    "/auth/mfa/verify",
    JSON.stringify({ code: proof.code ?? "", recovery_code: proof.recoveryCode ?? "" }),
    "That code was not accepted",
  );
}

export async function regenerateRecoveryCodes(proof: MfaReauth): Promise<string[]> {
  const data = await postJson<{ recovery_codes: string[] }>(
    "/auth/mfa/recovery-codes",
    reauthBody(proof),
    "Could not create new recovery codes",
  );
  return data.recovery_codes ?? [];
}

export async function disableMfa(proof: MfaReauth): Promise<void> {
  await postJson("/auth/mfa/totp/disable", reauthBody(proof), "Could not turn off two-factor authentication");
}

/** Second sign-in step: the challenge from /auth/login plus a code -> the session. */
export async function completeMfaLogin(
  challenge: string,
  proof: { code?: string; recoveryCode?: string },
): Promise<LoginSession & { recovery_codes_remaining?: number; mfa_method?: string }> {
  const res = await fetch(`${API_BASE}/auth/login/mfa`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ challenge, code: proof.code ?? "", recovery_code: proof.recoveryCode ?? "" }),
  });
  if (!res.ok) {
    const body = (await res.json().catch(() => ({}))) as { detail?: unknown };
    const message = typeof body.detail === "string" && body.detail ? body.detail : "That code was not accepted.";
    throw new ApiError(message, res.status);
  }
  return res.json();
}

// ── pure helpers (unit-tested) ──────────────────────────────────────────────

export function isMfaChallenge(result: LoginSession | LoginMfaChallenge): result is LoginMfaChallenge {
  return (result as LoginMfaChallenge).mfa_required === true && typeof (result as LoginMfaChallenge).challenge === "string";
}

/** Authenticator codes: digits only, at most six (apps show "123 456"). */
export function normalizeTotpInput(value: string): string {
  return value.replace(/\D/g, "").slice(0, 6);
}

export function isCompleteTotp(value: string): boolean {
  return /^\d{6}$/.test(value);
}

/** Recovery codes look like ``abcd-efgh-jkmn-pqrs``; case and separators do not matter. */
export function isRecoveryCodeShape(value: string): boolean {
  return value.replace(/[^a-z0-9]/gi, "").length === 16;
}

/** The setup key in groups of four, the way people type it into an app. */
export function formatSetupKey(secret: string): string {
  return secret.replace(/\s+/g, "").replace(/(.{4})/g, "$1 ").trim();
}

/** A plain-text file body for "Download recovery codes". */
export function recoveryCodesFile(codes: string[], account: string, issuer = "Datawrap"): string {
  return [
    `${issuer} recovery codes for ${account}`,
    "Each code works once, in place of a code from your authenticator app.",
    "",
    ...codes,
    "",
  ].join("\n");
}

/** Plain-language reason for a refused code, from the API's status. */
export function mfaErrorMessage(err: unknown): string {
  if (err instanceof ApiError) {
    if (err.status === 429) return "Too many incorrect codes. Two-factor sign-in is locked for a while — try again later.";
    return err.message;
  }
  return err instanceof Error ? err.message : "Something went wrong.";
}
