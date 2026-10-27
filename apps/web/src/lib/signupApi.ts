/**
 * Self-serve signup API client (flag-gated DATAWRAP_SIGNUP_ENABLED).
 *
 * The new auth surface answers the {error:{code,message,retry_after_sec}}
 * envelope rather than {detail}; errors surface as SignupApiError carrying
 * the machine code so the UI can react without sniffing message text.
 */
import type { WorkspaceSession } from "./session";
import { API_BASE } from "./types";

export class SignupApiError extends Error {
  readonly status: number;
  readonly code: string;
  readonly retryAfterSec: number;
  readonly challengeId: string;
  readonly attemptsRemaining: number | null;

  constructor(status: number, code: string, message: string, retryAfterSec = 0, challengeId = "", attemptsRemaining: number | null = null) {
    super(message);
    this.name = "SignupApiError";
    this.status = status;
    this.code = code;
    this.retryAfterSec = retryAfterSec;
    this.challengeId = challengeId;
    this.attemptsRemaining = attemptsRemaining;
  }
}

async function call(path: string, body?: unknown): Promise<Record<string, unknown>> {
  const res = await fetch(`${API_BASE}${path}`, {
    method: body === undefined ? "GET" : "POST",
    headers: body === undefined ? undefined : { "Content-Type": "application/json" },
    body: body === undefined ? undefined : JSON.stringify(body),
    credentials: "include",
  });
  const data = (await res.json().catch(() => ({}))) as Record<string, unknown>;
  if (!res.ok) {
    const err = (data.error ?? {}) as Record<string, unknown>;
    throw new SignupApiError(
      res.status,
      String(err.code ?? "request_failed"),
      String(err.message ?? data.detail ?? "Request failed."),
      Number(err.retry_after_sec ?? 0),
      String(data.challenge_id ?? ""),
      typeof err.attempts_remaining === "number" ? err.attempts_remaining : null,
    );
  }
  return data;
}

export interface ChallengeResponse {
  ok: boolean;
  challengeId: string;
  resendAfterSec: number;
  expiresInSec: number;
}

function challenge(data: Record<string, unknown>): ChallengeResponse {
  return {
    ok: Boolean(data.ok ?? true),
    challengeId: String(data.challenge_id ?? ""),
    resendAfterSec: Number(data.resend_after_sec ?? 60),
    expiresInSec: Number(data.expires_in_sec ?? 600),
  };
}

/** Null when self-serve signup is disabled on this deployment (route 404s). */
export async function fetchFormToken(purpose: string): Promise<string | null> {
  try {
    const res = await fetch(`${API_BASE}/auth/form-token?purpose=${encodeURIComponent(purpose)}`);
    if (!res.ok) return null;
    const data = (await res.json()) as Record<string, unknown>;
    return typeof data.form_token === "string" ? data.form_token : null;
  } catch {
    return null;
  }
}

export async function signupRequest(input: {
  email: string;
  name: string;
  password: string;
  formToken?: string;
  turnstileToken?: string;
}): Promise<ChallengeResponse> {
  const data = await call("/auth/signup", {
    email: input.email,
    name: input.name,
    password: input.password,
    form_token: input.formToken || undefined,
    turnstile_token: input.turnstileToken || undefined,
    honeypot: "",
  });
  return challenge(data);
}

export interface VerifiedSignup {
  token: string;
  expires_at: number;
  user: { email: string; name?: string; role?: string };
  next?: string;
}

/**
 * Verify the emailed code. The API answers with the same session payload as
 * password login; a response without a token is refused here rather than
 * stored, because an empty token reads back as "signed out" (D153).
 */
export async function verifyEmail(input: {
  challengeId: string;
  code: string;
}): Promise<VerifiedSignup> {
  const data = await call("/auth/verify-email", {
    challenge_id: input.challengeId,
    code: input.code,
  });
  const result = data as unknown as VerifiedSignup;
  if (typeof result.token !== "string" || !result.token || !result.user?.email) {
    throw new SignupApiError(502, "session_missing", "Your email is verified, but we couldn't sign you in. Sign in to continue.");
  }
  return result;
}

/** The browser session a verified signup opens — shaped exactly like a login's. */
export function verifiedSignupSession(result: VerifiedSignup): WorkspaceSession {
  return {
    email: result.user.email,
    name: result.user.name ?? "",
    role: result.user.role ?? "admin",
    token: result.token,
    expires_at: result.expires_at,
    signed_in_at: Date.now(),
  };
}

export async function resendCode(challengeId: string): Promise<ChallengeResponse> {
  const data = await call("/auth/resend", { challenge_id: challengeId });
  return challenge(data);
}

export async function forgotPassword(email: string, formToken?: string, turnstileToken?: string): Promise<ChallengeResponse> {
  const data = await call("/auth/password/forgot", {
    email,
    form_token: formToken || undefined,
    turnstile_token: turnstileToken || undefined,
    honeypot: "",
  });
  return challenge(data);
}

export async function resetPassword(input: {
  token: string;
  newPassword: string;
}): Promise<{ ok: boolean }> {
  const data = await call("/auth/password/reset", {
    token: input.token,
    new_password: input.newPassword,
  });
  return { ok: Boolean(data.ok) };
}

/** Landing-page probe for a reset link — never consumes the token. */
export async function inspectResetToken(token: string): Promise<{ ok: boolean; status: string }> {
  const data = await call("/auth/password/reset/inspect", { token });
  return { ok: Boolean(data.ok), status: String(data.status ?? "invalid") };
}

export interface SsoRoute {
  sso: string | null;
  start_url?: string;
}

export async function routeSsoForEmail(email: string): Promise<SsoRoute> {
  try {
    const data = await call("/auth/sso/route", { email });
    return { sso: (data.sso as string | null) ?? null, start_url: data.start_url as string | undefined };
  } catch {
    return { sso: null };
  }
}

export interface SignupConfig {
  enabled: boolean;
  turnstileSiteKey: string;
}

/** Capability probe on the always-mounted auth router — answers flag-off
 *  so login surfaces never mint a form token to feature-detect. */
export async function fetchSignupConfig(): Promise<SignupConfig> {
  try {
    const res = await fetch(`${API_BASE}/auth/signup/config`, { credentials: "include" });
    if (!res.ok) return { enabled: false, turnstileSiteKey: "" };
    const data = (await res.json()) as Record<string, unknown>;
    return {
      enabled: Boolean(data.enabled),
      turnstileSiteKey: String(data.turnstile_site_key ?? ""),
    };
  } catch {
    return { enabled: false, turnstileSiteKey: "" };
  }
}

async function authedCall(path: string, body: unknown): Promise<Record<string, unknown>> {
  const { apiBearerToken } = await import("./session");
  const token = apiBearerToken() ?? "";
  const csrf = typeof document !== "undefined"
    ? (document.cookie.match(/(?:^|;\s*)dw_csrf=([^;]+)/) ?? [])[1]
    : undefined;
  const res = await fetch(`${API_BASE}${path}`, {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
      ...(token ? { Authorization: `Bearer ${token}` } : {}),
      ...(csrf ? { "x-csrf-token": decodeURIComponent(csrf) } : {}),
    },
    body: JSON.stringify(body),
    credentials: "include",
  });
  const data = (await res.json().catch(() => ({}))) as Record<string, unknown>;
  if (!res.ok) {
    const err = (data.error ?? {}) as Record<string, unknown>;
    throw new SignupApiError(
      res.status,
      String(err.code ?? "request_failed"),
      String(err.message ?? data.detail ?? "Request failed."),
      Number(err.retry_after_sec ?? 0),
      String(data.challenge_id ?? ""),
      typeof err.attempts_remaining === "number" ? err.attempts_remaining : null,
    );
  }
  return data;
}

/** Authenticated: email a 6-digit code to the new address. */
export async function requestEmailChange(newEmail: string): Promise<ChallengeResponse> {
  const data = await authedCall("/auth/email/change", { new_email: newEmail });
  return challenge(data);
}

/** Authenticated: confirm the change with the code sent to the new address. */
export async function confirmEmailChange(challengeId: string, code: string): Promise<{ ok: boolean }> {
  const data = await authedCall("/auth/email/change/confirm", {
    challenge_id: challengeId,
    code,
  });
  return { ok: Boolean(data.ok) };
}
