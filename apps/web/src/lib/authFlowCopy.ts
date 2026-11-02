/** Server-error → sentence-case UI copy for the signup/auth flows.
 *  Pure functions so the mapping is unit-tested, not ad hoc per catch block. */

export interface AuthFlowError {
  code?: string;
  message?: string;
  attemptsRemaining?: number | null;
  retryAfterSec?: number;
}

function retryMinutes(retryAfterSec: number | undefined): number {
  return Math.max(1, Math.round((retryAfterSec ?? 0) / 60));
}

/** A wrong or expired 6-digit code (verify + reset + email-change confirm). */
export function codeEntryError(err: AuthFlowError, fallback: string): string {
  if (err.code === "invalid_code") {
    const left = err.attemptsRemaining;
    return left && left > 0
      ? `That code isn't right. ${left} attempt${left === 1 ? "" : "s"} left.`
      : "That code isn't right. Try again.";
  }
  if (err.code === "code_expired" || err.code === "otp_expired") {
    return "This code has expired. Request a new one.";
  }
  return err.message || fallback;
}

/** A code-send request refused by quota or velocity limits. */
export function mailSendError(err: AuthFlowError, fallback: string): string {
  if (err.code === "email_quota_exceeded") {
    return `We couldn't send your code right now — try again in ${retryMinutes(err.retryAfterSec)} min.`;
  }
  if (err.code === "signup_rate_limited") {
    return `Too many attempts from this network — try again in ${retryMinutes(err.retryAfterSec)} min.`;
  }
  return err.message || fallback;
}

/** POST /auth/email/change refusals on the request step. */
export function changeEmailRequestError(err: AuthFlowError): string {
  if (err.code === "same_address") return "That's already your sign-in email.";
  if (err.code === "email_taken") return "That address already belongs to another account.";
  if (err.code === "unavailable") return "Email change is temporarily unavailable — try again shortly.";
  return mailSendError(err, "Couldn't send the code. Try again.");
}

/** POST /auth/email/change/confirm refusals. */
export function changeEmailConfirmError(err: AuthFlowError): string {
  if (err.code === "email_taken") return "That address already belongs to another account.";
  if (err.code === "unavailable") return "Email change is temporarily unavailable — try again shortly.";
  return codeEntryError(err, "Couldn't confirm the change. Try again.");
}
