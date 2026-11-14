/**
 * UX-27: toasts raised while a person is still trying to get in. A successful
 * sign-in (password, MFA or email verification) clears them, so "Sign-in
 * failed" never sits next to "Signed in".
 */
export const AUTH_ATTEMPT_TOAST_TITLES = [
  "Sign-in failed",
  "Complete the form",
  "API offline",
  "Session expired",
  "Could not load connectors",
  "Couldn't start signup",
  "Resend unavailable",
  "Code sent again",
  "New code sent",
] as const;

export function clearAuthAttemptToasts(dismissMatching: (title: string) => void): void {
  for (const title of AUTH_ATTEMPT_TOAST_TITLES) dismissMatching(title);
}
