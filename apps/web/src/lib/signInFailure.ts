import { ApiError } from "./api";

/** Which sign-in failure happened, by what the request did — not by its words.
 *
 * The screen used to search the message for "api"/"deploy" to pick its heading,
 * so a rejected password whose message mentioned the deployment was announced as
 * "Control plane unreachable" and sent the operator to check infrastructure that
 * was answering normally.
 *
 * - `api`    — nothing that can answer a sign-in was reached: the fetch never
 *   completed, or a proxy in front of the API reported the upstream as dead.
 *   Behind the dev/reverse proxy a stopped API answers 500 with an empty body,
 *   which is not the API refusing a credential.
 * - `config` — the API answered 503: it has no identities configured yet.
 * - `auth`   — the API answered about the credentials; they were refused.
 */
export type SignInFailure = "api" | "config" | "auth";

export function classifySignInFailure(err: unknown): SignInFailure {
  const message = err instanceof Error ? err.message : "";
  const status = err instanceof ApiError ? err.status : 0;
  if (status === 0 && /Failed to fetch|NetworkError|timed out|fetch/i.test(message)) {
    return "api";
  }
  if (status === 503) return "config";
  // No 5xx is ever a statement about the credentials: a stopped API behind the
  // dev proxy answers 500 with an empty body, and a gateway answers 502/504.
  // Blaming the password there sends the operator to reset something that was
  // never wrong.
  if (status >= 500) return "api";
  return "auth";
}
