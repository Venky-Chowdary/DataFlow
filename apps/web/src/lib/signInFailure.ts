import { ApiError } from "./api";

/** Which sign-in failure happened, by what the request did — not by its words.
 *
 * The screen used to search the message for "api"/"deploy" to pick its heading,
 * so a rejected password whose message mentioned the deployment was announced as
 * "Control plane unreachable" and sent the operator to check infrastructure that
 * was answering normally.
 *
 * - `api`    — no answer arrived at all (fetch never completed).
 * - `config` — the API answered 503: it has no identities configured yet.
 * - `auth`   — the API answered; the credentials were refused.
 */
export type SignInFailure = "api" | "config" | "auth";

export function classifySignInFailure(err: unknown): SignInFailure {
  const message = err instanceof Error ? err.message : "";
  const status = err instanceof ApiError ? err.status : 0;
  if (status === 0 && /Failed to fetch|NetworkError|timed out|fetch/i.test(message)) {
    return "api";
  }
  return status === 503 ? "config" : "auth";
}
