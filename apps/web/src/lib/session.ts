import { clearActiveWorkspaceId } from "./workspace";

export interface WorkspaceSession {
  email: string;
  name: string;
  role: string;
  token: string;
  expires_at: number;
  signed_in_at: number;
}

const SESSION_KEY = "df2.session";

/** Credential the dev preview writes. It is not an API session and must not be sent as Bearer. */
export const UI_PREVIEW_TOKEN = "dev-ui-preview";

export function isUiPreviewSession(session: WorkspaceSession | null = readSession()): boolean {
  return session?.token === UI_PREVIEW_TOKEN;
}

/**
 * Bearer credential for API calls. The dev preview token stays in the browser
 * so the wizard can render, and is never presented as an API session.
 */
export function apiBearerToken(session: WorkspaceSession | null = readSession()): string | null {
  if (!session?.token || isUiPreviewSession(session)) return null;
  return session.token;
}

/**
 * A 401 retires a real API session. A UI preview has no API session to expire,
 * so the same 401 must leave the operator on the page they opened.
 */
export function apiUnauthorizedEndsSession(session: WorkspaceSession | null = readSession()): boolean {
  return !isUiPreviewSession(session);
}

export function readSession(): WorkspaceSession | null {
  try {
    const raw = localStorage.getItem(SESSION_KEY) || sessionStorage.getItem(SESSION_KEY);
    if (!raw) return null;
    const data = JSON.parse(raw) as WorkspaceSession;
    if (!data.token || !data.email) return null;
    if (data.expires_at && data.expires_at * 1000 < Date.now()) {
      clearSession();
      return null;
    }
    return data;
  } catch {
    return null;
  }
}

export function writeSession(session: WorkspaceSession, remember: boolean) {
  // Another account signing in on this browser must not inherit the previous
  // account's workspace id: it would be sent as X-Workspace-Id (D154).
  const previous = readSession();
  if (!previous || previous.email.trim().toLowerCase() !== session.email.trim().toLowerCase()) {
    clearActiveWorkspaceId();
  }
  const raw = JSON.stringify(session);
  try {
    if (remember) {
      localStorage.setItem(SESSION_KEY, raw);
      sessionStorage.removeItem(SESSION_KEY);
    } else {
      sessionStorage.setItem(SESSION_KEY, raw);
      localStorage.removeItem(SESSION_KEY);
    }
  } catch {
    // Private browsing — session still works in-memory for this tab via caller state.
  }
}

/**
 * End the session — sign-out, a 401, or expiry. The active workspace id goes
 * with it, so the next account in this browser never sends it (D154).
 */
export function clearSession() {
  try {
    localStorage.removeItem(SESSION_KEY);
    sessionStorage.removeItem(SESSION_KEY);
  } catch {
    /* ignore */
  }
  clearActiveWorkspaceId();
}

/**
 * The signed-in operator's name for an audit trail, or `null`.
 *
 * Sent as `X-Actor` on decision calls. The API prefers the identity it verified
 * itself and only falls back to this header when enforcement is off, so it can
 * name the operator of a single-operator deployment but never impersonate one.
 */
export function getSessionActor(): string | null {
  const s = readSession();
  const actor = (s?.email || s?.name || "").trim();
  return actor.length >= 2 ? actor : null;
}
