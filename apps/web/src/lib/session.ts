export interface WorkspaceSession {
  email: string;
  name: string;
  role: string;
  token: string;
  expires_at: number;
  signed_in_at: number;
}

const SESSION_KEY = "df2.session";

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

export function clearSession() {
  try {
    localStorage.removeItem(SESSION_KEY);
    sessionStorage.removeItem(SESSION_KEY);
  } catch {
    /* ignore */
  }
}

export function getAuthToken(): string | null {
  return readSession()?.token ?? null;
}
