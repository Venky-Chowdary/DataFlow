/**
 * Persist Datawrap Pilot conversations locally so refresh / nav does not wipe chats.
 * Scoped per browser profile (localStorage) — not a server transcript store.
 * Secrets (passwords, connection URLs with credentials) are redacted before write.
 */

import type {
  CopilotAction,
  CopilotChatMessage,
  CopilotPendingAction,
  CopilotSource,
} from "./api";

export interface PilotMessage {
  role: "user" | "assistant";
  text: string;
  method?: string;
  actions?: CopilotAction[];
  pending_actions?: CopilotPendingAction[];
  suggested_prompts?: string[];
  tools_used?: { name: string; success: boolean; summary: string }[];
  /** Citations the turn actually retrieved — absent means the answer is uncited. */
  sources?: CopilotSource[];
}

export interface PilotToolLogEntry {
  name: string;
  success: boolean;
  summary: string;
  at: string;
}

export interface PilotSession {
  id: string;
  title: string;
  messages: PilotMessage[];
  history: CopilotChatMessage[];
  toolLog: PilotToolLogEntry[];
  updatedAt: number;
  /** Last durable sample/query result ref from the API. */
  lastResultId?: string;
}

export interface PilotRailState {
  messages: PilotMessage[];
  history: CopilotChatMessage[];
  /** Stable id so follow-ups share working memory with the API. */
  sessionId: string;
  /** Last durable sample/query result ref (pr_…). */
  lastResultId?: string;
  updatedAt: number;
}

/** Pull the newest pr_… result id from tool summaries (PilotPage + rail). */
export function extractPilotResultId(
  tools?: { name: string; success: boolean; summary: string }[],
): string | undefined {
  if (!tools?.length) return undefined;
  for (let i = tools.length - 1; i >= 0; i -= 1) {
    const t = tools[i];
    if (!t.success) continue;
    if (
      ![
        "sample_connector_object",
        "run_query",
        "filter_result",
        "analyze_result",
        "aggregate_data",
      ].includes(t.name)
    ) {
      continue;
    }
    const m = /\b(pr_[a-f0-9]+)\b/i.exec(t.summary || "");
    if (m) return m[1];
  }
  return undefined;
}

const SESSIONS_KEY = "df2.pilot.sessions.v1";
const ACTIVE_KEY = "df2.pilot.activeId.v1";
const ASIDE_KEY = "df2.pilot.asideOpen.v1";
const RAIL_KEY = "df2.pilot.rail.v1";
const DELETED_KEY = "df2.pilot.deletedIds.v1";
const SIDEBAR_COMPACT_KEY = "df2.sidebar.navCompact.v1";
const MAX_DELETED = 80;

const MAX_SESSIONS = 40;
const MAX_MESSAGES = 120;
const MAX_HISTORY = 40;
const MAX_TOOL_LOG = 40;

/** Redact passwords / connection URLs before localStorage persistence. */
export function redactSecrets(text: string): string {
  if (!text) return text;
  let out = text;
  // postgres://user:pass@host → postgres://user:***@host
  out = out.replace(
    /\b((?:postgres(?:ql)?|mysql|mongodb(?:\+srv)?|mssql|sqlserver|redis):\/\/[^:@/\s]+:)([^@/\s]+)(@)/gi,
    "$1***$3",
  );
  // password: secret / "password": "secret"
  out = out.replace(
    /\b(password|passwd|pwd|secret|api[_-]?key|token|private[_-]?key)\b(\s*[:=]\s*)(["']?)([^\s"'\\,;]+)(["']?)/gi,
    "$1$2$3***$5",
  );
  // Authorization: Bearer …
  out = out.replace(/\b(Bearer\s+)[A-Za-z0-9._\-+=/]+/gi, "$1***");
  return out;
}

function redactMessage(m: PilotMessage): PilotMessage {
  return {
    ...m,
    text: redactSecrets(m.text || ""),
    pending_actions: (m.pending_actions || []).map((a) => {
      const payload = a.payload && typeof a.payload === "object"
        ? Object.fromEntries(
          Object.entries(a.payload as Record<string, unknown>).map(([k, v]) => {
            const key = k.toLowerCase();
            if (["password", "passwd", "pwd", "api_key", "token", "private_key", "connection_string"].includes(key)) {
              return [k, typeof v === "string" && v ? "***" : v];
            }
            if (typeof v === "string") return [k, redactSecrets(v)];
            return [k, v];
          }),
        )
        : a.payload;
      return { ...a, payload };
    }),
  };
}

function redactHistory(h: CopilotChatMessage[]): CopilotChatMessage[] {
  return (h || []).map((msg) => ({
    ...msg,
    content: redactSecrets(String(msg.content || "")),
  }));
}

function readJson<T>(key: string): T | null {
  try {
    const raw = localStorage.getItem(key);
    if (!raw) return null;
    return JSON.parse(raw) as T;
  } catch {
    return null;
  }
}

function writeJson(key: string, value: unknown) {
  try {
    localStorage.setItem(key, JSON.stringify(value));
  } catch {
    /* quota / private mode — ignore */
  }
}

function trimSession(s: PilotSession): PilotSession {
  return {
    ...s,
    messages: (s.messages || []).map(redactMessage).slice(-MAX_MESSAGES),
    history: redactHistory(s.history || []).slice(-MAX_HISTORY),
    toolLog: (s.toolLog || []).slice(0, MAX_TOOL_LOG),
    updatedAt: s.updatedAt || Date.now(),
  };
}

export function createEmptySession(title = "New conversation"): PilotSession {
  return {
    id: crypto.randomUUID(),
    title,
    messages: [],
    history: [],
    toolLog: [],
    updatedAt: Date.now(),
  };
}

export function loadPilotWorkspace(): { sessions: PilotSession[]; activeId: string } {
  const stored = readJson<PilotSession[]>(SESSIONS_KEY);
  const sessions = Array.isArray(stored) && stored.length
    ? stored.map(trimSession).sort((a, b) => (b.updatedAt || 0) - (a.updatedAt || 0))
    : [createEmptySession()];
  const activeStored = localStorage.getItem(ACTIVE_KEY);
  const activeId = sessions.some((s) => s.id === activeStored) ? (activeStored as string) : sessions[0].id;
  return { sessions, activeId };
}

export function savePilotWorkspace(sessions: PilotSession[], activeId: string) {
  const cleaned = sessions
    .map(trimSession)
    .sort((a, b) => (b.updatedAt || 0) - (a.updatedAt || 0))
    .slice(0, MAX_SESSIONS);
  // Keep empty "New conversation" only if it's the sole/active session.
  const filtered = cleaned.filter(
    (s) => s.messages.length > 0 || s.id === activeId || cleaned.length === 1,
  );
  writeJson(SESSIONS_KEY, filtered.length ? filtered : [createEmptySession()]);
  try {
    localStorage.setItem(ACTIVE_KEY, activeId);
  } catch {
    /* ignore */
  }
}

export function loadAsideOpen(defaultOpen = true): boolean {
  const raw = localStorage.getItem(ASIDE_KEY);
  if (raw == null) return defaultOpen;
  return raw === "1" || raw === "true";
}

export function saveAsideOpen(open: boolean) {
  try {
    localStorage.setItem(ASIDE_KEY, open ? "1" : "0");
  } catch {
    /* ignore */
  }
}

export function loadRailChat(): PilotRailState | null {
  const stored = readJson<PilotRailState>(RAIL_KEY);
  if (!stored || !Array.isArray(stored.messages)) return null;
  return {
    messages: stored.messages.slice(-MAX_MESSAGES),
    history: (stored.history || []).slice(-MAX_HISTORY),
    sessionId: stored.sessionId || crypto.randomUUID(),
    lastResultId: stored.lastResultId,
    updatedAt: stored.updatedAt || Date.now(),
  };
}

export function loadDeletedSessionIds(): string[] {
  const raw = readJson<string[]>(DELETED_KEY);
  return Array.isArray(raw) ? raw.filter((id) => typeof id === "string" && id.trim()) : [];
}

export function isDeletedPilotSession(id: string): boolean {
  const key = String(id || "").trim();
  return Boolean(key) && loadDeletedSessionIds().includes(key);
}

export function rememberDeletedSession(id: string) {
  const key = String(id || "").trim();
  if (!key) return;
  const next = [...new Set([...loadDeletedSessionIds(), key])].slice(-MAX_DELETED);
  writeJson(DELETED_KEY, next);
}

export function saveRailChat(
  state: Pick<PilotRailState, "messages" | "history" | "sessionId" | "lastResultId">,
) {
  if (isDeletedPilotSession(state.sessionId)) return;
  writeJson(RAIL_KEY, {
    messages: state.messages.map(redactMessage).slice(-MAX_MESSAGES),
    history: redactHistory(state.history).slice(-MAX_HISTORY),
    sessionId: state.sessionId,
    lastResultId: state.lastResultId,
    updatedAt: Date.now(),
  });
}

/**
 * Promote the FAB/rail conversation into the Pilot workspace so opening
 * Datawrap Pilot keeps the same session id, result ref, and message history.
 * Returns null when the rail has nothing worth promoting.
 */
export function promoteRailChatToPilotSession(): {
  sessions: PilotSession[];
  activeId: string;
} | null {
  const rail = loadRailChat();
  if (!rail) return null;
  if (isDeletedPilotSession(rail.sessionId)) {
    clearRailChat();
    return null;
  }
  const meaningful = (rail.messages || []).filter(
    (m) => m.role === "user" || (m.role === "assistant" && (m.text || "").length > 80),
  );
  if (meaningful.length === 0 && (rail.history || []).length === 0) return null;

  const { sessions, activeId } = loadPilotWorkspace();
  const existing = sessions.find((s) => s.id === rail.sessionId);
  const titleFromUser =
    (rail.messages || []).find((m) => m.role === "user")?.text?.slice(0, 48) || "Rail conversation";

  if (existing) {
    const merged: PilotSession = trimSession({
      ...existing,
      messages: rail.messages.length >= existing.messages.length ? rail.messages : existing.messages,
      history: rail.history.length >= existing.history.length ? rail.history : existing.history,
      lastResultId: rail.lastResultId || existing.lastResultId,
      updatedAt: Date.now(),
      title: existing.title === "New conversation" ? titleFromUser : existing.title,
    });
    const next = [merged, ...sessions.filter((s) => s.id !== merged.id)];
    savePilotWorkspace(next, merged.id);
    return { sessions: next, activeId: merged.id };
  }

  const promoted: PilotSession = trimSession({
    id: rail.sessionId || crypto.randomUUID(),
    title: titleFromUser,
    messages: rail.messages || [],
    history: rail.history || [],
    toolLog: [],
    lastResultId: rail.lastResultId,
    updatedAt: Date.now(),
  });
  const next = [promoted, ...sessions.filter((s) => s.messages.length > 0 || s.id === activeId)];
  savePilotWorkspace(next, promoted.id);
  return { sessions: next, activeId: promoted.id };
}

export function deletePilotSession(
  sessions: PilotSession[],
  id: string,
  activeId: string,
): { sessions: PilotSession[]; activeId: string } {
  rememberDeletedSession(id);
  const rail = loadRailChat();
  if (rail?.sessionId === id) clearRailChat();
  let next = sessions.filter((s) => s.id !== id);
  let nextActive = activeId;
  if (!next.length) {
    const empty = createEmptySession();
    next = [empty];
    nextActive = empty.id;
  } else if (id === activeId) {
    nextActive = next[0].id;
  }
  savePilotWorkspace(next, nextActive);
  return { sessions: next, activeId: nextActive };
}

export function clearRailChat() {
  try {
    localStorage.removeItem(RAIL_KEY);
  } catch {
    /* ignore */
  }
}

export function loadSidebarNavCompact(): boolean {
  return localStorage.getItem(SIDEBAR_COMPACT_KEY) === "1";
}

export function saveSidebarNavCompact(compact: boolean) {
  try {
    localStorage.setItem(SIDEBAR_COMPACT_KEY, compact ? "1" : "0");
  } catch {
    /* ignore */
  }
}
