/**
 * Persist Data Pilot conversations locally so refresh / nav does not wipe chats.
 * Scoped per browser profile (localStorage) — not a server transcript store.
 */

import type { CopilotAction, CopilotChatMessage, CopilotPendingAction } from "./api";

export interface PilotMessage {
  role: "user" | "assistant";
  text: string;
  method?: string;
  actions?: CopilotAction[];
  pending_actions?: CopilotPendingAction[];
  suggested_prompts?: string[];
  tools_used?: { name: string; success: boolean; summary: string }[];
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
}

export interface PilotRailState {
  messages: PilotMessage[];
  history: CopilotChatMessage[];
  updatedAt: number;
}

const SESSIONS_KEY = "df2.pilot.sessions.v1";
const ACTIVE_KEY = "df2.pilot.activeId.v1";
const ASIDE_KEY = "df2.pilot.asideOpen.v1";
const RAIL_KEY = "df2.pilot.rail.v1";
const SIDEBAR_COMPACT_KEY = "df2.sidebar.navCompact.v1";

const MAX_SESSIONS = 40;
const MAX_MESSAGES = 120;
const MAX_HISTORY = 40;
const MAX_TOOL_LOG = 40;

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
    messages: (s.messages || []).slice(-MAX_MESSAGES),
    history: (s.history || []).slice(-MAX_HISTORY),
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
    updatedAt: stored.updatedAt || Date.now(),
  };
}

export function saveRailChat(state: Pick<PilotRailState, "messages" | "history">) {
  writeJson(RAIL_KEY, {
    messages: state.messages.slice(-MAX_MESSAGES),
    history: state.history.slice(-MAX_HISTORY),
    updatedAt: Date.now(),
  });
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
