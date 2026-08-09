/**
 * Query workspace persistence — console tabs, per-tab bind parameters, and
 * run history. Scoped to the browser profile (localStorage); this is not a
 * server-side query store, so nothing here is an audit trail.
 *
 * Bind-parameter values are redacted on write: operators paste tokens and
 * passwords into filter values, and a console must not turn that into
 * durable plaintext on disk.
 */

const TABS_KEY = "df2.query.tabs.v1";
const ACTIVE_KEY = "df2.query.activeTab.v1";
const HISTORY_KEY = "df2.query.history.v1";
const LAYOUT_KEY = "df2.query.layout.v1";

const MAX_TABS = 24;
const MAX_HISTORY = 200;
const MAX_QUERY_CHARS = 200_000;

export interface QueryTab {
  id: string;
  title: string;
  /** Whether the operator renamed the tab; auto-titles stop updating if so. */
  titlePinned?: boolean;
  connectorId: string;
  database: string;
  collection: string;
  query: string;
  limit: number;
  /** Bind parameter values keyed by `:name`. */
  params: Record<string, string>;
  updatedAt: number;
}

export interface QueryHistoryEntry {
  id: string;
  query: string;
  connectorId: string;
  connectorLabel?: string;
  database?: string;
  /** Wall-clock duration reported by the API, when available. */
  durationMs?: number;
  rowCount?: number;
  truncated?: boolean;
  ok: boolean;
  error?: string;
  at: number;
}

export interface QueryLayout {
  schemaOpen: boolean;
  historyOpen: boolean;
  /** Editor height in px; operators on laptops want this smaller. */
  editorHeight: number;
}

export const DEFAULT_LAYOUT: QueryLayout = {
  schemaOpen: true,
  historyOpen: false,
  editorHeight: 260,
};

// ---------------------------------------------------------------------------
// Redaction
// ---------------------------------------------------------------------------

const SECRET_NAME = /pass|pwd|secret|token|key|credential|auth|bearer|session/i;

/**
 * Redact a bind-parameter map for persistence. Values whose parameter name
 * looks credential-bearing are dropped entirely rather than masked, so a
 * stale secret can never be replayed out of localStorage.
 */
export function redactParams(params: Record<string, string>): Record<string, string> {
  const out: Record<string, string> = {};
  for (const [k, v] of Object.entries(params || {})) {
    if (SECRET_NAME.test(k)) continue;
    out[k] = String(v ?? "").slice(0, 2_000);
  }
  return out;
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
    /* quota or private mode — persistence is a convenience, never required */
  }
}

function newId(): string {
  try {
    return crypto.randomUUID();
  } catch {
    return `q${Date.now()}${Math.random().toString(36).slice(2, 8)}`;
  }
}

// ---------------------------------------------------------------------------
// Tabs
// ---------------------------------------------------------------------------

export function createTab(partial: Partial<QueryTab> = {}): QueryTab {
  return {
    id: partial.id ?? newId(),
    title: partial.title ?? "Untitled query",
    titlePinned: partial.titlePinned ?? false,
    connectorId: partial.connectorId ?? "",
    database: partial.database ?? "",
    collection: partial.collection ?? "",
    query: partial.query ?? "",
    limit: partial.limit ?? 1000,
    params: partial.params ?? {},
    updatedAt: partial.updatedAt ?? Date.now(),
  };
}

function sanitizeTab(t: QueryTab): QueryTab {
  return {
    ...createTab(t),
    query: String(t.query ?? "").slice(0, MAX_QUERY_CHARS),
    params: redactParams(t.params ?? {}),
  };
}

/**
 * Derive a tab title from the query — first table reference wins, then the
 * leading keyword. Keeps tab strips readable without operators naming things.
 */
export function deriveTabTitle(query: string, fallback = "Untitled query"): string {
  const q = (query || "").trim();
  if (!q) return fallback;
  // Leading char may be a quote — `FROM "public"."customers"` is common.
  const table = q.match(/\b(?:from|join|into|update)\s+(["`\[]?[\w$][\w$."`\[\]]*)/i);
  if (table) {
    const name = table[1].replace(/["`\[\]]/g, "").split(".").pop();
    if (name) return name.slice(0, 40);
  }
  // Mongo-style JSON filter — use the first field name.
  const field = q.match(/^\s*\{\s*"([^"]{1,40})"/);
  if (field) return field[1];
  const kw = q.match(/^([A-Za-z]+)/);
  return kw ? kw[1].toUpperCase().slice(0, 40) : fallback;
}

/** Apply an auto-title unless the operator pinned their own. */
export function retitleTab(tab: QueryTab): QueryTab {
  if (tab.titlePinned) return tab;
  return { ...tab, title: deriveTabTitle(tab.query, "Untitled query") };
}

export function loadTabs(): { tabs: QueryTab[]; activeId: string } {
  const stored = readJson<QueryTab[]>(TABS_KEY);
  const tabs =
    Array.isArray(stored) && stored.length ? stored.map(sanitizeTab) : [createTab()];
  const storedActive = localStorage.getItem(ACTIVE_KEY);
  const activeId = tabs.some((t) => t.id === storedActive)
    ? (storedActive as string)
    : tabs[0].id;
  return { tabs, activeId };
}

export function saveTabs(tabs: QueryTab[], activeId: string) {
  const cleaned = tabs.map(sanitizeTab).slice(0, MAX_TABS);
  writeJson(TABS_KEY, cleaned.length ? cleaned : [createTab()]);
  try {
    localStorage.setItem(ACTIVE_KEY, activeId);
  } catch {
    /* ignore */
  }
}

/**
 * Close a tab and pick the next active one — the neighbour to the right, or
 * the left when closing the last tab. Closing the only tab yields a fresh
 * empty tab so the console is never in a no-tab state.
 */
export function closeTab(
  tabs: QueryTab[],
  activeId: string,
  closeId: string,
): { tabs: QueryTab[]; activeId: string } {
  const idx = tabs.findIndex((t) => t.id === closeId);
  if (idx < 0) return { tabs, activeId };
  const next = tabs.filter((t) => t.id !== closeId);
  if (next.length === 0) {
    const fresh = createTab();
    return { tabs: [fresh], activeId: fresh.id };
  }
  if (activeId !== closeId) return { tabs: next, activeId };
  const neighbour = next[Math.min(idx, next.length - 1)];
  return { tabs: next, activeId: neighbour.id };
}

/** Duplicate a tab, inserting the copy immediately after the original. */
export function duplicateTab(
  tabs: QueryTab[],
  sourceId: string,
): { tabs: QueryTab[]; activeId: string } {
  const idx = tabs.findIndex((t) => t.id === sourceId);
  if (idx < 0) return { tabs, activeId: sourceId };
  const copy = createTab({
    ...tabs[idx],
    id: newId(),
    title: `${tabs[idx].title} copy`,
    titlePinned: true,
  });
  const next = [...tabs.slice(0, idx + 1), copy, ...tabs.slice(idx + 1)];
  return { tabs: next.slice(0, MAX_TABS), activeId: copy.id };
}

// ---------------------------------------------------------------------------
// History
// ---------------------------------------------------------------------------

export function loadHistory(): QueryHistoryEntry[] {
  const stored = readJson<QueryHistoryEntry[]>(HISTORY_KEY);
  if (!Array.isArray(stored)) return [];
  return stored.slice(0, MAX_HISTORY);
}

/**
 * Prepend a run to history. Re-running an identical query against the same
 * connector replaces the previous entry instead of stacking duplicates, so
 * iterating on one query does not bury everything else.
 */
export function pushHistory(
  history: QueryHistoryEntry[],
  entry: Omit<QueryHistoryEntry, "id" | "at"> & { id?: string; at?: number },
): QueryHistoryEntry[] {
  const full: QueryHistoryEntry = {
    ...entry,
    query: String(entry.query ?? "").slice(0, MAX_QUERY_CHARS),
    id: entry.id ?? newId(),
    at: entry.at ?? Date.now(),
  };
  const deduped = history.filter(
    (h) => !(h.query.trim() === full.query.trim() && h.connectorId === full.connectorId),
  );
  return [full, ...deduped].slice(0, MAX_HISTORY);
}

export function saveHistory(history: QueryHistoryEntry[]) {
  writeJson(HISTORY_KEY, history.slice(0, MAX_HISTORY));
}

export function clearHistory() {
  try {
    localStorage.removeItem(HISTORY_KEY);
  } catch {
    /* ignore */
  }
}

/** Case-insensitive substring search over query text and connector label. */
export function filterHistory(
  history: QueryHistoryEntry[],
  term: string,
): QueryHistoryEntry[] {
  const t = term.trim().toLowerCase();
  if (!t) return history;
  return history.filter(
    (h) =>
      h.query.toLowerCase().includes(t) ||
      (h.connectorLabel ?? "").toLowerCase().includes(t),
  );
}

// ---------------------------------------------------------------------------
// Layout
// ---------------------------------------------------------------------------

export function loadLayout(): QueryLayout {
  const stored = readJson<Partial<QueryLayout>>(LAYOUT_KEY);
  if (!stored) return { ...DEFAULT_LAYOUT };
  return {
    schemaOpen: stored.schemaOpen ?? DEFAULT_LAYOUT.schemaOpen,
    historyOpen: stored.historyOpen ?? DEFAULT_LAYOUT.historyOpen,
    editorHeight: Math.min(
      900,
      Math.max(120, Number(stored.editorHeight) || DEFAULT_LAYOUT.editorHeight),
    ),
  };
}

export function saveLayout(layout: QueryLayout) {
  writeJson(LAYOUT_KEY, layout);
}

// ---------------------------------------------------------------------------
// Formatting helpers
// ---------------------------------------------------------------------------

/** Human-readable duration for the run badge. */
export function formatDuration(ms?: number): string {
  if (ms == null || !Number.isFinite(ms) || ms < 0) return "—";
  if (ms < 1) return "<1 ms";
  if (ms < 1000) return `${Math.round(ms)} ms`;
  if (ms < 60_000) return `${(ms / 1000).toFixed(2)} s`;
  const mins = Math.floor(ms / 60_000);
  const secs = Math.round((ms % 60_000) / 1000);
  return `${mins}m ${secs}s`;
}

/** Relative timestamp for history rows. */
export function formatRelativeTime(at: number, now = Date.now()): string {
  const delta = Math.max(0, now - at);
  if (delta < 60_000) return "just now";
  const mins = Math.floor(delta / 60_000);
  if (mins < 60) return `${mins}m ago`;
  const hours = Math.floor(mins / 60);
  if (hours < 24) return `${hours}h ago`;
  const days = Math.floor(hours / 24);
  if (days < 30) return `${days}d ago`;
  return new Date(at).toLocaleDateString();
}
