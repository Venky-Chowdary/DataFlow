/**
 * The workspace's connector list as the shell loads it (D410).
 *
 * Several reads of GET /connectors/saved overlap: boot, the workspace being
 * named after first paint, the 60 s poll, "connectors changed" events. The
 * shell used to keep only the newest request's answer. When that one failed
 * (aborted, timed out), an older successful answer had already been dropped,
 * the list stayed [], and the page drew "No connections yet" with the
 * connectors still there. A failed list was also returned as [].
 *
 * Rules:
 * - every request is tagged with the workspace it was made for; an answer
 *   for another workspace is never shown (workspace switch mid-load);
 * - a successful answer is kept unless a newer successful answer for the
 *   same workspace already is (stale answers never overwrite fresh ones);
 * - a failure never replaces rows; with nothing loaded it is an error state
 *   with its reason, once no other request for the workspace can still answer;
 * - the empty state needs a successful empty answer.
 */

export interface ConnectorListState<T> {
  workspaceId: string;
  rows: T[];
  /** A successful answer for this workspace has been applied. */
  loaded: boolean;
  /** Generation of the applied answer (-1 before any). */
  appliedGen: number;
  /** Generations still in flight for this workspace. */
  pending: number[];
  error: string;
}

export type ConnectorListView<T> =
  | { kind: "loading" }
  | { kind: "list"; rows: T[] }
  | { kind: "empty" }
  | { kind: "error"; message: string };

interface Tag {
  gen: number;
  workspaceId: string;
}

export function initialConnectorList<T>(): ConnectorListState<T> {
  return { workspaceId: "", rows: [], loaded: false, appliedGen: -1, pending: [], error: "" };
}

export function connectorListStarted<T>(state: ConnectorListState<T>, { gen, workspaceId }: Tag): ConnectorListState<T> {
  if (workspaceId !== state.workspaceId) {
    return { ...initialConnectorList<T>(), workspaceId, pending: [gen] };
  }
  return { ...state, pending: [...state.pending, gen], error: "" };
}

export function connectorListLoaded<T>(
  state: ConnectorListState<T>,
  { gen, workspaceId, rows }: Tag & { rows: T[] },
): ConnectorListState<T> {
  if (workspaceId !== state.workspaceId) return state;
  const pending = state.pending.filter((g) => g !== gen);
  if (gen < state.appliedGen) return { ...state, pending };
  return { ...state, rows, loaded: true, appliedGen: gen, pending, error: "" };
}

/** The reason the Connectors page gives when the list could not be read. */
export function connectorListErrorText(message: string): string {
  const text = (message || "").trim();
  if (/authentication required|sign in|401/i.test(text)) return "Your session has ended. Sign in again to see your connections";
  if (/timed out|abort/i.test(text)) return "The request for your connections timed out";
  return text.replace(/[.\s]+$/, "") || "The request for your connections failed";
}

export function connectorListFailed<T>(
  state: ConnectorListState<T>,
  { gen, workspaceId, message }: Tag & { message: string },
): ConnectorListState<T> {
  if (workspaceId !== state.workspaceId) return state;
  const pending = state.pending.filter((g) => g !== gen);
  return { ...state, pending, error: state.loaded ? "" : connectorListErrorText(message) };
}

/** Patch one loaded row (a probe result) without touching the load state. */
export function connectorListPatched<T extends { id: string }>(
  state: ConnectorListState<T>,
  id: string,
  patch: Partial<T>,
): ConnectorListState<T> {
  return { ...state, rows: state.rows.map((row) => (row.id === id ? { ...row, ...patch } : row)) };
}

export function connectorListView<T>(state: ConnectorListState<T>): ConnectorListView<T> {
  if (state.loaded) return state.rows.length ? { kind: "list", rows: state.rows } : { kind: "empty" };
  if (state.pending.length || !state.error) return { kind: "loading" };
  return { kind: "error", message: state.error };
}

/**
 * The props every page that shows connectors receives (D410): loading while a
 * read may answer, the reason when every read failed, and how to read again.
 * Only a successful answer leaves both unset, so only then may a page say the
 * workspace has no connections.
 */
export function connectorListPageState<T>(
  view: ConnectorListView<T>,
  onRetryConnectors?: () => void,
): { connectorsLoading: boolean; connectorsError: string; onRetryConnectors?: () => void } {
  return {
    connectorsLoading: view.kind === "loading",
    connectorsError: view.kind === "error" ? view.message : "",
    onRetryConnectors,
  };
}
