/**
 * The workspace the browser is looking at, named on every request.
 *
 * The API resolves authority per workspace: the same account can be an editor in
 * one and a viewer in another. A client that never says which workspace it is
 * viewing forces the API to answer from the platform label alone, which is how a
 * workspace editor ended up gated as a viewer. Every request carries this id as
 * ``X-Workspace-Id`` once one is chosen.
 */

const WORKSPACE_KEY = "df2.workspace";

/** Fired when the active workspace changes — permission state must be re-read. */
export const WORKSPACE_CHANGED_EVENT = "df2:workspace-changed";

export function getActiveWorkspaceId(): string {
  try {
    return (localStorage.getItem(WORKSPACE_KEY) || "").trim();
  } catch {
    return "";
  }
}

export function setActiveWorkspaceId(workspaceId: string): void {
  const next = (workspaceId || "").trim();
  if (next === getActiveWorkspaceId()) return;
  try {
    if (next) localStorage.setItem(WORKSPACE_KEY, next);
    else localStorage.removeItem(WORKSPACE_KEY);
  } catch {
    /* private browsing — the header is simply omitted */
  }
  if (typeof window !== "undefined") {
    window.dispatchEvent(new CustomEvent(WORKSPACE_CHANGED_EVENT, { detail: { workspaceId: next } }));
  }
}

export function clearActiveWorkspaceId(): void {
  setActiveWorkspaceId("");
}
