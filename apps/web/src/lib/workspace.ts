/**
 * The workspace the browser is looking at, named on every request.
 *
 * The API resolves authority per workspace: the same account can be an editor in
 * one and a viewer in another. A client that never says which workspace it is
 * viewing forces the API to answer from the platform label alone, which is how a
 * workspace editor ended up gated as a viewer. Every request carries this id as
 * ``X-Workspace-Id`` once one is chosen.
 */

import { useEffect, useState } from "react";

const WORKSPACE_KEY = "df2.workspace";

/** Fired when the active workspace changes — permission state must be re-read. */
export const WORKSPACE_CHANGED_EVENT = "df2:workspace-changed";

/**
 * Fired when the workspace directory changes (name, membership count, new row)
 * without switching the active id. Listeners should refetch the list, not
 * rehydrate connectors and jobs.
 */
export const WORKSPACE_DIRECTORY_EVENT = "df2:workspace-directory";

/** Fired after an enterprise tenant is saved, including its egress allowlist. */
export const TENANT_SAVED_EVENT = "df2:tenant-saved";

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

/**
 * The workspace to name when the browser has none (or a stale one): the API's
 * landing choice — last used, else the account's own (D218) — when it is in
 * the list, else the first row. The list is oldest-first, so taking row 0
 * alone landed every platform admin in the oldest workspace.
 */
export function landingWorkspaceId(rows: ReadonlyArray<{ id: string }>, preferred?: string): string {
  const want = (preferred || "").trim();
  if (want && rows.some((w) => w.id === want)) return want;
  return rows[0]?.id ?? "";
}

export function clearActiveWorkspaceId(): void {
  setActiveWorkspaceId("");
}

/** Tell chrome the workspace list (names, counts) changed. */
export function notifyWorkspaceDirectory(): void {
  if (typeof window === "undefined") return;
  window.dispatchEvent(new Event(WORKSPACE_DIRECTORY_EVENT));
}

/** Subscribe to the active workspace id. Re-renders when the switcher moves. */
export function useActiveWorkspaceId(): string {
  const [id, setId] = useState(getActiveWorkspaceId);
  useEffect(() => {
    const sync = () => setId(getActiveWorkspaceId());
    window.addEventListener(WORKSPACE_CHANGED_EVENT, sync);
    return () => window.removeEventListener(WORKSPACE_CHANGED_EVENT, sync);
  }, []);
  return id;
}
