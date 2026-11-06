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
 * A platform administrator's views beside the workspaces (D218), named by the
 * API in ``GET /team/workspaces`` ``views``. They travel as ``X-Workspace-Id``
 * like a workspace id; the API accepts them from platform admins only.
 * ``__all__`` is every workspace; ``__unassigned__`` is the data that predates
 * workspaces, which no workspace entry could ever show.
 */
export const ALL_WORKSPACES_ID = "__all__";
export const UNASSIGNED_WORKSPACE_ID = "__unassigned__";

export type WorkspaceViewEntry = { id: string; name: string; kind?: string; legacy_jobs?: number | null };

/** Whether ``id`` is something the switcher can show: a listed workspace or view. */
export function isKnownWorkspaceChoice(
  id: string,
  rows: ReadonlyArray<{ id: string }>,
  views: ReadonlyArray<{ id: string }> = [],
): boolean {
  const want = (id || "").trim();
  return Boolean(want) && (rows.some((w) => w.id === want) || views.some((v) => v.id === want));
}

/**
 * The workspace to name when the browser has none (or a stale one): the API's
 * landing choice — last used, else the account's own (D218), or a platform
 * admin's All-workspaces view when it owns none — when it is listed, else the
 * first row. The list is oldest-first, so taking row 0 alone landed every
 * platform admin in the oldest workspace.
 */
export function landingWorkspaceId(
  rows: ReadonlyArray<{ id: string }>,
  preferred?: string,
  views: ReadonlyArray<{ id: string }> = [],
): string {
  const want = (preferred || "").trim();
  if (isKnownWorkspaceChoice(want, rows, views)) return want;
  return rows[0]?.id ?? views[0]?.id ?? "";
}

/**
 * The workspace a per-workspace screen (members, tenant, posture) reads: the
 * active id, or ``""`` while a platform admin's view is active — a view is
 * not a workspace, so those screens say "choose a workspace" instead of
 * asking the API about ``__all__``.
 */
export function concreteWorkspaceId(id: string): string {
  const want = (id || "").trim();
  return want === ALL_WORKSPACES_ID || want === UNASSIGNED_WORKSPACE_ID ? "" : want;
}

/** One switcher option. Views come first, so "All workspaces" heads the list. */
export type WorkspaceChoice = { id: string; label: string; view: boolean };

export function workspaceChoices(
  rows: ReadonlyArray<{ id: string; name: string; member_count?: number }>,
  views: ReadonlyArray<WorkspaceViewEntry> = [],
): WorkspaceChoice[] {
  const viewChoices = views.map((v) => ({
    id: v.id,
    label:
      v.id === UNASSIGNED_WORKSPACE_ID && typeof v.legacy_jobs === "number"
        ? `${v.name} · ${v.legacy_jobs} job${v.legacy_jobs === 1 ? "" : "s"}`
        : v.name,
    view: true,
  }));
  const workspaceRows = rows.map((w) => ({
    id: w.id,
    label: `${w.name}${typeof w.member_count === "number" ? ` · ${w.member_count}` : ""}`,
    view: false,
  }));
  return [...viewChoices, ...workspaceRows];
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
