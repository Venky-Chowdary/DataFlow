/**
 * First-run workspace (UX-1).
 *
 * Everything a user creates (connections, transfers, schedules) lands in a
 * workspace. A fresh install has none, and the API refused the first save with
 * the raw header name ("X-Workspace-Id required"). The shell now asks a
 * platform administrator to create the first workspace before anything else,
 * the switcher offers "Create workspace…", and the API's header-level refusals
 * are rewritten into words a person can act on.
 */

/** Fired by the switcher's "Create workspace…" option; the shell opens the create dialog. */
export const WORKSPACE_CREATE_REQUEST_EVENT = "df2:workspace-create-request";

/** The value the switcher's create option carries. Never sent to the API. */
export const CREATE_WORKSPACE_CHOICE = "__create__";

export type FirstRunState =
  /** Loaded, at least one workspace: nothing to do. */
  | "ready"
  /** No workspace and the account may create one: ask it to. */
  | "create"
  /** No workspace and no right to create one: say who can help. */
  | "ask-admin"
  /** The directory has not loaded (or failed): say nothing yet. */
  | "unknown";

export function firstRunState(directory: { workspaces: ReadonlyArray<unknown>; platformAdmin: boolean } | null): FirstRunState {
  if (!directory) return "unknown";
  if (directory.workspaces.length > 0) return "ready";
  return directory.platformAdmin ? "create" : "ask-admin";
}

/** Copy for the first-run surfaces, kept here so tests pin one wording. */
export const FIRST_RUN_COPY = {
  createTitle: "Create your workspace",
  createBody:
    "Connections, transfers, schedules and jobs are kept in a workspace. Name the first one to get started; you can invite your team and add more workspaces later.",
  createLabel: "Workspace name",
  createPlaceholder: "e.g. Contoso Data Platform",
  createAction: "Create workspace",
  bannerTitle: "No workspace yet",
  bannerBody: "Create a workspace before you add connections or start a transfer.",
  askAdminTitle: "You are not in a workspace yet",
  askAdminBody:
    "Ask a Datawrap administrator to add you to a workspace. You can look around, but nothing can be saved until you are a member.",
} as const;

/** Rewrite for the API's workspace-header refusals; "" when ``detail`` is not one. */
export function workspaceRefusalCopy(detail: string): string {
  const text = (detail || "").trim();
  if (/^X-Workspace-Id required$/i.test(text)) {
    return "Choose a workspace first. Everything you save is kept in a workspace: pick one in the workspace switcher at the top of the page, or create one there.";
  }
  if (/^No workspace membership$/i.test(text)) {
    return "You are not a member of any workspace yet. Ask a Datawrap administrator to add you to one.";
  }
  return "";
}

/** A workspace name the API will accept: trimmed, 1–128 characters. */
export function workspaceNameProblem(name: string): string {
  const trimmed = (name || "").trim();
  if (!trimmed) return "Enter a name for the workspace.";
  if (trimmed.length > 128) return "Use 128 characters or fewer.";
  return "";
}
