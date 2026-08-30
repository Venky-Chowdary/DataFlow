/**
 * When to start the first Overview / Jobs / Connectors read.
 *
 * Permissions names the workspace *after* first paint. A boot fetch that
 * runs before ``X-Workspace-Id`` is set counts only unscoped (legacy) jobs —
 * Overview then shows a low total until a hard refresh, when localStorage
 * already has the id. Wait for a named workspace; fall back so a tenant
 * with none still hydrates.
 */

export const WORKSPACE_HYDRATE_FALLBACK_MS = 1500;

/** True once the browser has named a workspace, or the fallback elapsed. */
export function shouldStartWorkspaceHydrate(
  workspaceId: string,
  elapsedMs: number,
  fallbackMs = WORKSPACE_HYDRATE_FALLBACK_MS,
): boolean {
  return Boolean((workspaceId || "").trim()) || elapsedMs >= fallbackMs;
}

/** Drop a slower unscoped response that finishes after a scoped reload. */
export function isStaleGeneration(mine: number, latest: number): boolean {
  return mine !== latest;
}
