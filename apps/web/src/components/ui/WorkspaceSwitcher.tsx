import { useCallback, useEffect, useState } from "react";

import { fetchWorkspaces, type Workspace, type WorkspaceView } from "../../lib/api";
import {
  WORKSPACE_CHANGED_EVENT,
  WORKSPACE_DIRECTORY_EVENT,
  getActiveWorkspaceId,
  isKnownWorkspaceChoice,
  landingWorkspaceId,
  setActiveWorkspaceId,
  useActiveWorkspaceId,
  workspaceChoices,
} from "../../lib/workspace";
import { CREATE_WORKSPACE_CHOICE, WORKSPACE_CREATE_REQUEST_EVENT } from "../../lib/firstRunWorkspace";

/**
 * Canonical workspace picker. Changing it names `X-Workspace-Id` for every
 * later request and fires WORKSPACE_CHANGED_EVENT so Settings, permissions,
 * and job lists re-read the live workspace.
 */
export function WorkspaceSwitcher({
  id = "df2-workspace-switcher",
  className = "",
  showLabel = true,
}: {
  id?: string;
  className?: string;
  showLabel?: boolean;
}) {
  const activeId = useActiveWorkspaceId();
  const [workspaces, setWorkspaces] = useState<Workspace[]>([]);
  const [views, setViews] = useState<WorkspaceView[]>([]);
  const [loadError, setLoadError] = useState("");
  const [canCreate, setCanCreate] = useState(false);

  const reload = useCallback(() => {
    fetchWorkspaces()
      .then(({ workspaces: rows, defaultWorkspaceId, views: adminViews, platformAdmin }) => {
        setWorkspaces(rows);
        setViews(adminViews);
        // Only platform administrators may create a workspace (POST /team/workspaces).
        setCanCreate(platformAdmin);
        setLoadError("");
        const current = getActiveWorkspaceId();
        if (rows.length === 0 && adminViews.length === 0) return;
        if (!isKnownWorkspaceChoice(current, rows, adminViews)) {
          setActiveWorkspaceId(landingWorkspaceId(rows, defaultWorkspaceId, adminViews));
        }
      })
      .catch((err: unknown) => {
        setLoadError(err instanceof Error ? err.message : "Could not read workspaces.");
      });
  }, []);

  useEffect(() => {
    reload();
  }, [reload]);

  useEffect(() => {
    const onChange = () => reload();
    window.addEventListener(WORKSPACE_CHANGED_EVENT, onChange);
    window.addEventListener(WORKSPACE_DIRECTORY_EVENT, onChange);
    return () => {
      window.removeEventListener(WORKSPACE_CHANGED_EVENT, onChange);
      window.removeEventListener(WORKSPACE_DIRECTORY_EVENT, onChange);
    };
  }, [reload]);

  const choices = workspaceChoices(workspaces, views);
  const active = choices.find((c) => c.id === activeId);
  const activeName = views.find((v) => v.id === activeId)?.name ?? workspaces.find((w) => w.id === activeId)?.name;

  return (
    <div className={`df2-workspace-switcher ${className}`.trim()}>
      {showLabel ? (
        <label className="df2-workspace-switcher-label" htmlFor={id}>
          Workspace
        </label>
      ) : (
        <label className="df2-sr-only" htmlFor={id}>
          Active workspace
        </label>
      )}
      {choices.length <= 1 && !canCreate ? (
        <strong className="df2-workspace-switcher-name" data-testid="workspace-switcher-name">
          {activeName || (loadError ? "Workspace unavailable" : "Workspace")}
        </strong>
      ) : (
        <select
          id={id}
          className="df2-workspace-switcher-select"
          data-testid="workspace-switcher"
          value={active ? activeId : ""}
          title={loadError || undefined}
          onChange={(e) => {
            if (e.target.value === CREATE_WORKSPACE_CHOICE) {
              // Not a workspace: open the create dialog and keep the current choice.
              e.target.value = active ? activeId : "";
              window.dispatchEvent(new Event(WORKSPACE_CREATE_REQUEST_EVENT));
              return;
            }
            setActiveWorkspaceId(e.target.value);
          }}
        >
          {active ? null : (
            <option value="" disabled>
              Choose a workspace
            </option>
          )}
          {choices.map((c) => (
            <option key={c.id} value={c.id}>
              {c.label}
            </option>
          ))}
          {canCreate && (
            <option value={CREATE_WORKSPACE_CHOICE} data-testid="workspace-switcher-create">
              Create workspace…
            </option>
          )}
        </select>
      )}
    </div>
  );
}
