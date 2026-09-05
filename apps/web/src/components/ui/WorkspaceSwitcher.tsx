import { useCallback, useEffect, useState } from "react";

import { fetchWorkspaces, type Workspace } from "../../lib/api";
import {
  WORKSPACE_CHANGED_EVENT,
  WORKSPACE_DIRECTORY_EVENT,
  getActiveWorkspaceId,
  setActiveWorkspaceId,
  useActiveWorkspaceId,
} from "../../lib/workspace";

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
  const [loadError, setLoadError] = useState("");

  const reload = useCallback(() => {
    fetchWorkspaces()
      .then(({ workspaces: rows }) => {
        setWorkspaces(rows);
        setLoadError("");
        const current = getActiveWorkspaceId();
        if (rows.length === 0) return;
        if (!current || !rows.some((w) => w.id === current)) {
          setActiveWorkspaceId(rows[0].id);
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

  const active = workspaces.find((w) => w.id === activeId);

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
      {workspaces.length <= 1 ? (
        <strong className="df2-workspace-switcher-name" data-testid="workspace-switcher-name">
          {active?.name || (loadError ? "Workspace unavailable" : "Workspace")}
        </strong>
      ) : (
        <select
          id={id}
          className="df2-workspace-switcher-select"
          data-testid="workspace-switcher"
          value={activeId}
          title={loadError || undefined}
          onChange={(e) => setActiveWorkspaceId(e.target.value)}
        >
          {workspaces.map((w) => (
            <option key={w.id} value={w.id}>
              {w.name}
              {typeof w.member_count === "number" ? ` · ${w.member_count}` : ""}
            </option>
          ))}
        </select>
      )}
    </div>
  );
}
