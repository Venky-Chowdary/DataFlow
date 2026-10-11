import {
  SCIM_GROUP_MAPPING_ROLES,
  type ScimGroupMappingRow,
} from "../../lib/iamAdmin";

export type WorkspaceOption = { id: string; name: string };
export type ScimGroupMappingField = keyof ScimGroupMappingRow;

export function ScimGroupMappingRows({
  rows,
  workspaces,
  error,
  onChange,
  onRemove,
}: {
  rows: ScimGroupMappingRow[];
  workspaces: WorkspaceOption[];
  error: string;
  onChange: (index: number, field: ScimGroupMappingField, value: string) => void;
  onRemove: (index: number) => void;
}) {
  return (
    <div className="df2-settings-section-body">
      {error && <p className="df2-permission-notice" role="alert" data-testid="scim-group-mapping-error">{error}</p>}
      <div className="df2-scim-mapping-list" data-testid="scim-group-mapping-rows">
        {rows.map((row, index) => (
          <div className="df2-scim-mapping-row" key={`${index}-${row.group}`}>
            <label className="df2-settings-field" htmlFor={`scim-group-${index}`}>
              <span>IdP group name</span>
              <input
                id={`scim-group-${index}`}
                data-testid={`scim-group-${index}`}
                className="df2-input"
                value={row.group}
                onChange={(event) => onChange(index, "group", event.target.value)}
              />
            </label>
            <label className="df2-settings-field" htmlFor={`scim-workspace-${index}`}>
              <span>Workspace</span>
              <select
                id={`scim-workspace-${index}`}
                data-testid={`scim-workspace-${index}`}
                className="df2-select"
                value={row.workspace_id}
                onChange={(event) => onChange(index, "workspace_id", event.target.value)}
              >
                <option value="">Select a workspace</option>
                {workspaces.map((workspace) => (
                  <option key={workspace.id} value={workspace.id}>{workspace.name}</option>
                ))}
              </select>
            </label>
            <label className="df2-settings-field" htmlFor={`scim-role-${index}`}>
              <span>Role</span>
              <select
                id={`scim-role-${index}`}
                data-testid={`scim-role-${index}`}
                className="df2-select"
                value={row.role}
                onChange={(event) => onChange(index, "role", event.target.value)}
              >
                {SCIM_GROUP_MAPPING_ROLES.map((role) => (
                  <option key={role} value={role}>{role[0].toUpperCase() + role.slice(1)}</option>
                ))}
              </select>
            </label>
            <button
              type="button"
              className="df2-btn df2-btn-ghost df2-btn-sm df2-scim-mapping-remove"
              data-testid={`scim-mapping-remove-${index}`}
              onClick={() => onRemove(index)}
            >
              Remove
            </button>
          </div>
        ))}
      </div>
    </div>
  );
}
