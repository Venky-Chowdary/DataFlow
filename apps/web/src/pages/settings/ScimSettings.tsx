import { useEffect, useState, type FormEvent } from "react";
import { useToast } from "../../components/Toast";
import { PermissionNotice } from "../../components/PermissionNotice";
import { Button } from "../../components/ui/Button";
import { EmptyState } from "../../components/ui/EmptyState";
import { SectionLoader } from "../../components/LoadingState";
import {
  createScimToken,
  fetchScimGroupMappings,
  fetchWorkspaces,
  saveScimGroupMappings,
  type WorkspaceApiKeyLifetime,
} from "../../lib/api";
import {
  API_KEY_LIFETIME_OPTIONS,
  buildScimGroupMappingsPayload,
  type ScimGroupMappingRow,
} from "../../lib/iamAdmin";
import { PERMISSIONS, useWriteGate } from "../../lib/PermissionsContext";
import {
  ScimGroupMappingRows,
  type ScimGroupMappingField,
  type WorkspaceOption,
} from "./ScimGroupMappingRows";

export function ScimSettings({ onOpenTeamSettings }: { onOpenTeamSettings?: () => void }) {
  const manage = useWriteGate(PERMISSIONS.iamManage);
  const { toast } = useToast();
  const [loading, setLoading] = useState(true);
  const [mappingLoadError, setMappingLoadError] = useState("");
  const [workspaceLoadError, setWorkspaceLoadError] = useState("");
  const [rows, setRows] = useState<ScimGroupMappingRow[]>([]);
  const [workspaces, setWorkspaces] = useState<WorkspaceOption[]>([]);
  const [tokenName, setTokenName] = useState("");
  const [tokenExpiry, setTokenExpiry] = useState<WorkspaceApiKeyLifetime>("90d");
  const [tokenLoading, setTokenLoading] = useState(false);
  const [tokenError, setTokenError] = useState("");
  const [revealedToken, setRevealedToken] = useState("");
  const [mappingError, setMappingError] = useState("");
  const [saving, setSaving] = useState(false);

  useEffect(() => {
    if (!manage.allowed) {
      setLoading(false);
      return;
    }
    setLoading(true);
    void Promise.allSettled([fetchScimGroupMappings(), fetchWorkspaces()])
      .then(([mappingResult, workspaceResult]) => {
        if (mappingResult.status === "fulfilled") {
          setRows(Object.entries(mappingResult.value.mappings ?? {}).map(([group, mapping]) => ({
            group,
            workspace_id: mapping.workspace_id,
            role: mapping.role,
          })));
          setMappingLoadError("");
        } else {
          setMappingLoadError(mappingResult.reason instanceof Error
            ? mappingResult.reason.message
            : String(mappingResult.reason));
        }
        if (workspaceResult.status === "fulfilled") {
          setWorkspaces(workspaceResult.value.workspaces);
          setWorkspaceLoadError("");
        } else {
          setWorkspaceLoadError(workspaceResult.reason instanceof Error
            ? workspaceResult.reason.message
            : String(workspaceResult.reason));
        }
      })
      .finally(() => setLoading(false));
  }, [manage.allowed]);

  const generateToken = async (event: FormEvent) => {
    event.preventDefault();
    if (!tokenName.trim()) {
      setTokenError("Enter a token name.");
      return;
    }
    setTokenLoading(true);
    setTokenError("");
    try {
      const created = await createScimToken({ name: tokenName.trim(), expires_in: tokenExpiry });
      setRevealedToken(created.key);
      setTokenName("");
      toast({ title: "SCIM token created", tone: "success" });
    } catch (error) {
      setTokenError(error instanceof Error ? error.message : String(error));
    } finally {
      setTokenLoading(false);
    }
  };

  const updateRow = (index: number, field: ScimGroupMappingField, value: string) => {
    setRows((current) => current.map((row, rowIndex) => rowIndex === index ? { ...row, [field]: value } : row));
    setMappingError("");
  };

  const saveMappings = async () => {
    const result = buildScimGroupMappingsPayload(rows);
    if (!result.ok) {
      setMappingError(result.error);
      return;
    }
    setSaving(true);
    setMappingError("");
    try {
      const saved = await saveScimGroupMappings(result.mappings);
      setRows(Object.entries(saved.mappings ?? result.mappings).map(([group, mapping]) => ({
        group,
        workspace_id: mapping.workspace_id,
        role: mapping.role,
      })));
      toast({ title: "SCIM group mappings saved", tone: "success" });
    } catch (error) {
      setMappingError(error instanceof Error ? error.message : String(error));
    } finally {
      setSaving(false);
    }
  };

  return (
    <section className="df2-settings-section" data-testid="scim-settings">
      <div className="df2-settings-section-head">
        <div>
          <h2>SCIM provisioning</h2>
          <p>Provision workspace membership from your identity provider.</p>
        </div>
      </div>
      <PermissionNotice
        allowed={manage.allowed}
        reason={manage.reason}
        what="SCIM provisioning requires iam.manage."
      />
      {manage.allowed && (
        <div className="df2-settings-section-body">
          <form className="df2-api-key-toolbar df2-scim-token-form" data-testid="scim-token-form" onSubmit={(event) => void generateToken(event)}>
            <label className="df2-settings-field">
              <span>Token name</span>
              <input data-testid="scim-token-name" className="df2-input" value={tokenName} onChange={(event) => setTokenName(event.target.value)} />
            </label>
            <label className="df2-settings-field">
              <span>Expiration</span>
              <select data-testid="scim-token-expires" className="df2-select" value={tokenExpiry} onChange={(event) => setTokenExpiry(event.target.value as WorkspaceApiKeyLifetime)}>
                {API_KEY_LIFETIME_OPTIONS.map((option) => (
                  <option key={option.value} value={option.value}>{option.label}</option>
                ))}
              </select>
            </label>
            <Button type="submit" variant="primary" disabled={tokenLoading} data-testid="scim-token-generate">
              {tokenLoading ? "Creating…" : "Generate SCIM token"}
            </Button>
          </form>
          {tokenError && <p className="df2-permission-notice" role="alert">{tokenError}</p>}
          {revealedToken && (
            <div className="df2-alert df2-alert-success df2-alert-banner df2-mb-md" role="status" data-testid="scim-token-secret">
              <div className="df2-alert-banner-body">
                <strong>Copy your SCIM token</strong>
                <p className="df2-settings-hint">The plaintext token is shown once. Store it securely.</p>
                <p className="df2-settings-key-reveal"><code>{revealedToken}</code></p>
              </div>
              <div className="df2-alert-banner-actions">
                <Button size="sm" variant="primary" onClick={() => {
                  void navigator.clipboard.writeText(revealedToken);
                  toast({ title: "Copied to clipboard", tone: "success" });
                }}>Copy</Button>
                <Button size="sm" variant="ghost" onClick={() => setRevealedToken("")}>Dismiss</Button>
              </div>
            </div>
          )}
          <div className="df2-settings-section-head df2-mt-md">
            <div>
              <h3>Group mappings</h3>
              <p>Map each IdP group to a workspace and supported workspace role.</p>
            </div>
          </div>
          {mappingLoadError && (
            <div className="df2-permission-notice" role="alert" data-testid="scim-mappings-load-error">
              SCIM group mappings could not be listed. {mappingLoadError} This is not the same as having no mappings.
            </div>
          )}
          {workspaceLoadError && (
            <div className="df2-permission-notice" role="alert" data-testid="scim-workspaces-load-error">
              Workspaces could not be listed. {workspaceLoadError}
            </div>
          )}
          {loading ? (
            <SectionLoader title="Loading SCIM settings" hint="Fetching mappings and workspaces…" />
          ) : !mappingLoadError && (
            <>
              {!workspaceLoadError && workspaces.length === 0 && (
                <div className="df2-scim-no-workspaces" data-testid="scim-workspaces-empty">
                  <p>No workspaces are available for group mappings.</p>
                  <Button variant="secondary" size="sm" onClick={onOpenTeamSettings}>
                    Open Team settings to create a workspace
                  </Button>
                </div>
              )}
              {mappingError && <p className="df2-permission-notice" role="alert" data-testid="scim-group-mapping-error">{mappingError}</p>}
              {rows.length === 0 ? (
                <EmptyState
                  compact
                  icon="connectors"
                  title="No group mappings"
                  description="Add an IdP group to map it to a workspace role."
                />
              ) : (
                <ScimGroupMappingRows
                  rows={rows}
                  workspaces={workspaces}
                  error=""
                  onChange={updateRow}
                  onRemove={(index) => {
                    setRows((current) => current.filter((_row, rowIndex) => rowIndex !== index));
                    setMappingError("");
                  }}
                />
              )}
              <div className="df2-settings-section-actions">
                <Button variant="secondary" data-testid="scim-mapping-add" disabled={saving || workspaces.length === 0} onClick={() => {
                  setRows((current) => [...current, { group: "", workspace_id: "", role: "viewer" }]);
                  setMappingError("");
                }}>Add mapping</Button>
                <Button variant="primary" data-testid="scim-mapping-save" disabled={saving || Boolean(workspaceLoadError) || workspaces.length === 0} onClick={() => void saveMappings()}>
                  {saving ? "Saving…" : "Save mappings"}
                </Button>
              </div>
            </>
          )}
          <p className="df2-settings-hint">SCIM setup guidance: docs/SCIM.md</p>
        </div>
      )}
    </section>
  );
}
