import { useEffect, useState, type FormEvent } from "react";
import { useToast } from "../../components/Toast";
import { PermissionNotice } from "../../components/PermissionNotice";
import { Button } from "../../components/ui/Button";
import { useConfirm } from "../../components/ui/ConfirmDialog";
import { EmptyState } from "../../components/ui/EmptyState";
import { SectionLoader } from "../../components/LoadingState";
import {
  createServiceAccount,
  fetchServiceAccounts,
  revokeServiceAccount,
  rotateApiKey,
  type ServiceAccount,
  type WorkspaceApiKeyLifetime,
  type WorkspaceApiKeyRole,
} from "../../lib/api";
import { API_KEY_LIFETIME_OPTIONS, scopeOptions as getScopeOptions } from "../../lib/iamAdmin";
import { PERMISSIONS, useWriteGate, usePermissions } from "../../lib/PermissionsContext";
import { ROTATION_OVERLAPS, ServiceAccountList } from "./ServiceAccountList";

export function ServiceAccountsPanel() {
  const manage = useWriteGate(PERMISSIONS.iamManage);
  const { identity } = usePermissions();
  const { toast } = useToast();
  const { confirm } = useConfirm();
  const [accounts, setAccounts] = useState<ServiceAccount[]>([]);
  const [loading, setLoading] = useState(true);
  const [loadError, setLoadError] = useState("");
  const [name, setName] = useState("");
  const [role, setRole] = useState<WorkspaceApiKeyRole>("viewer");
  const [expiresIn, setExpiresIn] = useState<WorkspaceApiKeyLifetime>("90d");
  const [scopes, setScopes] = useState<string[]>([]);
  const [saving, setSaving] = useState(false);
  const [formError, setFormError] = useState("");
  const [revealedSecret, setRevealedSecret] = useState("");
  const [actionError, setActionError] = useState("");
  const [activeAction, setActiveAction] = useState("");
  const availableScopes = getScopeOptions(identity?.permissions ?? []);

  const loadAccounts = async () => {
    setLoading(true);
    setLoadError("");
    try {
      setAccounts(await fetchServiceAccounts());
    } catch (error) {
      setLoadError(error instanceof Error ? error.message : String(error));
    } finally {
      setLoading(false);
    }
  };

  useEffect(() => {
    if (!manage.allowed) {
      setLoading(false);
      return;
    }
    void loadAccounts();
  }, [manage.allowed]);

  const createAccount = async (event: FormEvent) => {
    event.preventDefault();
    if (!name.trim()) {
      setFormError("Enter a service account name.");
      return;
    }
    setSaving(true);
    setFormError("");
    try {
      const created = await createServiceAccount({
        name: name.trim(),
        role,
        scopes,
        expires_in: expiresIn,
      });
      setRevealedSecret(created.key);
      setName("");
      setScopes([]);
      await loadAccounts();
      toast({ title: "Service account created", tone: "success" });
    } catch (error) {
      setFormError(error instanceof Error ? error.message : String(error));
    } finally {
      setSaving(false);
    }
  };

  const rotateAccount = async (account: ServiceAccount, overlapSeconds: number) => {
    const overlapLabel = ROTATION_OVERLAPS.find((option) => option.seconds === overlapSeconds)?.label;
    if (!await confirm({
      title: "Rotate service-account key?",
      message: `Rotate the key for ${account.name}. The existing key will remain valid for ${overlapLabel ?? "the selected overlap"}.`,
      confirmLabel: "Rotate key",
      tone: "warning",
    })) return;
    setActiveAction(account.id);
    setActionError("");
    try {
      const rotated = await rotateApiKey(account.id, overlapSeconds);
      setRevealedSecret(rotated.key);
      await loadAccounts();
      toast({ title: "API key rotated", tone: "success" });
    } catch (error) {
      setActionError(error instanceof Error ? error.message : String(error));
    } finally {
      setActiveAction("");
    }
  };

  const revokeAccount = async (account: ServiceAccount) => {
    if (!await confirm({
      title: "Revoke service account?",
      message: `This immediately revokes ${account.name} and cannot be undone.`,
      confirmLabel: "Revoke",
      tone: "danger",
    })) return;
    setActiveAction(account.id);
    setActionError("");
    try {
      await revokeServiceAccount(account.id);
      await loadAccounts();
      toast({ title: "Service account revoked", tone: "success" });
    } catch (error) {
      setActionError(error instanceof Error ? error.message : String(error));
    } finally {
      setActiveAction("");
    }
  };

  return (
    <section className="df2-settings-section" data-testid="service-accounts-panel">
      <div className="df2-settings-section-head">
        <div>
          <h2>Service accounts</h2>
          <p>Manage scoped service credentials and SCIM provisioning tokens.</p>
        </div>
      </div>
      <PermissionNotice
        allowed={manage.allowed}
        reason={manage.reason}
        what="Service accounts and SCIM credentials require iam.manage."
      />
      {manage.allowed && (
        <div className="df2-settings-section-body">
          <form className="df2-iam-create-form" data-testid="service-account-create-form" onSubmit={(event) => void createAccount(event)}>
            <label className="df2-settings-field">
              <span>Name</span>
              <input data-testid="service-account-name" className="df2-input" value={name} onChange={(event) => setName(event.target.value)} />
            </label>
            <label className="df2-settings-field">
              <span>Role</span>
              <select data-testid="service-account-role" className="df2-select" value={role} onChange={(event) => setRole(event.target.value as WorkspaceApiKeyRole)}>
                <option value="viewer">Viewer</option>
                <option value="operator">Operator</option>
                <option value="editor">Editor</option>
                <option value="admin">Admin</option>
              </select>
            </label>
            <label className="df2-settings-field">
              <span>Expiration</span>
              <select data-testid="service-account-expires" className="df2-select" value={expiresIn} onChange={(event) => setExpiresIn(event.target.value as WorkspaceApiKeyLifetime)}>
                {API_KEY_LIFETIME_OPTIONS.map((option) => (
                  <option key={option.value} value={option.value}>{option.label}</option>
                ))}
              </select>
            </label>
            <Button type="submit" variant="primary" disabled={saving} data-testid="service-account-create">
              {saving ? "Creating…" : "Create service account"}
            </Button>
          </form>
          <fieldset className="df2-iam-scope-grid" data-testid="service-account-scopes">
            <legend>Scopes held by your identity</legend>
            {availableScopes.map((scope) => (
              <label key={scope} className="df2-iam-scope-option">
                <input
                  type="checkbox"
                  checked={scopes.includes(scope)}
                  onChange={(event) => setScopes((current) => event.target.checked
                    ? [...current, scope]
                    : current.filter((value) => value !== scope))}
                />
                <span>{scope}</span>
              </label>
            ))}
          </fieldset>
          {formError && <p className="df2-permission-notice" role="alert">{formError}</p>}
          {revealedSecret && (
            <div className="df2-alert df2-alert-success df2-alert-banner df2-mb-md" role="status" data-testid="service-account-secret">
              <div className="df2-alert-banner-body">
                <strong>Copy your new secret</strong>
                <p className="df2-settings-hint">The plaintext secret is shown once. Store it securely.</p>
                <p className="df2-settings-key-reveal"><code>{revealedSecret}</code></p>
              </div>
              <div className="df2-alert-banner-actions">
                <Button size="sm" variant="primary" onClick={() => {
                  void navigator.clipboard.writeText(revealedSecret);
                  toast({ title: "Copied to clipboard", tone: "success" });
                }}>Copy</Button>
                <Button size="sm" variant="ghost" onClick={() => setRevealedSecret("")}>Dismiss</Button>
              </div>
            </div>
          )}
          {loadError && (
            <div className="df2-permission-notice" role="alert" data-testid="service-accounts-load-error">
              Service accounts could not be listed. {loadError} This is not the same as having none.
            </div>
          )}
          {actionError && <div className="df2-permission-notice" role="alert">{actionError}</div>}
          {loading ? (
            <SectionLoader title="Loading service accounts" hint="Fetching IAM credentials…" />
          ) : loadError ? null : (
            <div aria-busy={Boolean(activeAction)}>
              {accounts.length === 0 ? (
                <div data-testid="service-account-list">
                  <EmptyState
                    compact
                    icon="key"
                    title="No service accounts"
                    description="Create a service account to grant scoped programmatic access."
                  />
                </div>
              ) : (
                <ServiceAccountList
                  accounts={accounts}
                  busyId={activeAction}
                  onRotate={(account, overlapSeconds) => void rotateAccount(account, overlapSeconds)}
                  onRevoke={(account) => void revokeAccount(account)}
                />
              )}
            </div>
          )}
        </div>
      )}
    </section>
  );
}
