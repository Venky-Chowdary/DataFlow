import { useEffect, useMemo, useState } from "react";
import { useToast } from "../../components/Toast";
import { ByokKey, createByokKey, createTenant, fetchByokKeys, fetchSecurityPosture, fetchTenant, fetchWorkspaces, rotateByokKey, SecurityPosture, Tenant, updateTenant } from "../../lib/api";
import { PermissionNotice } from "../../components/PermissionNotice";
import { PERMISSIONS, useWriteGate } from "../../lib/PermissionsContext";
import { WORKSPACE_CHANGED_EVENT, getActiveWorkspaceId } from "../../lib/workspace";

const REGIONS = [
  "us-east-1", "us-east-2", "us-west-1", "us-west-2",
  "eu-west-1", "eu-west-2", "eu-central-1",
  "ap-southeast-1", "ap-south-1", "ap-northeast-1",
  "ca-central-1", "sa-east-1",
];

const PROVIDER_LABELS: Record<ByokKey["provider"], string> = {
  local: "Datawrap-managed (local)",
  wrapped: "Customer-supplied wrapped key",
  aws_kms: "AWS KMS",
  azure_keyvault: "Azure Key Vault",
  gcp_kms: "Google Cloud KMS",
};

export function TenantSettings() {
  const { toast } = useToast();
  // Tenant identity, data region, security posture and BYOK are administration.
  const manage = useWriteGate(PERMISSIONS.workspaceManage);
  const [loading, setLoading] = useState(true);
  const [loadError, setLoadError] = useState("");
  const [saving, setSaving] = useState(false);
  const [tenant, setTenant] = useState<Tenant | null>(null);
  const [posture, setPosture] = useState<SecurityPosture | null>(null);
  const [keys, setKeys] = useState<ByokKey[]>([]);
  const [workspaces, setWorkspaces] = useState<{ id: string; name: string }[]>([]);

  const [name, setName] = useState("");
  const [customDomain, setCustomDomain] = useState("");
  const [dataRegion, setDataRegion] = useState("us-east-1");
  const [securityContact, setSecurityContact] = useState("");
  const [mfaRequired, setMfaRequired] = useState(false);
  const [sessionTimeout, setSessionTimeout] = useState(8);
  const [ipAllowlist, setIpAllowlist] = useState("");
  const [workspaceId, setWorkspaceId] = useState("");

  const [switching, setSwitching] = useState(false);

  const [newKeyLabel, setNewKeyLabel] = useState("");
  const [newKeyProvider, setNewKeyProvider] = useState<ByokKey["provider"]>("local");
  const [newKeyMaterial, setNewKeyMaterial] = useState("");
  const [rotatingKeyId, setRotatingKeyId] = useState<string | null>(null);

  // The workspace being viewed, re-read when the switcher moves: mounting before
  // the shell had named one read the tenant of no workspace and reported a saved
  // tenant as missing.
  const [activeWorkspace, setActiveWorkspace] = useState(getActiveWorkspaceId());
  useEffect(() => {
    const onChanged = () => setActiveWorkspace(getActiveWorkspaceId());
    window.addEventListener(WORKSPACE_CHANGED_EVENT, onChanged);
    return () => window.removeEventListener(WORKSPACE_CHANGED_EVENT, onChanged);
  }, []);

  useEffect(() => {
    setLoading(true);
    // A refused read is not an unconfigured tenant: reporting "no tenant yet"
    // after a 403 would invite a viewer to create one and fail at the button.
    let readFailure = "";
    const remember = (err: unknown) => {
      if (!readFailure) readFailure = err instanceof Error ? err.message : "Could not read enterprise settings.";
      return null;
    };
    Promise.all([
      fetchTenant(activeWorkspace).catch(remember),
      fetchSecurityPosture(activeWorkspace).catch(remember),
      fetchByokKeys(activeWorkspace).catch((err) => {
        remember(err);
        return { keys: [] };
      }),
      fetchWorkspaces().catch((err) => {
        remember(err);
        return { workspaces: [] };
      }),
    ])
      .then(([t, p, k, wsResult]) => {
        setLoadError(readFailure);
        const ws = wsResult.workspaces;
        setTenant(t);
        setPosture(p);
        setKeys(k.keys ?? []);
        setWorkspaces(ws.map((w) => ({ id: w.id, name: w.name })));
        if (t) {
          setName(t.name);
          setCustomDomain(t.custom_domain);
          setDataRegion(t.data_region || "us-east-1");
          setSecurityContact(t.security_contact_email);
          setMfaRequired(t.mfa_required);
          setSessionTimeout(t.session_timeout_hours);
          setIpAllowlist((t.ip_allowlist || []).join("\n"));
          setWorkspaceId(t.workspace_id);
        } else {
          // The workspace this session is working in — the same one the read is
          // scoped to. Defaulting to the first workspace in the list saved a
          // tenant the operator could not then read back, because GET resolves
          // the tenant by the active workspace.
          const scoped = ws.find((w) => w.id === activeWorkspace)?.id || (ws.length === 1 ? ws[0].id : "");
          setWorkspaceId(scoped);
        }
      })
      .finally(() => setLoading(false));
  }, [activeWorkspace]);

  /** Read the named workspace's tenant instead of assuming it has none.
   *
   * Pointing the selector at another workspace used to change only which
   * workspace a *new* tenant would be written to, so a workspace that already
   * had one still read "No tenant configured" — and saving raised a conflict
   * the operator could not see the cause of.
   */
  const selectWorkspace = async (id: string) => {
    setWorkspaceId(id);
    if (!id) return;
    setSwitching(true);
    try {
      const t = await fetchTenant(id).catch(() => null);
      setTenant(t);
      // A read that answered is not a failed read: leaving the earlier message up
      // rendered "No tenant configured" above the tenant it had just loaded.
      setLoadError("");
      if (t) {
        setName(t.name);
        setCustomDomain(t.custom_domain);
        setDataRegion(t.data_region || "us-east-1");
        setSecurityContact(t.security_contact_email);
        setMfaRequired(t.mfa_required);
        setSessionTimeout(t.session_timeout_hours);
        setIpAllowlist((t.ip_allowlist || []).join("\n"));
      }
    } finally {
      setSwitching(false);
    }
  };

  const canSave = useMemo(() => {
    if (!tenant) return name.trim() && workspaceId;
    return true;
  }, [tenant, name, workspaceId]);

  const allowManage = () => {
    if (manage.allowed) return true;
    toast({ title: "No write permission", message: manage.reason, tone: "warning" });
    return false;
  };

  const save = async () => {
    if (!allowManage()) return;
    setSaving(true);
    try {
      const payload = {
        name: name.trim() || "Enterprise tenant",
        custom_domain: customDomain.trim(),
        data_region: dataRegion,
        byok_key_id: tenant?.byok_key_id || "",
        security_contact_email: securityContact.trim(),
        mfa_required: mfaRequired,
        session_timeout_hours: Math.max(1, Math.min(24, Number(sessionTimeout) || 8)),
        ip_allowlist: ipAllowlist.split("\n").map((s) => s.trim()).filter(Boolean),
      };
      let next: Tenant;
      if (tenant) {
        next = await updateTenant(tenant.id, payload);
      } else {
        next = await createTenant({ workspace_id: workspaceId, ...payload });
      }
      setTenant(next);
      const p = await fetchSecurityPosture().catch(() => null);
      if (p) setPosture(p);
      toast({ title: "Tenant saved", message: "Enterprise SaaS settings applied.", tone: "success" });
    } catch (err) {
      toast({ title: "Save failed", message: err instanceof Error ? err.message : "Could not save tenant settings", tone: "error" });
    } finally {
      setSaving(false);
    }
  };

  const addByokKey = async () => {
    if (!allowManage()) return;
    if (!tenant) {
      toast({ title: "Create tenant first", message: "Save the tenant profile before adding BYOK keys.", tone: "warning" });
      return;
    }
    try {
      const key = await createByokKey({
        label: newKeyLabel.trim() || `${PROVIDER_LABELS[newKeyProvider]} key`,
        provider: newKeyProvider,
        key_material: newKeyMaterial.trim(),
      });
      setKeys((prev) => [key, ...prev]);
      if (!tenant.byok_key_id) {
        const updated = await updateTenant(tenant.id, { byok_key_id: key.id });
        setTenant(updated);
      }
      setNewKeyLabel("");
      setNewKeyMaterial("");
      toast({ title: "BYOK key added", message: `Key ${key.id.slice(0, 8)} created as ${key.provider}.`, tone: "success" });
    } catch (err) {
      toast({ title: "BYOK key failed", message: err instanceof Error ? err.message : "Could not add key", tone: "error" });
    }
  };

  const rotateActiveKey = async (keyId: string) => {
    if (!allowManage() || !tenant) return;
    setRotatingKeyId(keyId);
    try {
      const next = await rotateByokKey(keyId);
      setKeys((prev) => [next, ...prev.map((k) => (k.id === keyId ? { ...k, status: "rotated" as const } : k))]);
      const updated = await updateTenant(tenant.id, { byok_key_id: next.id });
      setTenant(updated);
      toast({
        title: "BYOK key rotated",
        message: "New connector secrets use the new key. Existing byok: ciphertext still decrypts with the rotated key. Not a SOC 2 letter.",
        tone: "success",
      });
    } catch (err) {
      toast({ title: "Rotate failed", message: err instanceof Error ? err.message : "Could not rotate key", tone: "error" });
    } finally {
      setRotatingKeyId(null);
    }
  };

  if (loading) return <p className="df2-cell-meta">Loading enterprise settings…</p>;

  return (
    <div className="df2-settings-enterprise">
      <PermissionNotice allowed={manage.allowed} reason={manage.reason} what="Enterprise tenant settings are read-only for you." />
      {loadError && <div className="df2-team-error df2-mb-md">{loadError}</div>}
      <section className="df2-settings-section">
        <div className="df2-settings-section-head">
          <div>
            <h2>Enterprise tenant</h2>
            <p>Configure the custom domain, data region, and security posture for your organization.</p>
          </div>
          <span className={`df2-badge ${posture?.environment === "production" ? "df2-badge-live" : "df2-badge-muted"}`}>
            {posture?.environment === "production" ? "Production" : "Development"}
          </span>
        </div>

        <div className="df2-settings-section-body">
          <div className="df2-settings-grid df2-settings-grid--row">
            {!tenant && (
              <div className="df2-settings-field">
                <label htmlFor="tenant-workspace">Workspace</label>
                <select id="tenant-workspace" className="df2-select" value={workspaceId} disabled={switching} onChange={(e) => void selectWorkspace(e.target.value)}>
                  {workspaces.map((w) => <option key={w.id} value={w.id}>{w.name}</option>)}
                </select>
              </div>
            )}
            <div className="df2-settings-field">
              <label htmlFor="tenant-name">Tenant name</label>
              <input id="tenant-name" className="df2-input" value={name} onChange={(e) => setName(e.target.value)} placeholder="Wells Fargo" />
            </div>
            <div className="df2-settings-field">
              <label htmlFor="tenant-domain">Custom domain</label>
              <input id="tenant-domain" className="df2-input" value={customDomain} onChange={(e) => setCustomDomain(e.target.value)} placeholder="dataflow.wellsfargo.com" />
            </div>
            <div className="df2-settings-field">
              <label htmlFor="tenant-region">Data region</label>
              <select id="tenant-region" className="df2-select" value={dataRegion} onChange={(e) => setDataRegion(e.target.value)}>
                {REGIONS.map((r) => <option key={r} value={r}>{r}</option>)}
              </select>
            </div>
            <div className="df2-settings-field">
              <label htmlFor="tenant-contact">Security contact email</label>
              <input id="tenant-contact" className="df2-input" value={securityContact} onChange={(e) => setSecurityContact(e.target.value)} placeholder="security@example.com" />
            </div>
            <div className="df2-settings-field">
              <label htmlFor="tenant-timeout">Session timeout (hours) — recorded</label>
              <input id="tenant-timeout" className="df2-input" type="number" min={1} max={24} value={sessionTimeout} onChange={(e) => setSessionTimeout(Number(e.target.value))} />
              <p className="df2-settings-hint">Policy memory only. Token lifetime is DATAFLOW_TOKEN_TTL_SEC, not this field.</p>
            </div>
          </div>

          <div className="df2-settings-policy-row" style={{ marginTop: 16 }}>
            <div>
              <h3>Require MFA for admins</h3>
              <p>Recorded policy. Login MFA is not wired — this switch does not challenge TOTP or WebAuthn yet.</p>
            </div>
            <button type="button" role="switch" aria-checked={mfaRequired} className={`df2-switch ${mfaRequired ? "on" : ""}`} onClick={() => setMfaRequired((v) => !v)}>
              <span className="df2-switch-thumb" />
            </button>
          </div>

          <div className="df2-settings-field" style={{ marginTop: 16 }}>
            <label htmlFor="tenant-allowlist">IP allowlist (enforced only on a custom domain)</label>
            <textarea
              id="tenant-allowlist"
              className="df2-input"
              rows={4}
              value={ipAllowlist}
              onChange={(e) => setIpAllowlist(e.target.value)}
              placeholder="10.0.0.0/8&#10;192.168.1.50"
            />
            <p className="df2-settings-hint">Evaluated only when Host resolves this tenant via custom domain. The default app host never applies the list.</p>
          </div>
        </div>

        <div className="df2-settings-section-footer">
          <button type="button" className="df2-btn df2-btn-primary" disabled={!canSave || saving || !manage.allowed} title={manage.reason || undefined} onClick={() => void save()}>
            {saving ? "Saving…" : tenant ? "Update tenant" : "Create tenant"}
          </button>
        </div>
      </section>

      {posture && (
        <section className="df2-settings-section">
          <div className="df2-settings-section-head">
            <div>
              <h2>Security posture</h2>
              <p>Live compliance and control status for security reviews.</p>
            </div>
          </div>
          <div className="df2-settings-section-body">
            <div className="df2-settings-sso-grid">
              {posture.compliance.map((c) => (
                <div key={c.framework} className={`df2-settings-sso-card ${c.status === "ready" ? "ready" : ""}`}>
                  <h3>{c.framework}</h3>
                  <p>{c.evidence}</p>
                  <span className={`df2-badge ${c.status === "ready" ? "df2-badge-live" : c.status === "in_progress" ? "df2-badge-warn" : "df2-badge-muted"}`}>{c.status}</span>
                </div>
              ))}
            </div>
          </div>
        </section>
      )}

      {tenant && (
        <section className="df2-settings-section">
          <div className="df2-settings-section-head">
            <div>
              <h2>Bring your own key (BYOK)</h2>
              <p>An Active key wraps newly saved connector secrets for this tenant. Existing platform-Fernet credentials stay readable until you re-save the connection. This is encryption evidence — not a SOC 2 or HIPAA letter.</p>
            </div>
          </div>
          <div className="df2-settings-section-body">
            <div className="df2-settings-grid df2-settings-grid--row">
              <div className="df2-settings-field">
                <label htmlFor="key-label">Key label</label>
                <input id="key-label" className="df2-input" value={newKeyLabel} onChange={(e) => setNewKeyLabel(e.target.value)} placeholder="Production key" />
              </div>
              <div className="df2-settings-field">
                <label htmlFor="key-provider">Provider</label>
                <select id="key-provider" className="df2-select" value={newKeyProvider} onChange={(e) => setNewKeyProvider(e.target.value as ByokKey["provider"])}>
                  {Object.entries(PROVIDER_LABELS).map(([k, label]) => <option key={k} value={k}>{label}</option>)}
                </select>
              </div>
              {newKeyProvider === "wrapped" && (
                <div className="df2-settings-field df2-settings-field--full">
                  <label htmlFor="key-material">Base64-encoded 256-bit key material</label>
                  <textarea id="key-material" className="df2-input" rows={3} value={newKeyMaterial} onChange={(e) => setNewKeyMaterial(e.target.value)} placeholder="Paste customer key material…" />
                </div>
              )}
              {newKeyProvider === "aws_kms" && (
                <div className="df2-settings-field df2-settings-field--full">
                  <label htmlFor="key-arn">KMS key ARN</label>
                  <input id="key-arn" className="df2-input" value={newKeyMaterial} onChange={(e) => setNewKeyMaterial(e.target.value)} placeholder="arn:aws:kms:us-east-1:123456789:key/…" />
                </div>
              )}
            </div>
            <button type="button" className="df2-btn df2-btn-secondary df2-btn-sm" disabled={!manage.allowed} title={manage.reason || undefined} onClick={() => void addByokKey()} style={{ marginTop: 12 }}>
              Add BYOK key
            </button>

            {keys.length > 0 && (
              <div className="df2-byok-key-list" style={{ marginTop: 16 }}>
                {keys.map((k) => (
                  <div key={k.id} className={`df2-settings-policy-row ${k.status}`}>
                    <div>
                      <h3>{k.label}</h3>
                      <p>{k.provider} · {k.id.slice(0, 8)}… · {k.status}</p>
                    </div>
                    <div className="df2-settings-policy-actions">
                      {k.id === tenant.byok_key_id && <span className="df2-badge df2-badge-live">Active</span>}
                      {k.status === "active" && (
                        <button
                          type="button"
                          className="df2-btn df2-btn-secondary df2-btn-sm"
                          disabled={!manage.allowed || rotatingKeyId === k.id}
                          title={manage.reason || "Mark this key rotated and mint a new active key. Existing secrets stay readable."}
                          onClick={() => void rotateActiveKey(k.id)}
                        >
                          {rotatingKeyId === k.id ? "Rotating…" : "Rotate"}
                        </button>
                      )}
                    </div>
                  </div>
                ))}
              </div>
            )}
          </div>
        </section>
      )}
    </div>
  );
}
