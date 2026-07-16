import { useCallback, useEffect, useMemo, useState } from "react";
import { DtIcon } from "../components/DtIcon";
import { EmptyState } from "../components/EmptyState";
import { SectionLoader } from "../components/LoadingState";
import { FilterTabs } from "../components/ui/FilterTabs";
import { PageFrame } from "../components/ui/PageFrame";
import { PageMetricsRow } from "../components/ui/PageMetricsRow";
import { PageShell } from "../components/ui/PageShell";
import { useToast } from "../components/Toast";
import { fetchAuditEvents, fetchAiProviderSettings, fetchModelCapabilities, fetchSsoConfigs, fetchSecurityPosture, fetchWorkspaceApiKeys, fetchWorkspaceSettings, ModelCapabilities, createWorkspaceApiKey, resolveApiBase, revokeWorkspaceApiKey, SecurityPosture, SsoConfig, SsoType, testSsoConfig, updateAiProviderSettings, updateSsoConfig, updateWorkspaceSettings, WorkspaceApiKey } from "../lib/api";
import { NotificationSettings } from "./settings/NotificationSettings";
import { TeamSettings } from "./settings/TeamSettings";
import { TenantSettings } from "./settings/TenantSettings";

const TABS = [
  { id: "general", label: "General", desc: "Workspace defaults", icon: "settings" },
  { id: "security", label: "Security", desc: "Policies & compliance", icon: "shield" },
  { id: "enterprise", label: "Enterprise", desc: "Tenant, BYOK, residency", icon: "shield" },
  { id: "auth", label: "SSO", desc: "Identity providers", icon: "gate" },
  { id: "team", label: "Team", desc: "Members & roles", icon: "connectors" },
  { id: "notifications", label: "Notifications", desc: "Alerts & integrations", icon: "bell" },
  { id: "models", label: "AI Models", desc: "Provider routing", icon: "sparkle" },
  { id: "api", label: "API Keys", desc: "Programmatic access", icon: "zap" },
  { id: "logs", label: "Audit Logs", desc: "Activity trail", icon: "activity" },
] as const;

type TabId = (typeof TABS)[number]["id"];

type AuditLog = {
  id: string;
  time: string;
  actor: string;
  action: string;
  resource: string;
  level: "info" | "success" | "warn" | "error";
};

export function SettingsPage() {
  const { toast } = useToast();
  const [tab, setTab] = useState<TabId>("general");
  const [orgName, setOrgName] = useState("DataFlow");
  const [timezone, setTimezone] = useState("UTC");
  const [retention, setRetention] = useState("90");
  const [settingsLoading, setSettingsLoading] = useState(true);
  const [settingsSaving, setSettingsSaving] = useState(false);
  const [logFilter, setLogFilter] = useState<"all" | AuditLog["level"]>("all");
  const [auditEvents, setAuditEvents] = useState<AuditLog[]>([]);
  const [auditLoading, setAuditLoading] = useState(false);
  const [modelCapabilities, setModelCapabilities] = useState<ModelCapabilities | null>(null);
  const [ssoConfigs, setSsoConfigs] = useState<Record<SsoType, SsoConfig> | null>(null);
  const [ssoEditor, setSsoEditor] = useState<SsoType | null>(null);
  const [ssoDraft, setSsoDraft] = useState<SsoConfig | null>(null);
  const [ssoSaving, setSsoSaving] = useState(false);
  const [aiEditor, setAiEditor] = useState<string | null>(null);
  const [aiDraft, setAiDraft] = useState({ api_key: "", model: "", base_url: "", enabled: true });
  const [aiSaving, setAiSaving] = useState(false);
  const [apiKeys, setApiKeys] = useState<WorkspaceApiKey[]>([]);
  const [apiKeysLoading, setApiKeysLoading] = useState(false);
  const [apiKeyGenerating, setApiKeyGenerating] = useState(false);
  const [revokingKeyId, setRevokingKeyId] = useState<string | null>(null);
  const [newKeyName, setNewKeyName] = useState("Production key");
  const [revealedKey, setRevealedKey] = useState<string | null>(null);
  const [posture, setPosture] = useState<SecurityPosture | null>(null);
  const [postureLoading, setPostureLoading] = useState(false);

  const loadPosture = useCallback(() => {
    setPostureLoading(true);
    fetchSecurityPosture()
      .then(setPosture)
      .catch(() => setPosture(null))
      .finally(() => setPostureLoading(false));
  }, []);

  useEffect(() => {
    if (tab === "security") loadPosture();
  }, [tab, loadPosture]);

  useEffect(() => {
    fetchModelCapabilities().then(setModelCapabilities).catch(() => setModelCapabilities(null));
    fetchSsoConfigs().then(setSsoConfigs).catch(() => setSsoConfigs(null));
    fetchWorkspaceSettings()
      .then((ws) => {
        setOrgName(ws.org_name);
        setTimezone(ws.timezone);
        setRetention(String(ws.retention_days));
      })
      .catch(() => {})
      .finally(() => setSettingsLoading(false));
  }, []);

  useEffect(() => {
    if (tab !== "api") return;
    setApiKeysLoading(true);
    fetchWorkspaceApiKeys()
      .then(setApiKeys)
      .catch(() => setApiKeys([]))
      .finally(() => setApiKeysLoading(false));
  }, [tab]);

  const openSsoEditor = (type: SsoType) => {
    const cfg = ssoConfigs?.[type];
    const defaultRedirect = `${resolveApiBase()}/auth/sso/${type}/callback`;
    setSsoDraft({
      enabled: cfg?.enabled ?? false,
      entity_id: cfg?.entity_id ?? "",
      sso_url: cfg?.sso_url ?? "",
      x509_cert: "",
      email_attribute: cfg?.email_attribute ?? "email",
      issuer: cfg?.issuer ?? "",
      client_id: cfg?.client_id ?? "",
      client_secret: "",
      redirect_uri: cfg?.redirect_uri || defaultRedirect,
      scopes: cfg?.scopes ?? "openid email profile",
      tenant_id: cfg?.tenant_id ?? "",
    });
    setSsoEditor(type);
  };

  const saveSsoConfig = async () => {
    if (!ssoEditor || !ssoDraft) return;
    setSsoSaving(true);
    try {
      const result = await updateSsoConfig(ssoEditor, ssoDraft);
      setSsoConfigs((prev) => ({ ...(prev ?? {} as Record<SsoType, SsoConfig>), [ssoEditor]: result.config }));
      toast({
        title: result.validation.ok ? "SSO configured" : "SSO saved — needs attention",
        message: result.validation.message,
        tone: result.validation.ok ? "success" : "warning",
      });
      setSsoEditor(null);
    } catch (err) {
      toast({
        title: "SSO save failed",
        message: err instanceof Error ? err.message : "Could not save SSO settings.",
        tone: "error",
      });
    } finally {
      setSsoSaving(false);
    }
  };

  const openAiEditor = async (provider: string, defaultModel: string) => {
    try {
      const settings = await fetchAiProviderSettings();
      const cfg = settings[provider];
      setAiDraft({
        api_key: "",
        model: cfg?.model ?? defaultModel,
        base_url: cfg?.base_url ?? "http://localhost:11434",
        enabled: cfg?.enabled ?? true,
      });
      setAiEditor(provider);
    } catch {
      setAiDraft({ api_key: "", model: defaultModel, base_url: "http://localhost:11434", enabled: true });
      setAiEditor(provider);
    }
  };

  const saveAiProvider = async () => {
    if (!aiEditor) return;
    setAiSaving(true);
    try {
      await updateAiProviderSettings(aiEditor, aiDraft);
      setModelCapabilities(await fetchModelCapabilities());
      toast({ title: "AI provider updated", message: `${aiEditor} settings saved and applied.`, tone: "success" });
      setAiEditor(null);
    } catch (err) {
      toast({
        title: "Save failed",
        message: err instanceof Error ? err.message : "Could not save AI provider settings.",
        tone: "error",
      });
    } finally {
      setAiSaving(false);
    }
  };

  const generateApiKey = async () => {
    if (apiKeyGenerating) return;
    setApiKeyGenerating(true);
    try {
      const created = await createWorkspaceApiKey(newKeyName.trim() || "API key");
      const optimistic: WorkspaceApiKey = {
        id: created.id,
        name: created.name,
        prefix: created.prefix,
        created_at: created.created_at,
        last_used_at: null,
      };
      setApiKeys((prev) => [optimistic, ...prev.filter((k) => k.id !== created.id)]);
      setRevealedKey(created.key);
      toast({ title: "API key created", message: "Copy it now — it won't be shown again.", tone: "success" });
      fetchWorkspaceApiKeys().then(setApiKeys).catch(() => {});
    } catch (err) {
      toast({
        title: "Generation failed",
        message: err instanceof Error ? err.message : "Could not create API key.",
        tone: "error",
      });
    } finally {
      setApiKeyGenerating(false);
    }
  };

  const revokeKey = async (keyId: string, keyName: string) => {
    if (revokingKeyId) return;
    if (!window.confirm(`Revoke "${keyName}"? Applications using this key will stop working immediately.`)) return;
    setRevokingKeyId(keyId);
    try {
      await revokeWorkspaceApiKey(keyId);
      setApiKeys((prev) => prev.filter((k) => k.id !== keyId));
      if (revealedKey) setRevealedKey(null);
      toast({ title: "API key revoked", tone: "info" });
    } catch (err) {
      toast({
        title: "Revoke failed",
        message: err instanceof Error ? err.message : "Could not revoke API key.",
        tone: "error",
      });
    } finally {
      setRevokingKeyId(null);
    }
  };

  const saveWorkspaceSettings = async () => {
    setSettingsSaving(true);
    try {
      const ws = await updateWorkspaceSettings({
        org_name: orgName,
        timezone,
        retention_days: Number(retention) || 90,
      });
      setOrgName(ws.org_name);
      setTimezone(ws.timezone);
      setRetention(String(ws.retention_days));
      toast({ title: "Settings saved", message: "Organization preferences updated.", tone: "success" });
    } catch (err) {
      toast({
        title: "Save failed",
        message: err instanceof Error ? err.message : "Could not persist workspace settings.",
        tone: "error",
      });
    } finally {
      setSettingsSaving(false);
    }
  };

  useEffect(() => {
    if (tab !== "logs") return;
    setAuditLoading(true);
    fetchAuditEvents(100, logFilter === "all" ? undefined : logFilter)
      .then((events) =>
        setAuditEvents(
          events.map((ev) => ({
            id: ev.id,
            time: new Date(ev.time).toLocaleString(),
            actor: ev.actor,
            action: ev.action,
            resource: ev.resource,
            level: (ev.level === "success" ? "success" : ev.level === "warn" ? "warn" : ev.level === "error" ? "error" : "info") as AuditLog["level"],
          })),
        ),
      )
      .catch(() => setAuditEvents([]))
      .finally(() => setAuditLoading(false));
  }, [tab, logFilter]);

  const filteredLogs = useMemo(
    () => auditEvents,
    [auditEvents],
  );

  return (
    <PageShell wide className="df2-page-settings" title="Settings">
      <PageFrame className="df2-settings-workspace">
        <PageMetricsRow
          compact
          columns={4}
          metrics={[
            { label: "Organization", value: orgName, icon: "settings" },
            { label: "Retention", value: `${retention}d`, icon: "activity" },
            { label: "AI provider", value: modelCapabilities?.active_provider ?? "local", icon: "sparkle" },
            { label: "Audit events", value: tab === "logs" && !auditLoading ? auditEvents.length : "—", icon: "activity" },
          ]}
        />

        <div className="df2-settings-layout">
          <nav className="df2-settings-nav" role="tablist" aria-label="Settings sections">
            {TABS.map((t) => (
              <button
                key={t.id}
                type="button"
                role="tab"
                aria-selected={tab === t.id}
                className={`df2-settings-nav-item${tab === t.id ? " active" : ""}`}
                onClick={() => setTab(t.id)}
              >
                <span className="df2-settings-nav-icon" aria-hidden>
                  <DtIcon name={t.icon} size={18} />
                </span>
                <span className="df2-settings-nav-text">
                  <span className="df2-settings-nav-label">{t.label}</span>
                  <span className="df2-settings-nav-desc">{t.desc}</span>
                </span>
              </button>
            ))}
          </nav>

          <div className="df2-settings-panel" role="tabpanel" aria-label={TABS.find((t) => t.id === tab)?.label ?? "Settings"}>
            {tab === "general" && (
              <>
                <section className="df2-settings-section">
                  <div className="df2-settings-section-head">
                    <div>
                      <h2>Organization profile</h2>
                      <p>Defaults applied across Transfer Studio, jobs, and connectors.</p>
                    </div>
                  </div>
                  <div className="df2-settings-section-body">
                    <div className="df2-settings-grid df2-settings-grid--row">
                      <div className="df2-settings-field">
                        <label htmlFor="org-name">Organization name</label>
                        <input id="org-name" className="df2-input" value={orgName} onChange={(e) => setOrgName(e.target.value)} />
                      </div>
                      <div className="df2-settings-field">
                        <label htmlFor="timezone">Default timezone</label>
                        <select id="timezone" className="df2-select" value={timezone} onChange={(e) => setTimezone(e.target.value)}>
                          <option value="UTC">UTC</option>
                          <option value="America/New_York">Eastern Time</option>
                          <option value="America/Los_Angeles">Pacific Time</option>
                          <option value="Europe/London">London</option>
                        </select>
                      </div>
                      <div className="df2-settings-field">
                        <label htmlFor="retention">Job retention (days)</label>
                        <input id="retention" className="df2-input" type="number" value={retention} onChange={(e) => setRetention(e.target.value)} />
                      </div>
                      <div className="df2-settings-field">
                        <label>Default destination</label>
                        <input className="df2-input" placeholder="Configure via Connectors" disabled />
                      </div>
                    </div>
                    <p className="df2-settings-hint">Completed jobs older than retention are archived. Default destination is managed on Connectors.</p>
                  </div>
                  <div className="df2-settings-section-footer">
                    <button
                      type="button"
                      className="df2-btn df2-btn-primary"
                      disabled={settingsLoading || settingsSaving}
                      onClick={() => void saveWorkspaceSettings()}
                    >
                      {settingsSaving ? "Saving…" : "Save changes"}
                    </button>
                  </div>
                </section>
              </>
            )}

            {tab === "security" && (
              <section className="df2-settings-section">
                <div className="df2-settings-section-head">
                  <div>
                    <h2>Security & compliance</h2>
                    <p>Platform-wide controls for data protection and access governance.</p>
                  </div>
                  <span className={`df2-badge ${posture?.environment === "production" ? "df2-badge-live" : "df2-badge-muted"}`}>
                    {posture?.environment === "production" ? "Production" : "Development"}
                  </span>
                </div>
                <div className="df2-settings-section-body">
                  {postureLoading && <p className="df2-cell-meta">Loading security posture…</p>}
                  {!postureLoading && posture && (
                    <>
                      <div className="df2-settings-section-head" style={{ marginBottom: 12 }}>
                        <h3>Posture</h3>
                      </div>
                      <div className="df2-settings-policy-grid" style={{ marginBottom: 24 }}>
                        {[
                          { title: "Encryption at rest", desc: "AES-256 for stored connector credentials and job artifacts.", on: posture.encryption_at_rest },
                          { title: "Audit logging", desc: "Immutable trail for transfers, configuration, and API access.", on: posture.audit_logging },
                          { title: "PII detection", desc: "Sensitive column tagging at ingest and mapping review.", on: posture.pii_detection },
                          { title: "IP allowlisting", desc: "Restrict API and MCP access to approved CIDR ranges.", on: posture.ip_allowlist_enabled },
                          { title: `Session timeout (${posture.session_timeout_hours}h)`, desc: "Automatically sign out idle workspace sessions.", on: posture.session_timeout_hours > 0 },
                          { title: "MFA required for admins", desc: "Enforce multi-factor authentication for owner and admin roles.", on: posture.mfa_required },
                        ].map((item) => (
                          <div key={item.title} className="df2-settings-policy-row">
                            <div>
                              <h3>{item.title}</h3>
                              <p>{item.desc}</p>
                            </div>
                            <span className={`df2-badge ${item.on ? "df2-badge-live" : "df2-badge-muted"}`}>
                              {item.on ? "Enabled" : "Disabled"}
                            </span>
                          </div>
                        ))}
                      </div>

                      <div className="df2-settings-section-head" style={{ marginBottom: 12 }}>
                        <h3>Compliance roadmap</h3>
                      </div>
                      <div className="df2-settings-policy-grid" style={{ marginBottom: 24 }}>
                        {posture.compliance.map((c) => (
                          <div key={c.framework} className="df2-settings-policy-row">
                            <div>
                              <h3>{c.framework}</h3>
                              <p>{c.evidence}</p>
                            </div>
                            <span className={`df2-badge ${c.status === "ready" ? "df2-badge-live" : c.status === "in_progress" ? "df2-badge-warn" : "df2-badge-muted"}`}>
                              {c.status === "ready" ? "Ready" : c.status === "in_progress" ? "In progress" : "Available"}
                            </span>
                          </div>
                        ))}
                      </div>

                      <div className="df2-settings-section-head" style={{ marginBottom: 12 }}>
                        <h3>BYOK / key management</h3>
                      </div>
                      <div className="df2-settings-policy-grid">
                        <div className="df2-settings-policy-row">
                          <div>
                            <h3>Bring-your-own-key</h3>
                            <p>Customer-managed encryption keys for tenant data.</p>
                          </div>
                          <span className={`df2-badge ${posture.byok.configured ? "df2-badge-live" : "df2-badge-muted"}`}>
                            {posture.byok.configured ? `Active (${posture.byok.active_count} key${posture.byok.active_count === 1 ? "" : "s"})` : "Not configured"}
                          </span>
                        </div>
                        <div className="df2-settings-policy-row">
                          <div>
                            <h3>Data region</h3>
                            <p>Primary region for job data and connector credentials.</p>
                          </div>
                          <span className="df2-badge df2-badge-live">{posture.data_region}</span>
                        </div>
                        <div className="df2-settings-policy-row">
                          <div>
                            <h3>TLS</h3>
                            <p>Minimum TLS version for API and MCP traffic.</p>
                          </div>
                          <span className="df2-badge df2-badge-live">{posture.tls_version}</span>
                        </div>
                      </div>
                    </>
                  )}
                  {!postureLoading && !posture && (
                    <EmptyState compact icon="shield" title="Security posture unavailable" description="Could not load posture from the backend." />
                  )}
                </div>
              </section>
            )}

            {tab === "auth" && (
              <section className="df2-settings-section">
                <div className="df2-settings-section-head">
                  <div>
                    <h2>Authentication & SSO</h2>
                    <p>Connect your identity provider for enterprise single sign-on.</p>
                  </div>
                </div>
                <div className="df2-settings-section-body">
                  <div className="df2-settings-sso-grid">
                    {([
                      { type: "saml" as SsoType, name: "SAML 2.0", desc: "Okta, Azure AD, OneLogin", action: "Configure SAML" },
                      { type: "oidc" as SsoType, name: "OpenID Connect", desc: "Google Workspace, Auth0", action: "Configure OIDC" },
                      { type: "azure_ad" as SsoType, name: "Azure AD", desc: "Microsoft Entra ID", action: "Connect Azure" },
                    ]).map((provider) => {
                      const cfg = ssoConfigs?.[provider.type];
                      const enabled = cfg?.enabled;
                      return (
                      <div key={provider.name} className={`df2-settings-sso-card ${enabled ? "ready" : ""}`}>
                        <h3>{provider.name}</h3>
                        <p>{provider.desc}</p>
                        <span className={`df2-badge ${enabled ? "df2-badge-live" : "df2-badge-muted"}`}>
                          {enabled ? "Enabled" : "Not configured"}
                        </span>
                        <button type="button" className="df2-btn df2-btn-sm df2-btn-primary" onClick={() => openSsoEditor(provider.type)}>
                          {provider.action}
                        </button>
                      </div>
                    );})}
                  </div>
                  <p className="df2-settings-hint df2-mt-md">
                    Set the callback URL in your IdP to <code>{resolveApiBase()}/auth/sso/&lt;type&gt;/callback</code>.
                    Enabled providers appear on the sign-in page.
                  </p>
                </div>
              </section>
            )}

            {tab === "team" && <TeamSettings />}

            {tab === "notifications" && <NotificationSettings />}

            {tab === "enterprise" && <TenantSettings />}

            {tab === "models" && (
              <>
                <section className="df2-settings-section">
                  <div className="df2-settings-section-head">
                    <div>
                      <h2>Active model route</h2>
                      <p>Cloud models are used only when credentials are configured.</p>
                    </div>
                  </div>
                  <div className="df2-settings-section-body">
                    <div className="df2-model-ops">
                      <div>
                        <span className="df2-rail-kicker">Agent mode</span>
                        <h2 className="df2-settings-model-title">{modelCapabilities?.agent_mode ?? "local_tools"}</h2>
                        <p className="df2-settings-hint">
                          {modelCapabilities
                            ? `${modelCapabilities.active_provider} · ${modelCapabilities.active_model}`
                            : "Local deterministic engine active while model status loads."}
                        </p>
                      </div>
                      <div className="df2-model-route">
                        {(modelCapabilities?.fallback_order ?? ["anthropic", "openai", "ollama", "rag", "local"]).map((provider, index) => (
                          <span key={provider}>
                            {index > 0 && <i />}
                            <strong>{provider}</strong>
                          </span>
                        ))}
                      </div>
                    </div>
                  </div>
                </section>

                <div className="df2-model-grid">
                  {!modelCapabilities ? (
                    <p className="df2-cell-meta">Loading model providers…</p>
                  ) : modelCapabilities.providers.length === 0 ? (
                    <p className="df2-cell-meta">No model providers reported by the API.</p>
                  ) : modelCapabilities.providers.map((provider) => (
                    <article key={provider.provider} className={`df2-model-card ${provider.available ? "ready" : ""}`}>
                      <div className="df2-model-card-head">
                        <div>
                          <h3>{provider.label}</h3>
                          <p>{provider.default_model}</p>
                        </div>
                        <span className={`df2-badge ${provider.available ? "df2-badge-live" : provider.tier === "cloud" ? "df2-badge-run" : "df2-badge-muted"}`}>
                          {provider.available ? "Ready" : provider.status}
                        </span>
                      </div>
                      <p className="df2-model-best">{provider.best_for}</p>
                      <div className="df2-model-roles">
                        {provider.roles.slice(0, 4).map((role) => (
                          <span key={role}>{role.replace(/_/g, " ")}</span>
                        ))}
                      </div>
                      {provider.provider !== "local" && (
                        <button
                          type="button"
                          className="df2-btn df2-btn-sm df2-btn-primary"
                          onClick={() => void openAiEditor(provider.provider, provider.default_model)}
                        >
                          {provider.available ? "Update credentials" : "Configure"}
                        </button>
                      )}
                    </article>
                  ))}
                </div>
              </>
            )}

            {tab === "api" && (
              <section className="df2-settings-section">
                <div className="df2-settings-section-head">
                  <div>
                    <h2>API keys</h2>
                    <p>Authenticate programmatic transfers, schedules, and MCP agent calls.</p>
                  </div>
                </div>
                <div className="df2-settings-section-body">
                  <div className="df2-api-key-toolbar">
                    <div className="df2-settings-field">
                      <label htmlFor="api-key-name">Key name</label>
                      <input
                        id="api-key-name"
                        className="df2-input"
                        value={newKeyName}
                        onChange={(e) => setNewKeyName(e.target.value)}
                        placeholder="e.g. Production ETL"
                      />
                    </div>
                    <button
                      type="button"
                      className="df2-btn df2-btn-primary"
                      disabled={apiKeyGenerating}
                      onClick={() => void generateApiKey()}
                    >
                      <DtIcon name="plus" size={14} />
                      {apiKeyGenerating ? "Generating…" : "Generate key"}
                    </button>
                  </div>

                  {revealedKey && (
                    <div className="df2-alert df2-alert-success df2-alert-banner df2-mb-md" role="status">
                      <div className="df2-alert-banner-body">
                        <strong>Copy your new API key</strong>
                        <p className="df2-settings-hint">This is the only time the full key is shown. Store it in your secrets manager.</p>
                        <p className="df2-settings-key-reveal"><code>{revealedKey}</code></p>
                      </div>
                      <div className="df2-alert-banner-actions">
                        <button
                          type="button"
                          className="df2-btn df2-btn-sm df2-btn-primary"
                          onClick={() => { void navigator.clipboard.writeText(revealedKey); toast({ title: "Copied to clipboard", tone: "success" }); }}
                        >
                          Copy
                        </button>
                        <button type="button" className="df2-btn df2-btn-sm df2-btn-ghost" onClick={() => setRevealedKey(null)}>
                          Dismiss
                        </button>
                      </div>
                    </div>
                  )}

                  {apiKeysLoading ? (
                    <SectionLoader title="Loading API keys" hint="Fetching workspace keys…" />
                  ) : apiKeys.length === 0 ? (
                    <EmptyState
                      compact
                      icon="key"
                      title="No API keys yet"
                      description="Generate a production key to authenticate programmatic transfers and MCP calls."
                      action={
                        <button type="button" className="df2-btn df2-btn-primary df2-btn-sm" disabled={apiKeyGenerating} onClick={() => void generateApiKey()}>
                          {apiKeyGenerating ? "Generating…" : "Generate key"}
                        </button>
                      }
                    />
                  ) : (
                    <div className="df2-api-key-list" role="list" aria-label="Workspace API keys">
                      {apiKeys.map((key) => (
                        <article key={key.id} className="df2-api-key-card" role="listitem">
                          <div className="df2-api-key-card-main">
                            <div className="df2-api-key-card-icon" aria-hidden>
                              <DtIcon name="key" size={18} />
                            </div>
                            <div className="df2-api-key-card-copy">
                              <strong>{key.name}</strong>
                              <code>{key.prefix}…</code>
                              <span className="df2-api-key-card-meta">
                                Created {key.created_at ? new Date(key.created_at).toLocaleString() : "—"}
                                {" · "}
                                Last used {key.last_used_at ? new Date(key.last_used_at).toLocaleString() : "Never"}
                              </span>
                            </div>
                          </div>
                          <div className="df2-api-key-card-actions">
                            <button
                              type="button"
                              className="df2-btn df2-btn-sm df2-btn-danger"
                              disabled={revokingKeyId === key.id}
                              onClick={() => void revokeKey(key.id, key.name)}
                            >
                              {revokingKeyId === key.id ? "Revoking…" : "Revoke"}
                            </button>
                          </div>
                        </article>
                      ))}
                    </div>
                  )}
                </div>
              </section>
            )}

            {tab === "logs" && (
              <section className="df2-settings-section">
                <div className="df2-settings-section-head">
                  <div>
                    <h2>Audit logs</h2>
                    <p>Configuration changes, transfers, connector tests, and MCP activity.</p>
                  </div>
                </div>
                <div className="df2-settings-section-body">
                  <FilterTabs
                    ariaLabel="Filter audit logs"
                    value={logFilter}
                    onChange={setLogFilter}
                    items={([
                      { id: "all", label: "All events" },
                      { id: "info", label: "Info" },
                      { id: "success", label: "Success" },
                      { id: "warn", label: "Warnings" },
                      { id: "error", label: "Errors" },
                    ] as const)}
                  />
                  <div className="df2-settings-logs-table-wrap">
                    {auditLoading ? (
                      <SectionLoader title="Loading audit events" hint="Fetching workspace activity…" />
                    ) : filteredLogs.length === 0 ? (
                      <EmptyState
                        compact
                        icon="activity"
                        title="No audit events yet"
                        description="Transfer plans, preflight runs, MCP calls, and API mutations are recorded here."
                      />
                    ) : (
                    <div className="df2-settings-table-wrap">
                    <table className="df2-settings-logs-table">
                      <thead>
                        <tr>
                          <th>Time</th>
                          <th>Actor</th>
                          <th>Action</th>
                          <th>Resource</th>
                          <th>Level</th>
                        </tr>
                      </thead>
                      <tbody>
                        {filteredLogs.map((log) => (
                          <tr key={log.id}>
                            <td>{log.time}</td>
                            <td>{log.actor}</td>
                            <td>{log.action}</td>
                            <td>{log.resource}</td>
                            <td>
                              <span className={`df2-settings-log-level df2-settings-log-level--${log.level}`}>
                                {log.level}
                              </span>
                            </td>
                          </tr>
                        ))}
                      </tbody>
                    </table>
                    </div>
                    )}
                  </div>
                </div>
              </section>
            )}
          </div>
        </div>

        {ssoEditor && ssoDraft && (
          <div className="dt-modal-overlay" onClick={() => setSsoEditor(null)} role="presentation">
            <div className="dt-modal dt-modal-lg" onClick={(e) => e.stopPropagation()} role="dialog" aria-modal="true">
              <div className="dt-modal-header">
                <div>
                  <h2 className="dt-modal-title">Configure {ssoEditor === "saml" ? "SAML 2.0" : ssoEditor === "oidc" ? "OpenID Connect" : "Azure AD"}</h2>
                  <p className="dt-modal-subtitle">Settings are persisted and used for workspace sign-in.</p>
                </div>
                <button type="button" className="df2-btn df2-btn-ghost df2-btn-sm" onClick={() => setSsoEditor(null)} aria-label="Close">
                  <DtIcon name="x" />
                </button>
              </div>
              <div className="dt-modal-body">
                <label className="df2-settings-policy-row">
                  <div><h3>Enable provider</h3><p>When enabled, this provider appears on the sign-in page.</p></div>
                  <button type="button" role="switch" aria-checked={ssoDraft.enabled} className={`df2-switch ${ssoDraft.enabled ? "on" : ""}`} onClick={() => setSsoDraft({ ...ssoDraft, enabled: !ssoDraft.enabled })}>
                    <span className="df2-switch-thumb" />
                  </button>
                </label>
                {ssoEditor === "saml" && (
                  <div className="df2-settings-grid df2-mt-md">
                    <div className="df2-settings-field"><label>Entity ID</label><input className="df2-input" value={ssoDraft.entity_id ?? ""} onChange={(e) => setSsoDraft({ ...ssoDraft, entity_id: e.target.value })} /></div>
                    <div className="df2-settings-field"><label>SSO URL</label><input className="df2-input" value={ssoDraft.sso_url ?? ""} onChange={(e) => setSsoDraft({ ...ssoDraft, sso_url: e.target.value })} /></div>
                    <div className="df2-settings-field df2-settings-field--full"><label>IdP X.509 certificate</label><textarea className="df2-input" rows={4} placeholder="Paste certificate PEM" value={ssoDraft.x509_cert ?? ""} onChange={(e) => setSsoDraft({ ...ssoDraft, x509_cert: e.target.value })} /></div>
                  </div>
                )}
                {ssoEditor === "oidc" && (
                  <div className="df2-settings-grid df2-mt-md">
                    <div className="df2-settings-field"><label>Issuer URL</label><input className="df2-input" placeholder="https://accounts.google.com" value={ssoDraft.issuer ?? ""} onChange={(e) => setSsoDraft({ ...ssoDraft, issuer: e.target.value })} /></div>
                    <div className="df2-settings-field"><label>Client ID</label><input className="df2-input" value={ssoDraft.client_id ?? ""} onChange={(e) => setSsoDraft({ ...ssoDraft, client_id: e.target.value })} /></div>
                    <div className="df2-settings-field"><label>Client secret</label><input className="df2-input" type="password" placeholder="Leave blank to keep existing" value={ssoDraft.client_secret ?? ""} onChange={(e) => setSsoDraft({ ...ssoDraft, client_secret: e.target.value })} /></div>
                    <div className="df2-settings-field df2-settings-field--full"><label>Redirect URI</label><input className="df2-input" value={ssoDraft.redirect_uri ?? ""} onChange={(e) => setSsoDraft({ ...ssoDraft, redirect_uri: e.target.value })} /></div>
                  </div>
                )}
                {ssoEditor === "azure_ad" && (
                  <div className="df2-settings-grid df2-mt-md">
                    <div className="df2-settings-field"><label>Tenant ID</label><input className="df2-input" value={ssoDraft.tenant_id ?? ""} onChange={(e) => setSsoDraft({ ...ssoDraft, tenant_id: e.target.value })} /></div>
                    <div className="df2-settings-field"><label>Client ID</label><input className="df2-input" value={ssoDraft.client_id ?? ""} onChange={(e) => setSsoDraft({ ...ssoDraft, client_id: e.target.value })} /></div>
                    <div className="df2-settings-field"><label>Client secret</label><input className="df2-input" type="password" placeholder="Leave blank to keep existing" value={ssoDraft.client_secret ?? ""} onChange={(e) => setSsoDraft({ ...ssoDraft, client_secret: e.target.value })} /></div>
                    <div className="df2-settings-field df2-settings-field--full"><label>Redirect URI</label><input className="df2-input" value={ssoDraft.redirect_uri ?? ""} onChange={(e) => setSsoDraft({ ...ssoDraft, redirect_uri: e.target.value })} /></div>
                  </div>
                )}
              </div>
              <div className="df2-card-footer">
                <button type="button" className="df2-btn df2-btn-ghost" onClick={() => void testSsoConfig(ssoEditor).then((r) => toast({ title: r.ok ? "SSO ready" : "SSO incomplete", message: r.message, tone: r.ok ? "success" : "warning" }))}>
                  Test configuration
                </button>
                <button type="button" className="df2-btn df2-btn-primary" disabled={ssoSaving} onClick={() => void saveSsoConfig()}>
                  {ssoSaving ? "Saving…" : "Save SSO settings"}
                </button>
              </div>
            </div>
          </div>
        )}

        {aiEditor && (
          <div className="dt-modal-overlay" onClick={() => setAiEditor(null)} role="presentation">
            <div className="dt-modal dt-modal-lg" onClick={(e) => e.stopPropagation()} role="dialog" aria-modal="true">
              <div className="dt-modal-header">
                <div>
                  <h2 className="dt-modal-title">Configure {aiEditor}</h2>
                  <p className="dt-modal-subtitle">API keys are encrypted at rest. Leave key blank to keep the existing value.</p>
                </div>
                <button type="button" className="df2-btn df2-btn-ghost df2-btn-sm" onClick={() => setAiEditor(null)} aria-label="Close">
                  <DtIcon name="x" />
                </button>
              </div>
              <div className="dt-modal-body">
                <div className="df2-settings-grid">
                  {aiEditor !== "ollama" && (
                    <div className="df2-settings-field df2-settings-field--full">
                      <label>API key</label>
                      <input className="df2-input" type="password" placeholder="sk-… or leave blank to keep existing" value={aiDraft.api_key} onChange={(e) => setAiDraft({ ...aiDraft, api_key: e.target.value })} />
                    </div>
                  )}
                  <div className="df2-settings-field">
                    <label>Model</label>
                    <input className="df2-input" value={aiDraft.model} onChange={(e) => setAiDraft({ ...aiDraft, model: e.target.value })} />
                  </div>
                  {aiEditor === "ollama" && (
                    <div className="df2-settings-field">
                      <label>Base URL</label>
                      <input className="df2-input" value={aiDraft.base_url} onChange={(e) => setAiDraft({ ...aiDraft, base_url: e.target.value })} />
                    </div>
                  )}
                </div>
              </div>
              <div className="df2-card-footer">
                <button type="button" className="df2-btn df2-btn-primary" disabled={aiSaving} onClick={() => void saveAiProvider()}>
                  {aiSaving ? "Saving…" : "Save provider settings"}
                </button>
              </div>
            </div>
          </div>
        )}
      </PageFrame>
    </PageShell>
  );
}
