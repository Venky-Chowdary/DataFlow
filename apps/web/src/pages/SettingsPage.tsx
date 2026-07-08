import { useEffect, useState } from "react";
import { DtIcon } from "../components/DtIcon";
import { PageShell } from "../components/ui/PageShell";
import { useToast } from "../components/Toast";
import { fetchModelCapabilities, ModelCapabilities } from "../lib/api";

const TABS = [
  { id: "general", label: "General", icon: "settings" },
  { id: "security", label: "Security", icon: "shield" },
  { id: "auth", label: "SSO", icon: "gate" },
  { id: "team", label: "Team", icon: "connectors" },
  { id: "models", label: "AI Models", icon: "sparkle" },
  { id: "api", label: "API Keys", icon: "zap" },
] as const;

type TabId = (typeof TABS)[number]["id"];
type ModelProviderRow = ModelCapabilities["providers"][number];

const MODEL_PROVIDER_FALLBACKS: ModelProviderRow[] = [
  {
    provider: "anthropic",
    label: "Anthropic Claude",
    default_model: "claude-sonnet-4-20250514",
    tier: "cloud",
    roles: ["agent_tool_use", "schema_reasoning", "migration_planning", "policy_explanation"],
    best_for: "Long-horizon Data Pilot agent runs, tool use, schema-policy reasoning, and migration plan review.",
    configured: false,
    package_installed: false,
    available: false,
    status: "configure",
  },
  {
    provider: "openai",
    label: "OpenAI",
    default_model: "gpt-4o-mini",
    tier: "cloud",
    roles: ["copilot_chat", "rag_answering", "mapping_explanation", "fallback_generation"],
    best_for: "Fast grounded chat, mapping explanation, RAG answers, and second-line cloud fallback.",
    configured: false,
    package_installed: false,
    available: false,
    status: "configure",
  },
  {
    provider: "ollama",
    label: "Ollama",
    default_model: "llama3.2",
    tier: "local",
    roles: ["private_local_generation", "offline_assist", "fallback_generation"],
    best_for: "Private/local assistant mode when cloud credentials are unavailable.",
    configured: true,
    package_installed: true,
    available: false,
    status: "offline",
  },
  {
    provider: "local",
    label: "Local deterministic engine",
    default_model: "local_knowledge",
    tier: "deterministic",
    roles: ["semantic_rules", "rag_retrieval", "preflight_gates", "mapping_assignment"],
    best_for: "Always-on fail-closed semantic analysis, RAG retrieval, preflight, and deterministic mapping safeguards.",
    configured: true,
    package_installed: true,
    available: true,
    status: "ready",
  },
];

export function SettingsPage() {
  const { toast } = useToast();
  const [tab, setTab] = useState<TabId>("general");
  const [orgName, setOrgName] = useState("DataFlow");
  const [timezone, setTimezone] = useState("UTC");
  const [retention, setRetention] = useState("90");
  const [modelCapabilities, setModelCapabilities] = useState<ModelCapabilities | null>(null);

  useEffect(() => {
    fetchModelCapabilities().then(setModelCapabilities).catch(() => setModelCapabilities(null));
  }, []);

  return (
    <PageShell
      wide
      title="Settings"
      description="Organization, security, SSO, team access, and API configuration."
    >
      <div className="df2-settings-layout">
        <nav className="df2-settings-nav" role="tablist" aria-label="Settings sections">
          {TABS.map((t) => (
            <button
              key={t.id}
              type="button"
              role="tab"
              aria-selected={tab === t.id}
              className={tab === t.id ? "active" : ""}
              onClick={() => setTab(t.id)}
            >
              <DtIcon name={t.icon} size={18} />
              <span>{t.label}</span>
            </button>
          ))}
        </nav>

        <div>
          {tab === "general" && (
            <div className="df2-card">
              <div className="df2-card-head"><h2 className="df2-card-title">General</h2></div>
              <div className="df2-card-body">
                <div className="df2-settings-grid">
                  <div className="df2-field">
                    <label className="df2-label" htmlFor="org-name">Organization name</label>
                    <input id="org-name" className="df2-input" value={orgName} onChange={(e) => setOrgName(e.target.value)} />
                  </div>
                  <div className="df2-field">
                    <label className="df2-label" htmlFor="timezone">Default timezone</label>
                    <select id="timezone" className="df2-select" value={timezone} onChange={(e) => setTimezone(e.target.value)}>
                      <option value="UTC">UTC</option>
                      <option value="America/New_York">Eastern Time</option>
                      <option value="America/Los_Angeles">Pacific Time</option>
                      <option value="Europe/London">London</option>
                    </select>
                  </div>
                  <div className="df2-field">
                    <label className="df2-label" htmlFor="retention">Job retention (days)</label>
                    <input id="retention" type="number" className="df2-input" value={retention} onChange={(e) => setRetention(e.target.value)} />
                  </div>
                  <div className="df2-field">
                    <label className="df2-label">Default destination</label>
                    <input className="df2-input" placeholder="Configure via Connectors" disabled />
                    <p style={{ margin: 0, fontSize: 12, color: "#94a3b8" }}>Managed on the Connectors page</p>
                  </div>
                </div>
              </div>
              <div className="df2-card-footer">
                <button
                  type="button"
                  className="df2-btn df2-btn-primary"
                  onClick={() => toast({ title: "Settings saved", message: "Organization preferences updated.", tone: "success" })}
                >
                  Save changes
                </button>
              </div>
            </div>
          )}

          {tab === "security" && (
            <div className="df2-card">
              <div className="df2-card-head"><h2 className="df2-card-title">Security & compliance</h2></div>
              <div className="df2-card-body df2-stack" style={{ gap: 16 }}>
                {[
                  { title: "Encryption at rest", desc: "AES-256 for stored connector credentials", on: true },
                  { title: "Audit logging", desc: "Transfer and configuration event trail", on: true },
                  { title: "PII detection", desc: "Sensitive column tagging at ingest", on: true },
                  { title: "IP allowlisting", desc: "Restrict API access to approved ranges", on: false },
                ].map((item) => (
                  <div key={item.title} style={{ display: "flex", justifyContent: "space-between", alignItems: "center", gap: 16 }}>
                    <div>
                      <div style={{ fontWeight: 600 }}>{item.title}</div>
                      <div style={{ fontSize: 13, color: "#64748b" }}>{item.desc}</div>
                    </div>
                    <span className={`df2-badge ${item.on ? "df2-badge-live" : "df2-badge-muted"}`}>
                      {item.on ? "Enabled" : "Disabled"}
                    </span>
                  </div>
                ))}
              </div>
            </div>
          )}

          {tab === "auth" && (
            <div className="df2-card">
              <div className="df2-card-head"><h2 className="df2-card-title">Authentication & SSO</h2></div>
              <div className="df2-empty">
                <DtIcon name="shield" size={28} />
                <h3 className="df2-empty-title">Enterprise SSO</h3>
                <p className="df2-empty-desc">Configure SAML 2.0 or OIDC with Azure AD, Okta, or Google Workspace.</p>
                <button type="button" className="df2-btn df2-btn-primary">Configure SSO provider</button>
              </div>
            </div>
          )}

          {tab === "team" && (
            <div className="df2-card">
              <div className="df2-card-head">
                <h2 className="df2-card-title">Team members</h2>
                <button type="button" className="df2-btn df2-btn-sm"><DtIcon name="plus" size={14} /> Invite</button>
              </div>
              <div className="df2-card-body">
                <div className="df2-cell-main">
                  <div className="df2-cell-icon">A</div>
                  <div style={{ flex: 1 }}>
                    <div className="df2-cell-title">Admin</div>
                    <div className="df2-cell-meta">admin@dataflow.local</div>
                  </div>
                  <span className="df2-badge df2-badge-beta">Owner</span>
                  <span className="df2-badge df2-badge-live">Active</span>
                </div>
              </div>
            </div>
          )}

          {tab === "models" && (
            <div className="df2-stack">
              <div className="df2-model-ops">
                <div>
                  <span className="df2-rail-kicker">Active model route</span>
                  <h2>{modelCapabilities?.agent_mode ?? "local_tools"}</h2>
                  <p>
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

              <div className="df2-model-grid">
                {(modelCapabilities?.providers ?? MODEL_PROVIDER_FALLBACKS).map((provider) => (
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
                    <div className="df2-model-checks">
                      <span>{provider.configured ? "Key configured" : provider.tier === "cloud" ? "Key missing" : "No key needed"}</span>
                      <span>{provider.package_installed ? "SDK installed" : "SDK missing"}</span>
                    </div>
                  </article>
                ))}
              </div>

              <div className="df2-card">
                <div className="df2-card-head">
                  <h2 className="df2-card-title">Model governance guarantees</h2>
                </div>
                <div className="df2-card-body df2-cap-list">
                  {(modelCapabilities?.guarantees ?? [
                    "Cloud models are used only when credentials are configured.",
                    "Preflight and schema blockers remain deterministic.",
                    "Ambiguous mappings require review.",
                  ]).map((item) => (
                    <div key={item} className="df2-cap-item"><DtIcon name="shield" size={16} /> {item}</div>
                  ))}
                </div>
              </div>
            </div>
          )}

          {tab === "api" && (
            <div className="df2-card">
              <div className="df2-card-head">
                <h2 className="df2-card-title">API keys</h2>
                <button type="button" className="df2-btn df2-btn-primary df2-btn-sm"><DtIcon name="plus" size={14} /> Generate key</button>
              </div>
              <div className="df2-card-body">
                <div className="df2-field">
                  <label className="df2-label">Production API key</label>
                  <input className="df2-input" style={{ fontFamily: "var(--df-font-mono)" }} readOnly value="df_live_••••••••••••••••••••••••" />
                  <p style={{ margin: 0, fontSize: 12, color: "#94a3b8" }}>Authorization: Bearer &lt;key&gt;</p>
                </div>
              </div>
            </div>
          )}
        </div>
      </div>
    </PageShell>
  );
}
