import { useState } from "react";
import { DtIcon } from "../components/DtIcon";

const TABS = [
  { id: "general", label: "General", icon: "settings" },
  { id: "security", label: "Security", icon: "shield" },
  { id: "auth", label: "Authentication / SSO", icon: "key" },
  { id: "team", label: "Team", icon: "users" },
  { id: "api", label: "API Keys", icon: "key" },
] as const;

type TabId = (typeof TABS)[number]["id"];

export function SettingsPage() {
  const [tab, setTab] = useState<TabId>("general");
  const [orgName, setOrgName] = useState("DataTransfer Enterprise");
  const [timezone, setTimezone] = useState("UTC");
  const [retention, setRetention] = useState("90");

  return (
    <div className="dt-content">
      <div className="dt-page-header">
        <h1 className="dt-page-title">Settings</h1>
        <p className="dt-page-subtitle">Enterprise security, SSO, team access, and API configuration.</p>
      </div>
      <div className="dt-tabs" role="tablist">
        {TABS.map((t) => (
          <button
            key={t.id}
            type="button"
            role="tab"
            aria-selected={tab === t.id}
            className={`dt-tab ${tab === t.id ? "active" : ""}`}
            onClick={() => setTab(t.id)}
          >
            {t.label}
          </button>
        ))}
      </div>

      {tab === "general" && (
        <div className="dt-card">
          <div className="dt-card-header"><h3 className="dt-card-title">General Settings</h3></div>
          <div className="dt-card-body">
            <div className="dt-settings-grid">
              <div className="dt-field">
                <label className="dt-label" htmlFor="org-name">Organization Name</label>
                <input id="org-name" className="dt-input" value={orgName} onChange={(e) => setOrgName(e.target.value)} />
              </div>
              <div className="dt-field">
                <label className="dt-label" htmlFor="timezone">Default Timezone</label>
                <select id="timezone" className="dt-select" value={timezone} onChange={(e) => setTimezone(e.target.value)}>
                  <option value="UTC">UTC</option>
                  <option value="America/New_York">Eastern Time</option>
                  <option value="America/Los_Angeles">Pacific Time</option>
                  <option value="Europe/London">London</option>
                </select>
              </div>
              <div className="dt-field">
                <label className="dt-label" htmlFor="retention">Job Retention (days)</label>
                <input id="retention" type="number" className="dt-input" value={retention} onChange={(e) => setRetention(e.target.value)} />
              </div>
              <div className="dt-field">
                <label className="dt-label">Default Destination</label>
                <input className="dt-input" placeholder="mongodb://localhost:27017" disabled />
                <p className="dt-field-hint">Configure via Connectors page</p>
              </div>
            </div>
          </div>
          <div className="dt-card-footer">
            <button type="button" className="dt-btn dt-btn-primary">Save Changes</button>
          </div>
        </div>
      )}

      {tab === "security" && (
        <div className="dt-card">
          <div className="dt-card-header"><h3 className="dt-card-title">Security & Compliance</h3></div>
          <div className="dt-card-body">
            <div className="dt-flex dt-flex-col dt-gap-4">
              {[
                { title: "Encryption at Rest", desc: "AES-256 encryption for stored connector credentials", on: true },
                { title: "Audit Logging", desc: "Track all transfer and configuration events", on: true },
                { title: "PII Detection", desc: "Automatically flag sensitive columns during transfer", on: true },
                { title: "IP Allowlisting", desc: "Restrict API access to approved IP ranges", on: false },
              ].map((item) => (
                <div key={item.title} className="dt-flex dt-items-center dt-justify-between" style={{ padding: "12px 0", borderBottom: "1px solid var(--dt-border)" }}>
                  <div>
                    <div className="dt-font-semibold">{item.title}</div>
                    <div className="dt-text-sm dt-text-muted">{item.desc}</div>
                  </div>
                  <span className={`dt-badge ${item.on ? "dt-badge-success" : "dt-badge-neutral"}`}>{item.on ? "Enabled" : "Disabled"}</span>
                </div>
              ))}
            </div>
          </div>
        </div>
      )}

      {tab === "auth" && (
        <div className="dt-card">
          <div className="dt-card-header"><h3 className="dt-card-title">Authentication & SSO</h3></div>
          <div className="dt-card-body">
            <div className="dt-empty" style={{ padding: "48px 24px" }}>
              <div className="dt-empty-icon"><DtIcon name="shield" size={28} /></div>
              <h3 className="dt-empty-title">Enterprise SSO Ready</h3>
              <p className="dt-empty-text">Configure SAML 2.0 or OIDC with Azure AD, Okta, or Google Workspace.</p>
              <button type="button" className="dt-btn dt-btn-primary">Configure SSO Provider</button>
            </div>
          </div>
        </div>
      )}

      {tab === "team" && (
        <div className="dt-card">
          <div className="dt-card-header">
            <h3 className="dt-card-title">Team Members</h3>
            <button type="button" className="dt-btn dt-btn-sm"><DtIcon name="plus" size={14} /> Invite</button>
          </div>
          <div className="dt-table-wrap">
            <table className="dt-table">
              <thead><tr><th>Name</th><th>Email</th><th>Role</th><th>Status</th></tr></thead>
              <tbody>
                <tr>
                  <td className="dt-font-semibold">Admin User</td>
                  <td>admin@datatransfer.space</td>
                  <td><span className="dt-badge dt-badge-info">Owner</span></td>
                  <td><span className="dt-badge dt-badge-success">Active</span></td>
                </tr>
              </tbody>
            </table>
          </div>
        </div>
      )}

      {tab === "api" && (
        <div className="dt-card">
          <div className="dt-card-header">
            <h3 className="dt-card-title">API Keys</h3>
            <button type="button" className="dt-btn dt-btn-primary dt-btn-sm"><DtIcon name="plus" size={14} /> Generate Key</button>
          </div>
          <div className="dt-card-body">
            <div className="dt-field">
              <label className="dt-label">Production API Key</label>
              <input className="dt-input dt-mono" readOnly value="dt_live_••••••••••••••••••••••••" />
              <p className="dt-field-hint">Use with Authorization: Bearer header</p>
            </div>
          </div>
        </div>
      )}
    </div>
  );
}
