import { useState } from "react";

const COMPLIANCE_FRAMEWORKS = [
  { id: "soc2", name: "SOC 2 Type II", status: "certified", icon: "🛡️", color: "#00FF9D" },
  { id: "iso27001", name: "ISO 27001", status: "certified", icon: "🔒", color: "#00D4FF" },
  { id: "gdpr", name: "GDPR", status: "compliant", icon: "🇪🇺", color: "#7B61FF" },
  { id: "hipaa", name: "HIPAA", status: "certified", icon: "🏥", color: "#00FF9D" },
  { id: "pci-dss", name: "PCI DSS", status: "certified", icon: "💳", color: "#00D4FF" },
  { id: "ccpa", name: "CCPA", status: "compliant", icon: "🌴", color: "#FFB800" },
];

const AUDIT_LOGS = [
  { id: 1, action: "Transfer completed", user: "john.doe@company.com", resource: "TRF-001", timestamp: "2 min ago", severity: "info" },
  { id: 2, action: "Schema mapping approved", user: "jane.smith@company.com", resource: "MAP-234", timestamp: "15 min ago", severity: "info" },
  { id: 3, action: "API key rotated", user: "admin@company.com", resource: "KEY-789", timestamp: "1 hour ago", severity: "warning" },
  { id: 4, action: "Failed login attempt", user: "unknown", resource: "AUTH", timestamp: "2 hours ago", severity: "danger" },
  { id: 5, action: "New connector added", user: "jane.smith@company.com", resource: "CON-456", timestamp: "3 hours ago", severity: "info" },
];

interface ComplianceCardProps {
  framework: typeof COMPLIANCE_FRAMEWORKS[0];
}

function ComplianceCard({ framework }: ComplianceCardProps) {
  return (
    <div className="dt-compliance-card" style={{ "--compliance-color": framework.color } as React.CSSProperties}>
      <div className="dt-compliance-card-header">
        <span className="dt-compliance-icon">{framework.icon}</span>
        <span className={`dt-badge dt-badge--${framework.status === "certified" ? "success" : "info"}`}>
          {framework.status}
        </span>
      </div>
      <h4 className="dt-compliance-name">{framework.name}</h4>
      <div className="dt-compliance-status">
        <span className="dt-compliance-check">✓</span>
        <span>Last audit: Jan 2024</span>
      </div>
    </div>
  );
}

function AuditLogRow({ log }: { log: typeof AUDIT_LOGS[0] }) {
  return (
    <tr>
      <td>
        <span className={`dt-audit-severity dt-audit-severity--${log.severity}`} />
      </td>
      <td>{log.action}</td>
      <td className="dt-table-mono">{log.user}</td>
      <td className="dt-table-mono">{log.resource}</td>
      <td>{log.timestamp}</td>
    </tr>
  );
}

export function GovernanceCenter() {
  const [activeTab, setActiveTab] = useState<"compliance" | "audit" | "rbac" | "encryption">("compliance");

  return (
    <div className="dt-governance">
      <div className="dt-governance-header">
        <div>
          <h2 className="dt-governance-title">
            <span>🔐</span> Governance & Security Center
          </h2>
          <p className="dt-governance-subtitle">
            Enterprise compliance, audit trails, and security controls
          </p>
        </div>
        <button className="dt-btn dt-btn-secondary">
          <svg width="16" height="16" viewBox="0 0 16 16" fill="none">
            <path d="M2 4H14M4 8H12M6 12H10" stroke="currentColor" strokeWidth="1.5" strokeLinecap="round"/>
          </svg>
          Export Report
        </button>
      </div>

      <div className="dt-governance-tabs">
        {[
          { id: "compliance", label: "Compliance", icon: "🛡️" },
          { id: "audit", label: "Audit Logs", icon: "📋" },
          { id: "rbac", label: "Access Control", icon: "👥" },
          { id: "encryption", label: "Encryption", icon: "🔒" },
        ].map((tab) => (
          <button
            key={tab.id}
            className={`dt-governance-tab ${activeTab === tab.id ? "dt-governance-tab--active" : ""}`}
            onClick={() => setActiveTab(tab.id as typeof activeTab)}
          >
            <span>{tab.icon}</span>
            <span>{tab.label}</span>
          </button>
        ))}
      </div>

      <div className="dt-governance-content">
        {activeTab === "compliance" && (
          <div className="dt-governance-section">
            <div className="dt-compliance-overview">
              <div className="dt-compliance-score">
                <div className="dt-compliance-score-ring">
                  <svg viewBox="0 0 100 100">
                    <circle cx="50" cy="50" r="42" fill="none" stroke="rgba(255,255,255,0.1)" strokeWidth="8" />
                    <circle
                      cx="50" cy="50" r="42"
                      fill="none" stroke="url(#score-gradient)" strokeWidth="8"
                      strokeDasharray={2 * Math.PI * 42}
                      strokeDashoffset={2 * Math.PI * 42 * 0.02}
                      strokeLinecap="round"
                      transform="rotate(-90 50 50)"
                    />
                    <defs>
                      <linearGradient id="score-gradient" x1="0%" y1="0%" x2="100%" y2="0%">
                        <stop offset="0%" stopColor="#00FF9D" />
                        <stop offset="100%" stopColor="#00D4FF" />
                      </linearGradient>
                    </defs>
                  </svg>
                  <span className="dt-compliance-score-value">98%</span>
                </div>
                <div className="dt-compliance-score-info">
                  <span className="dt-compliance-score-label">Compliance Score</span>
                  <span className="dt-compliance-score-desc">6 of 6 frameworks active</span>
                </div>
              </div>
            </div>

            <h3 className="dt-section-title">Active Frameworks</h3>
            <div className="dt-compliance-grid">
              {COMPLIANCE_FRAMEWORKS.map((framework) => (
                <ComplianceCard key={framework.id} framework={framework} />
              ))}
            </div>
          </div>
        )}

        {activeTab === "audit" && (
          <div className="dt-governance-section">
            <div className="dt-audit-filters">
              <input type="text" className="dt-input" placeholder="Search audit logs..." />
              <select className="dt-input dt-select">
                <option>All Severities</option>
                <option>Info</option>
                <option>Warning</option>
                <option>Danger</option>
              </select>
              <select className="dt-input dt-select">
                <option>Last 24 hours</option>
                <option>Last 7 days</option>
                <option>Last 30 days</option>
              </select>
            </div>

            <div className="dt-card">
              <table className="dt-table">
                <thead>
                  <tr>
                    <th style={{ width: 32 }}></th>
                    <th>Action</th>
                    <th>User</th>
                    <th>Resource</th>
                    <th>Time</th>
                  </tr>
                </thead>
                <tbody>
                  {AUDIT_LOGS.map((log) => (
                    <AuditLogRow key={log.id} log={log} />
                  ))}
                </tbody>
              </table>
            </div>
          </div>
        )}

        {activeTab === "rbac" && (
          <div className="dt-governance-section">
            <div className="dt-rbac-grid">
              <div className="dt-card">
                <div className="dt-card-header">
                  <span className="dt-card-title">Roles</span>
                  <button className="dt-btn dt-btn-ghost">+ Add Role</button>
                </div>
                <div className="dt-card-body">
                  {["Admin", "Data Engineer", "Analyst", "Viewer"].map((role) => (
                    <div key={role} className="dt-rbac-role">
                      <span className="dt-rbac-role-name">{role}</span>
                      <span className="dt-rbac-role-count">
                        {Math.floor(Math.random() * 10) + 1} users
                      </span>
                    </div>
                  ))}
                </div>
              </div>

              <div className="dt-card">
                <div className="dt-card-header">
                  <span className="dt-card-title">Permissions Matrix</span>
                </div>
                <div className="dt-card-body">
                  <div className="dt-permission-row">
                    <span>Create Transfers</span>
                    <span className="dt-permission-badges">
                      <span className="dt-badge dt-badge--success">Admin</span>
                      <span className="dt-badge dt-badge--success">Engineer</span>
                    </span>
                  </div>
                  <div className="dt-permission-row">
                    <span>Approve Mappings</span>
                    <span className="dt-permission-badges">
                      <span className="dt-badge dt-badge--success">Admin</span>
                      <span className="dt-badge dt-badge--success">Engineer</span>
                    </span>
                  </div>
                  <div className="dt-permission-row">
                    <span>View Audit Logs</span>
                    <span className="dt-permission-badges">
                      <span className="dt-badge dt-badge--success">Admin</span>
                    </span>
                  </div>
                  <div className="dt-permission-row">
                    <span>Manage Connectors</span>
                    <span className="dt-permission-badges">
                      <span className="dt-badge dt-badge--success">Admin</span>
                    </span>
                  </div>
                </div>
              </div>
            </div>
          </div>
        )}

        {activeTab === "encryption" && (
          <div className="dt-governance-section">
            <div className="dt-encryption-status">
              <div className="dt-encryption-card">
                <div className="dt-encryption-icon">🔒</div>
                <div className="dt-encryption-info">
                  <h4>Data at Rest</h4>
                  <p>AES-256 encryption enabled</p>
                </div>
                <span className="dt-badge dt-badge--success">Active</span>
              </div>
              <div className="dt-encryption-card">
                <div className="dt-encryption-icon">🔐</div>
                <div className="dt-encryption-info">
                  <h4>Data in Transit</h4>
                  <p>TLS 1.3 enforced</p>
                </div>
                <span className="dt-badge dt-badge--success">Active</span>
              </div>
              <div className="dt-encryption-card">
                <div className="dt-encryption-icon">🗝️</div>
                <div className="dt-encryption-info">
                  <h4>Key Management</h4>
                  <p>AWS KMS integration</p>
                </div>
                <span className="dt-badge dt-badge--success">Active</span>
              </div>
            </div>
          </div>
        )}
      </div>
    </div>
  );
}

export function GovernanceCenterStyles() {
  return (
    <style>{`
      .dt-governance {
        display: flex;
        flex-direction: column;
        gap: var(--dt-space-6);
      }

      .dt-governance-header {
        display: flex;
        align-items: flex-start;
        justify-content: space-between;
      }

      .dt-governance-title {
        display: flex;
        align-items: center;
        gap: var(--dt-space-3);
        font-size: var(--dt-text-2xl);
        font-weight: 700;
        color: var(--dt-text);
      }

      .dt-governance-subtitle {
        font-size: var(--dt-text-sm);
        color: var(--dt-text-tertiary);
        margin-top: var(--dt-space-1);
      }

      .dt-governance-tabs {
        display: flex;
        gap: var(--dt-space-2);
        padding: var(--dt-space-1);
        background: rgba(255, 255, 255, 0.02);
        border: 1px solid var(--dt-border);
        border-radius: var(--dt-radius-xl);
      }

      .dt-governance-tab {
        display: flex;
        align-items: center;
        gap: var(--dt-space-2);
        padding: var(--dt-space-3) var(--dt-space-5);
        font-family: inherit;
        font-size: var(--dt-text-sm);
        font-weight: 500;
        color: var(--dt-text-secondary);
        background: transparent;
        border: none;
        border-radius: var(--dt-radius-lg);
        cursor: pointer;
        transition: all var(--dt-duration-fast) var(--dt-ease);
      }

      .dt-governance-tab:hover {
        background: rgba(255, 255, 255, 0.05);
        color: var(--dt-text);
      }

      .dt-governance-tab--active {
        background: var(--dt-electric-dim);
        color: var(--dt-electric);
      }

      .dt-governance-content {
        animation: dt-fade-in var(--dt-duration-normal) var(--dt-ease);
      }

      .dt-governance-section {
        display: flex;
        flex-direction: column;
        gap: var(--dt-space-6);
      }

      .dt-section-title {
        font-size: var(--dt-text-md);
        font-weight: 600;
        color: var(--dt-text);
        margin-top: var(--dt-space-4);
      }

      .dt-compliance-overview {
        padding: var(--dt-space-6);
        background: var(--dt-surface);
        border: 1px solid var(--dt-border);
        border-radius: var(--dt-radius-xl);
      }

      .dt-compliance-score {
        display: flex;
        align-items: center;
        gap: var(--dt-space-6);
      }

      .dt-compliance-score-ring {
        position: relative;
        width: 100px;
        height: 100px;
      }

      .dt-compliance-score-ring svg {
        width: 100%;
        height: 100%;
      }

      .dt-compliance-score-value {
        position: absolute;
        inset: 0;
        display: flex;
        align-items: center;
        justify-content: center;
        font-size: var(--dt-text-2xl);
        font-weight: 700;
        color: var(--dt-emerald);
      }

      .dt-compliance-score-label {
        display: block;
        font-size: var(--dt-text-lg);
        font-weight: 600;
        color: var(--dt-text);
      }

      .dt-compliance-score-desc {
        display: block;
        font-size: var(--dt-text-sm);
        color: var(--dt-text-tertiary);
      }

      .dt-compliance-grid {
        display: grid;
        grid-template-columns: repeat(auto-fill, minmax(200px, 1fr));
        gap: var(--dt-space-4);
      }

      .dt-compliance-card {
        padding: var(--dt-space-5);
        background: var(--dt-surface);
        border: 1px solid var(--dt-border);
        border-radius: var(--dt-radius-xl);
        transition: all var(--dt-duration-normal) var(--dt-ease);
      }

      .dt-compliance-card:hover {
        border-color: var(--compliance-color);
        box-shadow: 0 0 20px color-mix(in srgb, var(--compliance-color) 20%, transparent);
      }

      .dt-compliance-card-header {
        display: flex;
        align-items: center;
        justify-content: space-between;
        margin-bottom: var(--dt-space-3);
      }

      .dt-compliance-icon {
        font-size: 24px;
      }

      .dt-compliance-name {
        font-size: var(--dt-text-md);
        font-weight: 600;
        color: var(--dt-text);
        margin: 0 0 var(--dt-space-2);
      }

      .dt-compliance-status {
        display: flex;
        align-items: center;
        gap: var(--dt-space-2);
        font-size: var(--dt-text-xs);
        color: var(--dt-text-tertiary);
      }

      .dt-compliance-check {
        color: var(--dt-emerald);
      }

      .dt-audit-filters {
        display: flex;
        gap: var(--dt-space-3);
      }

      .dt-audit-filters .dt-input:first-child {
        flex: 1;
      }

      .dt-audit-severity {
        display: inline-block;
        width: 8px;
        height: 8px;
        border-radius: 50%;
      }

      .dt-audit-severity--info { background: var(--dt-electric); }
      .dt-audit-severity--warning { background: var(--dt-amber); }
      .dt-audit-severity--danger { background: var(--dt-coral); }

      .dt-rbac-grid {
        display: grid;
        grid-template-columns: 1fr 2fr;
        gap: var(--dt-space-4);
      }

      .dt-rbac-role {
        display: flex;
        align-items: center;
        justify-content: space-between;
        padding: var(--dt-space-3);
        border-radius: var(--dt-radius-md);
        transition: background var(--dt-duration-fast) var(--dt-ease);
      }

      .dt-rbac-role:hover {
        background: rgba(255, 255, 255, 0.05);
      }

      .dt-rbac-role-name {
        font-size: var(--dt-text-sm);
        font-weight: 500;
        color: var(--dt-text);
      }

      .dt-rbac-role-count {
        font-size: var(--dt-text-xs);
        color: var(--dt-text-tertiary);
      }

      .dt-permission-row {
        display: flex;
        align-items: center;
        justify-content: space-between;
        padding: var(--dt-space-3) 0;
        border-bottom: 1px solid var(--dt-border);
      }

      .dt-permission-row:last-child {
        border-bottom: none;
      }

      .dt-permission-badges {
        display: flex;
        gap: var(--dt-space-2);
      }

      .dt-encryption-status {
        display: grid;
        grid-template-columns: repeat(3, 1fr);
        gap: var(--dt-space-4);
      }

      .dt-encryption-card {
        display: flex;
        align-items: center;
        gap: var(--dt-space-4);
        padding: var(--dt-space-5);
        background: var(--dt-surface);
        border: 1px solid var(--dt-border);
        border-radius: var(--dt-radius-xl);
      }

      .dt-encryption-icon {
        font-size: 32px;
      }

      .dt-encryption-info {
        flex: 1;
      }

      .dt-encryption-info h4 {
        font-size: var(--dt-text-sm);
        font-weight: 600;
        color: var(--dt-text);
        margin: 0 0 4px;
      }

      .dt-encryption-info p {
        font-size: var(--dt-text-xs);
        color: var(--dt-text-tertiary);
        margin: 0;
      }
    `}</style>
  );
}
