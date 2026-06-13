import { useState } from "react";

interface ConnectionConfig {
  host: string;
  port: string;
  database: string;
  username: string;
  password: string;
  ssl: boolean;
  authMethod: "password" | "oauth" | "api-key";
}

interface ConnectionWizardProps {
  connectorName: string;
  connectorIcon: string;
  onTest?: (config: ConnectionConfig) => Promise<boolean>;
  onSave?: (config: ConnectionConfig) => void;
  onCancel?: () => void;
}

export function ConnectionWizard({
  connectorName,
  connectorIcon,
  onTest,
  onSave,
  onCancel,
}: ConnectionWizardProps) {
  const [config, setConfig] = useState<ConnectionConfig>({
    host: "",
    port: "5432",
    database: "",
    username: "",
    password: "",
    ssl: true,
    authMethod: "password",
  });
  const [testing, setTesting] = useState(false);
  const [testResult, setTestResult] = useState<"success" | "error" | null>(null);
  const [testDetails, setTestDetails] = useState<{ latency?: number; version?: string } | null>(null);

  const handleTest = async () => {
    setTesting(true);
    setTestResult(null);
    try {
      const success = onTest ? await onTest(config) : true;
      await new Promise((r) => setTimeout(r, 1500)); // Simulate network
      setTestResult(success ? "success" : "error");
      if (success) {
        setTestDetails({ latency: Math.floor(Math.random() * 50) + 10, version: "15.2" });
      }
    } catch {
      setTestResult("error");
    } finally {
      setTesting(false);
    }
  };

  const handleChange = (field: keyof ConnectionConfig, value: string | boolean) => {
    setConfig((prev) => ({ ...prev, [field]: value }));
    setTestResult(null);
  };

  return (
    <div className="dt-connection-wizard">
      <div className="dt-connection-wizard-header">
        <div className="dt-connection-wizard-connector">
          <span className="dt-connection-wizard-icon">{connectorIcon}</span>
          <div>
            <h2 className="dt-connection-wizard-title">Configure {connectorName}</h2>
            <p className="dt-connection-wizard-subtitle">Enter your connection credentials</p>
          </div>
        </div>
      </div>

      <div className="dt-connection-wizard-body">
        <div className="dt-connection-wizard-form">
          <div className="dt-field-row">
            <div className="dt-field" style={{ flex: 2 }}>
              <label className="dt-label">Host</label>
              <input
                type="text"
                className="dt-input"
                placeholder="db.example.com"
                value={config.host}
                onChange={(e) => handleChange("host", e.target.value)}
              />
            </div>
            <div className="dt-field" style={{ flex: 1 }}>
              <label className="dt-label">Port</label>
              <input
                type="text"
                className="dt-input"
                placeholder="5432"
                value={config.port}
                onChange={(e) => handleChange("port", e.target.value)}
              />
            </div>
          </div>

          <div className="dt-field">
            <label className="dt-label">Database Name</label>
            <input
              type="text"
              className="dt-input"
              placeholder="my_database"
              value={config.database}
              onChange={(e) => handleChange("database", e.target.value)}
            />
          </div>

          <div className="dt-field">
            <label className="dt-label">Authentication Method</label>
            <div className="dt-auth-options">
              {[
                { id: "password", label: "Username & Password", icon: "🔑" },
                { id: "oauth", label: "OAuth 2.0", icon: "🔐" },
                { id: "api-key", label: "API Key", icon: "🎫" },
              ].map((method) => (
                <button
                  key={method.id}
                  type="button"
                  className={`dt-auth-option ${config.authMethod === method.id ? "dt-auth-option--active" : ""}`}
                  onClick={() => handleChange("authMethod", method.id as ConnectionConfig["authMethod"])}
                >
                  <span className="dt-auth-option-icon">{method.icon}</span>
                  <span>{method.label}</span>
                </button>
              ))}
            </div>
          </div>

          {config.authMethod === "password" && (
            <div className="dt-field-row">
              <div className="dt-field">
                <label className="dt-label">Username</label>
                <input
                  type="text"
                  className="dt-input"
                  placeholder="admin"
                  value={config.username}
                  onChange={(e) => handleChange("username", e.target.value)}
                />
              </div>
              <div className="dt-field">
                <label className="dt-label">Password</label>
                <input
                  type="password"
                  className="dt-input"
                  placeholder="••••••••"
                  value={config.password}
                  onChange={(e) => handleChange("password", e.target.value)}
                />
              </div>
            </div>
          )}

          <div className="dt-field">
            <label className="dt-checkbox-label">
              <input
                type="checkbox"
                className="dt-checkbox"
                checked={config.ssl}
                onChange={(e) => handleChange("ssl", e.target.checked)}
              />
              <span className="dt-checkbox-box">
                {config.ssl && "✓"}
              </span>
              <span>Enable SSL/TLS encryption</span>
              <span className="dt-badge dt-badge--success">Recommended</span>
            </label>
          </div>
        </div>

        <div className="dt-connection-wizard-sidebar">
          <div className="dt-connection-test-card">
            <h3 className="dt-connection-test-title">Connection Test</h3>
            
            {testResult === null && !testing && (
              <p className="dt-connection-test-desc">
                Test your connection before saving to ensure everything is configured correctly.
              </p>
            )}

            {testing && (
              <div className="dt-connection-testing">
                <div className="dt-loader" />
                <span>Testing connection...</span>
              </div>
            )}

            {testResult === "success" && testDetails && (
              <div className="dt-connection-success">
                <div className="dt-connection-success-icon">✓</div>
                <div className="dt-connection-success-info">
                  <span className="dt-connection-success-title">Connection successful!</span>
                  <span className="dt-connection-success-detail">Latency: {testDetails.latency}ms</span>
                  <span className="dt-connection-success-detail">Version: {testDetails.version}</span>
                </div>
              </div>
            )}

            {testResult === "error" && (
              <div className="dt-connection-error">
                <div className="dt-connection-error-icon">✕</div>
                <div className="dt-connection-error-info">
                  <span className="dt-connection-error-title">Connection failed</span>
                  <span className="dt-connection-error-detail">Check your credentials and try again</span>
                </div>
              </div>
            )}

            <button
              className="dt-btn dt-btn-secondary"
              onClick={handleTest}
              disabled={testing || !config.host || !config.database}
              style={{ width: "100%", marginTop: "var(--dt-space-4)" }}
            >
              {testing ? "Testing..." : "Test Connection"}
            </button>
          </div>

          <div className="dt-connection-health">
            <h4 className="dt-connection-health-title">Health Checks</h4>
            <div className="dt-connection-check">
              <span className={`dt-connection-check-icon ${config.ssl ? "dt-connection-check-icon--pass" : "dt-connection-check-icon--warn"}`}>
                {config.ssl ? "✓" : "!"}
              </span>
              <span>Encryption: {config.ssl ? "Enabled" : "Disabled"}</span>
            </div>
            <div className="dt-connection-check">
              <span className={`dt-connection-check-icon ${config.host ? "dt-connection-check-icon--pass" : ""}`}>
                {config.host ? "✓" : "○"}
              </span>
              <span>Host configured</span>
            </div>
            <div className="dt-connection-check">
              <span className={`dt-connection-check-icon ${config.database ? "dt-connection-check-icon--pass" : ""}`}>
                {config.database ? "✓" : "○"}
              </span>
              <span>Database specified</span>
            </div>
            <div className="dt-connection-check">
              <span className={`dt-connection-check-icon ${testResult === "success" ? "dt-connection-check-icon--pass" : ""}`}>
                {testResult === "success" ? "✓" : "○"}
              </span>
              <span>Connection verified</span>
            </div>
          </div>
        </div>
      </div>

      <div className="dt-connection-wizard-footer">
        <button className="dt-btn dt-btn-ghost" onClick={onCancel}>
          Cancel
        </button>
        <div className="dt-connection-wizard-footer-actions">
          <button
            className="dt-btn dt-btn-secondary"
            disabled={testResult !== "success"}
          >
            Save & Connect Later
          </button>
          <button
            className="dt-btn dt-btn-primary"
            onClick={() => onSave?.(config)}
            disabled={testResult !== "success"}
          >
            Save & Continue
          </button>
        </div>
      </div>
    </div>
  );
}

export function ConnectionWizardStyles() {
  return (
    <style>{`
      .dt-connection-wizard {
        display: flex;
        flex-direction: column;
        background: var(--dt-surface);
        border: 1px solid var(--dt-border);
        border-radius: var(--dt-radius-2xl);
        overflow: hidden;
      }

      .dt-connection-wizard-header {
        padding: var(--dt-space-6);
        background: linear-gradient(135deg, rgba(0, 212, 255, 0.05), rgba(123, 97, 255, 0.05));
        border-bottom: 1px solid var(--dt-border);
      }

      .dt-connection-wizard-connector {
        display: flex;
        align-items: center;
        gap: var(--dt-space-4);
      }

      .dt-connection-wizard-icon {
        width: 56px;
        height: 56px;
        display: flex;
        align-items: center;
        justify-content: center;
        background: rgba(255, 255, 255, 0.05);
        border: 1px solid var(--dt-border);
        border-radius: var(--dt-radius-xl);
        font-size: 28px;
      }

      .dt-connection-wizard-title {
        font-size: var(--dt-text-xl);
        font-weight: 700;
        color: var(--dt-text);
        margin: 0;
      }

      .dt-connection-wizard-subtitle {
        font-size: var(--dt-text-sm);
        color: var(--dt-text-tertiary);
        margin: var(--dt-space-1) 0 0;
      }

      .dt-connection-wizard-body {
        display: grid;
        grid-template-columns: 1fr 320px;
        gap: var(--dt-space-6);
        padding: var(--dt-space-6);
      }

      .dt-connection-wizard-form {
        display: flex;
        flex-direction: column;
        gap: var(--dt-space-5);
      }

      .dt-field-row {
        display: flex;
        gap: var(--dt-space-4);
      }

      .dt-field-row .dt-field {
        margin-bottom: 0;
      }

      .dt-auth-options {
        display: flex;
        gap: var(--dt-space-3);
      }

      .dt-auth-option {
        flex: 1;
        display: flex;
        align-items: center;
        gap: var(--dt-space-2);
        padding: var(--dt-space-3) var(--dt-space-4);
        font-family: inherit;
        font-size: var(--dt-text-sm);
        color: var(--dt-text-secondary);
        background: rgba(255, 255, 255, 0.03);
        border: 1px solid var(--dt-border);
        border-radius: var(--dt-radius-lg);
        cursor: pointer;
        transition: all var(--dt-duration-fast) var(--dt-ease);
      }

      .dt-auth-option:hover {
        border-color: var(--dt-border-strong);
        color: var(--dt-text);
      }

      .dt-auth-option--active {
        background: var(--dt-electric-dim);
        border-color: var(--dt-electric);
        color: var(--dt-electric);
      }

      .dt-auth-option-icon {
        font-size: 18px;
      }

      .dt-checkbox-label {
        display: flex;
        align-items: center;
        gap: var(--dt-space-3);
        font-size: var(--dt-text-sm);
        color: var(--dt-text);
        cursor: pointer;
      }

      .dt-checkbox {
        position: absolute;
        opacity: 0;
        width: 0;
        height: 0;
      }

      .dt-checkbox-box {
        width: 20px;
        height: 20px;
        display: flex;
        align-items: center;
        justify-content: center;
        background: rgba(255, 255, 255, 0.05);
        border: 1px solid var(--dt-border);
        border-radius: var(--dt-radius-sm);
        font-size: 12px;
        color: var(--dt-electric);
        transition: all var(--dt-duration-fast) var(--dt-ease);
      }

      .dt-checkbox:checked + .dt-checkbox-box {
        background: var(--dt-electric-dim);
        border-color: var(--dt-electric);
      }

      .dt-connection-wizard-sidebar {
        display: flex;
        flex-direction: column;
        gap: var(--dt-space-4);
      }

      .dt-connection-test-card {
        padding: var(--dt-space-5);
        background: rgba(255, 255, 255, 0.02);
        border: 1px solid var(--dt-border);
        border-radius: var(--dt-radius-xl);
      }

      .dt-connection-test-title {
        font-size: var(--dt-text-md);
        font-weight: 600;
        color: var(--dt-text);
        margin: 0 0 var(--dt-space-3);
      }

      .dt-connection-test-desc {
        font-size: var(--dt-text-sm);
        color: var(--dt-text-tertiary);
        line-height: 1.5;
        margin: 0;
      }

      .dt-connection-testing {
        display: flex;
        align-items: center;
        gap: var(--dt-space-3);
        padding: var(--dt-space-4);
        background: var(--dt-purple-dim);
        border-radius: var(--dt-radius-lg);
        font-size: var(--dt-text-sm);
        color: var(--dt-purple);
      }

      .dt-connection-success {
        display: flex;
        align-items: flex-start;
        gap: var(--dt-space-3);
        padding: var(--dt-space-4);
        background: var(--dt-emerald-dim);
        border-radius: var(--dt-radius-lg);
      }

      .dt-connection-success-icon {
        width: 24px;
        height: 24px;
        display: flex;
        align-items: center;
        justify-content: center;
        background: var(--dt-emerald);
        color: var(--dt-black);
        border-radius: 50%;
        font-size: 12px;
        font-weight: 700;
      }

      .dt-connection-success-info {
        display: flex;
        flex-direction: column;
        gap: 2px;
      }

      .dt-connection-success-title {
        font-size: var(--dt-text-sm);
        font-weight: 600;
        color: var(--dt-emerald);
      }

      .dt-connection-success-detail {
        font-size: var(--dt-text-xs);
        color: var(--dt-text-secondary);
      }

      .dt-connection-error {
        display: flex;
        align-items: flex-start;
        gap: var(--dt-space-3);
        padding: var(--dt-space-4);
        background: var(--dt-coral-dim);
        border-radius: var(--dt-radius-lg);
      }

      .dt-connection-error-icon {
        width: 24px;
        height: 24px;
        display: flex;
        align-items: center;
        justify-content: center;
        background: var(--dt-coral);
        color: var(--dt-black);
        border-radius: 50%;
        font-size: 12px;
        font-weight: 700;
      }

      .dt-connection-error-info {
        display: flex;
        flex-direction: column;
        gap: 2px;
      }

      .dt-connection-error-title {
        font-size: var(--dt-text-sm);
        font-weight: 600;
        color: var(--dt-coral);
      }

      .dt-connection-error-detail {
        font-size: var(--dt-text-xs);
        color: var(--dt-text-secondary);
      }

      .dt-connection-health {
        padding: var(--dt-space-5);
        background: rgba(255, 255, 255, 0.02);
        border: 1px solid var(--dt-border);
        border-radius: var(--dt-radius-xl);
      }

      .dt-connection-health-title {
        font-size: var(--dt-text-sm);
        font-weight: 600;
        color: var(--dt-text);
        margin: 0 0 var(--dt-space-4);
      }

      .dt-connection-check {
        display: flex;
        align-items: center;
        gap: var(--dt-space-3);
        padding: var(--dt-space-2) 0;
        font-size: var(--dt-text-sm);
        color: var(--dt-text-secondary);
      }

      .dt-connection-check-icon {
        width: 18px;
        height: 18px;
        display: flex;
        align-items: center;
        justify-content: center;
        border: 1px solid var(--dt-border);
        border-radius: 50%;
        font-size: 10px;
        color: var(--dt-text-muted);
      }

      .dt-connection-check-icon--pass {
        background: var(--dt-emerald-dim);
        border-color: var(--dt-emerald);
        color: var(--dt-emerald);
      }

      .dt-connection-check-icon--warn {
        background: var(--dt-amber-dim);
        border-color: var(--dt-amber);
        color: var(--dt-amber);
      }

      .dt-connection-wizard-footer {
        display: flex;
        align-items: center;
        justify-content: space-between;
        padding: var(--dt-space-5) var(--dt-space-6);
        background: rgba(0, 0, 0, 0.2);
        border-top: 1px solid var(--dt-border);
      }

      .dt-connection-wizard-footer-actions {
        display: flex;
        gap: var(--dt-space-3);
      }
    `}</style>
  );
}
