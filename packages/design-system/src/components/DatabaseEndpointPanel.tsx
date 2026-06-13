import { useState } from "react";
import { Button } from "./Button";
import { ConnectionStringField } from "./ConnectionStringPanel";
import { SavedConnectorPicker, type SavedConnectorOption } from "./SavedConnectorPicker";

export interface CredentialFields {
  type: string;
  host: string;
  port: number;
  database: string;
  username: string;
  password: string;
  schema: string;
  warehouse: string;
  ssl: boolean;
  connectionString: string;
}

interface DatabaseOption {
  id: string;
  label: string;
  defaultPort: number;
}

interface DatabaseEndpointPanelProps {
  label: string;
  hint: string;
  accent?: "orange" | "mint";
  connectionString: string;
  onConnectionStringChange: (value: string) => void;
  credentials: CredentialFields;
  onCredentialsChange: (fields: CredentialFields) => void;
  databaseOptions: DatabaseOption[];
  savedConnectors?: SavedConnectorOption[];
  savedConnectorId?: string;
  onSavedConnectorChange?: (id: string) => void;
  onRefreshConnectors?: () => void;
  loadingConnectors?: boolean;
  dbTypeLabel?: string;
  placeholder?: string;
}

export function DatabaseEndpointPanel({
  label,
  hint,
  accent = "orange",
  connectionString,
  onConnectionStringChange,
  credentials,
  onCredentialsChange,
  databaseOptions,
  savedConnectors = [],
  savedConnectorId = "",
  onSavedConnectorChange,
  onRefreshConnectors,
  loadingConnectors,
  dbTypeLabel,
  placeholder,
}: DatabaseEndpointPanelProps) {
  const [mode, setMode] = useState<"string" | "credentials">("credentials");

  function patch(partial: Partial<CredentialFields>) {
    onCredentialsChange({ ...credentials, ...partial });
  }

  return (
    <div className="df-endpoint-panel">
      {savedConnectors.length > 0 && onSavedConnectorChange && (
        <SavedConnectorPicker
          label="Saved connector"
          hint="Pick a configured source or paste credentials below"
          connectors={savedConnectors}
          value={savedConnectorId}
          onChange={onSavedConnectorChange}
          onRefresh={onRefreshConnectors}
          loading={loadingConnectors}
          accent={accent}
        />
      )}

      <div className="df-endpoint-mode">
        <Button variant={mode === "credentials" ? "primary" : "ghost"} onClick={() => setMode("credentials")}>
          Username & password
        </Button>
        <Button variant={mode === "string" ? "primary" : "ghost"} onClick={() => setMode("string")}>
          Connection string
        </Button>
      </div>

      {mode === "string" ? (
        <ConnectionStringField
          label={label}
          hint={hint}
          value={connectionString}
          onChange={onConnectionStringChange}
          dbTypeLabel={dbTypeLabel}
          placeholder={placeholder}
          accent={accent}
        />
      ) : (
        <div className="df-credential-form">
          <div className="df-conn-field-head">
            <div>
              <div className="df-conn-field-label">{label}</div>
              <div className="df-conn-field-hint">{hint}</div>
            </div>
            {dbTypeLabel && <span className="df-conn-field-badge">{dbTypeLabel}</span>}
          </div>

          <div className="df-form-field">
            <span className="df-label">Database engine</span>
            <select
              className="df-select"
              value={credentials.type}
              onChange={(e) => {
                const opt = databaseOptions.find((d) => d.id === e.target.value);
                patch({ type: e.target.value, port: opt?.defaultPort ?? credentials.port });
              }}
            >
              {databaseOptions.map((d) => (
                <option key={d.id} value={d.id}>
                  {d.label}
                </option>
              ))}
            </select>
          </div>

          <div className="df-form-row">
            <div className="df-form-field">
              <span className="df-label">{credentials.type === "snowflake" ? "Account" : "Host"}</span>
              <input
                className="df-input"
                value={credentials.host}
                onChange={(e) => patch({ host: e.target.value })}
                placeholder={credentials.type === "snowflake" ? "xy12345.us-east-1" : "db.example.com"}
              />
            </div>
            <div className="df-form-field df-form-field--narrow">
              <span className="df-label">Port</span>
              <input
                className="df-input"
                type="number"
                value={credentials.port}
                onChange={(e) => patch({ port: Number(e.target.value) })}
              />
            </div>
            <div className="df-form-field">
              <span className="df-label">Database</span>
              <input
                className="df-input"
                value={credentials.database}
                onChange={(e) => patch({ database: e.target.value })}
              />
            </div>
          </div>

          <div className="df-form-row">
            <div className="df-form-field">
              <span className="df-label">Username</span>
              <input
                className="df-input"
                value={credentials.username}
                onChange={(e) => patch({ username: e.target.value })}
                autoComplete="username"
              />
            </div>
            <div className="df-form-field">
              <span className="df-label">Password</span>
              <input
                className="df-input"
                type="password"
                value={credentials.password}
                onChange={(e) => patch({ password: e.target.value })}
                autoComplete="current-password"
              />
            </div>
            <div className="df-form-field">
              <span className="df-label">Schema</span>
              <input
                className="df-input"
                value={credentials.schema}
                onChange={(e) => patch({ schema: e.target.value })}
                placeholder={credentials.type === "snowflake" ? "PUBLIC" : "public"}
              />
            </div>
          </div>

          {credentials.type === "snowflake" && (
            <div className="df-form-field">
              <span className="df-label">Warehouse</span>
              <input
                className="df-input"
                value={credentials.warehouse}
                onChange={(e) => patch({ warehouse: e.target.value })}
                placeholder="COMPUTE_WH"
              />
            </div>
          )}

          <label className="df-checkbox-label">
            <input type="checkbox" checked={credentials.ssl} onChange={(e) => patch({ ssl: e.target.checked })} />
            SSL / TLS encryption
          </label>
        </div>
      )}
    </div>
  );
}
