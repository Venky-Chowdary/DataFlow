import { useEffect, useMemo, useState } from "react";
import { ConnectorCatalogPanel } from "./ConnectorCatalogPanel";
import { ButtonLoader } from "./LoadingState";
import { DtIcon } from "./DtIcon";
import { useToast } from "./Toast";
import type { CatalogConnector } from "../lib/api";
import type { Connector } from "../lib/types";
import { saveConnector, testConnection, updateConnector } from "../lib/api";
import {
  getConnectorDefaults,
  isAwsConnector,
  isGcpConnector,
  isConfigurableInStudio,
  resolveCatalogIdToType,
} from "../lib/connectorTypes";
import { CONNECTOR_CATALOG } from "../lib/types";

interface ConnectorModalProps {
  initialType?: string;
  editing?: Connector | null;
  onClose: () => void;
  onSaved: () => void;
}

type AuthMode = "user_pass" | "connection_string" | "service_account" | "aws_keys" | "api_key";

function inferAuthMode(conn: Connector | null | undefined, type: string): AuthMode {
  if (conn?.auth_mode) return conn.auth_mode as AuthMode;
  if (conn?.api_key) return "api_key";
  if (conn?.service_account) return "service_account";
  if (conn?.connection_string) return "connection_string";
  if (["s3", "dynamodb"].includes(type)) return "aws_keys";
  if (["bigquery", "gcs", "google_cloud_storage"].includes(type)) return "service_account";
  if (type === "elasticsearch") return conn?.username ? "user_pass" : "api_key";
  if (type === "adls") return "connection_string";
  if (type === "mongodb" && conn?.username) return "user_pass";
  return "user_pass";
}

function authModeOptions(type: string): { value: AuthMode; label: string }[] {
  const sqlish = ["postgresql", "mysql", "redshift", "mariadb", "sqlite", "generic_sql"].includes(type);
  const mongo = type === "mongodb";
  const snowflake = type === "snowflake";
  const elastic = type === "elasticsearch";
  const awsStore = ["s3", "dynamodb"].includes(type);
  const gcp = isGcpConnector(type);
  const azure = type === "adls";

  const options: { value: AuthMode; label: string }[] = [];
  if (sqlish || mongo || snowflake || elastic || azure) {
    options.push({ value: "user_pass", label: "Username & password" });
  }
  if (sqlish || mongo || snowflake || azure) {
    options.push({ value: "connection_string", label: "Connection string" });
  }
  if (gcp) {
    options.push({ value: "service_account", label: "Service account JSON / path" });
  }
  if (awsStore) {
    options.push({ value: "aws_keys", label: "AWS access keys" });
  }
  if (elastic) {
    options.push({ value: "api_key", label: "API key" });
  }
  if (options.length === 0) {
    options.push({ value: "user_pass", label: "Username & password" });
  }
  return options;
}

export function ConnectorModal({
  initialType,
  editing = null,
  onClose,
  onSaved,
}: ConnectorModalProps) {
  const { toast } = useToast();
  const startType = editing?.type ?? (initialType ? resolveCatalogIdToType(initialType) : "");

  const defaults = getConnectorDefaults(startType || "mongodb");
  const [step, setStep] = useState<"pick" | "configure">(editing || startType ? "configure" : "pick");

  const [name, setName] = useState(editing?.name ?? "");
  const [type, setType] = useState(startType || "mongodb");
  const [host, setHost] = useState(editing?.host ?? defaults.host);
  const [port, setPort] = useState<number>(editing?.port ?? defaults.port);
  const [database, setDatabase] = useState(editing?.database ?? "");
  const [username, setUsername] = useState(editing?.username ?? "");
  const [password, setPassword] = useState(editing?.password ?? "");
  const [connectionString, setConnectionString] = useState(editing?.connection_string ?? "");
  const [schema, setSchema] = useState(editing?.schema ?? "");
  const [warehouse, setWarehouse] = useState(editing?.warehouse ?? "");
  const [authRole, setAuthRole] = useState(editing?.auth_role ?? "");
  const [apiKey, setApiKey] = useState(editing?.api_key ?? "");
  const [serviceAccount, setServiceAccount] = useState(editing?.service_account ?? "");
  const [ssl, setSsl] = useState(editing?.ssl ?? false);
  const [authMode, setAuthMode] = useState<AuthMode>(inferAuthMode(editing, startType));

  const [testing, setTesting] = useState(false);
  const [saving, setSaving] = useState(false);
  const [testResult, setTestResult] = useState<{ success: boolean; message: string } | null>(null);
  const [fieldError, setFieldError] = useState<string | null>(null);

  const catalogItem = CONNECTOR_CATALOG.find((c) => c.id === type);
  const isBigQuery = type === "bigquery";
  const isSnowflake = type === "snowflake";
  const isMongo = type === "mongodb";
  const isDynamo = type === "dynamodb";
  const isS3 = type === "s3";
  const isAwsKeyed = isS3 || isDynamo;
  const isElastic = type === "elasticsearch";
  const isGcp = isGcpConnector(type);
  const isAzure = type === "adls";

  const modeOptions = useMemo(() => authModeOptions(type), [type]);

  useEffect(() => {
    if (modeOptions.find((o) => o.value === authMode)) return;
    setAuthMode(modeOptions[0]?.value ?? "user_pass");
  }, [type, modeOptions, authMode]);

  const applyType = (nextType: string) => {
    const d = getConnectorDefaults(nextType);
    setType(nextType);
    setHost(d.host);
    setPort(d.port);
    setDatabase("");
    setUsername("");
    setPassword("");
    setConnectionString("");
    setSchema("");
    setWarehouse("");
    setAuthRole("");
    setApiKey("");
    setServiceAccount("");
    setSsl(false);
    setAuthMode(inferAuthMode(null, nextType));
    setTestResult(null);
    setStep("configure");
    if (!name.trim()) {
      setName(`${d.label} connection`);
    }
  };

  const handleCatalogPick = (item: CatalogConnector) => {
    if (
      item.effective_status === "planned" ||
      (!item.transfer_ready && !item.connect_only && item.status !== "live" && item.status !== "beta")
    ) {
      toast({
        title: "Not available yet",
        message: `${item.name} is on the roadmap — no driver registered yet.`,
        tone: "info",
      });
      return;
    }
    applyType(resolveCatalogIdToType(item.id));
  };

  const validate = () => {
    if (!name.trim()) {
      setFieldError("Connection name is required.");
      return false;
    }
    if (authMode === "connection_string") {
      if (!connectionString.trim()) {
        setFieldError("Connection string is required.");
        return false;
      }
    } else if (authMode === "service_account") {
      if (!serviceAccount.trim()) {
        setFieldError("Service account JSON or file path is required.");
        return false;
      }
      if (!database.trim() && !isGcp) {
        setFieldError(isAzure ? "Storage account or container is required." : "Database / project is required.");
        return false;
      }
    } else if (authMode === "api_key") {
      if (!apiKey.trim()) {
        setFieldError("API key is required.");
        return false;
      }
      if (!host.trim()) {
        setFieldError("Host is required.");
        return false;
      }
    } else if (authMode === "aws_keys") {
      if (!host.trim() && !database.trim()) {
        setFieldError(isS3 ? "Region and bucket are required." : "Region and table name are required.");
        return false;
      }
      const local = (host || connectionString).includes("localhost") || (host || connectionString).startsWith("http");
      if (!local && (!username.trim() || !password.trim())) {
        setFieldError("AWS Access Key ID and Secret Access Key are required for cloud endpoints.");
        return false;
      }
    } else if (authMode === "user_pass") {
      if (isBigQuery) {
        if (!database.trim()) {
          setFieldError("GCP project ID is required.");
          return false;
        }
      } else if (!isGcp && !isAwsKeyed && !isElastic && !host.trim()) {
        setFieldError("Host is required.");
        return false;
      }
      if (["s3", "dynamodb"].includes(type)) {
        if (isS3 ? !database.trim() : !database.trim()) {
          setFieldError(isS3 ? "Bucket name is required." : "Table name is required.");
          return false;
        }
      }
    }
    setFieldError(null);
    return true;
  };

  useEffect(() => {
    const onKey = (e: KeyboardEvent) => {
      if (e.key === "Escape") onClose();
    };
    window.addEventListener("keydown", onKey);
    return () => window.removeEventListener("keydown", onKey);
  }, [onClose]);

  const buildPayload = () => {
    const payload: Record<string, unknown> = {
      name,
      type,
      host: isBigQuery ? "bigquery.googleapis.com" : isAwsKeyed ? (host || "us-east-1") : host,
      port: isBigQuery || isAwsKeyed ? 443 : port,
      database,
      schema: isBigQuery || isSnowflake ? schema : undefined,
      ssl,
      auth_mode: authMode,
      auth_role: isSnowflake ? authRole : undefined,
    };

    if (authMode === "user_pass") {
      payload.username = username || undefined;
      payload.password = password || undefined;
    }
    if (authMode === "connection_string") {
      payload.connection_string = connectionString || undefined;
    }
    if (authMode === "service_account") {
      payload.service_account = serviceAccount || undefined;
    }
    if (authMode === "api_key") {
      payload.api_key = apiKey || undefined;
    }
    if (authMode === "aws_keys") {
      payload.username = username || undefined;
      payload.password = password || undefined;
    }
    if (isSnowflake) {
      payload.warehouse = warehouse || undefined;
    }
    if (isGcp && !serviceAccount.trim() && connectionString.trim()) {
      // Allow a pasted path to ride connection_string as a convenience.
      payload.connection_string = connectionString || undefined;
    }

    return payload;
  };

  const handleTest = async () => {
    if (!validate()) return;
    setTesting(true);
    setTestResult(null);
    try {
      const result = await testConnection({
        type,
        host: isBigQuery ? undefined : host,
        port: isBigQuery ? undefined : port,
        database,
        schema: isBigQuery || isSnowflake ? schema : undefined,
        username: authMode === "user_pass" || authMode === "aws_keys" ? username : undefined,
        password: authMode === "user_pass" || authMode === "aws_keys" ? password : undefined,
        connection_string: authMode === "connection_string" ? connectionString : undefined,
        service_account: authMode === "service_account" ? serviceAccount : undefined,
        api_key: authMode === "api_key" ? apiKey : undefined,
        warehouse: isSnowflake ? warehouse : undefined,
        auth_role: isSnowflake ? authRole : undefined,
        auth_mode: authMode,
        ssl,
      });
      setTestResult(result);
      toast({
        title: result.success ? "Connection successful" : "Connection failed",
        message: result.message,
        tone: result.success ? "success" : "error",
      });
    } catch {
      setTestResult({ success: false, message: "Connection test failed" });
      toast({ title: "Connection test failed", tone: "error" });
    }
    setTesting(false);
  };

  const handleSave = async () => {
    if (!validate()) return;
    setSaving(true);
    try {
      const payload = buildPayload();
      if (editing) {
        await updateConnector(editing.id, payload as Parameters<typeof updateConnector>[1]);
        toast({ title: "Connector updated", message: name, tone: "success" });
      } else {
        await saveConnector(payload as Parameters<typeof saveConnector>[0]);
        toast({ title: "Connector saved", message: name, tone: "success" });
      }
      onSaved();
      onClose();
    } catch (e) {
      toast({ title: "Save failed", message: "Could not save connector settings.", tone: "error" });
      console.error(e);
    }
    setSaving(false);
  };

  const showUserPass = authMode === "user_pass";
  const showConnectionString = authMode === "connection_string";
  const showServiceAccount = authMode === "service_account";
  const showApiKey = authMode === "api_key";
  const showAwsKeys = authMode === "aws_keys";

  const hostLabel = useMemo(() => {
    if (isAwsKeyed) return isS3 ? "AWS region" : "AWS region or local endpoint";
    if (isGcp) return isBigQuery ? "GCP project (optional)" : "Project / endpoint";
    if (isAzure) return "Storage account";
    if (isElastic) return "Host";
    return "Host";
  }, [isAwsKeyed, isS3, isGcp, isBigQuery, isAzure, isElastic]);

  const databaseLabel = useMemo(() => {
    if (isS3) return "Bucket name";
    if (isDynamo) return "Table name";
    if (isBigQuery) return "GCP project ID";
    if (isGcp) return "Project ID";
    if (isAzure) return "Container / filesystem";
    if (isElastic) return "Index (optional)";
    return "Database";
  }, [isS3, isDynamo, isBigQuery, isGcp, isAzure, isElastic]);

  const schemaLabel = useMemo(() => {
    if (isBigQuery) return "Dataset";
    if (isSnowflake) return "Schema";
    return "Schema / dataset";
  }, [isBigQuery, isSnowflake]);

  return (
    <div className="dt-modal-overlay" onClick={onClose} role="presentation">
      <div
        className={`dt-modal ${step === "pick" ? "df2-modal-xl" : "dt-modal-lg"}`}
        onClick={(e) => e.stopPropagation()}
        role="dialog"
        aria-modal="true"
      >
        <div className="dt-modal-header">
          <div>
            <h2 className="dt-modal-title">
              {step === "pick" ? "Choose a connector" : editing ? "Edit connection" : "Configure connection"}
            </h2>
            <p className="dt-modal-subtitle">
              {step === "pick"
                ? "Transfer-ready connectors support full migration. Test-only entries save credentials but cannot transfer yet."
                : `${catalogItem?.label ?? type} · choose the authentication mode that matches your environment`}
            </p>
          </div>
          <button type="button" className="df2-btn df2-btn-ghost df2-btn-sm" onClick={onClose} aria-label="Close">
            <DtIcon name="x" />
          </button>
        </div>

        <div className="dt-modal-body">
          {step === "pick" ? (
            <ConnectorCatalogPanel
              role="all"
              onSelect={handleCatalogPick}
              limit={120}
              compact
              requireAvailable={false}
              initialStatus=""
            />
          ) : (
            <>
              <div className="df2-form-row">
                <div className="df2-field">
                  <label className="df2-label">Connection name</label>
                  <input
                    className="df2-input"
                    placeholder="Production PostgreSQL"
                    value={name}
                    onChange={(e) => {
                      setName(e.target.value);
                      setFieldError(null);
                    }}
                  />
                </div>
                <div className="df2-field">
                  <label className="df2-label">Type</label>
                  <input className="df2-input" value={catalogItem?.label ?? type} readOnly disabled />
                </div>
              </div>

              {modeOptions.length > 1 && (
                <div className="df2-form-row">
                  <div className="df2-field">
                    <label className="df2-label">Authentication mode</label>
                    <select
                      className="df2-input"
                      value={authMode}
                      onChange={(e) => {
                        setAuthMode(e.target.value as AuthMode);
                        setFieldError(null);
                      }}
                    >
                      {modeOptions.map((opt) => (
                        <option key={opt.value} value={opt.value}>
                          {opt.label}
                        </option>
                      ))}
                    </select>
                  </div>
                </div>
              )}

              {showConnectionString && (
                <div className="df2-field" style={{ marginTop: 8 }}>
                  <label className="df2-label">Connection string</label>
                  <input
                    className="df2-input"
                    placeholder={isMongo ? "mongodb://user:pass@host:27017/db" : isAzure ? "DefaultEndpointsProtocol=..." : "driver://user:pass@host:port/db"}
                    value={connectionString}
                    onChange={(e) => setConnectionString(e.target.value)}
                  />
                </div>
              )}

              {showServiceAccount && (
                <div className="df2-field" style={{ marginTop: 8 }}>
                  <label className="df2-label">Service account JSON or file path</label>
                  <textarea
                    className="df2-input"
                    rows={isGcp ? 6 : 3}
                    placeholder={isGcp ? '{\n  "type": "service_account",\n  ...\n}' : "/path/to/service-account.json"}
                    value={serviceAccount}
                    onChange={(e) => setServiceAccount(e.target.value)}
                  />
                  {isGcp && (
                    <p className="df2-field-note df2-label-hint">
                      Paste the JSON contents, or enter an absolute path to the key file on the server.
                    </p>
                  )}
                </div>
              )}

              {showApiKey && (
                <div className="df2-field" style={{ marginTop: 8 }}>
                  <label className="df2-label">API key</label>
                  <input
                    type="password"
                    className="df2-input"
                    autoComplete="new-password"
                    placeholder="••••••••"
                    value={apiKey}
                    onChange={(e) => setApiKey(e.target.value)}
                  />
                </div>
              )}

              {(showUserPass || showAwsKeys || showApiKey || showServiceAccount || showConnectionString) && (
                <div className="df2-form-row" style={{ marginTop: 8 }}>
                  <div className="df2-field">
                    <label className="df2-label">{hostLabel}</label>
                    <input
                      className="df2-input"
                      placeholder={isAwsKeyed ? "us-east-1" : isBigQuery ? "bigquery.googleapis.com" : "localhost"}
                      value={host}
                      onChange={(e) => setHost(e.target.value)}
                    />
                  </div>
                  {(!isBigQuery && !isAwsKeyed && !isGcp) && (
                    <div className="df2-field">
                      <label className="df2-label">Port</label>
                      <input
                        type="number"
                        className="df2-input"
                        value={port}
                        onChange={(e) => setPort(parseInt(e.target.value, 10) || 0)}
                      />
                    </div>
                  )}
                </div>
              )}

              {(showUserPass || showAwsKeys || showApiKey || showServiceAccount || showConnectionString) && (
                <div className="df2-form-row">
                  <div className="df2-field">
                    <label className="df2-label">{databaseLabel}</label>
                    <input
                      className="df2-input"
                      placeholder={isS3 ? "my-data-bucket" : isBigQuery ? "dataflow-project" : "dataflow"}
                      value={database}
                      onChange={(e) => setDatabase(e.target.value)}
                    />
                  </div>
                  {(isBigQuery || isSnowflake || isGcp) && (
                    <div className="df2-field">
                      <label className="df2-label">{schemaLabel}</label>
                      <input
                        className="df2-input"
                        placeholder={isBigQuery ? "dataflow" : "PUBLIC"}
                        value={schema}
                        onChange={(e) => setSchema(e.target.value)}
                      />
                    </div>
                  )}
                </div>
              )}

              {showUserPass && (
                <div className="df2-form-row">
                  <div className="df2-field">
                    <label className="df2-label">Username</label>
                    <input
                      className="df2-input"
                      autoComplete="off"
                      value={username}
                      onChange={(e) => setUsername(e.target.value)}
                    />
                  </div>
                  <div className="df2-field">
                    <label className="df2-label">Password</label>
                    <input
                      type="password"
                      className="df2-input"
                      autoComplete="new-password"
                      placeholder={editing ? "Leave blank to keep" : ""}
                      value={password}
                      onChange={(e) => setPassword(e.target.value)}
                    />
                  </div>
                </div>
              )}

              {showAwsKeys && (
                <div className="df2-form-row">
                  <div className="df2-field">
                    <label className="df2-label">Access Key ID</label>
                    <input
                      className="df2-input"
                      autoComplete="off"
                      placeholder={isDynamo ? "AKIA… or local" : "AKIA…"}
                      value={username}
                      onChange={(e) => setUsername(e.target.value)}
                    />
                  </div>
                  <div className="df2-field">
                    <label className="df2-label">Secret Access Key</label>
                    <input
                      type="password"
                      className="df2-input"
                      autoComplete="new-password"
                      placeholder={isDynamo ? "Optional for DynamoDB Local" : ""}
                      value={password}
                      onChange={(e) => setPassword(e.target.value)}
                    />
                  </div>
                </div>
              )}

              {isSnowflake && showUserPass && (
                <div className="df2-form-row">
                  <div className="df2-field">
                    <label className="df2-label">Warehouse</label>
                    <input
                      className="df2-input"
                      placeholder="COMPUTE_WH"
                      value={warehouse}
                      onChange={(e) => setWarehouse(e.target.value)}
                    />
                  </div>
                  <div className="df2-field">
                    <label className="df2-label">Role</label>
                    <input
                      className="df2-input"
                      placeholder="ACCOUNTADMIN"
                      value={authRole}
                      onChange={(e) => setAuthRole(e.target.value)}
                    />
                  </div>
                </div>
              )}

              {showUserPass && isConfigurableInStudio(type) && (
                <label className="df2-checkbox" style={{ display: "flex", alignItems: "center", gap: 8, marginTop: 8 }}>
                  <input
                    type="checkbox"
                    checked={ssl}
                    onChange={(e) => setSsl(e.target.checked)}
                  />
                  <span>Use SSL / TLS</span>
                </label>
              )}

              {fieldError && (
                <p style={{ color: "#dc2626", fontSize: 13, margin: "8px 0" }} role="alert">
                  {fieldError}
                </p>
              )}
              {testResult && (
                <span
                  className={`df2-badge ${testResult.success ? "df2-badge-live" : "df2-badge-error"}`}
                  style={{ marginTop: 8 }}
                >
                  {testResult.message}
                </span>
              )}
            </>
          )}
        </div>

        {step === "configure" && (
          <div className="df2-card-footer">
            <button
              type="button"
              className="df2-btn"
              onClick={handleTest}
              disabled={testing}
              aria-busy={testing}
            >
              {testing ? <ButtonLoader label="Testing…" /> : "Test connection"}
            </button>
            <button
              type="button"
              className="df2-btn df2-btn-primary"
              onClick={handleSave}
              disabled={saving}
              aria-busy={saving}
            >
              {saving ? <ButtonLoader label="Saving…" /> : editing ? "Update" : "Save & connect"}
            </button>
          </div>
        )}
      </div>
    </div>
  );
}
