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
  isGenericSql,
  resolveCatalogIdToType,
} from "../lib/connectorTypes";
import {
  AuthMode,
  ConnectorFormConfig,
  FormField,
  getConnectorFormConfig,
  validateConnectorPayload,
} from "../lib/connectorFormConfig";
import { CONNECTOR_CATALOG } from "../lib/types";

interface ConnectorModalProps {
  initialType?: string;
  editing?: Connector | null;
  onClose: () => void;
  onSaved: () => void;
}

function isFileFormat(type: string): boolean {
  return ["csv", "tsv", "json", "jsonl", "ndjson", "parquet", "excel"].includes(type);
}

function inferAuthMode(conn: Connector | null | undefined, type: string): AuthMode {
  const resolved = resolveCatalogIdToType(type);
  if (conn?.auth_mode) return conn.auth_mode as AuthMode;
  if (isFileFormat(resolved)) return "file_path";
  if (conn?.api_key) return "api_key";
  if (conn?.service_account) return "service_account";
  if (conn?.connection_string) return "connection_string";
  if (["s3", "dynamodb"].includes(resolved)) return "aws_keys";
  if (["bigquery", "gcs"].includes(resolved)) return "service_account";
  if (["salesforce", "hubspot", "stripe", "rest_api"].includes(resolved)) return "api_key";
  if (resolved === "elasticsearch") return conn?.username ? "user_pass" : "api_key";
  if (["weaviate", "pinecone"].includes(resolved)) return "api_key";
  if (resolved === "milvus") return conn?.api_key ? "api_key" : "user_pass";
  if (resolved === "adls" && conn?.connection_string) return "connection_string";
  if (resolved === "sftp" && conn?.connection_string) return "connection_string";
  if (resolved === "email" && conn?.connection_string) return "connection_string";
  return "user_pass";
}

function normalizeSqlDsn(connectionString: string, type: string): string {
  const raw = connectionString.trim();
  if (!raw) return "";
  if (raw.includes("://")) return raw;
  // user:pass@host:port/db — common Railway paste without scheme
  if (/^[^:/@\s]+:[^@\s]+@[^:/?\s]+/.test(raw)) {
    const t = type.toLowerCase();
    const scheme = t.includes("mysql") || t.includes("maria") ? "mysql://" : "postgresql://";
    return scheme + raw;
  }
  return raw;
}

function parseUriIfPossible(connectionString: string, typeHint = "postgresql"): { host?: string; port?: number; username?: string; password?: string; database?: string } | null {
  const normalized = normalizeSqlDsn(connectionString, typeHint);
  try {
    const url = new URL(normalized);
    const database = url.pathname.replace(/^\//, "").split("?")[0];
    return {
      host: url.hostname || undefined,
      port: url.port ? parseInt(url.port, 10) : undefined,
      username: decodeURIComponent(url.username || ""),
      password: decodeURIComponent(url.password || ""),
      database: database || undefined,
    };
  } catch {
    return null;
  }
}

function parseMongoUri(connectionString: string): ReturnType<typeof parseUriIfPossible> {
  const match = connectionString.match(/^mongodb(?:\+srv)?:\/\/(?:([^:@]+)(?::([^@]+))?@)?([^\/?#:]+)(?::(\d+))?\/?([^?#]*)?/);
  if (match) {
    const [, user, pass, rawHost, rawPort, rawDb] = match;
    const authMatch = connectionString.match(/[?&](?:authSource|authsource)=([^&#]*)/);
    const authSource = authMatch ? decodeURIComponent(authMatch[1]) : undefined;
    const out: Record<string, string | number | undefined> = { host: rawHost };
    if (rawPort) out.port = parseInt(rawPort, 10);
    if (user) out.username = user;
    if (pass) out.password = pass;
    if (rawDb) out.database = rawDb;
    if (authSource) out.authSource = authSource;
    return out;
  }
  return parseUriIfPossible(connectionString);
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
  const [showConnStr, setShowConnStr] = useState(false);
  const [schema, setSchema] = useState(editing?.schema ?? "");
  const [warehouse, setWarehouse] = useState(editing?.warehouse ?? "");
  const [authRole, setAuthRole] = useState(editing?.auth_role ?? "");
  const [authSource, setAuthSource] = useState(editing?.auth_source ?? "");
  const [apiKey, setApiKey] = useState(editing?.api_key ?? "");
  const [serviceAccount, setServiceAccount] = useState(editing?.service_account ?? "");
  const [privateKey, setPrivateKey] = useState(editing?.private_key ?? "");
  const [endpointUrl, setEndpointUrl] = useState(editing?.endpoint_url ?? "");
  const [pathStyle, setPathStyle] = useState(editing?.path_style ?? false);
  const [ssl, setSsl] = useState(editing?.ssl ?? false);
  const [authMode, setAuthMode] = useState<AuthMode>(inferAuthMode(editing, startType));

  const resolvedType = useMemo(() => resolveCatalogIdToType(type), [type]);
  const isMongo = resolvedType === "mongodb";
  const isSftp = resolvedType === "sftp";
  const isEmail = resolvedType === "email";

  const formConfig = useMemo<ConnectorFormConfig>(() => getConnectorFormConfig(type), [type]);

  useEffect(() => {
    const available = formConfig.authModes.map((m) => m.value);
    if (!available.includes(authMode)) {
      setAuthMode(formConfig.defaultAuthMode);
    }
  }, [formConfig, authMode]);

  // Auto-parse connection strings for SFTP / Email / MongoDB / Redis / Elasticsearch / Azure
  useEffect(() => {
    if (authMode !== "connection_string" || !connectionString.trim()) return;
    const parsed = isMongo
      ? parseMongoUri(connectionString)
      : parseUriIfPossible(connectionString, resolvedType);
    if (!parsed) return;
    if (parsed.host && !host) setHost(parsed.host);
    if (parsed.port && !port) setPort(parsed.port);
    if (parsed.username && !username) setUsername(parsed.username);
    if (parsed.password && !password) setPassword(parsed.password);
    if (parsed.database && !database) setDatabase(parsed.database);
    if (isMongo && (parsed as Record<string, unknown>).authSource && !authSource) {
      setAuthSource((parsed as Record<string, string>).authSource || "");
    }
    // Normalize scheme-less SQL DSNs in the field so Test/Save send a real URL
    if (!isMongo && !connectionString.includes("://") && /^[^:/@\s]+:[^@\s]+@/.test(connectionString.trim())) {
      const normalized = normalizeSqlDsn(connectionString, resolvedType);
      if (normalized !== connectionString) setConnectionString(normalized);
    }
    // Try to detect TLS from scheme
    if (connectionString.toLowerCase().startsWith("smtps://") || connectionString.toLowerCase().startsWith("rediss://") || connectionString.toLowerCase().startsWith("https://")) {
      setSsl(true);
    }
  }, [isMongo, authMode, connectionString, host, port, username, password, database, authSource, resolvedType]);

  const applyType = (nextType: string) => {
    const d = getConnectorDefaults(nextType);
    const cfg = getConnectorFormConfig(nextType);
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
    setAuthSource("");
    setApiKey("");
    setServiceAccount("");
    setPrivateKey("");
    setEndpointUrl("");
    setPathStyle(false);
    setSsl(false);
    setAuthMode(cfg.defaultAuthMode);
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
    applyType(resolveCatalogIdToType(item.id as string));
  };

  const values = useMemo(
    () => ({
      name,
      host,
      port,
      database,
      username,
      password,
      connection_string: connectionString,
      schema,
      warehouse,
      authRole,
      authSource,
      apiKey,
      serviceAccount,
      privateKey,
      endpointUrl,
      pathStyle,
      ssl,
    }),
    [name, host, port, database, username, password, connectionString, schema, warehouse, authRole, authSource, apiKey, serviceAccount, privateKey, endpointUrl, pathStyle, ssl]
  );

  const [testing, setTesting] = useState(false);
  const [saving, setSaving] = useState(false);
  const [testResult, setTestResult] = useState<{
    success: boolean;
    message: string;
    source_ha?: Record<string, unknown>;
  } | null>(null);
  const [fieldError, setFieldError] = useState<string | null>(null);

  const validate = () => {
    if (!name.trim()) {
      setFieldError("Connection name is required.");
      return false;
    }
    const msg = validateConnectorPayload(type, values, authMode);
    if (msg) {
      setFieldError(msg);
      return false;
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
      host: isGcpConnector(resolvedType) ? "bigquery.googleapis.com" : isAwsConnector(resolvedType) ? host || "us-east-1" : host,
      port: isGcpConnector(resolvedType) || isAwsConnector(resolvedType) ? 443 : port,
      database,
      schema: resolvedType === "bigquery" || resolvedType === "snowflake" ? schema : undefined,
      ssl,
      auth_mode: authMode,
      auth_role: resolvedType === "snowflake" ? authRole : undefined,
      auth_source: resolvedType === "mongodb" || resolvedType === "email" ? authSource : undefined,
    };

    if (authMode === "user_pass") {
      payload.username = username || undefined;
      payload.password = password || undefined;
      if (resolvedType === "sftp" && privateKey.trim()) {
        payload.private_key = privateKey || undefined;
      }
    }
    if (authMode === "connection_string" || authMode === "file_path") {
      const cs = (!isMongo && (resolvedType === "postgresql" || resolvedType === "mysql" || resolvedType === "mariadb"))
        ? normalizeSqlDsn(connectionString, resolvedType)
        : connectionString;
      payload.connection_string = cs || undefined;
      // Ensure discrete fields are filled from the DSN so probes never fall back to localhost.
      if (cs && (resolvedType === "postgresql" || resolvedType === "mysql" || resolvedType === "mariadb")) {
        const parsed = parseUriIfPossible(cs, resolvedType);
        if (parsed?.host) payload.host = parsed.host;
        if (parsed?.port) payload.port = parsed.port;
        if (parsed?.username) payload.username = parsed.username;
        if (parsed?.password) payload.password = parsed.password;
        if (parsed?.database) payload.database = parsed.database;
      }
      if (resolvedType === "sftp" && privateKey.trim()) {
        payload.private_key = privateKey || undefined;
      }
    }
    if (authMode === "service_account") {
      payload.service_account = serviceAccount || undefined;
    }
    if (authMode === "api_key") {
      payload.api_key = apiKey || undefined;
    }
    if (authMode === "aws_keys" || resolvedType === "s3" || resolvedType === "dynamodb") {
      payload.username = username || undefined;
      payload.password = password || undefined;
      if (endpointUrl.trim()) payload.endpoint_url = endpointUrl || undefined;
      if (resolvedType === "s3" && pathStyle) payload.path_style = pathStyle;
    }
    if (resolvedType === "snowflake") {
      payload.warehouse = warehouse || undefined;
    }
    if (isGcpConnector(resolvedType) && !serviceAccount.trim() && connectionString.trim()) {
      payload.connection_string = connectionString || undefined;
    }

    return payload;
  };

  const handleTest = async () => {
    if (!validate()) return;
    setTesting(true);
    setTestResult(null);
    try {
      // Always use buildPayload so connection-string mode fills host/port/user
      // from the DSN — never send stale localhost:5432 defaults that override the URL.
      const built = buildPayload();
      const result = await testConnection({
        type: String(built.type || type),
        host: built.host as string | undefined,
        port: built.port as number | undefined,
        database: String(built.database || ""),
        schema: built.schema as string | undefined,
        username: built.username as string | undefined,
        password: built.password as string | undefined,
        connection_string: built.connection_string as string | undefined,
        service_account: built.service_account as string | undefined,
        api_key: built.api_key as string | undefined,
        warehouse: built.warehouse as string | undefined,
        auth_role: built.auth_role as string | undefined,
        auth_mode: String(built.auth_mode || authMode),
        auth_source: built.auth_source as string | undefined,
        private_key: built.private_key as string | undefined,
        endpoint_url: built.endpoint_url as string | undefined,
        path_style: built.path_style as boolean | undefined,
        ssl: Boolean(built.ssl),
      });
      setTestResult(result);
      if (resolvedType === "mongodb" && result.success) {
        const authMatch = result.message.match(/authSource=([^\s)]+)/);
        if (authMatch && !authSource) setAuthSource(authMatch[1]);
      }
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
      const payload = {
        ...buildPayload(),
        // Carry the in-form Test result onto the saved profile so the list
        // shows "Test passed" immediately (list Test uses a different endpoint).
        ...(testResult ? { last_test_ok: testResult.success } : {}),
      };
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

  const handleFieldChange = (key: string, value: string | number | boolean) => {
    setFieldError(null);
    // Credential edits invalidate the prior in-form Test result.
    setTestResult(null);
    switch (key) {
      case "host":
        setHost(value as string);
        break;
      case "port":
        setPort(typeof value === "number" ? value : parseInt(value as string, 10) || 0);
        break;
      case "database":
        setDatabase(value as string);
        break;
      case "username":
        setUsername(value as string);
        break;
      case "password":
        setPassword(value as string);
        break;
      case "connection_string":
        setConnectionString(value as string);
        break;
      case "schema":
        setSchema(value as string);
        break;
      case "warehouse":
        setWarehouse(value as string);
        break;
      case "authRole":
        setAuthRole(value as string);
        break;
      case "authSource":
        setAuthSource(value as string);
        break;
      case "apiKey":
        setApiKey(value as string);
        break;
      case "serviceAccount":
        setServiceAccount(value as string);
        break;
      case "privateKey":
        setPrivateKey(value as string);
        break;
      case "endpointUrl":
        setEndpointUrl(value as string);
        break;
      case "pathStyle":
        setPathStyle(Boolean(value));
        break;
      case "ssl":
        setSsl(Boolean(value));
        break;
    }
  };

  const renderField = (field: FormField) => {
    const value = (values as Record<string, unknown>)[field.key];
    const inputClass = "df2-input";
    const commonProps = {
      id: field.key,
      name: field.key,
      className: inputClass,
      placeholder: field.placeholder,
      value: typeof value === "boolean" ? undefined : (value as string | number) ?? "",
      onChange: (e: React.ChangeEvent<HTMLInputElement | HTMLTextAreaElement | HTMLSelectElement>) =>
        handleFieldChange(field.key, e.target.type === "checkbox" ? (e.target as HTMLInputElement).checked : e.target.value),
    };

    if (field.type === "textarea") {
      return (
        <textarea
          {...commonProps}
          rows={field.rows || 3}
          onChange={(e) => handleFieldChange(field.key, e.target.value)}
        />
      );
    }
    if (field.type === "checkbox") {
      return (
        <label className="df2-checkbox" style={{ display: "flex", alignItems: "center", gap: 8, marginTop: 8 }}>
          <input
            type="checkbox"
            checked={Boolean(value)}
            onChange={(e) => handleFieldChange(field.key, e.target.checked)}
          />
          <span>{field.label}</span>
        </label>
      );
    }
    if (field.type === "password") {
      return <input {...commonProps} type="password" autoComplete="new-password" />;
    }
    if (field.type === "number") {
      return (
        <input
          {...commonProps}
          type="number"
          value={value as number}
          onChange={(e) => handleFieldChange(field.key, parseInt(e.target.value, 10) || 0)}
        />
      );
    }
    return <input {...commonProps} type="text" autoComplete="off" />;
  };

  const currentAuthMode = formConfig.authModes.find((m) => m.value === authMode) || formConfig.authModes[0];

  const catalogItem = CONNECTOR_CATALOG.find((c) => c.id === type);

  return (
    <div className="df2-modal-overlay" onClick={onClose} role="presentation">
      <div
        className="df2-modal df2-modal-full"
        onClick={(e) => e.stopPropagation()}
        role="dialog"
        aria-modal="true"
        aria-labelledby="df2-connector-modal-title"
      >
        <div className="df2-modal-header">
          <div>
            <h2 id="df2-connector-modal-title" className="df2-modal-title">
              {step === "pick" ? "Choose a connector" : editing ? "Edit connection" : "Configure connection"}
            </h2>
            <p className="df2-modal-subtitle">
              {step === "pick"
                ? "Transfer-ready connectors support full migration. Test-only entries save credentials but cannot transfer yet."
                : `${catalogItem?.label ?? formConfig.label ?? type} · choose the authentication mode that matches your environment`}
            </p>
          </div>
          <button type="button" className="df2-btn df2-btn-ghost df2-btn-sm" onClick={onClose} aria-label="Close">
            <DtIcon name="x" />
          </button>
        </div>

        <div className="df2-modal-body">
          {step === "pick" ? (
            <ConnectorCatalogPanel
              role="all"
              onSelect={handleCatalogPick}
              limit={200}
              compact
              requireAvailable={false}
              initialStatus="live"
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
                  <input className="df2-input" value={catalogItem?.label ?? formConfig.label ?? type} readOnly disabled />
                </div>
              </div>

              {isFileFormat(resolvedType) && (
                <p className="df2-field-note df2-label-hint" style={{ marginTop: 8, marginBottom: 12 }}>
                  File format connectors only need a path or URL. No database host, port, or credentials are required.
                </p>
              )}

              {formConfig.authModes.length > 1 && (
                <div className="df2-form-row">
                  <div className="df2-field">
                    <label className="df2-label">Authentication mode</label>
                    <select
                      className="df2-input"
                      value={authMode}
                      onChange={(e) => {
                        setAuthMode(e.target.value as AuthMode);
                        setFieldError(null);
                        setTestResult(null);
                      }}
                    >
                      {formConfig.authModes.map((opt) => (
                        <option key={opt.value} value={opt.value}>
                          {opt.label}
                        </option>
                      ))}
                    </select>
                  </div>
                </div>
              )}

              {currentAuthMode && (
                <div className="df2-form-fields">
                  {currentAuthMode.fields.map((field) => (
                    <div key={field.key} className="df2-field" style={{ marginTop: 8 }}>
                      <label className="df2-label" htmlFor={field.key}>
                        {field.label}
                        {!field.optional && <span style={{ color: "#dc2626", marginLeft: 4 }}>*</span>}
                      </label>
                      {renderField(field)}
                      {field.hint && <p className="df2-field-note df2-label-hint">{field.hint}</p>}
                    </div>
                  ))}
                </div>
              )}

              {authMode === "connection_string" && currentAuthMode?.fields.some((f) => f.key === "connection_string" && f.sensitive) && (
                <button
                  type="button"
                  style={{ marginTop: 4, background: "none", border: "none", padding: 0, cursor: "pointer", fontSize: 12, color: "#6b7280" }}
                  onClick={() => setShowConnStr((s) => !s)}
                >
                  {showConnStr ? "Hide connection string" : "Show connection string"}
                </button>
              )}

              {fieldError && (
                <p className="df2-field-error-text" role="alert">
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
              {testResult?.success && testResult.source_ha && (
                <span
                  className="df2-badge df2-badge-live"
                  style={{ marginTop: 8, marginLeft: 8 }}
                  title={String(testResult.source_ha.message || "")}
                >
                  HA: {String(testResult.source_ha.role || "—")}
                  {testResult.source_ha.topology && testResult.source_ha.topology !== "none"
                    ? ` · ${String(testResult.source_ha.topology)}`
                    : ""}
                </span>
              )}
            </>
          )}
        </div>

        {step === "configure" && (
          <div className="df2-modal-footer">
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
