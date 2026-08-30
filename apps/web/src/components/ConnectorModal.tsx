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
  AUTH_MODE_DETAIL,
  AuthMode,
  ConnectorFormConfig,
  FormField,
  getConnectorFormConfig,
  validateConnectorPayload,
} from "../lib/connectorFormConfig";
import { ConnectorIcon } from "../app/brand-icons";
import { CONNECTOR_CATALOG } from "../lib/types";
import {
  isPlaceholderSnowflakeAccount,
  isSnowflakeAccountHostOnly,
  parseSnowflakeUrl,
} from "../lib/snowflakeUrl";
import { getConnectorSetupGuide } from "../lib/connectorSetupGuide";
import { looksLikeUserinfoHost, parseUrlAuthority } from "../lib/urlAuthority";

interface ConnectorModalProps {
  initialType?: string;
  editing?: Connector | null;
  onClose: () => void;
  onSaved: () => void;
}

function isFileFormat(type: string): boolean {
  return [
    "csv", "tsv", "json", "jsonl", "ndjson", "parquet", "excel",
    "avro", "orc", "xml", "pdf", "docx", "html",
  ].includes(type);
}

function inferAuthMode(conn: Connector | null | undefined, type: string): AuthMode {
  const resolved = resolveCatalogIdToType(type);
  if (conn?.auth_mode) return conn.auth_mode as AuthMode;
  if (resolved === "snowflake" && conn?.private_key) return "key_pair";
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
  if (looksLikeUserinfoHost(raw)) {
    const t = type.toLowerCase();
    const scheme = t.includes("mysql") || t.includes("maria") ? "mysql://" : "postgresql://";
    return scheme + raw;
  }
  return raw;
}

function parseUriIfPossible(connectionString: string, typeHint = "postgresql"): { host?: string; port?: number; username?: string; password?: string; database?: string } | null {
  const normalized = normalizeSqlDsn(connectionString, typeHint);
  const parsed = parseUrlAuthority(normalized);
  if (!parsed.host) return null;
  const database = parsed.path.replace(/^\//, "").split("?")[0];
  return {
    host: parsed.host || undefined,
    port: parsed.port || undefined,
    username: parsed.user || undefined,
    password: parsed.password || undefined,
    database: database || undefined,
  };
}

function parseMongoUri(connectionString: string): ReturnType<typeof parseUriIfPossible> {
  const parsed = parseUrlAuthority(connectionString);
  if (!parsed.host || !connectionString.toLowerCase().startsWith("mongodb")) {
    return parseUriIfPossible(connectionString);
  }
  const authMatch = connectionString.match(/[?&](?:authSource|authsource)=([^&#]*)/);
  const authSource = authMatch ? decodeURIComponent(authMatch[1]) : undefined;
  const out: Record<string, string | number | undefined> = { host: parsed.host };
  if (parsed.port) out.port = parsed.port;
  if (parsed.user) out.username = parsed.user;
  if (parsed.password) out.password = parsed.password;
  const db = parsed.path.replace(/^\//, "").split("?")[0];
  if (db) out.database = db;
  if (authSource) out.authSource = authSource;
  return out;
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
  const [host, setHost] = useState(() => {
    const raw = editing?.host ?? defaults.host;
    return isPlaceholderSnowflakeAccount(raw) ? "" : raw;
  });
  const [port, setPort] = useState<number>(editing?.port ?? defaults.port);
  const [database, setDatabase] = useState(editing?.database ?? "");
  const [username, setUsername] = useState(editing?.username ?? "");
  const [password, setPassword] = useState(editing?.password ?? "");
  const [connectionString, setConnectionString] = useState(editing?.connection_string ?? "");
  const [showSecrets, setShowSecrets] = useState(false);
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
  const [helpOpen, setHelpOpen] = useState(false);

  const resolvedType = useMemo(() => resolveCatalogIdToType(type), [type]);
  const isMongo = resolvedType === "mongodb";
  const isSftp = resolvedType === "sftp";
  const isEmail = resolvedType === "email";
  const isSnowflake = resolvedType === "snowflake";

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
    if (isSnowflake) {
      const parsedSf = parseSnowflakeUrl(connectionString);
      if (parsedSf.account && (!host || isPlaceholderSnowflakeAccount(host))) {
        setHost(parsedSf.account);
      }
      if (parsedSf.user && !username) setUsername(parsedSf.user);
      if (parsedSf.password && !password) setPassword(parsedSf.password);
      if (parsedSf.database && !database) setDatabase(parsedSf.database);
      if (parsedSf.schema && !schema) setSchema(parsedSf.schema);
      if (parsedSf.warehouse && !warehouse) setWarehouse(parsedSf.warehouse);
      if (parsedSf.role && !authRole) setAuthRole(parsedSf.role);
      return;
    }
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
  }, [isMongo, isSnowflake, authMode, connectionString, host, port, username, password, database, schema, warehouse, authRole, authSource, resolvedType]);

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

    if (authMode === "user_pass" || authMode === "pat") {
      payload.username = username || undefined;
      payload.password = password || undefined;
      if ((resolvedType === "sftp" || resolvedType === "snowflake") && privateKey.trim()) {
        payload.private_key = privateKey || undefined;
      }
    }
    if (authMode === "key_pair") {
      payload.username = username || undefined;
      payload.password = password || undefined;
      payload.private_key = privateKey || undefined;
    }
    if (authMode === "connection_string" || authMode === "file_path") {
      const cs = (!isMongo && (resolvedType === "postgresql" || resolvedType === "mysql" || resolvedType === "mariadb"))
        ? normalizeSqlDsn(connectionString, resolvedType)
        : connectionString;
      payload.connection_string = cs || undefined;
      // Ensure discrete fields are filled from the DSN so probes never fall back to localhost.
      if (
        cs &&
        (resolvedType === "postgresql" ||
          resolvedType === "mysql" ||
          resolvedType === "mariadb" ||
          isGenericSql(resolvedType) ||
          resolvedType === "redis" ||
          resolvedType === "sftp")
      ) {
        const parsed = parseUriIfPossible(cs, resolvedType);
        if (parsed?.host) payload.host = parsed.host;
        if (parsed?.port) payload.port = parsed.port;
        if (parsed?.username) payload.username = parsed.username;
        if (parsed?.password) payload.password = parsed.password;
        if (parsed?.database) payload.database = parsed.database;
      }
      if (cs && isSnowflake) {
        const parsedSf = parseSnowflakeUrl(cs);
        if (parsedSf.account) payload.host = parsedSf.account;
        if (parsedSf.user) payload.username = parsedSf.user;
        if (parsedSf.password) payload.password = parsedSf.password;
        if (parsedSf.database) payload.database = parsedSf.database;
        if (parsedSf.schema) payload.schema = parsedSf.schema;
        if (parsedSf.warehouse) payload.warehouse = parsedSf.warehouse;
        if (parsedSf.role) payload.auth_role = parsedSf.role;
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
    if (resolvedType === "snowflake" || resolvedType === "iceberg") {
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
      const detail = e instanceof Error && e.message ? e.message : "Could not save connector settings.";
      toast({ title: "Save failed", message: detail, tone: "error" });
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
      return (
        <div className="df2-secret-field">
          <input {...commonProps} type={showSecrets ? "text" : "password"} autoComplete="new-password" />
          <button
            type="button"
            className="df2-secret-toggle"
            onClick={() => setShowSecrets((s) => !s)}
            aria-pressed={showSecrets}
          >
            {showSecrets ? "Hide" : "Show"}
          </button>
        </div>
      );
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

  const authDetail = (mode: { value: AuthMode; description?: string }) => {
    if (isSnowflake && mode.value === "user_pass") return "Account, user, password.";
    if (isSnowflake && mode.value === "pat") return "Snowsight token.";
    if (isSnowflake && mode.value === "key_pair") return "PKCS#8 key.";
    if (isSnowflake && mode.value === "connection_string") return "snowflake:// login URL.";
    return mode.description || AUTH_MODE_DETAIL[mode.value];
  };

  const setupGuide = getConnectorSetupGuide(resolvedType || type);

  const fieldSpan = (field: FormField): "full" | "half" => {
    if (
      field.type === "textarea" ||
      field.type === "checkbox" ||
      field.key === "connection_string" ||
      field.key === "privateKey" ||
      field.key === "serviceAccount"
    ) {
      return "full";
    }
    return "half";
  };

  return (
    <div className="df2-modal-overlay" onClick={onClose} role="presentation">
      <div
        className={`df2-modal ${step === "pick" ? "df2-modal-full" : "df2-conn-setup"}${helpOpen ? " is-help-open" : ""}`}
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
                ? "Pick one product. Cloud and edition tiles (Snowflake on AWS, Standard) use the same login."
                : "Name the connection, pick how you log in, then Test before you save."}
            </p>
          </div>
          <div className="df2-modal-header-actions">
            {step === "configure" && (
              <button
                type="button"
                className="df2-btn df2-btn-ghost df2-btn-sm"
                onClick={() => setHelpOpen((open) => !open)}
                aria-expanded={helpOpen}
                aria-controls="df2-conn-help"
              >
                How to set up
              </button>
            )}
            <button type="button" className="df2-close-btn" onClick={onClose} aria-label="Close">
              <DtIcon name="x" size={16} />
            </button>
          </div>
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
              collapseAliases
            />
          ) : (
            <div className="df2-conn-setup-layout">
              <aside className="df2-conn-setup-aside">
                <div className="df2-conn-setup-identity">
                  <span className="df2-conn-setup-icon" aria-hidden>
                    <ConnectorIcon id={resolvedType || type} size={36} />
                  </span>
                  <div>
                    <p className="df2-conn-setup-type">{catalogItem?.label ?? formConfig.label ?? type}</p>
                    <p className="df2-conn-setup-type-hint">
                      {isFileFormat(resolvedType)
                        ? "Path or URL only — no database host."
                        : "Pick the login method your admin issued."}
                    </p>
                  </div>
                </div>
                <div className="df2-field">
                  <label className="df2-label" htmlFor="df2-conn-name">
                    Connection name
                    <span className="df2-req">*</span>
                  </label>
                  <input
                    id="df2-conn-name"
                    className="df2-input"
                    placeholder="Production Snowflake"
                    value={name}
                    onChange={(e) => {
                      setName(e.target.value);
                      setFieldError(null);
                    }}
                  />
                </div>
                {formConfig.authModes.length > 1 && (
                  <div className="df2-field">
                    <p className="df2-label" id="df2-auth-mode-label">
                      Authentication
                    </p>
                    <div className="df2-auth-cards" role="tablist" aria-labelledby="df2-auth-mode-label">
                      {formConfig.authModes.map((opt) => (
                        <button
                          key={opt.value}
                          type="button"
                          role="tab"
                          aria-selected={authMode === opt.value}
                          className={`df2-auth-card${authMode === opt.value ? " is-active" : ""}`}
                          onClick={() => {
                            setAuthMode(opt.value);
                            setFieldError(null);
                            setTestResult(null);
                          }}
                        >
                          <strong>{opt.label}</strong>
                          <span>{authDetail(opt)}</span>
                        </button>
                      ))}
                    </div>
                  </div>
                )}
              </aside>
              <div className="df2-conn-setup-main">
                <div className="df2-conn-setup-main-head">
                  <h3 className="df2-conn-setup-section">{currentAuthMode?.label || "Credentials"}</h3>
                  <p className="df2-conn-setup-section-hint">
                    {currentAuthMode ? authDetail(currentAuthMode) : "Enter the login fields."}
                  </p>
                </div>
                {currentAuthMode && (
                  <div className="df2-conn-setup-fields">
                    {currentAuthMode.fields.map((field) => (
                      <div
                        key={field.key}
                        className={`df2-field${fieldSpan(field) === "full" ? " is-full" : ""}`}
                      >
                        {field.type !== "checkbox" && (
                          <label className="df2-label" htmlFor={field.key}>
                            {field.label}
                            {!field.optional && <span className="df2-req">*</span>}
                          </label>
                        )}
                        {renderField(field)}
                      </div>
                    ))}
                  </div>
                )}
                {isSnowflake &&
                  authMode === "connection_string" &&
                  isSnowflakeAccountHostOnly(parseSnowflakeUrl(connectionString)) && (
                  <div className="df2-conn-probe is-fail" role="status">
                    <p className="df2-conn-probe-msg">
                      That looks like a Snowflake account host, not a login URL.
                    </p>
                    <p className="df2-conn-probe-hint">
                      Switch to Username &amp; password, or paste
                      {" "}<code>snowflake://user:password@account/DATABASE/SCHEMA?warehouse=COMPUTE_WH</code>.
                    </p>
                    <button
                      type="button"
                      className="df2-btn df2-btn-sm"
                      onClick={() => {
                        const parsedSf = parseSnowflakeUrl(connectionString);
                        if (parsedSf.account) setHost(parsedSf.account);
                        setAuthMode("user_pass");
                        setFieldError(null);
                        setTestResult(null);
                      }}
                    >
                      Use Username &amp; password
                    </button>
                  </div>
                )}
                {fieldError && (
                  <p className="df2-field-error-text" role="alert">
                    {fieldError}
                  </p>
                )}
                {testResult && (
                  <div
                    className={`df2-conn-probe ${testResult.success ? "is-ok" : "is-fail"}`}
                    role={testResult.success ? "status" : "alert"}
                  >
                    <div className="df2-conn-probe-head">
                      <span className={`df2-badge ${testResult.success ? "df2-badge-live" : "df2-badge-error"}`}>
                        {testResult.success ? "Connected" : "Not connected"}
                      </span>
                      {testResult.success && testResult.source_ha && (
                        <span
                          className="df2-badge df2-badge-live"
                          title={String(testResult.source_ha.message || "")}
                        >
                          HA: {String(testResult.source_ha.role || "—")}
                          {testResult.source_ha.topology && testResult.source_ha.topology !== "none"
                            ? ` · ${String(testResult.source_ha.topology)}`
                            : ""}
                        </span>
                      )}
                    </div>
                    <p className="df2-conn-probe-msg">{testResult.message}</p>
                    {!testResult.success && /ssl|tls|certificate/i.test(testResult.message) && (
                      <p className="df2-conn-probe-hint">
                        Look for the <strong>SSL / TLS</strong> toggle in the fields above — local
                        emulators and plaintext Docker ports usually need it off.
                      </p>
                    )}
                    {!testResult.success && isSnowflake && (
                      <button
                        type="button"
                        className="df2-btn df2-btn-ghost df2-btn-sm"
                        onClick={() => setHelpOpen(true)}
                      >
                        How to set up
                      </button>
                    )}
                  </div>
                )}
              </div>
            </div>
          )}
        </div>

        {step === "configure" && helpOpen && (
          <aside id="df2-conn-help" className="df2-conn-help" role="dialog" aria-label={setupGuide.title}>
            <div className="df2-conn-help-head">
              <h3 className="df2-conn-help-title">{setupGuide.title}</h3>
              <button
                type="button"
                className="df2-btn df2-btn-ghost df2-btn-sm"
                onClick={() => setHelpOpen(false)}
                aria-label="Close setup help"
              >
                Close
              </button>
            </div>
            <ol className="df2-conn-help-steps">
              {setupGuide.steps.map((stepText) => (
                <li key={stepText}>{stepText}</li>
              ))}
            </ol>
          </aside>
        )}

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
