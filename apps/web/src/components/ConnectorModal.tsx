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
  getGenericSqlPlaceholder,
  isAwsConnector,
  isGcpConnector,
  isGenericSql,
  isConfigurableInStudio,
  resolveCatalogIdToType,
  resolveDriverType,
} from "../lib/connectorTypes";
import { CONNECTOR_CATALOG } from "../lib/types";

interface ConnectorModalProps {
  initialType?: string;
  editing?: Connector | null;
  onClose: () => void;
  onSaved: () => void;
}

type AuthMode = "user_pass" | "connection_string" | "service_account" | "aws_keys" | "api_key" | "file_path";

const FILE_FORMATS = ["csv", "tsv", "json", "jsonl", "ndjson", "parquet", "excel"];

function isFileFormat(type: string): boolean {
  return FILE_FORMATS.includes(type);
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
  if (resolved === "elasticsearch") return conn?.username ? "user_pass" : "api_key";
  if (resolved === "adls") return "connection_string";
  if (resolved === "databricks" || resolved === "athena") return "connection_string";
  if (resolved === "sftp") return "connection_string";
  if (resolved === "email") return conn?.connection_string ? "connection_string" : "user_pass";
  if (resolved === "mongodb" && conn?.username) return "user_pass";
  return "user_pass";
}

function authModeOptions(type: string): { value: AuthMode; label: string }[] {
  if (isFileFormat(type)) {
    return [
      { value: "file_path", label: "Local / mounted file path" },
      { value: "connection_string", label: "URL or object-store URI" },
    ];
  }
  const sqlish = ["postgresql", "mysql", "redshift", "mariadb", "sqlite", "generic_sql"].includes(type);
  const genericSql = isGenericSql(type);
  const mongo = type === "mongodb";
  const snowflake = type === "snowflake";
  const elastic = type === "elasticsearch";
  const awsStore = ["s3", "dynamodb"].includes(type);
  const gcp = isGcpConnector(type);
  const azure = type === "adls";
  const sftpOrEmail = type === "sftp" || type === "email";
  const connectionStringOnly = ["databricks", "athena"].includes(type);

  const options: { value: AuthMode; label: string }[] = [];
  if ((sqlish || genericSql || mongo || snowflake || elastic || azure || type === "sftp" || type === "email") && !connectionStringOnly) {
    options.push({ value: "user_pass", label: "Username & password" });
  }
  if (sqlish || genericSql || mongo || snowflake || azure || sftpOrEmail) {
    options.push({ value: "connection_string", label: type === "email" ? "SMTP URL" : type === "sftp" ? "SFTP URL" : "Connection string" });
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

  // Parse a pasted SFTP or SMTP URI into host/port/user/pass/path.
  useEffect(() => {
    if ((isSftp || isEmail) && authMode === "connection_string" && connectionString.trim()) {
      try {
        const url = new URL(connectionString);
        if (url.hostname) setHost(url.hostname);
        if (url.port) setPort(parseInt(url.port, 10));
        if (url.username) setUsername(decodeURIComponent(url.username));
        if (url.password) setPassword(decodeURIComponent(url.password));
        const path = url.pathname.replace(/^\//, "").split("?")[0];
        if (path && !database) setDatabase(path);
      } catch {
        // Leave raw connection string for the backend to parse.
      }
    }
  }, [isSftp, isEmail, authMode, connectionString, database]);

  useEffect(() => {
    if (isMongo && authMode === "connection_string" && connectionString.trim()) {
      try {
        const url = new URL(connectionString);
        if (url.hostname) setHost(url.hostname);
        if (url.port) setPort(parseInt(url.port, 10));
        if (url.username) setUsername(decodeURIComponent(url.username));
        if (url.password) setPassword(decodeURIComponent(url.password));
        const db = url.pathname.replace(/^\//, "").split("?")[0];
        if (db && !database) setDatabase(db);
        const params = url.searchParams;
        if (params.has("ssl") || params.has("tls")) setSsl(true);
        const as = params.get("authSource") || params.get("authsource") || "";
        if (as && !authSource) setAuthSource(as);
      } catch {
        // Fallback for browsers/environments that do not parse mongodb:// URLs.
        const match = connectionString.match(/^mongodb(?:\+srv)?:\/\/(?:([^:@]+)(?::([^@]+))?@)?([^\/?#:]+)(?::(\d+))?\/?([^?#]*)?/);
        if (match) {
          const [, user, pass, rawHost, rawPort, rawDb] = match;
          if (rawHost) setHost(rawHost);
          if (rawPort) setPort(parseInt(rawPort, 10));
          if (user) setUsername(user);
          if (pass) setPassword(pass);
          if (rawDb && !database) setDatabase(rawDb);
          const authMatch = connectionString.match(/[?&](?:authSource|authsource)=([^&#]*)/);
          if (authMatch && !authSource) setAuthSource(decodeURIComponent(authMatch[1]));
        }
      }
    }
  }, [isMongo, authMode, connectionString, database, authSource]);

  const [testing, setTesting] = useState(false);
  const [saving, setSaving] = useState(false);
  const [testResult, setTestResult] = useState<{ success: boolean; message: string } | null>(null);
  const [fieldError, setFieldError] = useState<string | null>(null);

  const catalogItem = CONNECTOR_CATALOG.find((c) => c.id === type);
  const isBigQuery = resolvedType === "bigquery";
  const isSnowflake = resolvedType === "snowflake";
  const isDynamo = resolvedType === "dynamodb";
  const isS3 = resolvedType === "s3";
  const isAwsKeyed = isS3 || isDynamo;
  const isElastic = resolvedType === "elasticsearch";
  const isGcp = isGcpConnector(resolvedType);
  const isGcs = resolvedType === "gcs";
  const isAzure = resolvedType === "adls";
  const isRedis = resolvedType === "redis";

  const modeOptions = useMemo(() => authModeOptions(resolvedType), [resolvedType]);

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
    setAuthSource("");
    setApiKey("");
    setServiceAccount("");
    setPrivateKey("");
    setEndpointUrl("");
    setPathStyle(false);
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
    if (authMode === "connection_string" || authMode === "file_path") {
      if (!connectionString.trim()) {
        setFieldError(authMode === "file_path" ? "File path or URL is required." : "Connection string is required.");
        return false;
      }
    } else if (authMode === "service_account") {
      if (!serviceAccount.trim()) {
        setFieldError("Service account JSON or file path is required.");
        return false;
      }
      if (!database.trim()) {
        setFieldError(isGcp ? "Project / bucket is required." : "Database / project is required.");
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
      if ((isS3 || !local) && (!username.trim() || !password.trim())) {
        setFieldError("AWS Access Key ID and Secret Access Key are required.");
        return false;
      }
    } else if (authMode === "user_pass") {
      if (isBigQuery) {
        if (!database.trim()) {
          setFieldError("GCP project ID is required.");
          return false;
        }
      } else if (!isGcp && !isAwsKeyed && !host.trim()) {
        setFieldError("Host is required.");
        return false;
      }
      if (
        !isGcp &&
        !isAwsKeyed &&
        !isElastic &&
        !isRedis &&
        !isAzure &&
        !["sqlite", "duckdb"].includes(type) &&
        port <= 0
      ) {
        setFieldError("Port is required.");
        return false;
      }
      if (
        !isGcp &&
        !isAwsKeyed &&
        !isElastic &&
        !isRedis &&
        !isBigQuery &&
        !["sqlite", "duckdb"].includes(type) &&
        (!username.trim() || !password.trim())
      ) {
        setFieldError("Username and password are required.");
        return false;
      }
      if (isAzure && !database.trim()) {
        setFieldError("Container is required.");
        return false;
      }
      if (type === "sftp" && !database.trim() && !connectionString.trim()) {
        setFieldError("Remote file path is required. Provide it as the SFTP URL or the path field.");
        return false;
      }
      if (type === "email" && !database.trim()) {
        setFieldError("At least one recipient (To) is required.");
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
      auth_source: isMongo || isEmail ? authSource : undefined,
    };

    if (authMode === "user_pass") {
      payload.username = username || undefined;
      payload.password = password || undefined;
      if (isSftp && privateKey.trim()) {
        payload.private_key = privateKey || undefined;
      }
    }
    if (authMode === "connection_string" || authMode === "file_path") {
      payload.connection_string = connectionString || undefined;
      if (isSftp && privateKey.trim()) {
        payload.private_key = privateKey || undefined;
      }
    }
    if (authMode === "service_account") {
      payload.service_account = serviceAccount || undefined;
    }
    if (authMode === "api_key") {
      payload.api_key = apiKey || undefined;
    }
    if (authMode === "aws_keys" || isS3 || isDynamo) {
      payload.username = username || undefined;
      payload.password = password || undefined;
      if (endpointUrl.trim()) {
        payload.endpoint_url = endpointUrl || undefined;
      }
      if (isS3 && pathStyle) {
        payload.path_style = pathStyle;
      }
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
        connection_string: authMode === "connection_string" || authMode === "file_path" ? connectionString : undefined,
        service_account: authMode === "service_account" ? serviceAccount : undefined,
        api_key: authMode === "api_key" ? apiKey : undefined,
        warehouse: isSnowflake ? warehouse : undefined,
        auth_role: isSnowflake ? authRole : undefined,
        auth_mode: authMode,
        auth_source: isMongo || isEmail ? authSource : undefined,
        private_key: isSftp && privateKey.trim() ? privateKey : undefined,
        endpoint_url: (isS3 || isDynamo) && endpointUrl.trim() ? endpointUrl : undefined,
        path_style: isS3 && pathStyle ? pathStyle : undefined,
        ssl,
      });
      setTestResult(result);
      if (isMongo && result.success) {
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
  const showFilePath = authMode === "file_path";
  const showServiceAccount = authMode === "service_account";
  const showApiKey = authMode === "api_key";
  const showAwsKeys = authMode === "aws_keys";

  const hostLabel = useMemo(() => {
    if (isAwsKeyed) return isS3 ? "AWS region or endpoint" : "AWS region or local endpoint";
    if (isGcp) return isBigQuery ? "GCP project (optional)" : "Project / endpoint";
    if (isAzure) return "Storage account";
    if (isElastic) return "Host";
    if (isSnowflake) return "Account host";
    if (isSftp) return "SFTP host";
    if (isEmail) return "SMTP host";
    return "Host";
  }, [isAwsKeyed, isS3, isGcp, isBigQuery, isAzure, isElastic, isSnowflake, isSftp, isEmail]);

  const databaseLabel = useMemo(() => {
    if (isFileFormat(type)) return "Filename / pattern";
    if (isS3 || isGcs) return "Bucket name";
    if (isDynamo) return "Table name";
    if (isBigQuery) return "GCP project ID";
    if (isGcp) return "Project ID";
    if (isAzure) return "Container / filesystem";
    if (isElastic) return "Index (optional)";
    if (isRedis) return "Database index";
    if (["sqlite", "duckdb"].includes(type)) return "Database file / :memory:";
    if (type === "databricks" || type === "athena") return "Catalog (optional)";
    if (type === "sftp") return "Remote path / directory";
    if (type === "email") return "Recipients (comma-separated)";
    return "Database";
  }, [isS3, isGcs, isDynamo, isBigQuery, isGcp, isAzure, isElastic, isRedis, type]);

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
                  <input className="df2-input" value={catalogItem?.label ?? type} readOnly disabled />
                </div>
              </div>

              {isFileFormat(type) && (
                <p className="df2-field-note df2-label-hint" style={{ marginTop: 8, marginBottom: 12 }}>
                  File format connectors only need a path or URL. No database host, port, or credentials are required.
                </p>
              )}

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
                    type={showConnStr ? "text" : "password"}
                    autoComplete="new-password"
                    placeholder={isMongo ? "mongodb://user:pass@host:27017/db" : isAzure ? "DefaultEndpointsProtocol=..." : isGenericSql(type) || ["mysql", "postgresql", "redshift", "sqlite"].includes(resolveCatalogIdToType(type)) ? getGenericSqlPlaceholder(resolveCatalogIdToType(type)) : "driver://user:pass@host:port/db"}
                    value={connectionString}
                    onChange={(e) => setConnectionString(e.target.value)}
                  />
                  <button
                    type="button"
                    style={{ marginTop: 4, background: "none", border: "none", padding: 0, cursor: "pointer", fontSize: 12, color: "#6b7280" }}
                    onClick={() => setShowConnStr((s) => !s)}
                  >
                    {showConnStr ? "Hide connection string" : "Show connection string"}
                  </button>
                </div>
              )}

              {showFilePath && (
                <div className="df2-field" style={{ marginTop: 8 }}>
                  <label className="df2-label">File path or URL</label>
                  <input
                    className="df2-input"
                    placeholder="/mnt/data/files, s3://bucket/path, or https://host/file.csv"
                    value={connectionString}
                    onChange={(e) => setConnectionString(e.target.value)}
                  />
                  <p className="df2-field-note df2-label-hint">
                    Local path, mounted NAS share, or object-store URI. Filename can be entered below.
                  </p>
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

              {(showUserPass || showAwsKeys || showApiKey || showServiceAccount) && !isFileFormat(type) && (
                <div className="df2-form-row" style={{ marginTop: 8 }}>
                  <div className="df2-field">
                    <label className="df2-label">{hostLabel}</label>
                    <input
                      className="df2-input"
                      placeholder={isS3 ? "us-east-1 or localhost:9000" : isAwsKeyed ? "us-east-1" : isBigQuery ? "bigquery.googleapis.com" : isSnowflake ? "account.snowflakecomputing.com" : "localhost"}
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

              {(showUserPass || showAwsKeys || showApiKey || showServiceAccount || showConnectionString || showFilePath) && (
                <div className="df2-form-row">
                  <div className="df2-field">
                    <label className="df2-label">{databaseLabel}</label>
                    <input
                      className="df2-input"
                      placeholder={
                        isS3 || isGcs
                          ? "my-data-bucket"
                          : isAzure
                            ? "my-container"
                            : isBigQuery
                              ? "dataflow-project"
                              : isRedis
                                ? "0"
                                : ["sqlite", "duckdb"].includes(type)
                                  ? ":memory: or /path/to/db"
                                  : isFileFormat(type)
                                    ? "sample.csv"
                                    : "dataflow"
                      }
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

              {(isMongo || isEmail) && (showConnectionString || showUserPass) && (
                <div className="df2-form-row" style={{ marginTop: 8 }}>
                  <div className="df2-field">
                    <label className="df2-label">{isEmail ? "From address" : "Auth source"}</label>
                    <input
                      className="df2-input"
                      placeholder={isEmail ? "noreply@dataflow.com" : "e.g. admin"}
                      value={authSource}
                      onChange={(e) => setAuthSource(e.target.value)}
                    />
                  </div>
                </div>
              )}

              {showUserPass && (
                <div className="df2-form-row">
                  <div className="df2-field">
                    <label className="df2-label">{isEmail ? "SMTP username" : "Username"}</label>
                    <input
                      className="df2-input"
                      autoComplete="off"
                      value={username}
                      onChange={(e) => setUsername(e.target.value)}
                    />
                  </div>
                  <div className="df2-field">
                    <label className="df2-label">{isEmail ? "SMTP password" : "Password"}</label>
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

              {isSftp && (showUserPass || showConnectionString) && (
                <div className="df2-form-row" style={{ marginTop: 8 }}>
                  <div className="df2-field">
                    <label className="df2-label">SSH private key (optional)</label>
                    <textarea
                      className="df2-input"
                      rows={4}
                      placeholder="-----BEGIN OPENSSH PRIVATE KEY----- ..."
                      value={privateKey}
                      onChange={(e) => setPrivateKey(e.target.value)}
                    />
                    <p className="df2-field-note df2-label-hint">
                      Paste the private key text. If provided, password is optional.
                    </p>
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

              {(isS3 || isDynamo) && showAwsKeys && (
                <div className="df2-form-row" style={{ marginTop: 8 }}>
                  <div className="df2-field">
                    <label className="df2-label">Custom endpoint URL (optional)</label>
                    <input
                      className="df2-input"
                      placeholder="https://s3.wasabisys.com or http://localhost:9000"
                      value={endpointUrl}
                      onChange={(e) => setEndpointUrl(e.target.value)}
                    />
                    <p className="df2-field-note df2-label-hint">
                      For MinIO, LocalStack, Wasabi, and other S3-compatible stores.
                    </p>
                  </div>
                  {isS3 && (
                    <div className="df2-field" style={{ display: "flex", alignItems: "center", gap: 8 }}>
                      <label className="df2-checkbox" style={{ display: "flex", alignItems: "center", gap: 8 }}>
                        <input
                          type="checkbox"
                          checked={pathStyle}
                          onChange={(e) => setPathStyle(e.target.checked)}
                        />
                        <span>Use path-style addressing</span>
                      </label>
                    </div>
                  )}
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
