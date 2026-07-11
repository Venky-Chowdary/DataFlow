import { useEffect, useState } from "react";
import { ConnectorCatalogPanel } from "./ConnectorCatalogPanel";
import { ButtonLoader } from "./LoadingState";
import { DtIcon } from "./DtIcon";
import { useToast } from "./Toast";
import type { CatalogConnector } from "../lib/api";
import { saveConnector, testConnection, updateConnector } from "../lib/api";
import {
  getConnectorDefaults,
  isAwsConnector,
  resolveCatalogIdToType,
} from "../lib/connectorTypes";
import { CONNECTOR_CATALOG, Connector } from "../lib/types";

interface ConnectorModalProps {
  initialType?: string;
  editing?: Connector | null;
  onClose: () => void;
  onSaved: () => void;
}

export function ConnectorModal({
  initialType,
  editing = null,
  onClose,
  onSaved,
}: ConnectorModalProps) {
  const { toast } = useToast();
  const startType = editing?.type ?? (initialType ? resolveCatalogIdToType(initialType) : "");
  const [step, setStep] = useState<"pick" | "configure">(editing || startType ? "configure" : "pick");

  const defaults = getConnectorDefaults(startType || "mongodb");
  const [name, setName] = useState(editing?.name ?? "");
  const [type, setType] = useState(startType || "mongodb");
  const [host, setHost] = useState(editing?.host ?? defaults.host);
  const [port, setPort] = useState<number>(editing?.port ?? defaults.port);
  const [database, setDatabase] = useState(editing?.database ?? "");
  const [username, setUsername] = useState("");
  const [password, setPassword] = useState("");
  const [connectionString, setConnectionString] = useState("");
  const [schema, setSchema] = useState("");
  const [warehouse, setWarehouse] = useState("");
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
  const isAwsKeyed = isDynamo || isS3;
  const isElastic = type === "elasticsearch";

  const applyType = (nextType: string) => {
    const d = getConnectorDefaults(nextType);
    setType(nextType);
    setHost(d.host);
    setPort(d.port);
    setTestResult(null);
    setStep("configure");
    if (!name.trim()) {
      setName(`${d.label} connection`);
    }
  };

  const handleCatalogPick = (item: CatalogConnector) => {
    if (item.effective_status === "planned" || (!item.transfer_ready && !item.connect_only && item.status !== "live" && item.status !== "beta")) {
      toast({
        title: "Not available yet",
        message: `${item.name} is on the roadmap — no driver registered yet.`,
        tone: "info",
      });
      return;
    }
    if (item.connect_only) {
      toast({
        title: "Connection test only",
        message: `${item.name} can be saved and tested, but full transfer routes are not implemented yet.`,
        tone: "warning",
      });
    }
    applyType(resolveCatalogIdToType(item.id));
  };

  const validate = () => {
    if (!name.trim()) {
      setFieldError("Connection name is required.");
      return false;
    }
    if (isAwsKeyed) {
      if (!database.trim()) {
        setFieldError(isS3 ? "Bucket name is required." : "Table name is required.");
        return false;
      }
      const localEndpoint = (host || connectionString).includes("localhost") || (host || connectionString).startsWith("http");
      if (!localEndpoint && (!username.trim() || !password.trim())) {
        setFieldError("AWS Access Key ID and Secret Access Key are required for cloud DynamoDB/S3.");
        return false;
      }
    } else if (!isBigQuery && !host.trim()) {
      setFieldError("Host is required.");
      return false;
    }
    setFieldError(null);
    return true;
  };

  useEffect(() => {
    const onKey = (e: KeyboardEvent) => { if (e.key === "Escape") onClose(); };
    window.addEventListener("keydown", onKey);
    return () => window.removeEventListener("keydown", onKey);
  }, [onClose]);

  const buildPayload = () => ({
    name,
    type,
    host: isBigQuery ? "bigquery.googleapis.com" : isAwsKeyed ? (host || "us-east-1") : host,
    port: isBigQuery || isAwsKeyed ? 443 : port,
    database,
    username: username || undefined,
    password: password || undefined,
    schema: isBigQuery || isSnowflake ? schema : undefined,
    connection_string: connectionString || undefined,
    warehouse: isSnowflake ? warehouse : undefined,
  });

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
        schema: isBigQuery ? schema : undefined,
        username: username || undefined,
        password: password || undefined,
        connection_string: connectionString || undefined,
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
        await updateConnector(editing.id, payload);
        toast({ title: "Connector updated", message: name, tone: "success" });
      } else {
        await saveConnector(payload);
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
                : `${catalogItem?.label ?? type} · test once, use everywhere`}
            </p>
          </div>
          <button type="button" className="df2-btn df2-btn-ghost df2-btn-sm" onClick={onClose} aria-label="Close">
            <DtIcon name="x" />
          </button>
        </div>

        <div className="dt-modal-body">
          {step === "pick" ? (
            <ConnectorCatalogPanel role="all" onSelect={handleCatalogPick} limit={120} compact requireAvailable initialStatus="live" />
          ) : (
            <>
              {!editing && (
                <div style={{ marginBottom: 16 }}>
                  <button type="button" className="df2-btn df2-btn-ghost df2-btn-sm" onClick={() => setStep("pick")}>
                    ← Back to catalog
                  </button>
                </div>
              )}

              <div className="df2-form-row">
                <div className="df2-field">
                  <label className="df2-label">Connection name</label>
                  <input
                    className="df2-input"
                    placeholder="Production PostgreSQL"
                    value={name}
                    onChange={(e) => { setName(e.target.value); setFieldError(null); }}
                  />
                </div>
                <div className="df2-field">
                  <label className="df2-label">Type</label>
                  <input className="df2-input" value={catalogItem?.label ?? type} readOnly disabled />
                </div>
              </div>

              {isAwsKeyed ? (
                <>
                  <div className="df2-form-row">
                    <div className="df2-field">
                      <label className="df2-label">{isDynamo ? "AWS region or local endpoint" : "AWS region"}</label>
                      <input
                        className="df2-input"
                        placeholder={isDynamo ? "us-east-1 or http://localhost:8000" : "us-east-1"}
                        value={host}
                        onChange={(e) => setHost(e.target.value)}
                      />
                    </div>
                    <div className="df2-field">
                      <label className="df2-label">{isS3 ? "Bucket name" : "Table name"}</label>
                      <input className="df2-input" placeholder={isS3 ? "my-data-bucket" : "orders"} value={database} onChange={(e) => setDatabase(e.target.value)} />
                    </div>
                  </div>
                  {isDynamo && (
                    <div className="df2-field">
                      <label className="df2-label">Local endpoint URL (optional)</label>
                      <input
                        className="df2-input"
                        placeholder="http://localhost:8000 for DynamoDB Local"
                        value={connectionString}
                        onChange={(e) => setConnectionString(e.target.value)}
                      />
                      <p className="df2-label-hint df2-field-note">
                        Use DynamoDB Local or a private cloud endpoint. Dummy credentials (local/local) work for local mode.
                      </p>
                    </div>
                  )}
                  <div className="df2-form-row">
                    <div className="df2-field">
                      <label className="df2-label">Access Key ID</label>
                      <input className="df2-input" autoComplete="off" placeholder={isDynamo ? "AKIA… or local" : undefined} value={username} onChange={(e) => setUsername(e.target.value)} />
                    </div>
                    <div className="df2-field">
                      <label className="df2-label">Secret Access Key</label>
                      <input type="password" className="df2-input" autoComplete="new-password" placeholder={isDynamo ? "Optional for DynamoDB Local" : undefined} value={password} onChange={(e) => setPassword(e.target.value)} />
                    </div>
                  </div>
                </>
              ) : (
                <>
                  {!isBigQuery && (
                    <div className="df2-form-row">
                      <div className="df2-field">
                        <label className="df2-label">{isAwsConnector(type) ? "Region / endpoint" : "Host"}</label>
                        <input className="df2-input" value={host} onChange={(e) => setHost(e.target.value)} />
                      </div>
                      {!isAwsConnector(type) && (
                        <div className="df2-field">
                          <label className="df2-label">Port</label>
                          <input type="number" className="df2-input" value={port} onChange={(e) => setPort(parseInt(e.target.value, 10) || 0)} />
                        </div>
                      )}
                    </div>
                  )}

                  <div className="df2-form-row">
                    <div className="df2-field">
                      <label className="df2-label">{isBigQuery ? "GCP project" : isElastic ? "Index (optional)" : "Database"}</label>
                      <input className="df2-input" placeholder={isElastic ? "logs-*" : undefined} value={database} onChange={(e) => setDatabase(e.target.value)} />
                    </div>
                    {(isSnowflake || isBigQuery) && (
                      <div className="df2-field">
                        <label className="df2-label">{isBigQuery ? "Dataset" : "Schema"}</label>
                        <input className="df2-input" value={schema} onChange={(e) => setSchema(e.target.value)} />
                      </div>
                    )}
                  </div>

                  {!isBigQuery && (
                    <div className="df2-form-row">
                      <div className="df2-field">
                        <label className="df2-label">Username</label>
                        <input className="df2-input" autoComplete="off" value={username} onChange={(e) => setUsername(e.target.value)} />
                      </div>
                      <div className="df2-field">
                        <label className="df2-label">Password</label>
                        <input type="password" className="df2-input" autoComplete="new-password" placeholder={editing ? "Leave blank to keep" : ""} value={password} onChange={(e) => setPassword(e.target.value)} />
                      </div>
                    </div>
                  )}

                  {isSnowflake && (
                    <div className="df2-field">
                      <label className="df2-label">Warehouse</label>
                      <input className="df2-input" placeholder="COMPUTE_WH" value={warehouse} onChange={(e) => setWarehouse(e.target.value)} />
                    </div>
                  )}

                  {isMongo && (
                    <div className="df2-field">
                      <label className="df2-label">Connection string (optional)</label>
                      <input className="df2-input" placeholder="mongodb://user:pass@host:27017/" value={connectionString} onChange={(e) => setConnectionString(e.target.value)} />
                    </div>
                  )}

                  {isBigQuery && (
                    <div className="df2-field">
                      <label className="df2-label">Service account JSON path</label>
                      <input className="df2-input" placeholder="/path/to/service-account.json" value={connectionString} onChange={(e) => setConnectionString(e.target.value)} />
                    </div>
                  )}
                </>
              )}

              {fieldError && (
                <p style={{ color: "#dc2626", fontSize: 13, margin: "8px 0" }} role="alert">
                  {fieldError}
                </p>
              )}
              {testResult && (
                <span className={`df2-badge ${testResult.success ? "df2-badge-live" : "df2-badge-error"}`} style={{ marginTop: 8 }}>
                  {testResult.message}
                </span>
              )}
            </>
          )}
        </div>

        {step === "configure" && (
          <div className="df2-card-footer">
            <button type="button" className="df2-btn" onClick={handleTest} disabled={testing} aria-busy={testing}>
              {testing ? <ButtonLoader label="Testing…" /> : "Test connection"}
            </button>
            <button type="button" className="df2-btn df2-btn-primary" onClick={handleSave} disabled={saving} aria-busy={saving}>
              {saving ? <ButtonLoader label="Saving…" /> : editing ? "Update" : "Save & connect"}
            </button>
          </div>
        )}
      </div>
    </div>
  );
}
