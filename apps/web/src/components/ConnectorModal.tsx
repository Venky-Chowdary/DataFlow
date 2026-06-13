import { useState } from "react";
import { DtIcon } from "./DtIcon";
import { CONNECTOR_CATALOG } from "../lib/types";
import { saveConnector, testConnection } from "../lib/api";

interface ConnectorModalProps {
  initialType?: string;
  onClose: () => void;
  onSaved: () => void;
}

export function ConnectorModal({ initialType = "mongodb", onClose, onSaved }: ConnectorModalProps) {
  const catalog = CONNECTOR_CATALOG.find((c) => c.id === initialType) ?? CONNECTOR_CATALOG[0];
  const [name, setName] = useState("");
  const [type, setType] = useState(initialType);
  const [host, setHost] = useState("localhost");
  const [port, setPort] = useState<number>(catalog.port || 27017);
  const [database, setDatabase] = useState("");
  const [testing, setTesting] = useState(false);
  const [saving, setSaving] = useState(false);
  const [testResult, setTestResult] = useState<{ success: boolean; message: string } | null>(null);

  const handleTypeChange = (next: string) => {
    setType(next);
    const item = CONNECTOR_CATALOG.find((c) => c.id === next);
    if (item?.port) setPort(item.port);
    setTestResult(null);
  };

  const handleTest = async () => {
    setTesting(true);
    setTestResult(null);
    try {
      setTestResult(await testConnection({ type, host, port, database }));
    } catch {
      setTestResult({ success: false, message: "Connection test failed" });
    }
    setTesting(false);
  };

  const handleSave = async () => {
    if (!name.trim() || !host.trim()) return;
    setSaving(true);
    try {
      await saveConnector({ name, type, host, port, database });
      onSaved();
      onClose();
    } catch (e) {
      console.error(e);
    }
    setSaving(false);
  };

  return (
    <div className="dt-modal-overlay" onClick={onClose} role="presentation">
      <div className="dt-modal dt-modal-lg" onClick={(e) => e.stopPropagation()} role="dialog" aria-labelledby="connector-modal-title">
        <div className="dt-modal-header">
          <div>
            <h2 className="dt-modal-title" id="connector-modal-title">Configure Connector</h2>
            <p className="dt-modal-subtitle">Test and save your connection settings</p>
          </div>
          <button type="button" className="dt-btn dt-btn-ghost dt-btn-icon" onClick={onClose} aria-label="Close">
            <DtIcon name="x" />
          </button>
        </div>
        <div className="dt-modal-body">
          <div className="dt-field">
            <label className="dt-label" htmlFor="conn-name">Connector Name</label>
            <input id="conn-name" className="dt-input" placeholder="Production MongoDB" value={name} onChange={(e) => setName(e.target.value)} />
          </div>
          <div className="dt-field">
            <label className="dt-label" htmlFor="conn-type">Type</label>
            <select id="conn-type" className="dt-select" value={type} onChange={(e) => handleTypeChange(e.target.value)}>
              {CONNECTOR_CATALOG.filter((c) => !["csv", "json"].includes(c.id)).map((c) => (
                <option key={c.id} value={c.id}>{c.label}</option>
              ))}
            </select>
          </div>
          <div className="dt-flex dt-gap-4">
            <div className="dt-field" style={{ flex: 2 }}>
              <label className="dt-label" htmlFor="conn-host">Host</label>
              <input id="conn-host" className="dt-input" value={host} onChange={(e) => setHost(e.target.value)} />
            </div>
            <div className="dt-field" style={{ flex: 1 }}>
              <label className="dt-label" htmlFor="conn-port">Port</label>
              <input id="conn-port" type="number" className="dt-input" value={port} onChange={(e) => setPort(parseInt(e.target.value, 10) || 0)} />
            </div>
          </div>
          <div className="dt-field">
            <label className="dt-label" htmlFor="conn-db">Database (optional)</label>
            <input id="conn-db" className="dt-input" placeholder="mydatabase" value={database} onChange={(e) => setDatabase(e.target.value)} />
          </div>
          {testResult && (
            <div className={`dt-badge ${testResult.success ? "dt-badge-success" : "dt-badge-error"}`}>
              <DtIcon name={testResult.success ? "check" : "x"} size={14} />
              {testResult.message}
            </div>
          )}
        </div>
        <div className="dt-modal-footer">
          <button type="button" className="dt-btn" onClick={handleTest} disabled={testing}>
            {testing ? <span className="dt-spinner" /> : "Test Connection"}
          </button>
          <button type="button" className="dt-btn dt-btn-primary" onClick={handleSave} disabled={saving || !name.trim()}>
            {saving ? <span className="dt-spinner" /> : "Save Connector"}
          </button>
        </div>
      </div>
    </div>
  );
}
