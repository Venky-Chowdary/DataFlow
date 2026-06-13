import { useState } from "react";
import { Button } from "./Button";

export interface ConnectorGenerateResult {
  connector_id: string;
  name: string;
  version: string;
  base_url: string;
  auth_type: string;
  endpoint_count: number;
  plugin_code: string;
  certification: { status: string; next_step: string };
}

interface ConnectorFactoryPanelProps {
  onGenerate: (spec: object) => Promise<ConnectorGenerateResult>;
}

export function ConnectorFactoryPanel({ onGenerate }: ConnectorFactoryPanelProps) {
  const [specText, setSpecText] = useState("");
  const [result, setResult] = useState<ConnectorGenerateResult | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [loading, setLoading] = useState(false);

  async function handleGenerate() {
    setError(null);
    setResult(null);
    let spec: object;
    try {
      spec = JSON.parse(specText);
    } catch {
      setError("Paste valid OpenAPI JSON");
      return;
    }
    setLoading(true);
    try {
      setResult(await onGenerate(spec));
    } catch (e) {
      setError(e instanceof Error ? e.message : "Generation failed");
    } finally {
      setLoading(false);
    }
  }

  return (
    <section className="df-factory-panel">
      <div className="df-factory-header">
        <h2 className="df-section-title">AI Connector Factory</h2>
        <p className="df-file-meta">
          Paste an OpenAPI 3 spec — generates a certifiable REST connector plugin stub.
        </p>
      </div>

      <textarea
        className="df-factory-input"
        value={specText}
        onChange={(e) => setSpecText(e.target.value)}
        placeholder='{"openapi":"3.0.0","info":{"title":"My API"},"paths":{...}}'
        rows={8}
      />

      <div className="df-factory-actions">
        <Button onClick={handleGenerate} disabled={loading || !specText.trim()}>
          {loading ? "Generating…" : "Generate connector"}
        </Button>
        {error && <span className="df-factory-error">{error}</span>}
      </div>

      {result && (
        <div className="df-factory-result">
          <dl className="df-recon-grid">
            <div className="df-recon-item">
              <dt>Connector ID</dt>
              <dd className="df-mono">{result.connector_id}</dd>
            </div>
            <div className="df-recon-item">
              <dt>Endpoints</dt>
              <dd>{result.endpoint_count}</dd>
            </div>
            <div className="df-recon-item">
              <dt>Auth</dt>
              <dd className="df-mono">{result.auth_type}</dd>
            </div>
            <div className="df-recon-item">
              <dt>Certification</dt>
              <dd>{result.certification.status}</dd>
            </div>
          </dl>
          <pre className="df-factory-code">{result.plugin_code}</pre>
          <p className="df-file-meta">{result.certification.next_step}</p>
        </div>
      )}
    </section>
  );
}
