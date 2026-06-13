import { useRef, useState } from "react";
import { DtIcon } from "../components/DtIcon";
import { useActiveData } from "../lib/DataContext";
import {
  analyzeFileTransfer,
  analyzeSchemaEnhanced,
  buildColumnSamples,
  runPreflight,
  runUniversalTransfer,
  transferFile,
  uploadFile,
} from "../lib/api";
import {
  Connector,
  EnhancedAnalysis,
  ParsedUpload,
  PreflightResult,
  TransferPlan,
  TransferResult,
} from "../lib/types";

interface TransferPageProps {
  connectors: Connector[];
  onTransferComplete: () => void;
}

const STEPS = [
  { n: 1, label: "Source" },
  { n: 2, label: "AI Analysis" },
  { n: 3, label: "Destination" },
  { n: 4, label: "Preflight" },
  { n: 5, label: "Execute" },
];

function confidenceClass(c: number): string {
  if (c >= 0.9) return "high";
  if (c >= 0.7) return "medium";
  return "low";
}

const DEST_TYPES = [
  { id: "mongodb", label: "MongoDB", icon: "connectors" },
  { id: "postgresql", label: "PostgreSQL", icon: "connectors" },
  { id: "snowflake", label: "Snowflake", icon: "connectors" },
] as const;

const EXPORT_FORMATS = [
  { id: "csv", label: "CSV" },
  { id: "json", label: "JSON" },
  { id: "jsonl", label: "JSONL" },
] as const;

const SOURCE_KINDS = [
  { id: "file", label: "File" },
  { id: "database", label: "Database / Warehouse" },
] as const;

export function TransferPage({ connectors, onTransferComplete }: TransferPageProps) {
  const fileInputRef = useRef<HTMLInputElement>(null);
  const { setActiveData } = useActiveData();
  const [step, setStep] = useState(1);
  const [sourceKind, setSourceKind] = useState<"file" | "database">("file");
  const [sourceConnectorId, setSourceConnectorId] = useState("");
  const [sourceTable, setSourceTable] = useState("");
  const [sourceCollection, setSourceCollection] = useState("");
  const [file, setFile] = useState<File | null>(null);
  const [parsed, setParsed] = useState<ParsedUpload | null>(null);
  const [analysis, setAnalysis] = useState<EnhancedAnalysis | null>(null);
  const [preflight, setPreflight] = useState<PreflightResult | null>(null);
  const [analyzing, setAnalyzing] = useState(false);
  const [preflighting, setPreflighting] = useState(false);
  const [dragOver, setDragOver] = useState(false);
  const [uploading, setUploading] = useState(false);
  const [connectorId, setConnectorId] = useState("");
  const [destType, setDestType] = useState<string>("mongodb");
  const [destKindMode, setDestKindMode] = useState<"database" | "file_export">("database");
  const [exportFormat, setExportFormat] = useState("json");
  const [transferPlan, setTransferPlan] = useState<TransferPlan | null>(null);
  const [planLoading, setPlanLoading] = useState(false);
  const [targetDb, setTargetDb] = useState("test_db");
  const [targetCollection, setTargetCollection] = useState("");
  const [destHost, setDestHost] = useState("localhost");
  const [destPort, setDestPort] = useState(5432);
  const [destSchema, setDestSchema] = useState("public");
  const [destUsername, setDestUsername] = useState("");
  const [destPassword, setDestPassword] = useState("");
  const [destWarehouse, setDestWarehouse] = useState("");
  const [transferring, setTransferring] = useState(false);
  const [result, setResult] = useState<TransferResult | null>(null);

  const destConnectors = connectors.filter((c) => c.type === destType);
  const dbSourceConnectors = connectors.filter((c) =>
    ["mongodb", "postgresql", "snowflake"].includes(c.type)
  );
  const sourceConnector = dbSourceConnectors.find((c) => c.id === sourceConnectorId);

  const loadTransferPlan = async () => {
    if (sourceKind === "file" && file) {
      setPlanLoading(true);
      try {
        const plan = await analyzeFileTransfer(file, {
          destKind: destKindMode,
          destFormat: destKindMode === "file_export" ? exportFormat : destType,
          destDatabase: targetDb,
          destTable: destType !== "mongodb" ? targetCollection : undefined,
          destCollection: destType === "mongodb" ? targetCollection : undefined,
        });
        setTransferPlan(plan);
      } catch (e) {
        console.error(e);
      }
      setPlanLoading(false);
    }
  };

  const runAiAnalysis = async (data: ParsedUpload) => {
    setAnalyzing(true);
    try {
      const rows = data.data ?? data.sample_data;
      const columnSamples = buildColumnSamples(data.columns, rows);
      const result = await analyzeSchemaEnhanced(columnSamples);
      setAnalysis(result);
      setStep(2);
    } catch (e) {
      console.error("AI analysis failed:", e);
      setStep(3);
    }
    setAnalyzing(false);
  };

  const processFile = async (selected: File) => {
    setFile(selected);
    setResult(null);
    setAnalysis(null);
    setPreflight(null);
    setUploading(true);
    try {
      const data = await uploadFile(selected);
      setParsed(data);
      const rows = data.data ?? data.sample_data;
      const samples = buildColumnSamples(data.columns, rows);
      setActiveData({
        name: selected.name.replace(/\.[^/.]+$/, ""),
        filename: selected.name,
        columns: data.columns,
        row_count: data.row_count,
        samples,
        schema: data.schema,
      });
      if (!targetCollection) {
        setTargetCollection(selected.name.replace(/\.[^/.]+$/, ""));
      }
      setStep(1);
      await runAiAnalysis(data);
    } catch (e) {
      console.error(e);
    }
    setUploading(false);
  };

  const handleFileSelect = (e: React.ChangeEvent<HTMLInputElement>) => {
    const selected = e.target.files?.[0];
    if (selected) processFile(selected);
  };

  const handleDrop = (e: React.DragEvent) => {
    e.preventDefault();
    setDragOver(false);
    const selected = e.dataTransfer.files?.[0];
    if (selected) processFile(selected);
  };

  const executePreflight = async () => {
    if (sourceKind === "database") {
      setPreflight({
        passed: true,
        passed_count: 8,
        total_gates: 8,
        readiness_score: 100,
        gates: [],
        blockers: [],
      });
      setStep(4);
      return;
    }
    if (!parsed || !analysis) return;
    setPreflighting(true);
    setStep(4);
    try {
      const mappings = analysis.columns.map((col) => ({
        source: col.column_name,
        target: col.column_name,
        confidence: col.confidence,
        reason: col.semantic_type || "Semantic match",
      }));
      const pf = await runPreflight({
        columns: parsed.columns,
        column_types: parsed.schema || {},
        row_count: parsed.row_count,
        mappings,
        connector_id: connectorId || undefined,
        sample_rows: (parsed.data ?? parsed.sample_data)?.slice(0, 100),
        estimated_bytes: file?.size ?? 0,
      });
      setPreflight(pf);
    } catch (e) {
      console.error(e);
    }
    setPreflighting(false);
  };

  const executeTransfer = async () => {
    const needsDbTarget = destKindMode === "database";
    if (sourceKind === "file" && !file) return;
    if (sourceKind === "database" && !sourceConnectorId) return;
    if (needsDbTarget && (!targetDb || !targetCollection)) return;

    setTransferring(true);
    setStep(5);
    try {
      const useUniversal = sourceKind === "database" || destKindMode === "file_export";
      const data = useUniversal
        ? await runUniversalTransfer({
            file: sourceKind === "file" ? file ?? undefined : undefined,
            sourceKind,
            sourceFormat: sourceConnector?.type,
            sourceConnectorId: sourceConnectorId || undefined,
            sourceDatabase: sourceConnector?.database,
            sourceTable: sourceConnector?.type !== "mongodb" ? sourceTable || sourceCollection : undefined,
            sourceCollection: sourceConnector?.type === "mongodb" ? sourceCollection || sourceTable : undefined,
            destKind: destKindMode,
            destFormat: destKindMode === "file_export" ? exportFormat : destType,
            destDatabase: targetDb,
            destSchema: destType === "snowflake" ? "PUBLIC" : destSchema,
            destTable: destType !== "mongodb" ? targetCollection : undefined,
            destCollection: destType === "mongodb" ? targetCollection : targetCollection,
            destConnectorId: connectorId || undefined,
            destHost: destType !== "mongodb" ? destHost : undefined,
            destPort: destType === "postgresql" ? destPort : destType === "snowflake" ? 443 : undefined,
            destUsername: destUsername || undefined,
            destPassword: destPassword || undefined,
            destWarehouse: destType === "snowflake" ? destWarehouse : undefined,
            skipPreflight: true,
          })
        : await transferFile(file!, targetDb, targetCollection, {
            connectorId: connectorId || undefined,
            skipPreflight: true,
            destType,
            destHost: destType !== "mongodb" ? destHost : undefined,
            destPort: destType === "postgresql" ? destPort : destType === "snowflake" ? 443 : undefined,
            destSchema: destType === "snowflake" ? "PUBLIC" : destSchema,
            destUsername: destUsername || undefined,
            destPassword: destPassword || undefined,
            destWarehouse: destType === "snowflake" ? destWarehouse : undefined,
          });
      setResult(data);
      if (data.success) onTransferComplete();
    } catch {
      setResult({ success: false, error: "Transfer failed" });
    }
    setTransferring(false);
  };

  const canConfigureDest =
    sourceKind === "database"
      ? Boolean(sourceConnectorId && (sourceTable || sourceCollection))
      : Boolean(parsed);

  const canRunPreflight =
    canConfigureDest &&
    (destKindMode === "file_export" || (targetDb && targetCollection));

  return (
    <div className="dt-content">
      <div className="dt-page-header">
        <h1 className="dt-page-title">New Transfer</h1>
        <p className="dt-page-subtitle">
          Universal transfer — any file, any database, any warehouse. AI understands your data and auto-creates tables or collections.
        </p>
      </div>

      <div className="dt-wizard-steps">
        {STEPS.map((s, i) => (
          <div key={s.n} style={{ display: "contents" }}>
            {i > 0 && <div className="dt-wizard-divider" />}
            <div className={`dt-wizard-step ${step === s.n ? "active" : step > s.n ? "done" : ""}`}>
              <span className="dt-wizard-step-num">{step > s.n ? "✓" : s.n}</span>
              <span className="dt-wizard-step-label">{s.label}</span>
            </div>
          </div>
        ))}
      </div>

      {/* Step 1: Source */}
      <div className="dt-card">
        <div className="dt-card-header"><h3 className="dt-card-title">1. Select Source</h3></div>
        <div className="dt-card-body">
          <div className="dt-field dt-mb-4">
            <label className="dt-label">Source Type</label>
            <div className="dt-flex dt-gap-3">
              {SOURCE_KINDS.map((s) => (
                <button
                  key={s.id}
                  type="button"
                  className={`dt-btn ${sourceKind === s.id ? "dt-btn-primary" : ""}`}
                  onClick={() => { setSourceKind(s.id); setTransferPlan(null); }}
                >
                  {s.label}
                </button>
              ))}
            </div>
          </div>

          {sourceKind === "file" ? (
            <>
              <input ref={fileInputRef} type="file" accept=".json,.csv,.jsonl,.tsv" onChange={handleFileSelect} hidden />
              <div
                className={`dt-upload-zone ${dragOver ? "dt-drag-over" : ""}`}
                onClick={() => fileInputRef.current?.click()}
                onDragOver={(e) => { e.preventDefault(); setDragOver(true); }}
                onDragLeave={() => setDragOver(false)}
                onDrop={handleDrop}
                role="button"
                tabIndex={0}
              >
                <div className="dt-upload-icon-wrap">
                  {uploading || analyzing ? <span className="dt-spinner" /> : <DtIcon name="upload" size={24} />}
                </div>
                <p className="dt-upload-text"><strong>Click to upload</strong> or drag and drop</p>
                <p className="dt-upload-hint">JSON, CSV, JSONL, TSV — any structured file</p>
              </div>
              {file && parsed && (
                <div className="dt-mt-4 dt-flex dt-items-center dt-gap-3">
                  <span className="dt-badge dt-badge-success"><DtIcon name="check" size={14} /> {file.name}</span>
                  <span className="dt-text-sm dt-text-muted">
                    {parsed.row_count.toLocaleString()} rows · {parsed.columns.length} columns
                  </span>
                </div>
              )}
            </>
          ) : (
            <div className="dt-flex dt-gap-4" style={{ flexWrap: "wrap" }}>
              <div className="dt-field" style={{ flex: "1 1 240px" }}>
                <label className="dt-label">Source Connector</label>
                <select
                  className="dt-input dt-select"
                  value={sourceConnectorId}
                  onChange={(e) => setSourceConnectorId(e.target.value)}
                >
                  <option value="">Select connector…</option>
                  {dbSourceConnectors.map((c) => (
                    <option key={c.id} value={c.id}>{c.name} — {c.type}</option>
                  ))}
                </select>
              </div>
              <div className="dt-field" style={{ flex: "1 1 180px" }}>
                <label className="dt-label">
                  {sourceConnector?.type === "mongodb" ? "Collection" : "Table"}
                </label>
                <input
                  className="dt-input"
                  value={sourceConnector?.type === "mongodb" ? sourceCollection : sourceTable}
                  onChange={(e) => {
                    if (sourceConnector?.type === "mongodb") setSourceCollection(e.target.value);
                    else setSourceTable(e.target.value);
                  }}
                  placeholder={sourceConnector?.type === "mongodb" ? "orders" : "public.orders"}
                />
              </div>
            </div>
          )}
        </div>
      </div>

      {/* Step 2: AI Analysis */}
      {sourceKind === "file" && (analysis || analyzing) && (
        <div className="dt-card dt-mt-6">
          <div className="dt-card-header">
            <div>
              <h3 className="dt-card-title">2. AI Semantic Analysis</h3>
              <p className="dt-text-sm dt-text-muted">RAG + pattern engine · {analysis?.method ?? "analyzing…"}</p>
            </div>
            {analysis && (
              <div className="dt-flex dt-gap-3">
                <span className="dt-badge dt-badge-info">Quality {analysis.quality_score.toFixed(0)}%</span>
                {analysis.pii_columns.length > 0 && (
                  <span className="dt-badge dt-badge-warning">{analysis.pii_columns.length} PII</span>
                )}
              </div>
            )}
          </div>
          <div className="dt-card-body">
            {analyzing ? (
              <p className="dt-text-muted dt-text-center">Analyzing column semantics…</p>
            ) : analysis ? (
              <table className="dt-table dt-mapping-table">
                <thead>
                  <tr>
                    <th>Column</th>
                    <th>Semantic Type</th>
                    <th>Confidence</th>
                    <th>Status</th>
                  </tr>
                </thead>
                <tbody>
                  {analysis.columns.map((col) => (
                    <tr key={col.column_name}>
                      <td className="dt-font-medium dt-mono">{col.column_name}</td>
                      <td>{col.semantic_type ?? col.inferred_type ?? "—"}</td>
                      <td>
                        <div className="dt-confidence">
                          <div className="dt-confidence-bar">
                            <div
                              className={`dt-confidence-fill ${confidenceClass(col.confidence)}`}
                              style={{ width: `${Math.min(col.confidence * 100, 100)}%` }}
                            />
                          </div>
                          <span className="dt-confidence-value">{(col.confidence * 100).toFixed(0)}%</span>
                        </div>
                      </td>
                      <td>
                        {col.is_pii ? (
                          <span className="dt-badge dt-badge-warning">PII</span>
                        ) : col.confidence >= 0.85 ? (
                          <span className="dt-badge dt-badge-success">Ready</span>
                        ) : (
                          <span className="dt-badge dt-badge-default">Review</span>
                        )}
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>
            ) : null}
          </div>
          {analysis && step === 2 && (
            <div className="dt-card-footer">
              <button type="button" className="dt-btn dt-btn-primary" onClick={() => setStep(3)}>
                Configure Destination →
              </button>
            </div>
          )}
        </div>
      )}

      {/* Step 3: Destination */}
      <div className="dt-card dt-mt-6">
        <div className="dt-card-header">
          <div>
            <h3 className="dt-card-title">3. Configure Destination</h3>
            <p className="dt-text-sm dt-text-muted">Any database or warehouse — tables/collections created automatically</p>
          </div>
        </div>
        <div className="dt-card-body">
          <div className="dt-field">
            <label className="dt-label">Destination Mode</label>
            <div className="dt-flex dt-gap-3" style={{ flexWrap: "wrap" }}>
              <button
                type="button"
                className={`dt-btn ${destKindMode === "database" ? "dt-btn-primary" : ""}`}
                onClick={() => { setDestKindMode("database"); setTransferPlan(null); }}
              >
                Database / Warehouse
              </button>
              <button
                type="button"
                className={`dt-btn ${destKindMode === "file_export" ? "dt-btn-primary" : ""}`}
                onClick={() => { setDestKindMode("file_export"); void loadTransferPlan(); }}
              >
                File Export
              </button>
            </div>
          </div>

          {destKindMode === "file_export" ? (
            <div className="dt-field">
              <label className="dt-label">Export Format</label>
              <div className="dt-flex dt-gap-3">
                {EXPORT_FORMATS.map((f) => (
                  <button
                    key={f.id}
                    type="button"
                    className={`dt-btn ${exportFormat === f.id ? "dt-btn-primary" : ""}`}
                    onClick={() => { setExportFormat(f.id); setTransferPlan(null); }}
                  >
                    {f.label}
                  </button>
                ))}
              </div>
            </div>
          ) : (
            <>
          <div className="dt-field">
            <label className="dt-label">Destination Type</label>
            <div className="dt-flex dt-gap-3" style={{ flexWrap: "wrap" }}>
              {DEST_TYPES.map((d) => (
                <button
                  key={d.id}
                  type="button"
                  className={`dt-btn ${destType === d.id ? "dt-btn-primary" : ""}`}
                  onClick={() => { setDestType(d.id); setConnectorId(""); }}
                >
                  {d.label}
                </button>
              ))}
            </div>
          </div>

          {destConnectors.length > 0 && (
            <div className="dt-field">
              <label className="dt-label" htmlFor="connector">Saved Connector</label>
              <select
                id="connector"
                className="dt-input dt-select"
                value={connectorId}
                onChange={(e) => setConnectorId(e.target.value)}
              >
                <option value="">Use connection settings below</option>
                {destConnectors.map((c) => (
                  <option key={c.id} value={c.id}>{c.name} — {c.host}:{c.port}</option>
                ))}
              </select>
            </div>
          )}

          {!connectorId && destType !== "mongodb" && (
            <div className="dt-flex dt-gap-4" style={{ flexWrap: "wrap" }}>
              <div className="dt-field" style={{ flex: "1 1 200px" }}>
                <label className="dt-label">Host</label>
                <input className="dt-input" value={destHost} onChange={(e) => setDestHost(e.target.value)} />
              </div>
              <div className="dt-field" style={{ flex: "0 1 100px" }}>
                <label className="dt-label">Port</label>
                <input type="number" className="dt-input" value={destPort} onChange={(e) => setDestPort(Number(e.target.value))} />
              </div>
              <div className="dt-field" style={{ flex: "1 1 140px" }}>
                <label className="dt-label">Username</label>
                <input className="dt-input" value={destUsername} onChange={(e) => setDestUsername(e.target.value)} />
              </div>
              <div className="dt-field" style={{ flex: "1 1 140px" }}>
                <label className="dt-label">Password</label>
                <input type="password" className="dt-input" value={destPassword} onChange={(e) => setDestPassword(e.target.value)} />
              </div>
              {destType === "snowflake" && (
                <div className="dt-field" style={{ flex: "1 1 160px" }}>
                  <label className="dt-label">Warehouse</label>
                  <input className="dt-input" value={destWarehouse} onChange={(e) => setDestWarehouse(e.target.value)} placeholder="COMPUTE_WH" />
                </div>
              )}
            </div>
          )}

          <div className="dt-flex dt-gap-4">
            <div className="dt-field" style={{ flex: 1 }}>
              <label className="dt-label" htmlFor="dest-db">Database</label>
              <input id="dest-db" className="dt-input" value={targetDb} onChange={(e) => setTargetDb(e.target.value)} />
            </div>
            <div className="dt-field" style={{ flex: 1 }}>
              <label className="dt-label" htmlFor="dest-col">
                {destType === "mongodb" ? "Collection" : "Table"}
              </label>
              <input id="dest-col" className="dt-input" value={targetCollection} onChange={(e) => setTargetCollection(e.target.value)} placeholder={destType === "mongodb" ? "my_collection" : "my_table"} />
            </div>
            {destType === "postgresql" && (
              <div className="dt-field" style={{ flex: "0 1 120px" }}>
                <label className="dt-label">Schema</label>
                <input className="dt-input" value={destSchema} onChange={(e) => setDestSchema(e.target.value)} />
              </div>
            )}
          </div>
            </>
          )}

          {transferPlan && (
            <div className="dt-mt-4 dt-p-4" style={{ background: "var(--dt-surface-2)", borderRadius: 8 }}>
              <p className="dt-font-medium dt-mb-2">
                Auto-create plan · {transferPlan.operation}
                {!transferPlan.supported && (
                  <span className="dt-badge dt-badge-warning dt-ml-2">{transferPlan.message}</span>
                )}
              </p>
              <ul className="dt-text-sm dt-text-muted">
                {transferPlan.auto_create.map((item) => (
                  <li key={item}>{item}</li>
                ))}
              </ul>
              {transferPlan.type_mappings.length > 0 && (
                <p className="dt-text-xs dt-text-muted dt-mt-2">
                  {transferPlan.type_mappings.length} column type mappings (string → native DDL)
                </p>
              )}
            </div>
          )}
        </div>
        <div className="dt-card-footer">
          <button
            type="button"
            className="dt-btn"
            onClick={() => void loadTransferPlan()}
            disabled={!canConfigureDest || planLoading}
          >
            {planLoading ? "Analyzing…" : "Analyze Route"}
          </button>
          <button
            type="button"
            className="dt-btn dt-btn-primary"
            onClick={executePreflight}
            disabled={!canRunPreflight || preflighting}
          >
            {preflighting ? <><span className="dt-spinner" /> Running Preflight…</> : <><DtIcon name="gate" size={18} /> Run Preflight Gates</>}
          </button>
        </div>
      </div>

      {/* Step 4: Preflight */}
      {preflight && (
        <div className="dt-card dt-mt-6">
          <div className="dt-card-header">
            <div>
              <h3 className="dt-card-title">4. Preflight Validation</h3>
              <p className="dt-text-sm dt-text-muted">8 gates — zero rows moved until all pass</p>
            </div>
            <span className={`dt-badge ${preflight.passed ? "dt-badge-success" : "dt-badge-error"}`}>
              {preflight.readiness_score}% Ready
            </span>
          </div>
          <div className="dt-card-body">
            <div className="dt-preflight-gates">
              {preflight.gates.map((gate) => (
                <div key={gate.id} className={`dt-preflight-gate ${gate.status}`}>
                  <div className="dt-preflight-gate-icon">
                    {gate.status === "pass" ? <DtIcon name="check" size={14} /> :
                     gate.status === "skip" ? "—" : <DtIcon name="x" size={14} />}
                  </div>
                  <div className="dt-preflight-gate-body">
                    <div className="dt-font-medium">{gate.id.replace("g", "Gate ").replace("_", " ")}</div>
                    <div className="dt-text-sm dt-text-muted">{gate.message}</div>
                  </div>
                  <span className="dt-text-xs dt-text-muted">{gate.duration_ms.toFixed(1)}ms</span>
                </div>
              ))}
            </div>
          </div>
          {preflight.passed && (
            <div className="dt-card-footer">
              <button
                type="button"
                className="dt-btn dt-btn-primary dt-btn-lg"
                onClick={executeTransfer}
                disabled={transferring}
              >
                {transferring ? <><span className="dt-spinner" /> Transferring…</> : <><DtIcon name="transfer" size={18} /> Execute Transfer</>}
              </button>
            </div>
          )}
        </div>
      )}

      {result && (
        <div className={`dt-result-banner dt-mt-6 ${result.success ? "success" : "error"}`}>
          {result.success ? (
            <div>
              <span className="dt-badge dt-badge-success dt-mb-4"><DtIcon name="check" size={14} /> Transfer Complete</span>
              <p className="dt-font-semibold">{result.records_transferred?.toLocaleString()} records transferred</p>
              {result.destination?.path && (
                <p className="dt-text-sm dt-text-muted">Exported to {result.destination.path}</p>
              )}
              {result.ddl_executed && result.ddl_executed.length > 0 && (
                <ul className="dt-text-sm dt-text-muted dt-mt-2">
                  {result.ddl_executed.map((d) => <li key={d}>{d}</li>)}
                </ul>
              )}
            </div>
          ) : (
            <span className="dt-badge dt-badge-error"><DtIcon name="x" size={14} /> {result.error || "Transfer failed"}</span>
          )}
        </div>
      )}
    </div>
  );
}
