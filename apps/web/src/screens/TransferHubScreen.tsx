import { useCallback, useEffect, useState } from "react";
import {
  Button,
  DatabaseEndpointPanel,
  FileDropzone,
  FilePreviewTable,
  LoadingState,
  ProgressBar,
  SampleFilePicker,
  SemanticPreview,
  TransferHub,
  useToast,
  type CredentialFields,
} from "@dataflow/design-system";
import type { MappingResult, SemanticColumnAnalysis } from "../lib/api";
import {
  analyzeColumnSchema,
  fetchSavedConnector,
  fetchSavedConnectors,
  uploadFile,
} from "../lib/api";
import {
  autoConnectDatabases,
  prepareApiToDatabase,
  prepareDatabaseToFile,
  prepareFileToDatabase,
  type AutoConnectProgress,
} from "../lib/autoDbSetup";
import { detectDatabaseType } from "../lib/connectionString";
import { emptyCredentials, resolveConnectionString, SAMPLE_FILES } from "../lib/samples";
import { TRANSFER_TEMPLATES, operationDescription } from "../lib/transferModes";
import {
  DATABASE_OPTIONS,
  emptyEndpoint,
  inferOperation,
  operationLabel,
  type EndpointConfig,
  type FileFormat,
  FILE_FORMAT_OPTIONS,
} from "../lib/types";

interface TransferHubScreenProps {
  source: EndpointConfig;
  destination: EndpointConfig;
  onSourceChange: (ep: EndpointConfig) => void;
  onDestinationChange: (ep: EndpointConfig) => void;
  onMappingsReady: (mappings: MappingResult[]) => void;
  onNext: () => void;
}

const PHASE_PROGRESS: Record<AutoConnectProgress["phase"], number> = {
  idle: 0,
  source: 25,
  destination: 45,
  schema: 65,
  semantic: 85,
  done: 100,
  error: 0,
};

export function TransferHubScreen({
  source,
  destination,
  onSourceChange,
  onDestinationChange,
  onMappingsReady,
  onNext,
}: TransferHubScreenProps) {
  const { toast } = useToast();
  const [templateId, setTemplateId] = useState("file-db");
  const template = TRANSFER_TEMPLATES.find((t) => t.id === templateId) ?? TRANSFER_TEMPLATES[0];

  const [sourceConnStr, setSourceConnStr] = useState(source.database?.connectionString ?? "");
  const [destConnStr, setDestConnStr] = useState(destination.database?.connectionString ?? "");
  const [sourceCreds, setSourceCreds] = useState<CredentialFields>(() => emptyCredentials());
  const [destCreds, setDestCreds] = useState<CredentialFields>(() => emptyCredentials());
  const [apiUrl, setApiUrl] = useState(source.api?.url ?? "");
  const [exportFormat, setExportFormat] = useState<FileFormat>(destination.exportFormat ?? "csv");
  const [uploading, setUploading] = useState(false);
  const [working, setWorking] = useState(false);
  const [progress, setProgress] = useState<AutoConnectProgress | null>(null);
  const [ready, setReady] = useState(false);
  const [autoCreate, setAutoCreate] = useState(true);
  const [semanticAnalysis, setSemanticAnalysis] = useState<SemanticColumnAnalysis[]>([]);
  const [savedConnectors, setSavedConnectors] = useState<
    { id: string; name: string; type: string; role: string; last_test_ok?: boolean }[]
  >([]);
  const [sourceConnectorId, setSourceConnectorId] = useState("");
  const [destConnectorId, setDestConnectorId] = useState("");
  const [loadingConnectors, setLoadingConnectors] = useState(false);

  const loadConnectors = useCallback(async () => {
    setLoadingConnectors(true);
    try {
      setSavedConnectors(await fetchSavedConnectors());
    } catch {
      setSavedConnectors([]);
    } finally {
      setLoadingConnectors(false);
    }
  }, []);

  useEffect(() => {
    loadConnectors();
  }, [loadConnectors]);

  async function applySavedConnector(id: string, side: "source" | "dest") {
    if (!id) return;
    try {
      const conn = await fetchSavedConnector(id);
      if (side === "source") {
        setSourceConnStr(conn.connection_string);
        setSourceCreds({
          type: conn.type,
          host: conn.host,
          port: conn.port,
          database: conn.database,
          username: conn.username,
          password: conn.password,
          schema: conn.schema,
          warehouse: conn.warehouse,
          ssl: conn.ssl,
          connectionString: conn.connection_string,
        });
      } else {
        setDestConnStr(conn.connection_string);
        setDestCreds({
          type: conn.type,
          host: conn.host,
          port: conn.port,
          database: conn.database,
          username: conn.username,
          password: conn.password,
          schema: conn.schema,
          warehouse: conn.warehouse,
          ssl: conn.ssl,
          connectionString: conn.connection_string,
        });
      }
      setReady(false);
      setSemanticAnalysis([]);
    } catch (e) {
      toast({
        title: "Connector load failed",
        message: e instanceof Error ? e.message : "Could not load connector",
        tone: "error",
      });
    }
  }

  const operation = inferOperation(
    { ...source, kind: template.sourceKind },
    { ...destination, kind: template.destKind === "file" ? "file" : template.destKind, exportFormat }
  );

  async function loadSampleFile(filename: string) {
    const res = await fetch(`/samples/${filename}`);
    if (!res.ok) throw new Error(`Sample not found: ${filename}`);
    const blob = await res.blob();
    handleFile(new File([blob], filename, { type: blob.type || "application/octet-stream" }));
  }

  async function handleFile(file: File) {
    setUploading(true);
    setReady(false);
    setSemanticAnalysis([]);
    try {
      const result = await uploadFile(file);
      onSourceChange({
        ...emptyEndpoint("file", "Source"),
        kind: "file",
        connected: true,
        file: {
          fileName: result.filename,
          fileId: result.file_id,
          detectedFormat: result.format,
          format: result.format === "csv" ? "csv" : "auto",
          encoding: result.encoding ?? "utf-8",
          rowCount: result.row_count,
          columns: result.columns,
          previewRows: result.preview_rows ?? [],
        },
      });
      try {
        const analysis = await analyzeColumnSchema(result.columns);
        setSemanticAnalysis(analysis.columns);
      } catch {
        setSemanticAnalysis([]);
      }
      toast({
        title: "File analyzed",
        message: `${result.row_count.toLocaleString()} rows · ${result.columns.length} columns · zero parse errors`,
        tone: "success",
      });
    } catch (e) {
      toast({
        title: "Upload failed",
        message: e instanceof Error ? e.message : "Could not parse file",
        tone: "error",
      });
    } finally {
      setUploading(false);
    }
  }

  const handlePrepare = useCallback(async () => {
    setWorking(true);
    setReady(false);
    setProgress(null);

    const srcStr = resolveConnectionString(sourceConnStr, sourceCreds);
    const dstStr = resolveConnectionString(destConnStr, destCreds);

    try {
      if (template.id === "db-db") {
        const result = await autoConnectDatabases(srcStr, dstStr, {
          autoCreateDestination: autoCreate,
          onProgress: setProgress,
        });
        if (result.error) throw new Error(result.error);
        onSourceChange(result.source);
        onDestinationChange(result.destination);
        onMappingsReady(result.identityMappings);
        setSemanticAnalysis(result.semanticAnalysis ?? []);
        toast({ title: "Databases connected", message: result.selectedTables[0], tone: "success" });
      } else if (template.id === "file-db") {
        const result = await prepareFileToDatabase(source, dstStr, { onProgress: setProgress });
        if (result.error) throw new Error(result.error);
        onDestinationChange(result.destination);
        onMappingsReady(result.identityMappings);
        setSemanticAnalysis(result.semanticAnalysis ?? []);
        toast({
          title: "Semantic mapping complete",
          message: `${result.identityMappings.length} columns mapped · ready for preflight`,
          tone: "success",
        });
      } else if (template.id === "file-file") {
        if (!source.file.fileName) throw new Error("Upload a source file");
        onDestinationChange({
          ...emptyEndpoint("file", "Destination"),
          kind: "file",
          connected: true,
          exportFormat,
        });
        onMappingsReady(
          source.file.columns.map((c) => ({
            source: c.name,
            target: c.name,
            confidence: 1,
            reasoning: "Format conversion — column preserved",
          }))
        );
        toast({ title: "Conversion ready", message: `Export as ${exportFormat.toUpperCase()}`, tone: "success" });
      } else if (template.id === "db-file") {
        const result = await prepareDatabaseToFile(srcStr, { onProgress: setProgress });
        if (result.error) throw new Error(result.error);
        onSourceChange(result.source);
        onDestinationChange({
          ...emptyEndpoint("file", "Destination"),
          kind: "file",
          connected: true,
          exportFormat,
        });
        onMappingsReady(result.identityMappings);
        toast({ title: "Export configured", message: `Dump to ${exportFormat.toUpperCase()}`, tone: "success" });
      } else if (template.id === "api-db") {
        const result = await prepareApiToDatabase(apiUrl, dstStr, { onProgress: setProgress });
        if (result.error) throw new Error(result.error);
        onSourceChange(result.source);
        onDestinationChange(result.destination);
        onMappingsReady(result.identityMappings);
        toast({ title: "API ingest ready", tone: "success" });
      }

      setReady(true);
    } catch (e) {
      const msg = e instanceof Error ? e.message : "Setup failed";
      toast({ title: "Setup failed", message: msg, tone: "error" });
      setProgress({ phase: "error", message: msg });
    } finally {
      setWorking(false);
    }
  }, [
    template.id,
    sourceConnStr,
    destConnStr,
    sourceCreds,
    destCreds,
    apiUrl,
    exportFormat,
    autoCreate,
    source,
    onSourceChange,
    onDestinationChange,
    onMappingsReady,
    toast,
  ]);

  function handleTemplateChange(id: string) {
    setTemplateId(id);
    setReady(false);
    setSemanticAnalysis([]);
    setProgress(null);
    const t = TRANSFER_TEMPLATES.find((x) => x.id === id)!;
    onSourceChange(emptyEndpoint(t.sourceKind, "Source"));
    onDestinationChange(
      t.destKind === "file"
        ? { ...emptyEndpoint("file", "Destination"), exportFormat: t.destFormat ?? "csv" }
        : emptyEndpoint("database", "Destination")
    );
    if (t.destFormat) setExportFormat(t.destFormat);
  }

  const connectPct = progress ? PHASE_PROGRESS[progress.phase] : 0;
  const sourceConnectors = savedConnectors.filter((c) => c.role === "source" || c.role === "both");
  const destConnectors = savedConnectors.filter((c) => c.role === "destination" || c.role === "both");

  const sourcePanel =
    template.sourceKind === "file" ? (
      <>
        <FileDropzone
          title="Drop any file"
          hint="CSV · Excel · JSON · PDF · Word · Parquet · SQL dump"
          accept=".csv,.json,.txt,.tsv,.xlsx,.xls,.parquet,.pdf,.doc,.docx,.xml,.sql"
          busy={uploading}
          fileName={source.file.fileName}
          rowCount={source.file.rowCount}
          onFileSelect={handleFile}
        />
        <SampleFilePicker
          samples={SAMPLE_FILES}
          disabled={uploading}
          onSelect={(filename) => {
            loadSampleFile(filename).catch((e) =>
              toast({ title: "Sample load failed", message: String(e), tone: "error" })
            );
          }}
        />
        {source.file.fileName && (
          <>
            <FilePreviewTable
              columns={source.file.columns.map((c) => c.name)}
              rows={source.file.previewRows}
              format={source.file.detectedFormat ?? undefined}
              rowCount={source.file.rowCount ?? undefined}
            />
            {semanticAnalysis.length > 0 && <SemanticPreview columns={semanticAnalysis} />}
          </>
        )}
      </>
    ) : template.sourceKind === "database" ? (
      <DatabaseEndpointPanel
        label="Source database"
        hint="PostgreSQL · Snowflake · MongoDB · SQL Server · 600+ connectors"
        accent="orange"
        connectionString={sourceConnStr}
        onConnectionStringChange={(v) => {
          setSourceConnStr(v);
          setSourceConnectorId("");
          setReady(false);
          setSemanticAnalysis([]);
        }}
        credentials={sourceCreds}
        onCredentialsChange={setSourceCreds}
        databaseOptions={DATABASE_OPTIONS}
        savedConnectors={sourceConnectors}
        savedConnectorId={sourceConnectorId}
        onSavedConnectorChange={(id) => {
          setSourceConnectorId(id);
          void applySavedConnector(id, "source");
        }}
        onRefreshConnectors={loadConnectors}
        loadingConnectors={loadingConnectors}
        dbTypeLabel={
          sourceCreds.username || sourceConnStr.trim()
            ? detectDatabaseType(sourceConnStr || sourceCreds.type).toUpperCase()
            : undefined
        }
        placeholder="postgresql://user:pass@host:5432/source_db"
      />
    ) : (
      <>
        <span className="df-label">REST endpoint URL</span>
        <input
          className="df-input"
          value={apiUrl}
          onChange={(e) => {
            setApiUrl(e.target.value);
            setReady(false);
            setSemanticAnalysis([]);
          }}
          placeholder="https://api.example.com/v1/data"
        />
      </>
    );

  const destinationPanel =
    template.destKind === "database" ? (
      <>
        <DatabaseEndpointPanel
          label="Destination database"
          hint="Username & password or connection string — any supported engine"
          accent="mint"
          connectionString={destConnStr}
          onConnectionStringChange={(v) => {
            setDestConnStr(v);
            setDestConnectorId("");
            setReady(false);
            setSemanticAnalysis([]);
          }}
          credentials={destCreds}
          onCredentialsChange={setDestCreds}
          databaseOptions={DATABASE_OPTIONS}
          savedConnectors={destConnectors}
          savedConnectorId={destConnectorId}
          onSavedConnectorChange={(id) => {
            setDestConnectorId(id);
            void applySavedConnector(id, "dest");
          }}
          onRefreshConnectors={loadConnectors}
          loadingConnectors={loadingConnectors}
          dbTypeLabel={
            destCreds.username || destConnStr.trim()
              ? detectDatabaseType(destConnStr || destCreds.type).toUpperCase()
              : undefined
          }
          placeholder="snowflake://user:pass@account.snowflakecomputing.com/warehouse_db"
        />
        {template.id === "db-db" && (
          <label className="df-checkbox-label">
            <input type="checkbox" checked={autoCreate} onChange={(e) => setAutoCreate(e.target.checked)} />
            Auto-create tables on destination
          </label>
        )}
      </>
    ) : (
      <>
        <span className="df-label">Export format</span>
        <select
          className="df-select"
          value={exportFormat}
          onChange={(e) => {
            setExportFormat(e.target.value as FileFormat);
            setReady(false);
            setSemanticAnalysis([]);
          }}
        >
          {FILE_FORMAT_OPTIONS.filter((f) => f.id !== "auto").map((f) => (
            <option key={f.id} value={f.id}>
              {f.label}
            </option>
          ))}
        </select>
        <p className="df-field-hint">CSV · Excel · PDF · Word · JSON · SQL and more</p>
      </>
    );

  return (
    <>
      <p className="df-page-lead">
        Universal one-click transfer — AI detects schema and semantics automatically. Every row validated before upload.
      </p>

      <TransferHub
        templates={TRANSFER_TEMPLATES.map((t) => ({ id: t.id, label: t.label, description: t.description }))}
        activeTemplateId={templateId}
        onTemplateChange={handleTemplateChange}
        operationLabel={operationLabel(operation)}
        operationHint={operationDescription(operation)}
        sourcePanel={sourcePanel}
        destinationPanel={destinationPanel}
        status={
          working ? (
            <div className="df-transfer-hub-status">
              <LoadingState label={progress?.message ?? "Analyzing…"} compact />
              <ProgressBar value={connectPct} tone="brand" size="sm" />
            </div>
          ) : null
        }
        footer={
          <div className="df-transfer-hub-footer">
            <Button variant="primary" disabled={working} onClick={ready ? onNext : handlePrepare}>
              {ready ? "Run preflight & transfer" : "Analyze & prepare transfer"}
            </Button>
          </div>
        }
      />
    </>
  );
}
