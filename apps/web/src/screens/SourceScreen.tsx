import {
  ActionBar,
  AlertBanner,
  Button,
  DatabaseConnectionForm,
  FileDropzone,
  FilePreviewTable,
  FormSection,
  PageHead,
  SegmentedControl,
} from "@dataflow/design-system";
import { useState } from "react";
import { introspectSchema, testConnection, uploadFile } from "../lib/api";
import {
  DATABASE_OPTIONS,
  emptyDatabase,
  type DatabaseConnection,
  type EndpointConfig,
  type EndpointKind,
} from "../lib/types";

interface SourceScreenProps {
  endpoint: EndpointConfig;
  onChange: (endpoint: EndpointConfig) => void;
  onNext: () => void;
}

const SOURCE_MODES = [
  { id: "file", label: "File" },
  { id: "database", label: "Database" },
  { id: "api", label: "API" },
];

async function loadDemoFile(): Promise<File> {
  const res = await fetch("/sample_payments.csv");
  const blob = await res.blob();
  return new File([blob], "sample_payments.csv", { type: "text/csv" });
}

export function SourceScreen({ endpoint, onChange, onNext }: SourceScreenProps) {
  const [useConnStr, setUseConnStr] = useState(true);
  const [testing, setTesting] = useState(false);
  const [introspecting, setIntrospecting] = useState(false);
  const [uploading, setUploading] = useState(false);
  const [uploadError, setUploadError] = useState<string | null>(null);
  const [selectedTable, setSelectedTable] = useState(endpoint.database?.sourceTable ?? "");

  function setKind(kind: EndpointKind) {
    onChange({
      ...endpoint,
      kind,
      label: "Source",
      database: kind === "database" ? endpoint.database ?? emptyDatabase() : null,
      api: kind === "api" ? endpoint.api ?? { url: "", authType: "none" } : null,
      connected: kind === "file" ? !!endpoint.file.fileName : false,
      connectionError: null,
    });
  }

  async function handleFile(file: File) {
    setUploading(true);
    setUploadError(null);
    try {
      const result = await uploadFile(file);
      onChange({
        ...endpoint,
        connected: true,
        file: {
          ...endpoint.file,
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
    } catch (e) {
      setUploadError(e instanceof Error ? e.message : "Upload failed");
    } finally {
      setUploading(false);
    }
  }

  async function handleTestDb() {
    if (!endpoint.database) return;
    setTesting(true);
    const result = await testConnection(endpoint.database);
    const db: DatabaseConnection = {
      ...endpoint.database,
      tables: result.tables ?? [],
      targetColumns: [],
    };
    onChange({
      ...endpoint,
      database: db,
      connected: result.ok,
      connectionError: result.error ?? null,
    });
    setTesting(false);
    if (result.ok && result.tables?.length) {
      const table = result.tables[0];
      setSelectedTable(table);
      await loadSourceSchema(db, table);
    }
  }

  async function loadSourceSchema(db: DatabaseConnection, table: string) {
    setIntrospecting(true);
    try {
      const schema = await introspectSchema(db, table);
      if (schema.ok) {
        onChange({
          ...endpoint,
          database: {
            ...db,
            sourceTable: table,
            targetColumns: schema.columns,
            tables: schema.tables.length ? schema.tables : db.tables,
          },
          connected: true,
        });
      }
    } finally {
      setIntrospecting(false);
    }
  }

  async function handleTableChange(table: string) {
    setSelectedTable(table);
    if (endpoint.database) await loadSourceSchema(endpoint.database, table);
  }

  function canContinue(): boolean {
    if (endpoint.kind === "file") return !!endpoint.file.fileName;
    if (endpoint.kind === "database") {
      return endpoint.connected && !!endpoint.database?.sourceTable && endpoint.database.targetColumns.length > 0;
    }
    if (endpoint.kind === "api") return !!endpoint.api?.url;
    return false;
  }

  const previewColumns = endpoint.file.columns.map((c) => c.name);

  return (
    <>
      <PageHead description="File upload, connection string, or REST API." />

      <SegmentedControl
        options={SOURCE_MODES}
        value={endpoint.kind}
        onChange={(id) => setKind(id as EndpointKind)}
        ariaLabel="Source type"
      />

      {endpoint.kind === "file" && (
        <FormSection
          title="File upload"
          subtitle={endpoint.file.fileName ?? "Drop or select a file"}
          connected={!!endpoint.file.fileName}
        >
          <FileDropzone
            title="Drop your file here"
            hint="CSV, JSON, TSV, Excel, Parquet"
            actionLabel="Try sample"
            onAction={() => loadDemoFile().then(handleFile).catch(() => setUploadError("Sample unavailable"))}
            onFileSelect={handleFile}
            busy={uploading}
          />
          {uploadError && <AlertBanner variant="danger" message={uploadError} />}
          {endpoint.file.fileName && previewColumns.length > 0 && (
            <FilePreviewTable
              columns={previewColumns}
              rows={endpoint.file.previewRows}
              format={endpoint.file.detectedFormat ?? undefined}
              rowCount={endpoint.file.rowCount ?? undefined}
            />
          )}
        </FormSection>
      )}

      {endpoint.kind === "database" && endpoint.database && (
        <FormSection
          title="Database connection"
          subtitle={`${endpoint.database.type} · ${endpoint.database.database || "paste connection string"}`}
          connected={endpoint.connected}
          loading={testing || introspecting}
          loadingLabel={testing ? "Testing connection…" : "Reading schema…"}
          onTest={handleTestDb}
        >
          <DatabaseConnectionForm
            values={endpoint.database}
            onChange={(db) =>
              onChange({
                ...endpoint,
                database: { ...endpoint.database!, ...db, targetColumns: [], sourceTable: "" } as DatabaseConnection,
                connected: false,
              })
            }
            databaseOptions={DATABASE_OPTIONS}
            useConnectionString={useConnStr}
            onToggleConnectionString={setUseConnStr}
          />
          {endpoint.database.tables.length > 0 && (
            <div className="df-form-field df-form-field-spaced">
              <span className="df-label">Table</span>
              <select className="df-select" value={selectedTable} onChange={(e) => handleTableChange(e.target.value)}>
                {endpoint.database.tables.map((t) => (
                  <option key={t} value={t}>{t}</option>
                ))}
              </select>
            </div>
          )}
          {endpoint.database.targetColumns.length > 0 && (
            <div className="df-form-field-spaced">
              <span className="df-label">{endpoint.database.targetColumns.length} columns detected</span>
              <div className="df-schema-chips">
                {endpoint.database.targetColumns.map((c) => (
                  <span key={c.name} className="df-schema-chip df-mono">
                    {c.name}
                    <span className="df-schema-chip-type">{c.inferred_type}</span>
                  </span>
                ))}
              </div>
            </div>
          )}
          {(testing || introspecting) ? null : endpoint.connectionError && (
            <AlertBanner variant="danger" message={endpoint.connectionError} />
          )}
        </FormSection>
      )}

      {endpoint.kind === "api" && endpoint.api && (
        <FormSection title="REST endpoint" subtitle={endpoint.api.url || "Enter URL"} connected={!!endpoint.api.url}>
          <span className="df-label">URL</span>
          <input
            className="df-input"
            placeholder="https://api.example.com/v1/data"
            value={endpoint.api.url}
            onChange={(e) =>
              onChange({
                ...endpoint,
                connected: !!e.target.value,
                api: { ...endpoint.api!, url: e.target.value },
              })
            }
          />
          <p className="df-file-meta df-form-field-spaced">Use Connectors for custom API ingest.</p>
        </FormSection>
      )}

      <ActionBar align="end">
        <Button variant="primary" disabled={!canContinue()} onClick={onNext}>
          Continue
        </Button>
      </ActionBar>
    </>
  );
}
