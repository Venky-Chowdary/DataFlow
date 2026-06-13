import {
  ActionBar,
  AlertBanner,
  Button,
  DatabaseConnectionForm,
  FormSection,
  PageHead,
  SegmentedControl,
} from "@dataflow/design-system";
import { useState } from "react";
import { introspectSchema, testConnection } from "../lib/api";
import {
  DATABASE_OPTIONS,
  emptyDatabase,
  FILE_FORMAT_OPTIONS,
  type DatabaseConnection,
  type EndpointConfig,
  type EndpointKind,
} from "../lib/types";

interface DestinationScreenProps {
  endpoint: EndpointConfig;
  onChange: (endpoint: EndpointConfig) => void;
  onBack: () => void;
  onNext: () => void;
}

const DEST_MODES = [
  { id: "database", label: "Database" },
  { id: "file", label: "Export" },
];

export function DestinationScreen({ endpoint, onChange, onBack, onNext }: DestinationScreenProps) {
  const [useConnStr, setUseConnStr] = useState(true);
  const [testing, setTesting] = useState(false);
  const [introspecting, setIntrospecting] = useState(false);
  const [autoCreateSchema, setAutoCreateSchema] = useState(true);
  const [selectedTable, setSelectedTable] = useState("");

  function setKind(kind: EndpointKind) {
    onChange({
      ...endpoint,
      kind,
      label: "Destination",
      database: kind === "database" ? endpoint.database ?? emptyDatabase("postgresql") : null,
      exportFormat: kind === "file" ? endpoint.exportFormat ?? "csv" : null,
      connected: false,
      connectionError: null,
    });
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
      setSelectedTable(result.tables[0]);
      await loadSchema(db, result.tables[0]);
    }
  }

  async function loadSchema(db: DatabaseConnection, table: string) {
    setIntrospecting(true);
    try {
      const schema = await introspectSchema(db, table);
      if (schema.ok) {
        onChange({
          ...endpoint,
          database: {
            ...db,
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
    if (endpoint.database) await loadSchema(endpoint.database, table);
  }

  function canContinue(): boolean {
    if (endpoint.kind === "database") {
      return endpoint.connected && (autoCreateSchema || (endpoint.database?.targetColumns.length ?? 0) > 0);
    }
    if (endpoint.kind === "file") return !!endpoint.exportFormat;
    return false;
  }

  return (
    <>
      <PageHead description="Paste destination connection string — tables are auto-created to match source schema when enabled." />

      <SegmentedControl
        options={DEST_MODES}
        value={endpoint.kind}
        onChange={(id) => setKind(id as EndpointKind)}
        ariaLabel="Destination type"
      />

      {endpoint.kind === "database" && endpoint.database && (
        <FormSection
          title="Target database"
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
                database: { ...endpoint.database!, ...db, targetColumns: [] } as DatabaseConnection,
                connected: false,
              })
            }
            databaseOptions={DATABASE_OPTIONS}
            useConnectionString={useConnStr}
            onToggleConnectionString={setUseConnStr}
          />

          <label className="df-checkbox-label df-form-field-spaced">
            <input type="checkbox" checked={autoCreateSchema} onChange={(e) => setAutoCreateSchema(e.target.checked)} />
            Auto-create table from mapped schema
          </label>

          {endpoint.database.tables.length > 0 && (
            <div className="df-form-field df-form-field-spaced">
              <span className="df-label">Reference table</span>
              <select className="df-select" value={selectedTable} onChange={(e) => handleTableChange(e.target.value)}>
                {endpoint.database.tables.map((t) => (
                  <option key={t} value={t}>{t}</option>
                ))}
              </select>
            </div>
          )}

          {endpoint.database.targetColumns.length > 0 && (
            <div className="df-form-field-spaced">
              <span className="df-label">{endpoint.database.targetColumns.length} columns</span>
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

      {endpoint.kind === "file" && (
        <FormSection title="File export" subtitle={endpoint.exportFormat?.toUpperCase() ?? "CSV"} connected={!!endpoint.exportFormat}>
          <span className="df-label">Format</span>
          <select
            className="df-select"
            value={endpoint.exportFormat ?? "csv"}
            onChange={(e) =>
              onChange({
                ...endpoint,
                connected: true,
                exportFormat: e.target.value as NonNullable<typeof endpoint.exportFormat>,
              })
            }
          >
            {FILE_FORMAT_OPTIONS.filter((f) => f.id !== "auto").map((f) => (
              <option key={f.id} value={f.id}>{f.label}</option>
            ))}
          </select>
        </FormSection>
      )}

      <ActionBar align="split" sticky>
        <Button variant="ghost" onClick={onBack}>Back</Button>
        <Button variant="primary" disabled={!canContinue()} onClick={onNext}>Continue</Button>
      </ActionBar>
    </>
  );
}
