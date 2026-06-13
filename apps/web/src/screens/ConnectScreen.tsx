import { useCallback, useEffect, useState } from "react";
import {
  AlertBanner,
  Button,
  DualConnectionPanel,
  FormSection,
  LoadingState,
  PreflightGateRail,
  ProductValueStrip,
  ProgressBar,
} from "@dataflow/design-system";
import type { MappingResult } from "../lib/api";
import { autoConnectDatabases, type AutoConnectProgress } from "../lib/autoDbSetup";
import { detectDatabaseType } from "../lib/connectionString";
import type { EndpointConfig } from "../lib/types";

interface ConnectScreenProps {
  source: EndpointConfig;
  destination: EndpointConfig;
  initialSourceConnStr?: string;
  initialDestConnStr?: string;
  onSourceChange: (ep: EndpointConfig) => void;
  onDestinationChange: (ep: EndpointConfig) => void;
  onMappingsReady: (mappings: MappingResult[]) => void;
  onNext: () => void;
  onAdvanced: () => void;
}

const PHASE_PROGRESS: Record<AutoConnectProgress["phase"], number> = {
  idle: 0,
  source: 25,
  destination: 50,
  schema: 70,
  semantic: 90,
  done: 100,
  error: 0,
};

export function ConnectScreen({
  source,
  destination,
  initialSourceConnStr,
  initialDestConnStr,
  onSourceChange,
  onDestinationChange,
  onMappingsReady,
  onNext,
  onAdvanced,
}: ConnectScreenProps) {
  const [sourceStr, setSourceStr] = useState(initialSourceConnStr ?? source.database?.connectionString ?? "");
  const [destStr, setDestStr] = useState(initialDestConnStr ?? destination.database?.connectionString ?? "");
  const [autoCreate, setAutoCreate] = useState(true);
  const [selectedTable, setSelectedTable] = useState(source.database?.sourceTable ?? "");
  const [tables, setTables] = useState<string[]>(source.database?.tables ?? []);
  const [connecting, setConnecting] = useState(false);
  const [progress, setProgress] = useState<AutoConnectProgress | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [ready, setReady] = useState(source.connected && destination.connected);

  const handleConnect = useCallback(async () => {
    setConnecting(true);
    setError(null);
    setReady(false);

    const result = await autoConnectDatabases(sourceStr, destStr, {
      selectedTable: selectedTable || undefined,
      autoCreateDestination: autoCreate,
      onProgress: setProgress,
    });

    if (result.error) {
      setError(result.error);
      setConnecting(false);
      return;
    }

    onSourceChange(result.source);
    onDestinationChange(result.destination);
    onMappingsReady(result.identityMappings);
    setTables(result.selectedTables);
    setSelectedTable(result.source.database?.sourceTable ?? "");
    setReady(true);
    setConnecting(false);
  }, [sourceStr, destStr, selectedTable, autoCreate, onSourceChange, onDestinationChange, onMappingsReady]);

  useEffect(() => {
    if (initialSourceConnStr && initialDestConnStr && !ready && !connecting) {
      handleConnect();
    }
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []);

  async function handleTableChange(table: string) {
    setSelectedTable(table);
    if (!sourceStr.trim() || !destStr.trim()) return;
    setConnecting(true);
    setError(null);
    const result = await autoConnectDatabases(sourceStr, destStr, {
      selectedTable: table,
      autoCreateDestination: autoCreate,
      onProgress: setProgress,
    });
    if (result.error) setError(result.error);
    else {
      onSourceChange(result.source);
      onDestinationChange(result.destination);
      onMappingsReady(result.identityMappings);
      setReady(true);
    }
    setConnecting(false);
  }

  const sourceType = sourceStr.trim() ? detectDatabaseType(sourceStr) : null;
  const destType = destStr.trim() ? detectDatabaseType(destStr) : null;
  const columnCount = source.database?.targetColumns.length ?? 0;
  const connectPct = progress ? PHASE_PROGRESS[progress.phase] : 0;

  return (
    <>
      <ProductValueStrip />
      <PreflightGateRail activeIndex={ready ? 0 : undefined} passedCount={ready ? 2 : 0} />

      <section className="df-workspace-panel">
        <div className="df-workspace-panel-head">
          <div>
            <h3 className="df-workspace-panel-title">Step 1 — Connect endpoints</h3>
            <p className="df-workspace-panel-sub">
              Paste source and destination connection strings. Schema is discovered before preflight runs.
            </p>
          </div>
          <Button variant="ghost" onClick={onAdvanced}>
            File / API / manual fields
          </Button>
        </div>

        <DualConnectionPanel
          source={{
            label: "Source",
            hint: "Database connection string",
            value: sourceStr,
            onChange: (v) => {
              setSourceStr(v);
              setReady(false);
            },
            dbTypeLabel: sourceType?.toUpperCase(),
            status: connecting && progress?.phase === "source" ? "connecting" : ready ? "connected" : error ? "error" : "idle",
            statusMessage: ready ? `${tables.length} tables discovered` : undefined,
            placeholder: "postgresql://user:pass@host:5432/source_db",
          }}
          destination={{
            label: "Destination",
            hint: "Database connection string",
            value: destStr,
            onChange: (v) => {
              setDestStr(v);
              setReady(false);
            },
            dbTypeLabel: destType?.toUpperCase(),
            status:
              connecting && (progress?.phase === "destination" || progress?.phase === "schema")
                ? "connecting"
                : ready
                  ? "connected"
                  : "idle",
            placeholder: "postgresql://user:pass@host:5432/target_db",
          }}
        />

        <label className="df-checkbox-label df-workspace-panel-option">
          <input type="checkbox" checked={autoCreate} onChange={(e) => setAutoCreate(e.target.checked)} />
          Auto-create destination table from source schema (Gate G6)
        </label>

        {connecting && (
          <ProgressBar
            value={connectPct}
            label={progress?.message ?? "Connecting…"}
            sublabel={`${connectPct}%`}
            tone="brand"
          />
        )}

        {!ready && !connecting && (
          <Button variant="primary" disabled={!sourceStr.trim() || !destStr.trim()} onClick={handleConnect}>
            Validate connections
          </Button>
        )}

        {error && <AlertBanner variant="danger" message={error} onRetry={handleConnect} />}
      </section>

      {ready && tables.length > 0 && (
        <FormSection
          title="Schema scope"
          subtitle={`Table ${selectedTable} · ${columnCount} columns for mapping (Gate G4)`}
          connected
        >
          <div className="df-form-field">
            <span className="df-label">Source table</span>
            <select className="df-select" value={selectedTable} onChange={(e) => handleTableChange(e.target.value)} disabled={connecting}>
              {tables.map((t) => (
                <option key={t} value={t}>
                  {t}
                </option>
              ))}
            </select>
          </div>

          {connecting ? (
            <LoadingState label="Reading schema…" compact />
          ) : (
            columnCount > 0 && (
              <div className="df-schema-chips">
                {source.database!.targetColumns.slice(0, 16).map((c) => (
                  <span key={c.name} className="df-schema-chip df-mono">
                    {c.name}
                    <span className="df-schema-chip-type">{c.inferred_type}</span>
                  </span>
                ))}
                {columnCount > 16 && <span className="df-schema-chip">+{columnCount - 16}</span>}
              </div>
            )
          )}
        </FormSection>
      )}

      <div className="df-action-bar df-action-bar--sticky df-action-bar--end">
        <Button variant="primary" disabled={!ready || connecting} onClick={onNext}>
          Continue to preflight
        </Button>
      </div>
    </>
  );
}
