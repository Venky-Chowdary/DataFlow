import { useEffect, useState, type ReactNode } from "react";
import { ConnectorIcon } from "../../app/brand-icons";
import { DtIcon } from "../DtIcon";
import { StructurePreview } from "../ui/StructurePreview";
import { MultiStreamSchemaPreview } from "./MultiStreamSchemaPreview";
import type { Connector } from "../../lib/types";
import type { SourceKind } from "../ui/SourceKindTiles";
import type { StreamSchemaPreview } from "../../lib/sourceStreams";

const FILE_FORMATS = ["CSV", "JSON", "JSONL", "TSV", "Parquet"];

interface SourceStepAsideProps {
  sourceKind: SourceKind;
  parsed: {
    columns: string[];
    schema?: Record<string, string>;
    row_count: number;
    data?: Record<string, unknown>[];
    sample_data?: Record<string, unknown>[];
  } | null;
  samplePreviewRows: Record<string, unknown>[];
  sourceConnector: Connector | undefined;
  sourceColumns: string[];
  sourceSchema: Record<string, string>;
  cloudPath: string;
  dbConnectors: Connector[];
  cloudConnectors: Connector[];
  uploading?: boolean;
  sourceManual?: boolean;
  sourceManualType?: string;
  sourceIntrospecting?: boolean;
  sourceIntrospectError?: string | null;
  onRetrySourceIntrospect?: () => void;
  sourceObjectLabel?: string;
  streamNames?: string[];
  streamPreviews?: StreamSchemaPreview[];
  activeStreamTab?: string;
  onActiveStreamTabChange?: (name: string) => void;
}

function ProfilingSteps({ active }: { active: boolean }) {
  const steps = [
    { id: "upload", label: "Upload file", detail: "Drop or browse your dataset" },
    { id: "profile", label: "Profile schema", detail: "Detect columns and types" },
    { id: "preview", label: "Preview structure", detail: "Sample rows appear here" },
  ];
  const activeIdx = active ? 1 : 0;

  return (
    <ol className="df2-source-aside-steps" aria-label="Profiling steps">
      {steps.map((step, i) => (
        <li
          key={step.id}
          className={
            i < activeIdx ? "done"
              : i === activeIdx ? "active"
              : ""
          }
        >
          <span className="df2-source-aside-step-marker" aria-hidden>
            {i < activeIdx ? <DtIcon name="check" size={12} /> : i + 1}
          </span>
          <div>
            <strong>{step.label}</strong>
            <span>{step.detail}</span>
          </div>
        </li>
      ))}
    </ol>
  );
}

function SchemaSkeleton() {
  return (
    <div className="df2-source-aside-skeleton" aria-hidden>
      <div className="df2-source-aside-skeleton-row" />
      <div className="df2-source-aside-skeleton-row short" />
      <div className="df2-source-aside-skeleton-chips">
        {Array.from({ length: 6 }).map((_, i) => (
          <span key={i} className="df2-source-aside-skeleton-chip" />
        ))}
      </div>
      <div className="df2-source-aside-skeleton-table">
        {Array.from({ length: 4 }).map((_, i) => (
          <div key={i} className="df2-source-aside-skeleton-line" />
        ))}
      </div>
    </div>
  );
}

const ANALYZE_PHASES = [
  "Connecting to source…",
  "Reading table & column metadata…",
  "Sampling representative rows…",
  "Inferring types & building preview…",
];

/**
 * Polished, honest loader shown while live schema introspection runs. The bar
 * is intentionally indeterminate (the backend returns one result, not a stream
 * of progress) but the cycling caption reflects the real phases of the analysis.
 */
function SchemaAnalyzingPanel({ target }: { target?: string }) {
  const [phaseIdx, setPhaseIdx] = useState(0);
  useEffect(() => {
    const t = window.setInterval(
      () => setPhaseIdx((i) => (i + 1) % ANALYZE_PHASES.length),
      1100,
    );
    return () => window.clearInterval(t);
  }, []);

  return (
    <div className="df2-source-aside df2-source-analyze">
      <div className="df2-source-aside-head">
        <div>
          <h4>Analyzing source{target ? ` · ${target}` : ""}</h4>
          <p>Introspecting schema and sampling live rows from the connector…</p>
        </div>
        <span className="df2-badge df2-badge-xs df2-badge-live">Analyzing</span>
      </div>

      <div
        className="df2-source-analyze-progress"
        role="progressbar"
        aria-label="Schema analysis in progress"
        aria-busy="true"
      >
        <div className="df2-source-analyze-track">
          <span className="df2-source-analyze-indeterminate" />
        </div>
        <p className="df2-source-analyze-phase" aria-live="polite">
          <DtIcon name="sparkle" size={13} />
          {ANALYZE_PHASES[phaseIdx]}
        </p>
      </div>

      <ProfilingSteps active />
      <SchemaSkeleton />
    </div>
  );
}

function SchemaErrorPanel({ message, onRetry }: { message: string; onRetry?: () => void }) {
  return (
    <div className="df2-source-aside df2-source-aside-error">
      <div className="df2-source-aside-head">
        <div>
          <h4>Couldn’t read the source</h4>
          <p>{message}</p>
        </div>
        <span className="df2-badge df2-badge-xs df2-badge-run">Error</span>
      </div>
      <div className="df2-source-aside-empty">
        <DtIcon name="alert" size={22} />
        <p>Verify the table or collection name, credentials, and network access.</p>
        {onRetry && (
          <button type="button" className="df2-btn df2-btn-secondary df2-btn-sm" onClick={onRetry}>
            Retry schema read
          </button>
        )}
      </div>
    </div>
  );
}

function FileAwaitingPanel({ uploading }: { uploading?: boolean }) {
  return (
    <div className="df2-source-aside">
      <div className="df2-source-aside-head">
        <div>
          <h4>Structure preview</h4>
          <p>
            {uploading
              ? "Profiling your file — real columns and sample values load here."
              : "Upload a file on the left. Detected schema and sample rows show here automatically."}
          </p>
        </div>
        <span className={`df2-badge df2-badge-xs ${uploading ? "df2-badge-live" : ""}`}>
          {uploading ? "Profiling…" : "Waiting"}
        </span>
      </div>

      <ProfilingSteps active={Boolean(uploading)} />

      {uploading ? (
        <SchemaSkeleton />
      ) : (
        <div className="df2-source-aside-formats">
          <span className="df2-source-aside-formats-label">Supported formats</span>
          <div className="df2-source-aside-format-chips">
            {FILE_FORMATS.map((fmt) => (
              <span key={fmt} className="df2-source-aside-format-chip">{fmt}</span>
            )).reduce((acc: ReactNode[], chip, i) => {
              acc.push(chip);
              if (i < FILE_FORMATS.length - 1) acc.push(" ");
              return acc;
            }, [])}
          </div>
        </div>
      )}

      <div className="df2-source-aside-hint">
        <DtIcon name="sparkle" size={14} />
        No sample data shown until your file is profiled — avoids confusion with real results.
      </div>
    </div>
  );
}

export function SourceStepAside({
  sourceKind,
  parsed,
  samplePreviewRows,
  sourceConnector,
  sourceColumns,
  sourceSchema,
  cloudPath,
  dbConnectors,
  cloudConnectors,
  uploading,
  sourceManual,
  sourceManualType,
  sourceIntrospecting,
  sourceIntrospectError,
  onRetrySourceIntrospect,
  sourceObjectLabel,
  streamNames,
  streamPreviews = [],
  activeStreamTab,
  onActiveStreamTabChange,
}: SourceStepAsideProps) {
  const multiStream = (streamNames?.length ?? 0) > 1 || streamPreviews.length > 1;
  // Tabbed card only when the user entered multiple streams — single-stream keeps the simple preview.
  const hasStreamTabs = multiStream && streamPreviews.length > 0;

  if (sourceKind === "file" && parsed) {
    return (
      <StructurePreview
        columns={parsed.columns}
        schema={parsed.schema}
        rows={samplePreviewRows}
        rowCount={parsed.row_count}
        title="Detected structure"
        subtitle={`${parsed.columns.length} fields · ${parsed.row_count.toLocaleString()} rows`}
        fill
        showBadge
        allowJson
      />
    );
  }

  // Comma-separated multi-stream: one tab per table/collection with its own schema.
  if (sourceKind === "database" && hasStreamTabs) {
    return (
      <div className="df2-source-aside df2-source-aside-multistream">
        <MultiStreamSchemaPreview
          streams={streamPreviews}
          connectorName={sourceConnector?.name}
          activeName={activeStreamTab}
          onActiveChange={onActiveStreamTabChange}
          loading={sourceIntrospecting}
        />
        {sourceIntrospectError && (
          <div className="df2-source-aside-stream-warn" role="status">
            <DtIcon name="alert" size={14} />
            <p>{sourceIntrospectError}</p>
            {onRetrySourceIntrospect && (
              <button type="button" className="df2-btn df2-btn-sm df2-btn-ghost" onClick={onRetrySourceIntrospect}>
                Retry
              </button>
            )}
          </div>
        )}
      </div>
    );
  }

  if (sourceColumns.length > 0 && !sourceIntrospecting) {
    const missingSamples = samplePreviewRows.length === 0;
    return (
      <StructurePreview
        columns={sourceColumns}
        schema={sourceSchema}
        rows={samplePreviewRows}
        rowCount={parsed?.row_count}
        title="Source schema"
        subtitle={
          sourceConnector
            ? `${sourceColumns.length} fields${samplePreviewRows.length ? ` · ${samplePreviewRows.length} sample rows` : " · no sample rows yet"} from ${sourceConnector.name}${
              sourceObjectLabel ? ` · ${sourceObjectLabel}` : ""
            }`
            : "Fields discovered from the selected connector"
        }
        fill
        showBadge
        allowJson
        sampleWarning={
          missingSamples
            ? (sourceIntrospectError
              || "No preview rows returned for this table. Reload sample so Validate dry-run can run.")
            : null
        }
        onRetrySample={missingSamples ? onRetrySourceIntrospect : undefined}
      />
    );
  }

  if (sourceKind === "file") {
    return <FileAwaitingPanel uploading={uploading} />;
  }

  if (sourceIntrospecting) {
    return <SchemaAnalyzingPanel target={sourceObjectLabel} />;
  }

  if (sourceIntrospectError) {
    return <SchemaErrorPanel message={sourceIntrospectError} onRetry={onRetrySourceIntrospect} />;
  }

  if (sourceKind === "database") {
    const pool = dbConnectors;
    return (
      <div className="df2-source-aside">
        <div className="df2-source-aside-head">
          <div>
            <h4>Connector preview</h4>
            <p>
              {sourceManual
                ? `Manual ${sourceManualType} source. Schema loads when you continue.`
                : sourceConnector
                  ? `Enter table/collection name(s) on the left — each is read separately for preview.`
                  : pool.length
                    ? `Select one of ${pool.length} saved database connector${pool.length === 1 ? "" : "s"} on the left.`
                    : "Add a database connector to read tables and collections, or use manual connection."}
            </p>
          </div>
        </div>

        {sourceConnector ? (
          <>
            <article className="df2-source-aside-connector">
              <ConnectorIcon id={sourceConnector.type} size={28} />
              <div>
                <strong>{sourceConnector.name}</strong>
                <span>{sourceConnector.type} · {sourceConnector.host}:{sourceConnector.port}</span>
                <span>{sourceConnector.database || "default database"}</span>
              </div>
              <span className={`df2-badge df2-badge-xs ${sourceConnector.last_test_ok !== false ? "df2-badge-live" : "df2-badge-run"}`}>
                {sourceConnector.last_test_ok !== false ? "Tested" : "Untested"}
              </span>
            </article>
            <p className="df2-source-aside-path">
              {sourceObjectLabel
                ? <>Waiting for schema from <strong>{sourceObjectLabel}</strong></>
                : "Enter one name, or comma-separate several for multi-stream CDC / incremental (one preview tab each)."}
            </p>
          </>
        ) : pool.length > 0 ? (
          <div className="df2-source-aside-connector-list">
            {pool.slice(0, 6).map((c) => (
              <div key={c.id} className="df2-source-aside-connector-row">
                <ConnectorIcon id={c.type} size={18} />
                <div>
                  <strong>{c.name}</strong>
                  <span>{c.type} · {c.database || c.host}</span>
                </div>
              </div>
            ))}
            {pool.length > 6 && (
              <p className="df2-source-aside-more">+{pool.length - 6} more connectors</p>
            )}
          </div>
        ) : (
          <div className="df2-source-aside-empty">
            <DtIcon name="connectors" size={22} />
            <p>No database connectors yet. Add PostgreSQL, MongoDB, or a warehouse in Connectors.</p>
          </div>
        )}

        <div className="df2-source-aside-hint">
          <DtIcon name="sparkle" size={14} />
          {multiStream
            ? "Multi-stream preview opens one tab per name once schemas are read."
            : "Schema appears here as soon as the table or collection is readable."}
        </div>
      </div>
    );
  }

  const pool = cloudConnectors;
  return (
    <div className="df2-source-aside">
      <div className="df2-source-aside-head">
        <div>
          <h4>Object preview</h4>
          <p>
            {sourceConnector && cloudPath.trim()
              ? "Format is inferred from the object key when you continue."
              : sourceConnector
                ? "Enter an object path or prefix to profile the file."
                : pool.length
                  ? "Pick a cloud connector and object path on the left."
                  : "Add S3, GCS, or Azure Blob storage in Connectors."}
          </p>
        </div>
      </div>

      {sourceConnector ? (
        <article className="df2-source-aside-connector">
          <ConnectorIcon id={sourceConnector.type} size={28} />
          <div>
            <strong>{sourceConnector.name}</strong>
            <span>{sourceConnector.type}</span>
            {cloudPath.trim() && <code className="df2-source-aside-path">{cloudPath.trim()}</code>}
          </div>
        </article>
      ) : pool.length > 0 ? (
        <div className="df2-source-aside-connector-list">
          {pool.slice(0, 4).map((c) => (
            <div key={c.id} className="df2-source-aside-connector-row">
              <ConnectorIcon id={c.type} size={18} />
              <div>
                <strong>{c.name}</strong>
                <span>{c.type}</span>
              </div>
            </div>
          ))}
        </div>
      ) : (
        <div className="df2-source-aside-empty">
          <DtIcon name="connectors" size={22} />
          <p>No cloud storage connectors configured.</p>
        </div>
      )}

      <div className="df2-source-aside-hint">
        <DtIcon name="upload" size={14} />
        Supports JSON, JSONL, CSV, TSV, and Parquet objects in your bucket.
      </div>
    </div>
  );
}
