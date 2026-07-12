import type { ReactNode } from "react";
import { ConnectorIcon } from "../../app/brand-icons";
import { DtIcon } from "../DtIcon";
import { StructurePreview } from "../ui/StructurePreview";
import type { Connector } from "../../lib/types";
import type { SourceKind } from "../ui/SourceKindTiles";

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
            )).reduce((acc: React.ReactNode[], chip, i) => {
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
}: SourceStepAsideProps) {
  if (sourceKind === "file" && parsed) {
    return (
      <StructurePreview
        columns={parsed.columns}
        schema={parsed.schema}
        rows={samplePreviewRows}
        rowCount={parsed.row_count}
        title="Detected structure"
        subtitle={`${parsed.columns.length} fields · ${parsed.row_count.toLocaleString()} rows`}
      />
    );
  }

  if (sourceColumns.length > 0) {
    return (
      <StructurePreview
        columns={sourceColumns}
        schema={sourceSchema}
        title="Source schema"
        subtitle={
          sourceConnector
            ? `${sourceColumns.length} fields from ${sourceConnector.name}`
            : "Fields discovered from the selected connector"
        }
      />
    );
  }

  if (sourceKind === "file") {
    return <FileAwaitingPanel uploading={uploading} />;
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
                  ? `Schema from ${sourceConnector.name} loads when you continue.`
                  : pool.length
                    ? `Select one of ${pool.length} saved database connector${pool.length === 1 ? "" : "s"} on the left.`
                    : "Add a database connector to read tables and collections, or use manual connection."}
            </p>
          </div>
        </div>

        {sourceConnector ? (
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
          Table or collection schema appears here after you continue.
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
