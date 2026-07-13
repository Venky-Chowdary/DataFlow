import { DtIcon } from "../DtIcon";
import { JobTheater } from "../JobTheater";
import { ConnectorIcon } from "../../app/brand-icons";
import type { TransferResult } from "../../lib/types";

interface TransferResultDashboardProps {
  result: TransferResult;
  sourceLabel?: string;
  sourceType?: string;
  destLabel?: string;
  destType?: string;
  onNewTransfer?: () => void;
  onViewJobs?: () => void;
  onSchedule?: () => void;
}

function fmt(value: string | number | undefined): string | null {
  if (value === undefined || value === null || value === "") return null;
  return typeof value === "number" ? value.toLocaleString() : String(value);
}

export function TransferResultDashboard({
  result,
  sourceLabel = "Source",
  sourceType = "file",
  destLabel = "Destination",
  destType = "database",
  onNewTransfer,
  onViewJobs,
  onSchedule,
}: TransferResultDashboardProps) {
  const ds = result.destination_summary;
  const rec = result.records_transferred ?? 0;
  const rejected = ds?.rejected_rows ?? result.reconciliation?.rejected_rows ?? 0;
  const sourceRows = result.reconciliation?.source_rows ?? rec;
  const targetRows = result.reconciliation?.target_rows ?? rec;
  const passed = result.reconciliation?.passed ?? result.success;

  const destinationLine =
    ds?.table ? `${ds.schema || ds.database || "default"}.${ds.table}` :
    ds?.collection ? [ds.database, ds.collection].filter(Boolean).join(".") :
    ds?.database ? ds.database :
    ds?.dataset ? ds.dataset :
    ds?.filename ? ds.filename :
    result.destination?.filename ? result.destination.filename :
    result.destination?.path ? result.destination.path :
    result.destination?.database || result.destination?.collection || "Destination";

  const destinationPath =
    ds?.table ? `${ds.type || destType} · ${ds.database}${ds.schema ? ` · ${ds.schema}` : ""}` :
    ds?.collection ? `${ds.type || destType} · ${[ds.database, ds.collection].filter(Boolean).join(".")}` :
    ds?.filename ? `${result.destination?.format || destType} · ${ds.filename}` :
    result.destination?.path ? `${result.destination?.format || destType} · ${result.destination.path}` :
    result.destination?.filename ? `${result.destination?.format || destType} · ${result.destination.filename}` :
    result.destination?.database ? `${destType} · ${result.destination.database}` :
    destType;

  return (
    <div className={`df2-result-dashboard ${result.success ? "success" : "error"}`}>
      <div className="df2-result-scroll">
      <div className="df2-result-hero">
        <span className={`df2-result-badge ${result.success ? "df2-badge-live" : "df2-badge-error"}`}>
          <DtIcon name={result.success ? "check" : "x"} size={14} />
          {result.success ? "Transfer complete" : result.error || "Transfer failed"}
        </span>
        <h2 className="df2-result-title">{result.success ? "Data transferred" : "Transfer could not complete"}</h2>
        <p className="df2-result-subtitle">
          {result.success
            ? `${rec.toLocaleString()} records moved and reconciled`
            : "Fix the reported issues and try again."}
        </p>
      </div>

      <div className="df2-result-route-card">
        <div className="df2-result-endpoint">
          <ConnectorIcon id={sourceType} size={22} />
          <div>
            <span>{sourceType ? sourceType.toUpperCase() : "Source"}</span>
            <strong title={sourceLabel}>{sourceLabel}</strong>
          </div>
        </div>
        <div className="df2-result-arrow" aria-hidden>
          <DtIcon name="arrow-right" size={18} />
        </div>
        <div className="df2-result-endpoint">
          <ConnectorIcon id={destType} size={22} />
          <div>
            <span>{destinationPath}</span>
            <strong title={destLabel}>{destinationLine}</strong>
          </div>
        </div>
      </div>

      <details className="df2-disclosure df2-result-explain-wrap">
        <summary className="df2-result-explain">
          <DtIcon name="shield" size={16} />
          <span><strong>Trust & reconciliation</strong> — checksum verification details</span>
          <span className="df2-disclosure-chevron" aria-hidden />
        </summary>
        <p className="df2-result-explain-body">
          Checksums are computed over the source and destination rows and compared. If reconciliation passed, the transfer is complete and unchanged. The checksum is stored on this job for audit and replay.
        </p>
      </details>

      <div className="df2-result-stats">
        <div className="df2-result-stat-card">
          <DtIcon name="trend" size={18} />
          <strong>{rec.toLocaleString()}</strong>
          <span>Records transferred</span>
        </div>
        <div className="df2-result-stat-card">
          <DtIcon name="database" size={18} />
          <strong>{targetRows.toLocaleString()}</strong>
          <span>Rows at destination</span>
        </div>
        {sourceRows !== rec && sourceRows > 0 && (
          <div className="df2-result-stat-card">
            <DtIcon name="scan" size={18} />
            <strong>{sourceRows.toLocaleString()}</strong>
            <span>Source rows</span>
          </div>
        )}
        {rejected > 0 && (
          <div className="df2-result-stat-card warn">
            <DtIcon name="alert" size={18} />
            <strong>{rejected.toLocaleString()}</strong>
            <span>Rejected rows</span>
          </div>
        )}
        {passed !== undefined && (
          <div className={`df2-result-stat-card ${passed ? "ok" : "warn"}`}>
            <DtIcon name={passed ? "check" : "shield"} size={18} />
            <strong>{passed ? "Passed" : "Failed"}</strong>
            <span>Reconciliation</span>
          </div>
        )}
        {fmt(ds?.checksum) || fmt(result.reconciliation?.target_checksum) ? (
          <div className="df2-result-stat-card" title="Checksum is computed over the source rows and destination rows and compared to verify that the transfer is complete and unchanged.">
            <DtIcon name="lock" size={18} />
            <strong>{(ds?.checksum || result.reconciliation?.target_checksum || "").slice(0, 16)}</strong>
            <span>Checksum</span>
          </div>
        ) : null}
      </div>

      {result.reconciliation && (result.reconciliation.message || result.reconciliation.source_checksum || result.reconciliation.target_checksum || result.reconciliation.source_rows != null || result.reconciliation.target_rows != null) && (
        <div className="df2-result-section">
          <h4><DtIcon name="shield" size={14} /> Reconciliation</h4>
          {result.reconciliation.message && <p>{result.reconciliation.message}</p>}
          <div className="df2-result-row">
            {result.reconciliation.source_rows != null && (
              <span>Source rows: {result.reconciliation.source_rows.toLocaleString()}</span>
            )}
            {result.reconciliation.target_rows != null && (
              <span>Target rows: {result.reconciliation.target_rows.toLocaleString()}</span>
            )}
          </div>
          {(result.reconciliation.source_checksum || result.reconciliation.target_checksum) && (
            <div className="df2-result-row">
              {result.reconciliation.source_checksum && (
                <span title={result.reconciliation.source_checksum}>Source checksum: {result.reconciliation.source_checksum.slice(0, 16)}…</span>
              )}
              {result.reconciliation.target_checksum && (
                <span title={result.reconciliation.target_checksum}>Target checksum: {result.reconciliation.target_checksum.slice(0, 16)}…</span>
              )}
              {result.reconciliation.source_checksum && result.reconciliation.target_checksum && (
                <span className={result.reconciliation.source_checksum === result.reconciliation.target_checksum ? "ok" : "warn"}>
                  {result.reconciliation.source_checksum === result.reconciliation.target_checksum ? "Checksums match" : "Checksums differ"}
                </span>
              )}
            </div>
          )}
        </div>
      )}

      {ds?.warnings && ds.warnings.length > 0 && (
        <div className="df2-result-section">
          <h4><DtIcon name="alert" size={14} /> Warnings</h4>
          <ul className="df2-result-warnings">
            {ds.warnings.map((w) => <li key={w}>{w}</li>)}
          </ul>
        </div>
      )}

      {result.ddl_executed && result.ddl_executed.length > 0 && (
        <div className="df2-result-section">
          <h4><DtIcon name="code" size={14} /> DDL executed</h4>
          <ul className="df2-result-ddl">
            {result.ddl_executed.map((d) => <li key={d}><code>{d}</code></li>)}
          </ul>
        </div>
      )}

      {result.destination?.download_url && (
        <div className="df2-result-section">
          <a
            href={result.destination.download_url}
            className="df2-btn df2-btn-primary"
            download={result.destination.filename || `export.${result.destination?.format || "json"}`}
          >
            <DtIcon name="download" size={16} /> Download {result.destination.filename || "export"}
          </a>
        </div>
      )}

      {!result.success && (
        <div className="df2-result-section error">
          <h4><DtIcon name="x" size={14} /> Error</h4>
          <p className="df2-result-error-detail">{result.error}</p>
        </div>
      )}

      {result.job_id && (
        <details className="df2-disclosure df2-result-section df2-result-live-log">
          <summary>
            <DtIcon name="activity" size={14} />
            <strong>Job log</strong>
            <span className="df2-disclosure-chevron" aria-hidden />
          </summary>
          <JobTheater
            jobId={result.job_id}
            sourceLabel={sourceLabel}
            destLabel={destLabel}
            sourceType={sourceType}
            destType={destType}
          />
        </details>
      )}
      </div>

      <div className="df2-result-actions">
        <button type="button" className="df2-btn df2-btn-primary" onClick={onNewTransfer}>
          <DtIcon name="plus" size={14} /> New transfer
        </button>
        {onViewJobs && (
          <button type="button" className="df2-btn" onClick={onViewJobs}>
            <DtIcon name="jobs" size={14} /> View Job Theater
          </button>
        )}
        {onSchedule && (
          <button type="button" className="df2-btn" onClick={onSchedule}>
            <DtIcon name="activity" size={14} /> Schedule this route
          </button>
        )}
      </div>
    </div>
  );
}
