import { useMemo } from "react";
import { DtIcon } from "../DtIcon";
import { ConnectorIcon } from "../../app/brand-icons";
import { readJobEventLog } from "../../lib/jobEventLog";
import type { TransferResult } from "../../lib/types";
import { QuarantinePanel } from "./QuarantinePanel";

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
  const coercedNull = ds?.coerced_null_rows ?? result.reconciliation?.coerced_null_rows ?? 0;
  // rejected_rows is the superset; rows kept-but-coerced are not dropped.
  const droppedRows = Math.max(rejected - coercedNull, 0);
  const hasIntegrityLoss = result.success && (rejected > 0 || coercedNull > 0);
  const sourceRows = result.reconciliation?.source_rows ?? rec;
  const targetRows = result.reconciliation?.target_rows ?? rec;
  const passed = result.reconciliation?.passed ?? result.success;

  const eventLog = useMemo(() => {
    if (result.event_log?.length) return result.event_log;
    if (result.job_id) return readJobEventLog(result.job_id);
    return [];
  }, [result.event_log, result.job_id]);

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
    <div className={`df2-result-dashboard ${result.success ? (hasIntegrityLoss ? "success is-quarantine" : "success") : "error"}`}>
        <div className="df2-result-top">
        <div className="df2-result-hero df2-result-hero-compact">
          <span className={`df2-result-badge ${!result.success ? "df2-badge-error" : hasIntegrityLoss ? "df2-badge-warn" : "df2-badge-live"}`}>
            <DtIcon name={!result.success ? "x" : hasIntegrityLoss ? "alert" : "check"} size={14} />
            {!result.success ? result.error || "Transfer failed" : hasIntegrityLoss ? "Completed with quarantine" : "Transfer complete"}
          </span>
          <div className="df2-result-hero-copy">
            <h2 className="df2-result-title">
              {!result.success ? "Transfer could not complete" : hasIntegrityLoss ? "Data transferred — not full fidelity" : "Data transferred"}
            </h2>
            <p className="df2-result-subtitle">
              {!result.success
                ? "Fix the reported issues and try again."
                : hasIntegrityLoss
                  ? `${rec.toLocaleString()} records landed, but some rows were rejected or values coerced to NULL`
                  : `${rec.toLocaleString()} records moved and reconciled`}
            </p>
          </div>
        </div>

        <div className="df2-result-route-card df2-result-route-compact">
          <div className="df2-result-endpoint">
            <ConnectorIcon id={sourceType} size={18} />
            <div>
              <span>{sourceType ? sourceType.toUpperCase() : "Source"}</span>
              <strong title={sourceLabel}>{sourceLabel}</strong>
            </div>
          </div>
          <div className="df2-result-arrow" aria-hidden>
            <DtIcon name="arrow-right" size={16} />
          </div>
          <div className="df2-result-endpoint">
            <ConnectorIcon id={destType} size={18} />
            <div>
              <span>{destinationPath}</span>
              <strong title={destLabel}>{destinationLine}</strong>
            </div>
          </div>
        </div>

        <div className="df2-result-stats df2-result-stats-compact">
          <div className="df2-result-stat-card">
            <strong>{rec.toLocaleString()}</strong>
            <span>Transferred</span>
          </div>
          <div className="df2-result-stat-card">
            <strong>{targetRows.toLocaleString()}</strong>
            <span>At destination</span>
          </div>
          {sourceRows !== rec && sourceRows > 0 && (
            <div className="df2-result-stat-card">
              <strong>{sourceRows.toLocaleString()}</strong>
              <span>Source rows</span>
            </div>
          )}
          {droppedRows > 0 && (
            <div className="df2-result-stat-card warn" title="Rows isolated in quarantine — not written to the destination">
              <strong>{droppedRows.toLocaleString()}</strong>
              <span>Dropped / rejected</span>
            </div>
          )}
          {coercedNull > 0 && (
            <div className="df2-result-stat-card warn" title="Rows kept, but a value was altered to NULL — original value not preserved">
              <strong>{coercedNull.toLocaleString()}</strong>
              <span>Coerced to NULL</span>
            </div>
          )}
          {passed !== undefined && (
            <div className={`df2-result-stat-card ${passed ? "ok" : "warn"}`}>
              <strong>{passed ? "Passed" : "Failed"}</strong>
              <span>Reconcile</span>
            </div>
          )}
          {fmt(ds?.checksum) || fmt(result.reconciliation?.target_checksum) ? (
            <div className="df2-result-stat-card" title={ds?.checksum || result.reconciliation?.target_checksum}>
              <strong>{(ds?.checksum || result.reconciliation?.target_checksum || "").slice(0, 12)}</strong>
              <span>Checksum</span>
            </div>
          ) : null}
        </div>

        {hasIntegrityLoss && (
          <section className="df2-result-fidelity" role="alert" aria-label="Data fidelity warning">
            <div className="df2-result-fidelity-head">
              <DtIcon name="alert" size={18} />
              <div>
                <strong>Completed, but NOT full fidelity</strong>
                <p>
                  {result.reconciliation?.message
                    || `${coercedNull > 0 ? `${coercedNull.toLocaleString()} row(s) had a value coerced to NULL. ` : ""}${droppedRows > 0 ? `${droppedRows.toLocaleString()} row(s) were rejected.` : ""}`.trim()
                    || "Some rows were affected during this transfer."}
                </p>
              </div>
            </div>
            <div className="df2-result-fidelity-metrics">
              <article className="is-dropped">
                <strong>{droppedRows.toLocaleString()}</strong>
                <span>Dropped / rejected rows</span>
                <small>Isolated in quarantine — not written to the destination.</small>
              </article>
              <article className="is-coerced">
                <strong>{coercedNull.toLocaleString()}</strong>
                <span>Values coerced to NULL</span>
                <small>Row was kept, but a value was altered to NULL — the original value was not preserved.</small>
              </article>
            </div>
          </section>
        )}

        {result.success && (
          <section className="df2-result-proof-panel" aria-label="Transfer proof">
            <header className="df2-result-proof-head">
              <DtIcon name="check" size={16} />
              <strong>Proof summary</strong>
              {result.job_id && (
                <span className="df2-theater-v3-job-id" title={result.job_id}>Job #{result.job_id.slice(0, 8)}</span>
              )}
            </header>
            <dl className="df2-result-proof-dl">
              <div>
                <dt>Route</dt>
                <dd>{sourceLabel} → {destLabel}</dd>
              </div>
              <div>
                <dt>Reconciliation</dt>
                <dd>
                  {hasIntegrityLoss
                    ? result.reconciliation?.message || "Completed, but not full fidelity — see the fidelity summary above."
                    : passed
                      ? "Source and destination row counts and checksums matched"
                      : result.reconciliation?.message || "Pending verification"}
                </dd>
              </div>
              {result.operation && (
                <div>
                  <dt>Write mode</dt>
                  <dd>{result.operation}</dd>
                </div>
              )}
              {ds?.error_policy && (
                <div>
                  <dt>Error policy</dt>
                  <dd>{ds.error_policy}</dd>
                </div>
              )}
              {droppedRows > 0 && (
                <div>
                  <dt>Dropped / rejected</dt>
                  <dd>{droppedRows.toLocaleString()} rows isolated in quarantine — failed validation, not silently dropped. Inspect below to fix source data or mapping.</dd>
                </div>
              )}
              {coercedNull > 0 && (
                <div>
                  <dt>Coerced to NULL</dt>
                  <dd>{coercedNull.toLocaleString()} rows were kept but a value was altered to NULL — the original value was not preserved (not full fidelity).</dd>
                </div>
              )}
            </dl>
            {result.destination?.download_url && (
              <a
                href={result.destination.download_url}
                className="df2-btn df2-btn-sm df2-btn-primary"
                download={result.destination.filename || `export.${result.destination?.format || "json"}`}
              >
                <DtIcon name="download" size={14} /> Download export
              </a>
            )}
          </section>
        )}

        {!result.success && (
          <section className="df2-result-proof-panel is-error" aria-label="Transfer failure">
            <header className="df2-result-proof-head">
              <DtIcon name="alert" size={16} />
              <strong>Failure details</strong>
            </header>
            <p className="df2-result-error-detail">{result.error || "The transfer could not complete."}</p>
          </section>
        )}

        {((result.reconciliation?.message && !hasIntegrityLoss) || (ds?.warnings && ds.warnings.length > 0) || (result.ddl_executed && result.ddl_executed.length > 0)) && (
          <div className="df2-result-more-body df2-result-more-inline">
            <p className="df2-result-explain-body">
              Checksums are computed over source and destination rows and compared. If reconciliation passed, the transfer is complete and unchanged.
            </p>
            {result.reconciliation?.message && !hasIntegrityLoss && <p>{result.reconciliation.message}</p>}
            {ds?.warnings && ds.warnings.length > 0 && (
              <ul className="df2-result-warnings">
                {ds.warnings.map((w) => <li key={w}>{w}</li>)}
              </ul>
            )}
            {result.ddl_executed && result.ddl_executed.length > 0 && (
              <ul className="df2-result-ddl">
                {result.ddl_executed.map((d) => <li key={d}><code>{d}</code></li>)}
              </ul>
            )}
            {!result.success && result.error && (
              <p className="df2-result-error-detail">{result.error}</p>
            )}
          </div>
        )}
      </div>

      {rejected > 0 && result.job_id && (
        <QuarantinePanel jobId={result.job_id} rejectedRows={rejected} coercedNullRows={coercedNull} />
      )}

      <section className="df2-job-log-panel is-result is-open" aria-label="Job event log">
        <header className="df2-job-log-panel-head">
          <div className="df2-job-log-panel-title">
            <DtIcon name="activity" size={14} />
            <strong>Job log</strong>
            <span className="df2-job-log-count">{eventLog.length} events</span>
            {result.job_id && (
              <span className="df2-theater-v3-job-id" title={result.job_id}>#{result.job_id.slice(0, 8)}</span>
            )}
          </div>
        </header>
        <div className="df2-job-log-panel-body" role="log">
          {eventLog.length === 0 ? (
            <div className="df2-job-log-empty">
              No captured events for this job yet. Re-run a transfer to collect a full live event stream.
            </div>
          ) : (
            eventLog.map((line, i) => (
              <div key={`${i}-${line.slice(0, 32)}`} className="df2-job-log-line">
                {line}
              </div>
            ))
          )}
        </div>
      </section>

      <div className="df2-result-actions">
        <button type="button" className="df2-btn df2-btn-primary" onClick={onNewTransfer}>
          New transfer
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
