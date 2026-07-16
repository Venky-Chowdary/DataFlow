import { useMemo, useState } from "react";
import { DtIcon } from "../DtIcon";
import { ConnectorIcon } from "../../app/brand-icons";
import { readJobEventLog } from "../../lib/jobEventLog";
import { fetchJobQuarantine, exportJobQuarantine } from "../../lib/api";
import type { TransferResult } from "../../lib/types";
import { useToast } from "../Toast";

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
  const { toast } = useToast();
  const ds = result.destination_summary;
  const rec = result.records_transferred ?? 0;
  const rejected = ds?.rejected_rows ?? result.reconciliation?.rejected_rows ?? 0;
  const [showQuarantine, setShowQuarantine] = useState(false);
  const [quarantineLoading, setQuarantineLoading] = useState(false);
  const [quarantine, setQuarantine] = useState<{ row?: number; column?: string; target?: string; value?: string; reason?: string; policy?: string }[]>(ds?.rejected_details || []);
  const sourceRows = result.reconciliation?.source_rows ?? rec;
  const targetRows = result.reconciliation?.target_rows ?? rec;
  const passed = result.reconciliation?.passed ?? result.success;

  const loadQuarantine = async () => {
    if (!result.job_id) return;
    setQuarantineLoading(true);
    try {
      const data = await fetchJobQuarantine(result.job_id);
      setQuarantine(data.quarantine);
      setShowQuarantine(true);
    } catch (e) {
      toast({ title: "Could not load quarantine", message: (e as Error).message, tone: "error" });
    } finally {
      setQuarantineLoading(false);
    }
  };

  const downloadQuarantine = async () => {
    if (!result.job_id) return;
    try {
      const data = await exportJobQuarantine(result.job_id);
      if (data.download_url) {
        const a = document.createElement("a");
        a.href = data.download_url;
        a.download = data.filename || `quarantine-${result.job_id}.csv`;
        a.click();
      }
    } catch (e) {
      toast({ title: "Could not export quarantine", message: (e as Error).message, tone: "error" });
    }
  };

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
    <div className={`df2-result-dashboard ${result.success ? "success" : "error"}`}>
        <div className="df2-result-top">
        <div className="df2-result-hero df2-result-hero-compact">
          <span className={`df2-result-badge ${result.success ? "df2-badge-live" : "df2-badge-error"}`}>
            <DtIcon name={result.success ? "check" : "x"} size={14} />
            {result.success ? "Transfer complete" : result.error || "Transfer failed"}
          </span>
          <div className="df2-result-hero-copy">
            <h2 className="df2-result-title">{result.success ? "Data transferred" : "Transfer could not complete"}</h2>
            <p className="df2-result-subtitle">
              {result.success
                ? `${rec.toLocaleString()} records moved and reconciled`
                : "Fix the reported issues and try again."}
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
          {rejected > 0 && (
            <div className="df2-result-stat-card warn">
              <strong>{rejected.toLocaleString()}</strong>
              <span>Rejected</span>
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

        {(result.reconciliation?.message || (ds?.warnings && ds.warnings.length > 0) || (result.ddl_executed && result.ddl_executed.length > 0) || (!result.success && result.error) || result.destination?.download_url) && (
          <div className="df2-result-more-body df2-result-more-inline">
            <p className="df2-result-explain-body">
              Checksums are computed over source and destination rows and compared. If reconciliation passed, the transfer is complete and unchanged.
            </p>
            {result.reconciliation?.message && <p>{result.reconciliation.message}</p>}
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
            {result.destination?.download_url && (
              <a
                href={result.destination.download_url}
                className="df2-btn df2-btn-primary df2-btn-sm"
                download={result.destination.filename || `export.${result.destination?.format || "json"}`}
              >
                <DtIcon name="download" size={14} /> Download export
              </a>
            )}
            {rejected > 0 && (
              <button
                type="button"
                className="df2-btn df2-btn-sm"
                onClick={() => void loadQuarantine()}
                disabled={quarantineLoading}
              >
                <DtIcon name="warning" size={14} /> {quarantineLoading ? "Loading…" : "View quarantine"}
              </button>
            )}
          </div>
        )}
      </div>

      {showQuarantine && rejected > 0 && (
        <section className="df2-job-log-panel is-result is-open" aria-label="Quarantine">
          <header className="df2-job-log-panel-head">
            <div className="df2-job-log-panel-title">
              <DtIcon name="warning" size={14} />
              <strong>Quarantine</strong>
              <span className="df2-job-log-count">{quarantine.length} rows</span>
            </div>
            <div className="df2-job-log-actions">
              <button type="button" className="df2-btn df2-btn-sm df2-btn-secondary" onClick={() => void downloadQuarantine()}>
                <DtIcon name="download" size={14} /> Export CSV
              </button>
              <button type="button" className="df2-btn df2-btn-sm df2-btn-ghost" onClick={() => setShowQuarantine(false)}>Close</button>
            </div>
          </header>
          <div className="df2-job-log-panel-body" role="log">
            {quarantine.length === 0 ? (
              <div className="df2-job-log-empty">No quarantined rows recorded.</div>
            ) : (
              <table className="df2-query-table">
                <thead>
                  <tr>
                    <th>Row</th>
                    <th>Column</th>
                    <th>Value</th>
                    <th>Reason</th>
                  </tr>
                </thead>
                <tbody>
                  {quarantine.map((q, i) => (
                    <tr key={i}>
                      <td>{q.row ?? "—"}</td>
                      <td>{q.column ?? "—"}</td>
                      <td className="df2-quarantine-value">{q.value ?? "—"}</td>
                      <td>{q.reason ?? "—"}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            )}
          </div>
        </section>
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
