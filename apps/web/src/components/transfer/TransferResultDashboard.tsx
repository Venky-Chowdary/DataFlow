import { useEffect, useMemo } from "react";
import { DtIcon } from "../DtIcon";
import { ConnectorIcon } from "../../app/brand-icons";
import { CopyIdChip } from "../ui/CopyIdChip";
import { readJobEventLog } from "../../lib/jobEventLog";
import { classifyJobLogLine } from "../../lib/transferFailure";
import { useActiveData } from "../../lib/DataContext";
import type { LoadHistoryReport, TransferResult } from "../../lib/types";
import { LoadHistoryPanel } from "./LoadHistoryPanel";
import { NotificationDeliveryStrip } from "./NotificationDeliveryStrip";
import { QuarantinePanel } from "./QuarantinePanel";
import { Gate8ProofCard } from "./Gate8ProofCard";

interface TransferResultDashboardProps {
  result: TransferResult;
  sourceLabel?: string;
  sourceType?: string;
  destLabel?: string;
  destType?: string;
  onNewTransfer?: () => void;
  onViewJobs?: () => void;
  onSchedule?: () => void;
  /** Jump back to Validate so Strip / Quarantine / Fix bad data stay reachable from Run. */
  onOpenValidate?: () => void;
}

function fmt(value: string | number | undefined): string | null {
  if (value === undefined || value === null || value === "") return null;
  return typeof value === "number" ? value.toLocaleString() : String(value);
}

function MetricCell({
  value,
  label,
  tone,
  title,
}: {
  value: string;
  label: string;
  tone?: "warn" | "ok" | "danger";
  title?: string;
}) {
  return (
    <div
      className={`df2-result-metric${tone ? ` is-${tone}` : ""}`}
      title={title}
    >
      <strong>{value}</strong>
      <span>{label}</span>
    </div>
  );
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
  onOpenValidate,
}: TransferResultDashboardProps) {
  const { setActiveData } = useActiveData();
  const ds = result.destination_summary;
  const rec = result.records_transferred ?? 0;
  const errDetails = (result.error_details || {}) as Record<string, unknown>;
  const rejected = Number(
    ds?.rejected_rows
    ?? result.reconciliation?.rejected_rows
    ?? errDetails.quarantine_row_count
    ?? 0,
  );
  const issueFindings = Number(errDetails.quarantine_issue_count ?? 0);
  const coercedNull = ds?.coerced_null_rows ?? result.reconciliation?.coerced_null_rows ?? 0;
  const droppedRows = Math.max(rejected - coercedNull, 0);
  const hasIntegrityLoss = result.success && (rejected > 0 || coercedNull > 0);
  const showQuarantine = Boolean(result.job_id) && (!result.success || hasIntegrityLoss || rejected > 0 || issueFindings > 0);
  const sourceRows = result.reconciliation?.source_rows ?? rec;
  const targetRows = result.reconciliation?.target_rows ?? rec;
  const passed = result.reconciliation?.passed ?? result.success;
  const throughput = result.records_per_second ?? ds?.records_per_second;
  const checksum = fmt(ds?.checksum) || fmt(result.reconciliation?.target_checksum);

  const eventLog = useMemo(() => {
    if (result.event_log?.length) return result.event_log;
    if (result.job_id) return readJobEventLog(result.job_id);
    return [];
  }, [result.event_log, result.job_id]);

  useEffect(() => {
    if (!result.job_id) return;
    setActiveData((prev) => ({
      name: prev?.name || sourceLabel,
      filename: prev?.filename,
      columns: prev?.columns || [],
      row_count: rec || prev?.row_count || 0,
      samples: prev?.samples,
      schema: prev?.schema,
      preflight_run_id: prev?.preflight_run_id,
      job_id: result.job_id,
      validation_status: result.success ? (hasIntegrityLoss ? "completed_with_quarantine" : "completed") : "failed",
      route: `${sourceLabel} → ${destLabel}`,
      blockers: result.error ? [result.error] : prev?.blockers,
    }));
  }, [destLabel, hasIntegrityLoss, rec, result.error, result.job_id, result.success, setActiveData, sourceLabel]);

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

  const outcomeTone = !result.success ? "error" : hasIntegrityLoss ? "warn" : "success";
  const badgeClass =
    !result.success ? "df2-badge-error" : hasIntegrityLoss ? "df2-badge-warn" : "df2-badge-live";
  const badgeIcon = !result.success ? "x" : hasIntegrityLoss ? "alert" : "check";
  const badgeLabel = !result.success
    ? "Transfer failed"
    : hasIntegrityLoss
      ? "Completed with quarantine"
      : "Transfer complete";
  const title = !result.success
    ? "Transfer could not complete"
    : hasIntegrityLoss
      ? "Data transferred — not full fidelity"
      : "Data transferred";
  const subtitle = !result.success
    ? "Review failure details and bad-data findings below, then fix on Validate or Map."
    : hasIntegrityLoss
      ? `${rec.toLocaleString()} records landed, but some rows were rejected or values coerced to NULL`
      : `${rec.toLocaleString()} records moved and reconciled`;

  const metaChips: Array<{ label: string; value: string; tone?: "warn" | "ok"; title?: string }> = [];
  if (ds?.load_method) {
    metaChips.push({ label: "Load", value: ds.load_method });
  }
  if (ds?.chunk_size != null && Number(ds.chunk_size) > 0) {
    metaChips.push({ label: "Batch", value: Number(ds.chunk_size).toLocaleString() });
  }
  if (sourceRows !== rec && sourceRows > 0) {
    metaChips.push({ label: "Source rows", value: sourceRows.toLocaleString() });
  }
  if (result.operation) {
    metaChips.push({ label: "Mode", value: result.operation });
  }
  if (ds?.error_policy) {
    metaChips.push({ label: "Policy", value: ds.error_policy });
  }
  if (checksum) {
    metaChips.push({ label: "Checksum", value: checksum.slice(0, 12), title: checksum });
  }
  if (issueFindings > 0) {
    metaChips.push({
      label: "Findings",
      value: issueFindings.toLocaleString(),
      tone: "warn",
      title: "Cell-level integrity findings from preflight or write",
    });
  }

  const showMore =
    (result.reconciliation?.message && !hasIntegrityLoss)
    || (ds?.warnings && ds.warnings.length > 0)
    || (result.ddl_executed && result.ddl_executed.length > 0)
    || Boolean(result.reconciliation?.source_checksum || result.reconciliation?.target_checksum);

  const failedPhase = String(errDetails.phase || errDetails.failed_phase || "").trim();
  const loadHistory =
    (ds?.load_history_report as LoadHistoryReport | undefined)
    || (errDetails.load_history_report as LoadHistoryReport | undefined);

  return (
    <div className={`df2-result-dashboard is-${outcomeTone}${hasIntegrityLoss ? " is-quarantine" : ""}`}>
      <header className="df2-result-head">
        <div className="df2-result-head-main">
          <span
            className={`df2-badge df2-result-badge ${badgeClass}`}
            title={!result.success ? (result.error || "Transfer failed") : undefined}
          >
            <DtIcon name={badgeIcon} size={12} />
            {badgeLabel}
          </span>
          <div className="df2-result-head-copy">
            <h2 className="df2-result-title">{title}</h2>
            <p className="df2-result-subtitle">{subtitle}</p>
          </div>
          {result.job_id && (
            <div className="df2-result-head-meta">
              <CopyIdChip id={result.job_id} label="Job" compact />
            </div>
          )}
        </div>

        <div className="df2-result-route" aria-label="Transfer route">
          <div className="df2-result-endpoint">
            <ConnectorIcon id={sourceType} size={16} />
            <div>
              <span>{sourceType ? sourceType.toUpperCase() : "Source"}</span>
              <strong title={sourceLabel}>{sourceLabel}</strong>
            </div>
          </div>
          <div className="df2-result-arrow" aria-hidden>
            <DtIcon name="arrow-right" size={14} />
          </div>
          <div className="df2-result-endpoint">
            <ConnectorIcon id={destType} size={16} />
            <div>
              <span>{destinationPath}</span>
              <strong title={destLabel}>{destinationLine}</strong>
            </div>
          </div>
        </div>
      </header>

      <section className="df2-result-metrics" aria-label="Transfer metrics">
        <MetricCell value={rec.toLocaleString()} label="Transferred" />
        <MetricCell value={targetRows.toLocaleString()} label="At destination" />
        <MetricCell
          value={droppedRows.toLocaleString()}
          label="Rejected"
          tone={droppedRows > 0 ? "warn" : undefined}
          title="Rows isolated in quarantine — not silently dropped"
        />
        <MetricCell
          value={coercedNull.toLocaleString()}
          label="Coerced NULL"
          tone={coercedNull > 0 ? "warn" : undefined}
          title="Real NULL coercions only — ISO→DATETIME normalize is not counted here"
        />
        <MetricCell
          value={passed ? "Passed" : "Failed"}
          label="Reconcile"
          tone={passed ? "ok" : "danger"}
        />
        <MetricCell
          value={throughput != null ? Math.round(Number(throughput)).toLocaleString() : "—"}
          label="Rows / sec"
          title={throughput != null ? `${sourceType} → ${destType} — this job only` : "Throughput not reported for this job"}
        />
      </section>

      {metaChips.length > 0 && (
        <div className="df2-result-meta-row" aria-label="Transfer details">
          {metaChips.map((chip) => (
            <span
              key={`${chip.label}-${chip.value}`}
              className={`df2-result-meta-chip${chip.tone ? ` is-${chip.tone}` : ""}`}
              title={chip.title}
            >
              <em>{chip.label}</em>
              <strong>{chip.value}</strong>
            </span>
          ))}
        </div>
      )}

      <NotificationDeliveryStrip
        notifications={result.notifications}
        className="df2-result-notify"
        compact
      />

      {result.reconciliation && (
        <Gate8ProofCard
          report={result.reconciliation}
          explanation={result.explanation}
          className="df2-result-gate8"
        />
      )}

      {(result.cdc_plugin || result.cdc_delivery || result.cdc_shared_reader || result.snapshot_mode || result.watermark || result.cdc_lease_holder) && (
        <section className="df2-result-cdc-strip" aria-label="CDC run summary">
          <header>
            <DtIcon name="activity" size={14} />
            <strong>CDC</strong>
            <span>{result.cdc_delivery || "at-least-once"} · not platform exactly-once</span>
          </header>
          <dl>
            {result.cdc_plugin && <div><dt>Plugin</dt><dd>{result.cdc_plugin}</dd></div>}
            {result.snapshot_mode && <div><dt>Snapshot</dt><dd>{result.snapshot_mode}</dd></div>}
            {result.cdc_shared_reader && <div><dt>Topology</dt><dd>Shared log reader</dd></div>}
            {result.cdc_lag_seconds != null && Number.isFinite(Number(result.cdc_lag_seconds)) && (
              <div><dt>Lag</dt><dd>{Number(result.cdc_lag_seconds).toFixed(1)}s</dd></div>
            )}
            {result.cdc_lease_holder && (
              <div><dt>Lease</dt><dd>{result.cdc_lease_holder}{result.cdc_lease_backend ? ` · ${result.cdc_lease_backend}` : ""}</dd></div>
            )}
            {result.watermark && (
              <div className="is-wide"><dt>Watermark</dt><dd className="df2-mono" title={result.watermark}>{result.watermark.slice(0, 64)}{result.watermark.length > 64 ? "…" : ""}</dd></div>
            )}
          </dl>
        </section>
      )}

      <div className="df2-result-body">
        {!result.success && (
          <section className="df2-result-alert is-error" aria-label="What went wrong">
            <header className="df2-result-alert-head">
              <DtIcon name="alert" size={15} />
              <strong>What went wrong</strong>
              {failedPhase ? <span className="df2-result-phase-chip">Phase: {failedPhase}</span> : null}
            </header>
            <p className="df2-result-error-detail">{result.error || "The transfer could not complete."}</p>
            {/preflight|dry-run|integrity|lossy coercion|invalid boolean/i.test(result.error || "") && (
              <p className="df2-result-error-hint">
                Preflight blocked this job — <strong>0 rows were written</strong>.
                Findings labeled quarantine here are for inspection only.
                Fix Map types/targets, re-Validate, then Execute.
              </p>
            )}
            {/incorrect datetime|invalid input syntax for type|data truncation/i.test(result.error || "") && (
              <p className="df2-result-error-hint">
                Destination rejected a typed value (often ISO timestamps). Open Validate to see the
                wire-form probe, or Inspect quarantine below for the exact column and sample.
              </p>
            )}
          </section>
        )}

        {hasIntegrityLoss && (
          <section className="df2-result-alert is-warn" role="alert" aria-label="Data fidelity warning">
            <header className="df2-result-alert-head">
              <DtIcon name="alert" size={15} />
              <strong>Completed, but not full fidelity</strong>
            </header>
            <p>
              {result.reconciliation?.message
                || `${coercedNull > 0 ? `${coercedNull.toLocaleString()} row(s) had a value coerced to NULL. ` : ""}${droppedRows > 0 ? `${droppedRows.toLocaleString()} row(s) were rejected.` : ""}`.trim()
                || "Some rows were affected during this transfer."}
            </p>
            <div className="df2-result-fidelity-inline">
              <span className="is-dropped">
                <strong>{droppedRows.toLocaleString()}</strong> dropped / rejected
              </span>
              <span className="is-coerced">
                <strong>{coercedNull.toLocaleString()}</strong> coerced to NULL
              </span>
            </div>
          </section>
        )}

        {result.success && (
          <section className="df2-result-proof" aria-label="Transfer proof">
            <header className="df2-result-proof-head">
              <DtIcon name="check" size={14} />
              <strong>Proof</strong>
              {result.destination?.download_url && (
                <a
                  href={result.destination.download_url}
                  className="df2-btn df2-btn-sm df2-btn-primary"
                  download={result.destination.filename || `export.${result.destination?.format || "json"}`}
                >
                  <DtIcon name="download" size={14} /> Download export
                </a>
              )}
            </header>
            <dl className="df2-result-proof-dl">
              <div>
                <dt>Route</dt>
                <dd>{sourceLabel} → {destLabel}</dd>
              </div>
              {throughput != null && (
                <div>
                  <dt>Throughput</dt>
                  <dd>
                    {Math.round(Number(throughput)).toLocaleString()} rows/s
                    {" "}({sourceType} → {destType})
                  </dd>
                </div>
              )}
              {ds?.load_method && (
                <div>
                  <dt>Load method</dt>
                  <dd>{ds.load_method}{ds.chunk_size ? ` · batch ${Number(ds.chunk_size).toLocaleString()}` : ""}</dd>
                </div>
              )}
              <div>
                <dt>Reconciliation</dt>
                <dd>
                  {hasIntegrityLoss
                    ? result.reconciliation?.message || "Completed, but not full fidelity — see fidelity note above."
                    : passed
                      ? "Source and destination row counts and checksums matched"
                      : result.reconciliation?.message || "Pending verification"}
                </dd>
              </div>
              {droppedRows > 0 && (
                <div>
                  <dt>Rejected</dt>
                  <dd>{droppedRows.toLocaleString()} rows isolated in quarantine — not silently dropped.</dd>
                </div>
              )}
              {coercedNull > 0 && (
                <div>
                  <dt>Coerced NULL</dt>
                  <dd>{coercedNull.toLocaleString()} rows kept with a value altered to NULL.</dd>
                </div>
              )}
            </dl>
          </section>
        )}

        {showMore && (
          <details className="df2-result-more">
            <summary>Checksums, warnings &amp; DDL</summary>
            <div className="df2-result-more-body">
              <p className="df2-result-explain-body">
                Checksums are computed over source and destination rows and compared.
                If reconciliation passed, the transfer is complete and unchanged.
              </p>
              {(result.reconciliation?.source_checksum || result.reconciliation?.target_checksum) && (
                <dl className="df2-result-checksum-pair">
                  <div>
                    <dt>Source checksum</dt>
                    <dd><code>{(result.reconciliation?.source_checksum || "—").slice(0, 16)}</code></dd>
                  </div>
                  <div>
                    <dt>Destination checksum</dt>
                    <dd><code>{(result.reconciliation?.target_checksum || checksum || "—").slice(0, 16)}</code></dd>
                  </div>
                  <div>
                    <dt>Match</dt>
                    <dd>
                      {result.reconciliation?.source_checksum
                        && result.reconciliation?.target_checksum
                        && result.reconciliation.source_checksum === result.reconciliation.target_checksum
                        ? "Yes — fingerprints equal"
                        : result.reconciliation?.passed
                          ? "Passed (see reconcile message)"
                          : "Not matched"}
                    </dd>
                  </div>
                </dl>
              )}
              {result.reconciliation?.message && !hasIntegrityLoss && <p>{result.reconciliation.message}</p>}
              {ds?.warnings && ds.warnings.length > 0 && (
                <div className="df2-result-warnings-block">
                  <p className="df2-result-warnings-note">
                    Showing {ds.warnings.length} sample writer message{ds.warnings.length === 1 ? "" : "s"}
                    {" "}(display capped — not the full row count).
                  </p>
                  <ul className="df2-result-warnings">
                    {ds.warnings.map((w) => <li key={w}>{w}</li>)}
                  </ul>
                </div>
              )}
              {result.ddl_executed && result.ddl_executed.length > 0 && (
                <ul className="df2-result-ddl">
                  {result.ddl_executed.map((d) => <li key={d}><code>{d}</code></li>)}
                </ul>
              )}
            </div>
          </details>
        )}

        {loadHistory && (
          <div className="df2-result-section-wrap">
            <LoadHistoryPanel report={loadHistory} title="Compared to prior loads" />
          </div>
        )}

        {(showQuarantine || !result.success) && result.job_id && (
          <div className="df2-result-section-wrap">
            <QuarantinePanel
              jobId={result.job_id}
              rejectedRows={rejected || issueFindings}
              coercedNullRows={coercedNull}
              initialDetails={result.destination_summary?.rejected_details}
              autoLoad
              initiallyOpen
            />
          </div>
        )}
      </div>

      <div className="df2-result-actions df2-result-actions-remediate">
        {onOpenValidate && (!result.success || hasIntegrityLoss) && (
          <button type="button" className="df2-btn df2-btn-primary" onClick={onOpenValidate}>
            <DtIcon name="gate" size={14} /> Open Validate
          </button>
        )}
        <button type="button" className="df2-btn df2-btn-primary" onClick={onNewTransfer}>
          New transfer
        </button>
        {onViewJobs && (
          <button type="button" className="df2-btn" onClick={onViewJobs}>
            <DtIcon name="jobs" size={14} /> Job Theater
          </button>
        )}
        {onSchedule && (
          <button type="button" className="df2-btn" onClick={onSchedule}>
            <DtIcon name="activity" size={14} /> Schedule route
          </button>
        )}
      </div>

      <section className="df2-job-log-panel is-result is-open" aria-label="Job event log">
        <header className="df2-job-log-panel-head">
          <div className="df2-job-log-panel-title">
            <DtIcon name="activity" size={14} />
            <strong>Job log</strong>
            <span className="df2-job-log-count">{eventLog.length} events</span>
            {result.job_id && <CopyIdChip id={result.job_id} label="Job" compact />}
          </div>
        </header>
        <div className="df2-job-log-panel-body" role="log">
          {eventLog.length === 0 ? (
            <div className="df2-job-log-empty">
              No captured events for this job yet. Re-run a transfer to collect a full live event stream.
            </div>
          ) : (
            eventLog.map((line, i) => (
              <div
                key={`${i}-${line.slice(0, 32)}`}
                className={`df2-job-log-line is-${classifyJobLogLine(line)}`}
              >
                {line}
              </div>
            ))
          )}
        </div>
      </section>
    </div>
  );
}
