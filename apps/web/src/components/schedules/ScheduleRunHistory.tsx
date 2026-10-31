import { useCallback, useEffect, useRef, useState } from "react";
import { DtIcon } from "../DtIcon";
import { Spinner } from "../LoadingState";
import { CopyIdChip } from "../ui/CopyIdChip";
import { acceptScheduleSourceSchema, fetchScheduleHistory } from "../../lib/api";
import { Button } from "../ui/Button";
import { jobStatusBadgeClass, jobStatusLabel } from "../../lib/uiUtils";
import type { ScheduleRun } from "../../lib/types";
import { formatJobRowMetric } from "../../lib/conservationLedger";
import { historyPollMs, isOpenRun, runLabel, runReason } from "../../lib/scheduleRunStatus";
import { createPoller, type Poller } from "../../lib/poller";

interface ScheduleRunHistoryProps {
  scheduleId: string;
  /** Re-read now when this changes (the schedule started/finished a run). */
  refreshKey?: string;
  onOpenJob?: (jobId: string) => void;
  /** Open this schedule's mapping so a drift finding can be acted on. */
  onEditMapping?: () => void;
}

/**
 * A run refused because the source changed shape is a finding the operator has
 * to resolve, not just an error to read. Without a control the message asked
 * for a review that nothing performed.
 */
function isSourceDriftError(message: string): boolean {
  return /source schema changed since the last run/i.test(message);
}

function formatWhen(iso: string | null | undefined): string {
  if (!iso) return "—";
  try {
    return new Date(iso).toLocaleString(undefined, {
      month: "short",
      day: "numeric",
      hour: "2-digit",
      minute: "2-digit",
    });
  } catch {
    return iso;
  }
}

function formatDuration(seconds: number | undefined): string {
  if (seconds == null || Number.isNaN(seconds)) return "—";
  if (seconds < 60) return `${seconds.toFixed(seconds < 10 ? 1 : 0)}s`;
  const mins = Math.floor(seconds / 60);
  const secs = Math.round(seconds % 60);
  return `${mins}m ${secs}s`;
}

export function ScheduleRunHistory({ scheduleId, refreshKey, onOpenJob, onEditMapping }: ScheduleRunHistoryProps) {
  const [accepting, setAccepting] = useState(false);
  const [accepted, setAccepted] = useState<string | null>(null);
  const [acceptError, setAcceptError] = useState<string | null>(null);
  const [runs, setRuns] = useState<ScheduleRun[] | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  const load = useCallback(async (quiet = false) => {
    if (!quiet) {
      setLoading(true);
      setError(null);
    }
    try {
      const res = await fetchScheduleHistory(scheduleId, 25);
      setRuns(res.runs ?? []);
      setError(null);
    } catch (e) {
      if (!quiet) setError(e instanceof Error ? e.message : "Could not load run history");
    }
    if (!quiet) setLoading(false);
  }, [scheduleId]);

  useEffect(() => {
    void load();
  }, [load]);

  // Loaded once, the panel kept showing a run as it was when opened — a run
  // that finished, failed or was declared "worker lost" never appeared. The
  // poller re-arms after a failed read too, so one blip does not end refresh.
  const runsRef = useRef<ScheduleRun[] | null>(null);
  runsRef.current = runs;
  const pollerRef = useRef<Poller | null>(null);
  useEffect(() => {
    if (loading || error) return;
    const poller = createPoller<ScheduleRun[]>({
      read: async () => (await fetchScheduleHistory(scheduleId, 25)).runs ?? [],
      delayMs: (last) => historyPollMs(last ?? runsRef.current),
      onResult: setRuns,
    });
    pollerRef.current = poller;
    return () => {
      poller.stop();
      pollerRef.current = null;
    };
  }, [scheduleId, loading, error]);

  // The schedule started or finished a run: read History now.
  useEffect(() => {
    pollerRef.current?.poke();
  }, [refreshKey]);

  if (loading) {
    return (
      <div className="df2-sched-history-state">
        <Spinner size="sm" /> Loading run history…
      </div>
    );
  }

  if (error) {
    return (
      <div className="df2-sched-history-state is-error">
        <DtIcon name="alert" size={15} /> {error}
        <button type="button" className="df2-btn df2-btn-ghost df2-btn-sm" onClick={() => void load()}>Retry</button>
      </div>
    );
  }

  if (!runs || runs.length === 0) {
    return (
      <div className="df2-sched-history-state">
        <DtIcon name="clock" size={15} /> No runs yet — history appears here after the first scheduled or manual run.
      </div>
    );
  }

  return (
    <div className="df2-sched-history">
      <div className="df2-sched-run-list" role="list" aria-label="Run history">
        {runs.map((run, i) => {
          const rejected = run.rejected_rows ?? 0;
          const coerced = run.coerced_null_rows ?? 0;
          const rows = formatJobRowMetric({
            status: run.status,
            records_processed: run.records_transferred,
            records_transferred: run.records_transferred,
            row_accounting: run.row_accounting,
          });
          const reason = runReason(run);
          const showReason =
            Boolean(run.error?.trim()) ||
            (Boolean(reason) && !isOpenRun(run.status) && !/^completed/.test(run.status));
          const drift = isSourceDriftError(reason);
          return (
            <article
              className={`df2-sched-run${run.retry_scheduled ? " is-retry" : ""}`}
              role="listitem"
              key={run.run_id || `${run.job_id}-${run.attempt}-${i}`}
            >
              <header className="df2-sched-run-head">
                <span className={jobStatusBadgeClass(run.status)} title={reason || jobStatusLabel(run.status)}>
                  {runLabel(run)}
                </span>
                {run.attempt > 0 && (
                  <span className="df2-sched-attempt" title={`Retry attempt #${run.attempt}`}>
                    <DtIcon name="trend" size={11} /> retry #{run.attempt}
                  </span>
                )}
                {run.retry_scheduled && (
                  <span className="df2-sched-attempt" title="A retry was scheduled after this attempt">retry queued</span>
                )}
                {run.failure_class?.kind === "deterministic" && (
                  <span
                    className="df2-sched-attempt is-blocked"
                    title="This failure is decided by the configuration — a retry reaches the same verdict"
                  >
                    not retryable
                  </span>
                )}
                <time className="df2-sched-run-when" dateTime={run.started_at || undefined}>
                  {formatWhen(run.started_at)}
                </time>
                <span className="df2-sched-run-dur">{formatDuration(run.duration_seconds)}</span>
              </header>
              <dl className="df2-sched-run-metrics">
                <div>
                  <dt>{rows.label}</dt>
                  <dd title={rows.title}>{rows.value}</dd>
                </div>
                <div>
                  <dt>Rejected</dt>
                  <dd className={rejected > 0 ? "df2-sched-num-warn" : undefined}>{rejected.toLocaleString()}</dd>
                </div>
                <div>
                  <dt>Coerced NULL</dt>
                  <dd className={coerced > 0 ? "df2-sched-num-warn" : undefined}>{coerced.toLocaleString()}</dd>
                </div>
                <div className="df2-sched-run-job">
                  <dt>Job</dt>
                  <dd>
                    {run.job_id ? (
                      <>
                        <CopyIdChip id={run.job_id} label="Job" compact />
                        <button
                          type="button"
                          className="df2-sched-job-link"
                          onClick={() => onOpenJob?.(run.job_id)}
                          title="Open job in Jobs"
                        >
                          View <DtIcon name="arrow-up-right" size={12} />
                        </button>
                      </>
                    ) : "—"}
                  </dd>
                </div>
              </dl>
              {showReason && (
                <div className="df2-sched-run-error">
                  <p title={reason}>{reason}</p>
                  {run.failure_class?.corrective_action && (
                    <p className="df2-sched-run-fix">{run.failure_class.corrective_action}</p>
                  )}
                  {drift && (
                    <span className="df2-sched-history-error-actions">
                      {onEditMapping && (
                        <Button variant="ghost" size="sm" onClick={onEditMapping}>
                          Review mapping
                        </Button>
                      )}
                      <Button
                        variant="secondary"
                        size="sm"
                        disabled={accepting}
                        onClick={async () => {
                          setAccepting(true);
                          setAcceptError(null);
                          try {
                            const res = await acceptScheduleSourceSchema(scheduleId);
                            setAccepted(res.message || "Baseline updated.");
                            await load();
                          } catch (err) {
                            const raw = err instanceof Error ? err.message : "";
                            setAcceptError(
                              /timed out/i.test(raw)
                                ? "Accept stopped waiting for the source. The finding already has the compared shape — retry once, or check the Snowflake/MySQL connector is reachable."
                                : raw || "Could not update the baseline",
                            );
                          } finally {
                            setAccepting(false);
                          }
                        }}
                      >
                        {accepting ? "Recording…" : "Accept new source shape"}
                      </Button>
                    </span>
                  )}
                </div>
              )}
            </article>
          );
        })}
      </div>
      {accepted && (
        <p role="status" className="df2-sched-history-accepted">{accepted}</p>
      )}
      {acceptError && (
        <p role="alert" className="df2-sched-history-error-text">{acceptError}</p>
      )}
    </div>
  );
}
