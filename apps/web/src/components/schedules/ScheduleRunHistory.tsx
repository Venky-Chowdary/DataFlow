import { useCallback, useEffect, useState } from "react";
import { DtIcon } from "../DtIcon";
import { Spinner } from "../LoadingState";
import { CopyIdChip } from "../ui/CopyIdChip";
import { fetchScheduleHistory } from "../../lib/api";
import { jobStatusBadgeClass, jobStatusLabel } from "../../lib/uiUtils";
import type { ScheduleRun } from "../../lib/types";

interface ScheduleRunHistoryProps {
  scheduleId: string;
  onOpenJob?: (jobId: string) => void;
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

export function ScheduleRunHistory({ scheduleId, onOpenJob }: ScheduleRunHistoryProps) {
  const [runs, setRuns] = useState<ScheduleRun[] | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  const load = useCallback(async () => {
    setLoading(true);
    setError(null);
    try {
      const res = await fetchScheduleHistory(scheduleId, 25);
      setRuns(res.runs ?? []);
    } catch (e) {
      setError(e instanceof Error ? e.message : "Could not load run history");
    }
    setLoading(false);
  }, [scheduleId]);

  useEffect(() => {
    void load();
  }, [load]);

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
      <div className="df2-sched-history-table" role="table" aria-label="Run history">
        <div className="df2-sched-history-row is-head" role="row">
          <span role="columnheader">Status</span>
          <span role="columnheader">Started</span>
          <span role="columnheader">Duration</span>
          <span role="columnheader">Rows</span>
          <span role="columnheader">Rejected</span>
          <span role="columnheader">Coerced NULL</span>
          <span role="columnheader">Job</span>
        </div>
        {runs.map((run, i) => {
          const rejected = run.rejected_rows ?? 0;
          const coerced = run.coerced_null_rows ?? 0;
          return (
            <div className={`df2-sched-history-row${run.retry_scheduled ? " is-retry" : ""}`} role="row" key={`${run.job_id}-${run.attempt}-${i}`}>
              <span role="cell" data-label="Status">
                <span className={jobStatusBadgeClass(run.status)}>{jobStatusLabel(run.status)}</span>
                {run.attempt > 0 && (
                  <span className="df2-sched-attempt" title={`Retry attempt #${run.attempt}`}>
                    <DtIcon name="trend" size={11} /> retry #{run.attempt}
                  </span>
                )}
                {run.retry_scheduled && <span className="df2-sched-attempt" title="A retry was scheduled after this attempt">retry queued</span>}
              </span>
              <span role="cell" data-label="Started">{formatWhen(run.started_at)}</span>
              <span role="cell" data-label="Duration">{formatDuration(run.duration_seconds)}</span>
              <span role="cell" data-label="Rows">{(run.records_transferred ?? 0).toLocaleString()}</span>
              <span role="cell" data-label="Rejected" className={rejected > 0 ? "df2-sched-num-warn" : ""}>{rejected.toLocaleString()}</span>
              <span role="cell" data-label="Coerced NULL" className={coerced > 0 ? "df2-sched-num-warn" : ""}>{coerced.toLocaleString()}</span>
              <span role="cell" data-label="Job" className="df2-sched-job-cell">
                {run.job_id ? (
                  <>
                    <CopyIdChip id={run.job_id} label="Job" compact />
                    <button type="button" className="df2-sched-job-link" onClick={() => onOpenJob?.(run.job_id)} title="Open job in Jobs">
                      View <DtIcon name="arrow-up-right" size={12} />
                    </button>
                  </>
                ) : "—"}
              </span>
            </div>
          );
        })}
      </div>
      {runs.some((r) => (r.error || "").trim()) && (
        <ul className="df2-sched-history-errors">
          {runs.filter((r) => (r.error || "").trim()).slice(0, 3).map((r, i) => (
            <li key={`${r.job_id}-err-${i}`}>
              <span className={jobStatusBadgeClass(r.status)}>{jobStatusLabel(r.status)}</span>
              <span className="df2-sched-history-error-text" title={r.error}>{r.error}</span>
            </li>
          ))}
        </ul>
      )}
    </div>
  );
}
