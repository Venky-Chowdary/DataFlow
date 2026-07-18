import { useEffect, useMemo, useRef, useState } from "react";
import { ConnectorIcon } from "../app/brand-icons";
import { DtIcon } from "./DtIcon";
import { Spinner } from "./LoadingState";
import { JobPhase, JobProgress, PreflightResult } from "../lib/types";
import { cancelJob, streamJobProgress } from "../lib/api";
import { isJobSuccess, isJobTerminal, jobStatusBadgeClass, jobStatusLabel } from "../lib/uiUtils";
import { QuarantinePanel } from "./transfer/QuarantinePanel";
import { useToast } from "./Toast";

interface JobTheaterProps {
  jobId: string;
  sourceLabel?: string;
  destLabel?: string;
  sourceType?: string;
  destType?: string;
  preflight?: PreflightResult;
  onComplete?: (job: JobProgress) => void;
  onFailed?: (job: JobProgress) => void;
  onCancelled?: (job: JobProgress) => void;
}

const PHASES = [
  { id: "queued", label: "Queued" },
  { id: "reading", label: "Read" },
  { id: "preflight", label: "Gates" },
  { id: "writing", label: "Write" },
  { id: "reconcile", label: "Reconcile" },
  { id: "completed", label: "Done" },
];

const PHASE_LABELS: Record<string, string> = {
  preflight: "Gates",
  extract: "Extract",
  transform: "Transform",
  load: "Load",
  reconcile: "Reconcile",
};

function phaseIndex(phase?: string, status?: string): number {
  if (isJobSuccess(status)) return 5;
  if (status === "failed" || status === "cancelled") return -1;
  const idx = PHASES.findIndex((p) => p.id === (phase || "queued"));
  return idx >= 0 ? idx : 0;
}

function formatDuration(ms: number): string {
  const s = Math.floor(ms / 1000);
  if (s < 60) return `${s}s`;
  const m = Math.floor(s / 60);
  return `${m}m ${s % 60}s`;
}

function toEpochMs(value?: string): number | null {
  if (!value) return null;
  const ms = Date.parse(value);
  return Number.isFinite(ms) ? ms : null;
}

/**
 * Container: owns the live SSE/polling subscription and derived telemetry
 * (throughput, event log), then renders the presentational JobTheaterView.
 */
export function JobTheater({
  jobId,
  sourceLabel,
  destLabel,
  sourceType = "file",
  destType = "database",
  preflight,
  onComplete,
  onFailed,
  onCancelled,
}: JobTheaterProps) {
  const { toast } = useToast();
  const [job, setJob] = useState<JobProgress | null>(null);
  const [throughput, setThroughput] = useState(0);
  const [log, setLog] = useState<string[]>([]);
  const [cancelling, setCancelling] = useState(false);
  const startRef = useRef<number>(Date.now());
  const doneRef = useRef(false);
  const prevRef = useRef<{ message?: string; phase?: string; chunk?: number; loggedRows: number }>({
    loggedRows: 0,
  });

  useEffect(() => {
    startRef.current = Date.now();
    doneRef.current = false;
    prevRef.current = { loggedRows: 0 };
    const append = (line: string) =>
      setLog((l) => [...l.slice(-200), `${new Date().toLocaleTimeString()} — ${line}`]);
    setLog([`${new Date().toLocaleTimeString()} — Connecting to live job stream…`]);
    const stop = streamJobProgress(
      jobId,
      (update) => {
        const prev = prevRef.current;

        if (update.phase && update.phase !== prev.phase) {
          append(`Entered ${update.phase} phase`);
          prev.phase = update.phase;
        }
        if (update.message && update.message !== prev.message) {
          append(update.message);
          prev.message = update.message;
        }
        if (update.chunk_current != null && update.chunk_current !== prev.chunk) {
          const total = update.chunk_total != null ? `/${update.chunk_total}` : "";
          append(`Batch ${update.chunk_current}${total} written`);
          prev.chunk = update.chunk_current;
        }
        const processed = update.records_processed ?? 0;
        // Log a row milestone at least every 10k rows so the feed keeps moving
        // even when the backend only streams counters.
        if (processed - prev.loggedRows >= 10000) {
          prev.loggedRows = processed;
          append(`${processed.toLocaleString()} rows processed`);
        }

        setJob(update);
        const elapsed = (Date.now() - startRef.current) / 1000;
        if (elapsed > 0.5 && processed > 0) {
          setThroughput(Math.round(processed / elapsed));
        }
        if (!doneRef.current && isJobSuccess(update.status)) {
          doneRef.current = true;
          const quarantine = update.status === "completed_with_quarantine";
          append(
            quarantine
              ? `Job completed with quarantine — ${processed.toLocaleString()} rows landed, some rows rejected or coerced to NULL`
              : `Job completed — ${processed.toLocaleString()} rows transferred`,
          );
          onComplete?.(update);
        }
        if (!doneRef.current && update.status === "failed") {
          doneRef.current = true;
          append(`Job failed${update.error ? ` — ${update.error}` : ""}`);
          onFailed?.(update);
        }
        if (!doneRef.current && update.status === "cancelled") {
          doneRef.current = true;
          append("Job cancelled by user");
          onCancelled?.(update);
        }
      },
      () => {
        append("Live stream interrupted — connection lost");
        setJob((j) => (j && !isJobTerminal(j.status) ? { ...j, status: "failed", progress_pct: j.progress_pct ?? 0 } : j));
      },
    );
    return stop;
  }, [jobId, onComplete, onFailed, onCancelled]);

  const handleCancel = async () => {
    if (cancelling || doneRef.current) return;
    setCancelling(true);
    try {
      await cancelJob(jobId);
      toast({ title: "Cancellation requested", message: "The job will stop at the next checkpoint.", tone: "info" });
    } catch (e) {
      toast({ title: "Could not cancel job", message: (e as Error).message, tone: "error" });
    } finally {
      setCancelling(false);
    }
  };

  if (!job) {
    return (
      <div className="df2-theater-v3 df2-theater-v3-loading">
        <Spinner size="md" label="Connecting" />
        <p>Connecting to live job stream…</p>
      </div>
    );
  }

  return (
    <JobTheaterView
      job={job}
      jobId={jobId}
      sourceLabel={sourceLabel}
      destLabel={destLabel}
      sourceType={sourceType}
      destType={destType}
      throughput={throughput}
      log={log}
      startedAtFallback={startRef.current}
      preflight={preflight}
      cancelling={cancelling}
      onCancel={handleCancel}
    />
  );
}

interface JobTheaterViewProps {
  job: JobProgress;
  jobId: string;
  sourceLabel?: string;
  destLabel?: string;
  sourceType?: string;
  destType?: string;
  throughput: number;
  log: string[];
  startedAtFallback?: number;
  preflight?: PreflightResult;
  cancelling?: boolean;
  onCancel?: () => void;
}

/** Presentational live-transfer theater. Pure — driven entirely by props. */
export function JobTheaterView({
  job,
  jobId,
  sourceLabel,
  destLabel,
  sourceType = "file",
  destType = "database",
  throughput,
  log,
  startedAtFallback,
  cancelling,
  onCancel,
}: JobTheaterViewProps) {
  const logRef = useRef<HTMLDivElement>(null);

  useEffect(() => {
    logRef.current?.scrollTo({ top: logRef.current.scrollHeight, behavior: "smooth" });
  }, [log]);

  const total = job.total_rows ?? 0;
  const processed = job.records_processed ?? 0;
  const currentPhase = phaseIndex(job.phase, job.status);
  const isFailed = job.status === "failed";
  const isCancelled = job.status === "cancelled";
  const isComplete = isJobSuccess(job.status);
  const isQuarantine = job.status === "completed_with_quarantine";
  const isRunning = !isFailed && !isComplete && !isCancelled;

  // Reconcile reported progress with row-derived progress so the bar never
  // looks frozen while a large batch is being written.
  const reportedPct = job.progress_pct ?? 0;
  const derivedPct = total > 0 ? (processed / total) * 100 : 0;
  const rawProgress = Math.max(reportedPct, derivedPct);
  const progress = isComplete ? 100 : Math.min(99, Math.max(isRunning ? 3 : 0, Math.round(rawProgress)));

  // Detect a stalled bar: same progress value for a few seconds while running.
  const [stalled, setStalled] = useState(false);
  const lastProgressRef = useRef({ value: progress, at: Date.now() });
  useEffect(() => {
    if (lastProgressRef.current.value !== progress) {
      lastProgressRef.current = { value: progress, at: Date.now() };
      setStalled(false);
      return;
    }
    if (!isRunning) {
      setStalled(false);
      return;
    }
    const timer = window.setTimeout(() => setStalled(true), 4000);
    return () => window.clearTimeout(timer);
  }, [progress, isRunning]);

  const startMs = toEpochMs(job.started_at) ?? startedAtFallback ?? Date.now();
  const endMs = toEpochMs(job.completed_at) ?? Date.now();
  const elapsed = Math.max(0, endMs - startMs);

  const destinationSummary = (job.destination_summary ?? {}) as Record<string, unknown>;
  const rejectedRows = Number(job.rejected_rows ?? destinationSummary.rejected_rows ?? 0);
  const coercedNullRows = Number(job.coerced_null_rows ?? destinationSummary.coerced_null_rows ?? 0);
  const droppedRows = Math.max(rejectedRows - coercedNullRows, 0);
  const warningCount = Array.isArray(destinationSummary.warnings) ? destinationSummary.warnings.length : 0;
  const checksum = typeof destinationSummary.checksum === "string" ? destinationSummary.checksum : "";

  const timelinePhases = useMemo(() => {
    if (job.phases?.length) {
      return job.phases.map((phase: JobPhase) => ({
        id: phase.name,
        label: PHASE_LABELS[phase.name] ?? phase.name,
        state: phase.status,
      }));
    }

    return PHASES.map((phase, i) => {
      const state = (isFailed || isCancelled) && i === currentPhase ? "failed"
        : i < currentPhase || isComplete ? "done"
        : i === currentPhase ? "active"
        : "pending";
      return { id: phase.id, label: phase.label, state };
    });
  }, [job.phases, isFailed, isCancelled, currentPhase, isComplete]);

  const activePhase = timelinePhases.find((p) => p.state === "active");
  const phaseLabel = activePhase?.label
    || (isComplete ? "Done" : isCancelled ? "Cancelled" : isFailed ? "Failed" : "Queued");

  const eta = useMemo(() => {
    if (!isRunning || throughput <= 0 || total <= processed) return null;
    const secs = Math.ceil((total - processed) / throughput);
    return secs < 60 ? `${secs}s` : `${Math.ceil(secs / 60)}m`;
  }, [isRunning, throughput, total, processed]);

  const ringCircumference = 2 * Math.PI * 24;

  return (
    <div className={`df2-theater-v3 ${isRunning ? "is-live" : ""} ${isFailed || isCancelled ? "is-failed" : ""} ${isComplete ? "is-done" : ""}`}>
      <div className="df2-theater-v3-scroll">
      <header className="df2-theater-v3-header">
        <div className="df2-theater-v3-route">
          <div className="df2-theater-v3-endpoint">
            <ConnectorIcon id={sourceType} size={22} />
            <div className="df2-theater-v3-endpoint-copy">
              <span>Source</span>
              <strong title={sourceLabel}>{sourceLabel || "Source"}</strong>
            </div>
          </div>
          <div className="df2-theater-v3-arrow" aria-hidden>
            <DtIcon name="transfer" size={16} />
          </div>
          <div className="df2-theater-v3-endpoint">
            <ConnectorIcon id={destType} size={22} />
            <div className="df2-theater-v3-endpoint-copy">
              <span>Destination</span>
              <strong title={destLabel}>{destLabel || "Destination"}</strong>
            </div>
          </div>
        </div>
        <div className="df2-theater-v3-header-meta">
          <span className={`df2-theater-v3-live-pill ${isRunning ? "is-live" : isQuarantine ? "is-quarantine" : isComplete ? "is-done" : "is-failed"}`}>
            <span className="df2-theater-v3-live-dot" aria-hidden />
            {isRunning ? "Live" : isQuarantine ? "Quarantine" : isComplete ? "Finalized" : isCancelled ? "Cancelled" : "Attention"}
          </span>
          <span className={jobStatusBadgeClass(job.status)}>{jobStatusLabel(job.status)}</span>
          <span className="df2-theater-v3-job-id" title={jobId}>#{jobId.slice(0, 8)}</span>
          {isRunning && onCancel && (
            <button
              type="button"
              className="df2-btn df2-btn-sm df2-btn-ghost"
              onClick={onCancel}
              disabled={cancelling}
            >
              <DtIcon name="x" size={14} /> {cancelling ? "Cancelling…" : "Cancel"}
            </button>
          )}
        </div>
      </header>

      {isCancelled && (
        <div className="df2-theater-v3-alert error">
          <DtIcon name="x" size={18} />
          <div>
            <strong>Transfer cancelled</strong>
            <p>{job.message || "The job was stopped before completing. Re-run from Transfer Studio when ready."}</p>
          </div>
        </div>
      )}

      {isFailed && (
        <div className="df2-theater-v3-alert error">
          <DtIcon name="alert" size={18} />
          <div>
            <strong>Transfer failed{job.phase ? ` during ${job.phase}` : ""}</strong>
            <p>{job.error || job.message || "The job stopped before completing. Review the event log below and re-run."}</p>
          </div>
        </div>
      )}

      <div className="df2-theater-v3-progress-block">
        <div className="df2-theater-v3-progress-top">
          <div className={`df2-theater-v3-ring ${isFailed ? "is-failed" : isComplete ? "is-done" : ""}`} aria-hidden>
            <svg viewBox="0 0 56 56">
              <circle cx="28" cy="28" r="24" className="track" />
              <circle
                cx="28"
                cy="28"
                r="24"
                className="fill"
                strokeDasharray={`${(progress / 100) * ringCircumference} ${ringCircumference}`}
                transform="rotate(-90 28 28)"
              />
            </svg>
            <strong>{progress}%</strong>
          </div>
          <div className="df2-theater-v3-progress-copy">
            <h3>{isQuarantine ? "Completed with quarantine" : isComplete ? "Transfer complete" : isCancelled ? "Transfer cancelled" : isFailed ? "Transfer failed" : "Transferring data"}</h3>
            <p title={job.message || phaseLabel}>
              {job.message || (isRunning ? `${phaseLabel} — streaming rows to destination…` : "Job finished")}
            </p>
            <div className="df2-theater-v3-progress-tags">
              <span className="df2-theater-v3-chunk">
                <DtIcon name="activity" size={11} /> {phaseLabel}
              </span>
              {job.chunk_current != null && job.chunk_total != null && job.chunk_total > 0 && (
                <span className="df2-theater-v3-chunk">
                  <DtIcon name="database" size={11} /> Batch {job.chunk_current}/{job.chunk_total}
                </span>
              )}
              {stalled && isRunning && (
                <span className="df2-theater-v3-chunk is-working">
                  <Spinner size="sm" label="" /> Writing large batch…
                </span>
              )}
            </div>
          </div>
        </div>
        <div
          className={`df2-theater-v3-bar ${isRunning ? "is-live" : ""} ${stalled && isRunning ? "is-stalled" : ""} ${isFailed ? "is-failed" : ""}`}
          role="progressbar"
          aria-valuenow={progress}
          aria-valuemin={0}
          aria-valuemax={100}
        >
          <div className="df2-theater-v3-bar-fill" style={{ width: `${Math.min(progress, 100)}%` }}>
            {isRunning && <span className="df2-theater-v3-bar-shimmer" aria-hidden />}
          </div>
        </div>
        <div className="df2-theater-v3-bar-legend">
          <span>{processed.toLocaleString()} rows</span>
          {total > 0 && <span>{total.toLocaleString()} total</span>}
        </div>
      </div>

      <div className="df2-theater-v3-metrics">
        <article className="df2-theater-v3-metric">
          <DtIcon name="trend" size={16} />
          <div>
            <strong>{processed.toLocaleString()}</strong>
            <span>Rows moved</span>
          </div>
        </article>
        {total > 0 && (
          <article className="df2-theater-v3-metric">
            <DtIcon name="database" size={16} />
            <div>
              <strong>{total.toLocaleString()}</strong>
              <span>Total rows</span>
            </div>
          </article>
        )}
        <article className="df2-theater-v3-metric">
          <DtIcon name="activity" size={16} />
          <div>
            <strong>{throughput > 0 ? `${throughput.toLocaleString()}/s` : "—"}</strong>
            <span>Throughput</span>
          </div>
        </article>
        {eta && (
          <article className="df2-theater-v3-metric">
            <DtIcon name="gate" size={16} />
            <div>
              <strong>{eta}</strong>
              <span>ETA</span>
            </div>
          </article>
        )}
        <article className="df2-theater-v3-metric">
          <DtIcon name="jobs" size={16} />
          <div>
            <strong>{formatDuration(elapsed)}</strong>
            <span>Elapsed</span>
          </div>
        </article>
      </div>

      <div className="df2-theater-v3-phases" aria-label="Pipeline phases">
        {timelinePhases.map((phase) => {
          const state = phase.state;
          return (
            <div key={phase.id} className={`df2-theater-v3-phase ${state}`}>
              <span className="df2-theater-v3-phase-dot" aria-hidden>
                {state === "done" && <DtIcon name="check" size={10} />}
                {state === "failed" && <DtIcon name="x" size={10} />}
                {state === "active" && <span className="df2-theater-v3-phase-pulse" />}
                {state === "skipped" && <span>—</span>}
              </span>
              <span className="df2-theater-v3-phase-label">{phase.label}</span>
            </div>
          );
        })}
      </div>

      <div className="df2-theater-v3-sla" aria-label="Execution quality and evidence">
        <article className="df2-theater-v3-sla-card">
          <span>Dropped / rejected</span>
          <strong>{droppedRows.toLocaleString()}</strong>
          <small>{droppedRows > 0 ? "Isolated in quarantine — not written" : "No rows dropped"}</small>
        </article>
        <article className="df2-theater-v3-sla-card">
          <span>Coerced to NULL</span>
          <strong>{coercedNullRows.toLocaleString()}</strong>
          <small>{coercedNullRows > 0 ? "Value altered to NULL — not full fidelity" : "No values coerced"}</small>
        </article>
        <article className="df2-theater-v3-sla-card">
          <span>Writer warnings</span>
          <strong>{warningCount.toLocaleString()}</strong>
          <small>{warningCount ? "Review in destination summary" : "No destination warnings"}</small>
        </article>
        <article className="df2-theater-v3-sla-card">
          <span>Checksum evidence</span>
          <strong>{checksum ? checksum.slice(0, 12) : "Pending"}</strong>
          <small>{checksum ? "Writer checksum captured" : "Captured on completion"}</small>
        </article>
      </div>

      {isComplete && !isQuarantine && (
        <div className="df2-theater-v3-alert success">
          <DtIcon name="check" size={18} />
          <div>
            <strong>Success</strong>
            <p>{job.message || `${processed.toLocaleString()} rows transferred successfully`}</p>
          </div>
        </div>
      )}

      {isQuarantine && (
        <div className="df2-theater-v3-alert warn">
          <DtIcon name="alert" size={18} />
          <div>
            <strong>Completed with quarantine — not full fidelity</strong>
            <p>
              {processed.toLocaleString()} rows landed
              {droppedRows > 0 ? `, ${droppedRows.toLocaleString()} dropped/rejected` : ""}
              {coercedNullRows > 0 ? `, ${coercedNullRows.toLocaleString()} value(s) coerced to NULL` : ""}. Review the details below.
            </p>
          </div>
        </div>
      )}

      {rejectedRows > 0 && (
        <div className="df2-theater-v3-alert warn">
          <DtIcon name="alert" size={18} />
          <div>
            <strong>{droppedRows > 0 ? `${droppedRows.toLocaleString()} rows rejected` : `${coercedNullRows.toLocaleString()} value(s) coerced to NULL`}</strong>
            <p>Review the quarantine details below and export them for remediation.</p>
          </div>
          <QuarantinePanel jobId={jobId} rejectedRows={rejectedRows} coercedNullRows={coercedNullRows} />
        </div>
      )}

      </div>

      <div className="df2-theater-v3-log-section">
        <div className="df2-theater-v3-log" ref={logRef}>
          <div className="df2-theater-v3-log-head">
            <strong><span className="df2-theater-v3-log-dot" aria-hidden /> Live event log</strong>
            <span>{log.length ? `${log.length} events` : "Waiting…"}</span>
          </div>
          {log.length === 0 ? (
            <div className="df2-theater-v3-log-empty">Waiting for job events…</div>
          ) : (
            log.map((line, i) => <div key={i} className="df2-theater-v3-log-line">{line}</div>)
          )}
        </div>
      </div>
    </div>
  );
}
