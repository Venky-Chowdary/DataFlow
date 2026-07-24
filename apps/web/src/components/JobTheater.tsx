import { useEffect, useMemo, useRef, useState } from "react";
import { ConnectorIcon } from "../app/brand-icons";
import { DtIcon } from "./DtIcon";
import { Spinner } from "./LoadingState";
import { CopyIdChip } from "./ui/CopyIdChip";
import { JobPhase, JobProgress, LoadHistoryReport, PreflightResult } from "../lib/types";
import { cancelJob, fetchJobMappingProof, resumeJob, streamJobProgress, type RepairMapping } from "../lib/api";
import { useActiveData } from "../lib/DataContext";
import { isJobSuccess, isJobTerminal, jobStatusBadgeClass, jobStatusLabel } from "../lib/uiUtils";
import { LoadHistoryPanel } from "./transfer/LoadHistoryPanel";
import { NotificationDeliveryStrip } from "./transfer/NotificationDeliveryStrip";
import { QuarantinePanel } from "./transfer/QuarantinePanel";
import { Gate8ProofCard } from "./transfer/Gate8ProofCard";
import { JobTrustScoreCard } from "./transfer/JobTrustScoreCard";
import { inferTransferFailureHint, isDestinationCapacityFailure } from "../lib/transferFailure";
import { CdcLeaseConflictPanel } from "./transfer/CdcLeaseConflictPanel";
import { CdcCursorGapPanel } from "./transfer/CdcCursorGapPanel";
import { CdcRetentionPanel } from "./transfer/CdcRetentionPanel";
import { CdcIncrementalSnapshotPanel } from "./transfer/CdcIncrementalSnapshotPanel";
import { LiveEventLog, type LiveLogEntry } from "./ui/LiveEventLog";
import { writeJobEventLog } from "../lib/jobEventLog";
import { useToast } from "./Toast";
import { MappingProofDrawer, type MappingProof } from "./MappingProofDrawer";
import { hashForScreen } from "../lib/appNavigation";

function asMappingProof(raw: unknown): MappingProof | null {
  if (!raw || typeof raw !== "object") return null;
  const proof = raw as MappingProof;
  if (!Array.isArray(proof.mappings) || proof.mappings.length === 0) return null;
  return proof;
}

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
  /** Clear cancel/fail dead-end — start a fresh transfer. */
  onNewTransfer?: () => void;
  /** Return to Validate with current mappings intact. */
  onBackToValidate?: () => void;
  /** Return to Map to adjust columns after a stop. */
  onBackToMap?: () => void;
  /** Resume from last checkpoint without leaving Transfer Studio. */
  onResumed?: (jobId: string) => void;
  /** Open another job (e.g. CDC lease holder) in Jobs / Theater. */
  onOpenJob?: (jobId: string) => void;
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
  queued: "Queued",
  reading: "Read",
  writing: "Write",
  preflight: "Gates",
  extract: "Extract",
  transform: "Transform",
  load: "Load",
  reconcile: "Reconcile",
  completed: "Done",
};

function phaseIndex(phase?: string, status?: string): number {
  if (isJobSuccess(status)) return 5;
  if (status === "failed" || status === "cancelled") return -1;
  const normalized =
    phase === "load" || phase === "writing" ? "writing"
    : phase === "extract" || phase === "reading" ? "reading"
    : phase === "reconciling" || phase === "verification" ? "reconcile"
    : phase;
  const idx = PHASES.findIndex((p) => p.id === (normalized || "queued"));
  return idx >= 0 ? idx : 0;
}

/** True when writes are done and the engine is proving the load (or about to). */
function isReconcilePhase(job: JobProgress): boolean {
  const phase = (job.phase || "").toLowerCase();
  if (phase === "reconcile" || phase === "reconciling" || phase === "verification") {
    return true;
  }
  const total = job.total_rows ?? 0;
  const processed = job.records_processed ?? 0;
  if (total > 0 && processed >= total && !isJobTerminal(job.status) && !isJobSuccess(job.status)) {
    // All rows reported written but job still running — reconcile owns the last percent.
    return true;
  }
  const phases = job.phases || [];
  if (phases.some((p) => p.name === "reconcile" && p.status === "active")) {
    return true;
  }
  return false;
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
  onNewTransfer,
  onBackToValidate,
  onBackToMap,
  onResumed,
  onOpenJob,
}: JobTheaterProps) {
  const { toast } = useToast();
  const { setActiveData } = useActiveData();
  const [job, setJob] = useState<JobProgress | null>(null);
  const [throughput, setThroughput] = useState(0);
  const [log, setLog] = useState<LiveLogEntry[]>([]);
  const [cancelling, setCancelling] = useState(false);
  const [resuming, setResuming] = useState(false);
  const startRef = useRef<number>(Date.now());
  const doneRef = useRef(false);
  const logSeqRef = useRef(0);
  const prevRef = useRef<{ message?: string; phase?: string; chunk?: number; loggedRows: number }>({
    loggedRows: 0,
  });

  useEffect(() => {
    setActiveData((prev) => ({
      name: prev?.name || sourceLabel || "transfer",
      filename: prev?.filename,
      columns: prev?.columns || [],
      row_count: job?.records_processed ?? prev?.row_count ?? 0,
      samples: prev?.samples,
      schema: prev?.schema,
      preflight_run_id: preflight?.run_id || prev?.preflight_run_id,
      job_id: jobId,
      validation_status: job?.status || prev?.validation_status,
      route: `${sourceLabel || "source"} → ${destLabel || "destination"}`,
      blockers: job?.error ? [job.error] : prev?.blockers,
    }));
  }, [destLabel, job?.error, job?.records_processed, job?.status, jobId, preflight?.run_id, setActiveData, sourceLabel]);

  useEffect(() => {
    startRef.current = Date.now();
    doneRef.current = false;
    prevRef.current = { loggedRows: 0 };
    logSeqRef.current = 0;
    const append = (line: string) => {
      const stamped = `${new Date().toLocaleTimeString()} — ${line}`;
      setLog((l) => {
        const entry: LiveLogEntry = { id: ++logSeqRef.current, text: stamped };
        // Trim oldest only — stable ids on remaining lines prevent remount flicker.
        const next = l.length >= 400 ? [...l.slice(-(399)), entry] : [...l, entry];
        writeJobEventLog(jobId, next.map((e) => e.text));
        return next;
      });
    };
    const boot: LiveLogEntry = {
      id: ++logSeqRef.current,
      text: `${new Date().toLocaleTimeString()} — Connecting to live job stream…`,
    };
    setLog([boot]);
    writeJobEventLog(jobId, [boot.text]);
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

  const handleResume = async () => {
    if (resuming) return;
    setResuming(true);
    try {
      const res = await resumeJob(jobId);
      const nextId = res.job_id || jobId;
      toast({
        title: "Resume started",
        message: "Continuing from the last checkpoint in Transfer Studio.",
        tone: "success",
      });
      onResumed?.(nextId);
      if (nextId !== jobId) {
        // Parent should swap jobId; local stream will remount via key/effect.
        doneRef.current = false;
      }
    } catch (e) {
      toast({ title: "Could not resume job", message: (e as Error).message, tone: "error" });
    } finally {
      setResuming(false);
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
      resuming={resuming}
      onCancel={handleCancel}
      onResume={handleResume}
      onNewTransfer={onNewTransfer}
      onBackToValidate={onBackToValidate}
      onBackToMap={onBackToMap}
      onOpenJob={onOpenJob}
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
  log: LiveLogEntry[];
  startedAtFallback?: number;
  preflight?: PreflightResult;
  cancelling?: boolean;
  resuming?: boolean;
  onCancel?: () => void;
  onResume?: () => void;
  onNewTransfer?: () => void;
  onBackToValidate?: () => void;
  onBackToMap?: () => void;
  onOpenJob?: (jobId: string) => void;
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
  preflight,
  cancelling,
  resuming,
  onCancel,
  onResume,
  onNewTransfer,
  onBackToValidate,
  onBackToMap,
  onOpenJob,
}: JobTheaterViewProps) {
  const total = job.total_rows ?? 0;
  const processed = job.records_processed ?? 0;
  const isFailed = job.status === "failed";
  const isCancelled = job.status === "cancelled";
  const isComplete = isJobSuccess(job.status);
  const isQuarantine = job.status === "completed_with_quarantine";
  const isRunning = !isFailed && !isComplete && !isCancelled;
  const reconciling = isRunning && isReconcilePhase(job);
  const currentPhase = reconciling
    ? PHASES.findIndex((p) => p.id === "reconcile")
    : phaseIndex(job.phase, job.status);
  const [mappingProofOpen, setMappingProofOpen] = useState(false);
  const [resolvedProof, setResolvedProof] = useState<MappingProof | null>(() => asMappingProof(job.mapping_proof));

  useEffect(() => {
    const fromJob = asMappingProof(job.mapping_proof);
    if (fromJob) {
      setResolvedProof(fromJob);
      return;
    }
    let cancelled = false;
    void fetchJobMappingProof(jobId)
      .then((res) => {
        if (cancelled) return;
        setResolvedProof(asMappingProof(res.mapping_proof));
      })
      .catch(() => {
        /* optional — job may predate mapping_proof persistence */
      });
    return () => {
      cancelled = true;
    };
  }, [jobId, job.mapping_proof]);

  const failureHint = isFailed
    ? inferTransferFailureHint(
      job.error || job.message,
      job.error_code,
      job.error_title,
      job.error_fix,
      job.error_confidence,
    )
    : null;
  const capacityFailure = isDestinationCapacityFailure(failureHint, job.error || job.message);

  // Prefer row-derived progress while writing. Once reconcile starts (or all rows
  // are written), hold 99% — never imply "done" until status is terminal success.
  const reportedPct = job.progress_pct ?? 0;
  const derivedPct = total > 0 ? (processed / Math.max(total, 1)) * 100 : null;
  const indeterminate = Boolean((job as { progress_indeterminate?: boolean }).progress_indeterminate) && !(total > 0);
  let rawProgress: number;
  if (reconciling) {
    rawProgress = Math.max(reportedPct || 99, 99);
  } else if (derivedPct != null) {
    rawProgress = derivedPct;
  } else {
    rawProgress = indeterminate ? Math.min(reportedPct || 5, 5) : reportedPct;
  }
  const progress = isComplete
    ? 100
    : Math.min(99, Math.max(isRunning ? 1 : 0, Math.round(rawProgress)));

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
  const loadMethod = typeof destinationSummary.load_method === "string" ? destinationSummary.load_method : "";
  const batchSize = Number(job.chunk_size ?? destinationSummary.chunk_size ?? 0) || 0;
  const jobRps = Number(job.records_per_second ?? destinationSummary.records_per_second ?? 0) || 0;
  const displayRps = isComplete && jobRps > 0 ? Math.round(jobRps) : throughput;
  const routeLabel = [sourceType, destType].filter(Boolean).join(" → ") || "this job";

  const timelinePhases = useMemo(() => {
    if (job.phases?.length) {
      return job.phases.map((phase: JobPhase) => {
        let state = phase.status;
        // If writes finished but phase list lags, force Reconcile active for honesty.
        if (reconciling && phase.name === "reconcile" && state !== "done" && state !== "failed") {
          state = "active";
        }
        if (reconciling && (phase.name === "load" || phase.name === "transform") && state === "active") {
          state = "done";
        }
        return {
          id: phase.name,
          label: PHASE_LABELS[phase.name] ?? phase.name,
          state,
          elapsedMs: typeof phase.elapsed_ms === "number" ? phase.elapsed_ms : null,
        };
      });
    }

    return PHASES.map((phase, i) => {
      const state = (isFailed || isCancelled) && i === currentPhase ? "failed"
        : i < currentPhase || isComplete ? "done"
        : i === currentPhase ? "active"
        : "pending";
      return { id: phase.id, label: phase.label, state, elapsedMs: null as number | null };
    });
  }, [job.phases, isFailed, isCancelled, currentPhase, isComplete, reconciling]);

  const activePhase = timelinePhases.find((p) => p.state === "active");
  const phaseLabel = reconciling
    ? "Reconcile"
    : activePhase?.label
      || (isComplete ? "Done" : isCancelled ? "Cancelled" : isFailed ? "Failed" : "Queued");

  const headline = isQuarantine
    ? "Completed with quarantine"
    : isComplete
      ? "Transfer complete"
      : isCancelled
        ? "Transfer cancelled"
        : isFailed
          ? "Transfer failed"
          : indeterminate
            ? "Streaming changes"
            : reconciling
              ? "Reconciling data"
              : "Transferring data";

  const statusCopy = (() => {
    if (!isRunning) return "Job finished";
    if (reconciling) {
      const msg = (job.message || "").trim();
      if (msg && !/^writing batch/i.test(msg)) return msg;
      return "All rows written — verifying destination row counts and checksums. This can take a few minutes on large loads.";
    }
    if (job.message) return job.message;
    if (indeterminate) return `${phaseLabel} — ${processed.toLocaleString()} change(s) applied…`;
    return `${phaseLabel} — ${processed.toLocaleString()}${total > 0 ? ` / ${total.toLocaleString()}` : ""} rows…`;
  })();

  const eta = useMemo(() => {
    const rps = displayRps;
    if (!isRunning || reconciling || rps <= 0 || total <= processed) return null;
    const secs = Math.ceil((total - processed) / rps);
    return secs < 60 ? `${secs}s` : `${Math.ceil(secs / 60)}m`;
  }, [isRunning, reconciling, displayRps, total, processed]);

  const slowSnowflakeTip =
    destType === "snowflake"
    && displayRps > 0
    && displayRps < 100
    && (loadMethod === "insert" || !loadMethod);

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
          <CopyIdChip id={jobId} label="Job" compact />
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
            <p>{job.message || "The job was stopped before completing. Choose where to go next — mappings and validation are still available."}</p>
          </div>
        </div>
      )}

      {isFailed && (
        <div className="df2-theater-v3-alert error">
          <DtIcon name="alert" size={18} />
          <div>
            <strong>
              {failureHint?.title
                || (job.failed_at_phase && !["failed", "cancelled"].includes(String(job.failed_at_phase).toLowerCase())
                  ? `Transfer failed during ${job.failed_at_phase}`
                  : "Transfer failed")}
            </strong>
            <p>{job.error || job.message || "The job stopped before completing. Review the event log below and re-run."}</p>
            {failureHint?.fix && (
              <p className="df2-theater-v3-fail-fix">
                <strong>
                  {failureHint.confidence === "high" ? "Likely checks: " : failureHint.confidence === "medium" ? "Suggested checks: " : "Next step: "}
                </strong>
                {failureHint.fix}
              </p>
            )}
            <p className="df2-theater-v3-fail-meta">
              {processed > 0 ? `${processed.toLocaleString()} rows written before failure. ` : "No rows committed. "}
              {rejectedRows > 0 ? `${rejectedRows.toLocaleString()} quarantined (data-quality findings — separate from this load failure). ` : ""}
              {job.chunk_current != null
                ? `Checkpoint at batch ${job.chunk_current}${job.chunk_total != null ? `/${job.chunk_total}` : ""}.`
                : "Use Resume below if a checkpoint was saved."}
            </p>
          </div>
        </div>
      )}

      {(isCancelled || isFailed) && (onNewTransfer || onBackToValidate || onBackToMap || onResume) && (
        <div className="df2-theater-v3-next" role="navigation" aria-label="After cancelled or failed transfer">
          <div className="df2-theater-v3-next-copy">
            <strong>
              {isCancelled
                ? "What next?"
                : job.cdc_lease_conflict
                  ? "Recover · lease conflict"
                  : job.cdc_cursor_gap
                    || job.error_code === "cdc_cursor_gap"
                    || job.error_code === "cdc_lsn_gap"
                    || job.error_code === "cdc_scn_gap"
                    ? "Recover · CDC cursor gap"
                    : job.cdc_append_only_sink || job.error_code === "cdc_append_only_sink"
                      ? "Recover · append-only sink"
                      : "Recover from failure"}
            </strong>
            <span>
              {isCancelled
                ? "Resume from checkpoint, adjust Map / Validate, or start clean."
                : job.cdc_lease_conflict
                  ? "Clear the lease or stop the holder first — Resume alone will hit the same conflict while the lease is live."
                : job.cdc_cursor_gap
                  || job.error_code === "cdc_cursor_gap"
                  || job.error_code === "cdc_lsn_gap"
                  || job.error_code === "cdc_scn_gap"
                  ? "Reset the watermark, then re-run with snapshot when_needed or initial — Resume with the same cursor will hit the same gap."
                : job.cdc_append_only_sink || job.error_code === "cdc_append_only_sink"
                  ? "Switch to a PK upsert destination or enable Allow append-only CDC in Destination Advanced."
                : capacityFailure
                  ? "Free destination capacity first, then Resume from checkpoint — Resume alone will hit the same error."
                  : "Resume from the last checkpoint, or fix mappings and re-run from Validate."}
            </span>
          </div>
          <div className="df2-theater-v3-next-actions">
            {onResume && (job.chunk_current != null || job.checkpoint) && !job.cdc_lease_conflict && !job.cdc_cursor_gap && (
              <button
                type="button"
                className="df2-btn df2-btn-sm df2-btn-primary"
                onClick={onResume}
                disabled={resuming}
              >
                <DtIcon name="play" size={14} /> {resuming ? "Resuming…" : "Resume from checkpoint"}
              </button>
            )}
            {onBackToMap && (
              <button type="button" className="df2-btn df2-btn-sm df2-btn-ghost" onClick={onBackToMap}>
                <DtIcon name="layers" size={14} /> Back to Map
              </button>
            )}
            {onBackToValidate && (
              <button type="button" className="df2-btn df2-btn-sm df2-btn-secondary" onClick={onBackToValidate}>
                <DtIcon name="gate" size={14} /> Back to Validate
              </button>
            )}
            {onNewTransfer && (
              <button type="button" className="df2-btn df2-btn-sm df2-btn-ghost" onClick={onNewTransfer}>
                <DtIcon name="plus" size={14} /> New transfer
              </button>
            )}
          </div>
        </div>
      )}

      {isFailed && job.cdc_lease_conflict && (
        <CdcLeaseConflictPanel
          job={job}
          onResume={onResume}
          resuming={resuming}
          onOpenJob={onOpenJob}
        />
      )}
      {isFailed && (
        <CdcCursorGapPanel job={job} onResume={onResume} resuming={resuming} />
      )}
      <CdcRetentionPanel
        status={job.cdc_retention_status}
        resume={job.cdc_retention_resume}
        retained={job.cdc_retention_retained}
        message={job.cdc_retention_message}
        dialect={job.cdc_retention_dialect}
        cursorKey={job.cdc_lease_cursor_key}
        onResume={onResume}
        resuming={resuming}
      />
      {(job.cdc_plugin || job.watermark || job.cdc_delivery || job.sync_mode === "cdc") && job._id && (
        <CdcIncrementalSnapshotPanel jobId={job._id} enabled />
      )}

      {(isComplete || isFailed || isCancelled || isQuarantine) && (
        <JobTrustScoreCard
          job={job}
          onOpenQuarantine={rejectedRows > 0 ? () => {
            document.getElementById("df2-theater-quarantine")?.scrollIntoView({ behavior: "smooth", block: "start" });
          } : undefined}
          onOpenValidate={onBackToValidate}
          onOpenMap={onBackToMap}
          onResume={onResume}
        />
      )}

      {slowSnowflakeTip && (
        <div className="df2-theater-v3-alert warn" role="note">
          <DtIcon name="zap" size={18} />
          <div>
            <strong>Low Snowflake throughput on this job</strong>
            <p>
              ~{displayRps.toLocaleString()} rows/s with load method {loadMethod || "insert"}.
              Prefer COPY INTO / larger batches (warehouse stream path) after redeploy if you still see INSERT-only loads.
            </p>
          </div>
        </div>
      )}

      <div className={`df2-theater-v3-progress-block${reconciling ? " is-reconciling" : ""}`}>
        <div className="df2-theater-v3-progress-top">
          <div className={`df2-theater-v3-ring ${isFailed ? "is-failed" : isComplete ? "is-done" : reconciling ? "is-reconcile" : ""}`} aria-hidden>
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
            <strong>{indeterminate && isRunning && !reconciling ? "…" : `${progress}%`}</strong>
          </div>
          <div className="df2-theater-v3-progress-copy">
            <h3>{headline}</h3>
            <p title={statusCopy}>{statusCopy}</p>
            <div className="df2-theater-v3-progress-tags">
              <span className={`df2-theater-v3-chunk${reconciling ? " is-reconcile" : ""}`}>
                <DtIcon name={reconciling ? "shield" : "activity"} size={11} /> {phaseLabel}
              </span>
              {!reconciling && job.chunk_current != null && job.chunk_total != null && job.chunk_total > 0 && (
                <span className="df2-theater-v3-chunk">
                  <DtIcon name="database" size={11} /> Batch {job.chunk_current}/{job.chunk_total}
                </span>
              )}
              {reconciling && (
                <span className="df2-theater-v3-chunk is-working is-reconcile">
                  <Spinner size="sm" label="" /> Verifying fidelity…
                </span>
              )}
              {stalled && isRunning && !reconciling && (
                <span className="df2-theater-v3-chunk is-working">
                  <Spinner size="sm" label="" /> Writing large batch…
                </span>
              )}
              {stalled && reconciling && (
                <span className="df2-theater-v3-chunk is-working is-reconcile">
                  Still reconciling — large destinations can take a few minutes
                </span>
              )}
            </div>
          </div>
        </div>
        <div
          className={`df2-theater-v3-bar ${isRunning ? "is-live" : ""} ${stalled && isRunning ? "is-stalled" : ""} ${reconciling ? "is-reconcile" : ""} ${isFailed ? "is-failed" : ""}`}
          role="progressbar"
          aria-valuenow={progress}
          aria-valuemin={0}
          aria-valuemax={100}
          aria-label={reconciling ? "Reconciling destination data" : "Transfer progress"}
        >
          <div className="df2-theater-v3-bar-fill" style={{ width: `${Math.min(progress, 100)}%` }}>
            {isRunning && <span className="df2-theater-v3-bar-shimmer" aria-hidden />}
          </div>
        </div>
        <div className="df2-theater-v3-bar-legend">
          <span>{processed.toLocaleString()} rows</span>
          {total > 0 && <span>{total.toLocaleString()} total</span>}
          {reconciling && <span className="df2-theater-v3-bar-legend-note">99% reserved for reconcile proof</span>}
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
        <article className="df2-theater-v3-metric" title={`${routeLabel} — live job throughput, not Proofs CSV→SQLite`}>
          <DtIcon name="activity" size={16} />
          <div>
            <strong>{displayRps > 0 ? `${displayRps.toLocaleString()}/s` : "—"}</strong>
            <span>This job rows/s</span>
          </div>
        </article>
        {loadMethod && (
          <article className="df2-theater-v3-metric" title="Snowflake/warehouse load path for this job">
            <DtIcon name="transfer" size={16} />
            <div>
              <strong>{loadMethod}</strong>
              <span>Load method</span>
            </div>
          </article>
        )}
        {batchSize > 0 && (
          <article className="df2-theater-v3-metric">
            <DtIcon name="database" size={16} />
            <div>
              <strong>{batchSize.toLocaleString()}</strong>
              <span>Batch size</span>
            </div>
          </article>
        )}
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
        {typeof destinationSummary.staging_table === "string" && destinationSummary.staging_table && (
          <article
            className="df2-theater-v3-metric"
            title={`Pre-ingestion staging · ${Number(destinationSummary.promoted_rows ?? 0).toLocaleString()} promoted of ${Number(destinationSummary.staged_rows ?? 0).toLocaleString()} staged`}
          >
            <DtIcon name="database" size={16} />
            <div>
              <strong>{String(destinationSummary.staging_table)}</strong>
              <span>
                {destinationSummary.promote_blocked
                  ? "Staging blocked promote"
                  : `Staging · ${Number(destinationSummary.promoted_rows ?? 0).toLocaleString()} promoted`}
              </span>
            </div>
          </article>
        )}
        {job.cdc_lag_seconds != null && Number.isFinite(Number(job.cdc_lag_seconds)) && (
          <article className="df2-theater-v3-metric">
            <DtIcon name="activity" size={16} />
            <div>
              <strong>{`${Number(job.cdc_lag_seconds).toFixed(1)}s`}</strong>
              <span>CDC lag</span>
            </div>
          </article>
        )}
        {(job.cdc_plugin || job.cdc_delivery) && (
          <article className="df2-theater-v3-metric">
            <DtIcon name="database" size={16} />
            <div>
              <strong>{job.cdc_plugin || "CDC"}</strong>
              <span>{job.cdc_delivery || "at-least-once"}{job.cdc_slot_name ? ` · ${job.cdc_slot_name}` : ""}</span>
            </div>
          </article>
        )}
        {job.cdc_row_filter && (
          <article
            className="df2-theater-v3-metric"
            title="SQL Server CDC TVF row_filter_option used for this run"
          >
            <DtIcon name="layers" size={16} />
            <div>
              <strong className="df2-mono">{job.cdc_row_filter}</strong>
              <span>CDC row filter</span>
            </div>
          </article>
        )}
        {job.replication_lag_bytes != null && Number.isFinite(Number(job.replication_lag_bytes)) && (
          <article className="df2-theater-v3-metric">
            <DtIcon name="database" size={16} />
            <div>
              <strong>
                {Number(job.replication_lag_bytes) >= 1_048_576
                  ? `${(Number(job.replication_lag_bytes) / 1_048_576).toFixed(1)} MB`
                  : Number(job.replication_lag_bytes) >= 1024
                    ? `${(Number(job.replication_lag_bytes) / 1024).toFixed(1)} KB`
                    : `${Number(job.replication_lag_bytes)} B`}
              </strong>
              <span>WAL / binlog lag</span>
            </div>
          </article>
        )}
        {job.cdc_heartbeat_at && (
          <article className="df2-theater-v3-metric">
            <DtIcon name="activity" size={16} />
            <div>
              <strong>{String(job.cdc_heartbeat_at).replace("T", " ").slice(0, 19)}</strong>
              <span>CDC heartbeat</span>
            </div>
          </article>
        )}
        {job.cdc_last_ddl_at && (
          <article className="df2-theater-v3-metric">
            <DtIcon name="database" size={16} />
            <div>
              <strong>{String(job.cdc_last_ddl_at).replace("T", " ").slice(0, 19)}</strong>
              <span>Last DDL seen</span>
            </div>
          </article>
        )}
        {job.watermark && (
          <article className="df2-theater-v3-metric">
            <DtIcon name="gate" size={16} />
            <div>
              <strong title={String(job.watermark)}>
                {String(job.watermark).length > 28
                  ? `${String(job.watermark).slice(0, 28)}…`
                  : String(job.watermark)}
              </strong>
              <span>CDC watermark</span>
            </div>
          </article>
        )}
        {(job.cdc_lease_holder || job.cdc_lease_conflict) && (
          <article
            className="df2-theater-v3-metric"
            title={
              job.cdc_lease_conflict
                ? `Lease conflict — held by ${job.cdc_lease_holder || "another worker"}${
                    job.cdc_lease_resource ? ` · ${job.cdc_lease_resource}` : ""
                  }`
                : job.cdc_lease_resource
                  ? String(job.cdc_lease_resource)
                  : "CDC resource lease"
            }
          >
            <DtIcon name={job.cdc_lease_conflict ? "alert" : "gate"} size={16} />
            <div>
              <strong>
                {job.cdc_lease_conflict
                  ? "Lease conflict"
                  : String(job.cdc_lease_holder || "").length > 24
                    ? `${String(job.cdc_lease_holder).slice(0, 24)}…`
                    : job.cdc_lease_holder}
              </strong>
              <span>
                {job.cdc_lease_conflict
                  ? `Held by ${job.cdc_lease_holder || "another worker"}`
                  : job.cdc_lease_stale
                    ? `CDC lease (stale)${job.cdc_lease_backend ? ` · ${job.cdc_lease_backend}` : ""}${
                        job.cdc_lease_generation != null ? ` · gen ${job.cdc_lease_generation}` : ""
                      }`
                    : `CDC lease${job.cdc_lease_backend ? ` · ${job.cdc_lease_backend}` : ""}${
                        job.cdc_lease_generation != null ? ` · gen ${job.cdc_lease_generation}` : ""
                      }`}
              </span>
            </div>
          </article>
        )}
        {job.source_ha_role && (
          <article
            className="df2-theater-v3-metric"
            title={job.source_ha_message || `Source HA role ${job.source_ha_role}`}
          >
            <DtIcon name="activity" size={16} />
            <div>
              <strong>{job.source_ha_role}</strong>
              <span>
                Source HA
                {job.source_ha_topology && job.source_ha_topology !== "none"
                  ? ` · ${job.source_ha_topology}`
                  : ""}
                {job.source_ha_group ? ` · ${job.source_ha_group}` : ""}
              </span>
            </div>
          </article>
        )}
        {job.cdc_retention_status && job.cdc_retention_status !== "n_a" && (
          <article
            className="df2-theater-v3-metric"
            title={job.cdc_retention_message || `Retention ${job.cdc_retention_status}`}
          >
            <DtIcon name={job.cdc_retention_status === "gap" || job.cdc_retention_status === "at_risk" ? "alert" : "gate"} size={16} />
            <div>
              <strong>{job.cdc_retention_status}</strong>
              <span>CDC retention</span>
            </div>
          </article>
        )}
      </div>

      {Array.isArray(job.streams) && job.streams.length > 1 && (
        <div className="df2-theater-v3-streams" aria-label="Per-stream health">
          {job.streams.map((stream) => (
            <div key={stream.name} className="df2-theater-v3-stream">
              <strong>{stream.name}</strong>
              <span>{stream.status || "—"}</span>
              <span>{(stream.records_processed ?? 0).toLocaleString()} rows</span>
              {stream.cdc_lag_seconds != null && (
                <span>{Number(stream.cdc_lag_seconds).toFixed(1)}s lag</span>
              )}
              {stream.watermark && (
                <span className="df2-mono" title={String(stream.watermark)}>
                  {`${String(stream.watermark).slice(0, 20)}${String(stream.watermark).length > 20 ? "…" : ""}`}
                </span>
              )}
              {stream.error && <span className="df2-muted">{stream.error}</span>}
            </div>
          ))}
        </div>
      )}

      {(job.cdc_shared_reader || job.snapshot_mode || job.cdc_row_filter) && (
        <div className="df2-theater-v3-cdc-meta" aria-label="CDC topology">
          {job.cdc_shared_reader && (
            <span className="df2-theater-cdc-chip is-ok">Shared log reader · one slot / server_id</span>
          )}
          {job.snapshot_mode && (
            <span className="df2-theater-cdc-chip">Snapshot · {job.snapshot_mode}</span>
          )}
          {job.cdc_delivery && (
            <span className="df2-theater-cdc-chip">{job.cdc_delivery} delivery</span>
          )}
          {job.cdc_row_filter && (
            <span className="df2-theater-cdc-chip" title="SQL Server CDC row_filter_option">
              Row filter · {job.cdc_row_filter}
            </span>
          )}
        </div>
      )}

      {(isComplete || isFailed || isCancelled) && (
        <NotificationDeliveryStrip
          notifications={job.notifications}
          className="df2-theater-v3-notify"
          compact
        />
      )}

      <div className="df2-theater-v3-phases" aria-label="Pipeline phases">
        {timelinePhases.map((phase) => {
          const state = phase.state;
          const elapsedLabel =
            phase.elapsedMs != null && phase.elapsedMs >= 0
              ? formatDuration(phase.elapsedMs)
              : null;
          return (
            <div key={phase.id} className={`df2-theater-v3-phase ${state}`}>
              <span className="df2-theater-v3-phase-dot" aria-hidden>
                {state === "done" && <DtIcon name="check" size={10} />}
                {state === "failed" && <DtIcon name="x" size={10} />}
                {state === "active" && <span className="df2-theater-v3-phase-pulse" />}
                {state === "skipped" && <span>—</span>}
              </span>
              <span className="df2-theater-v3-phase-label">{phase.label}</span>
              {elapsedLabel && (
                <span className="df2-theater-v3-phase-elapsed">{elapsedLabel}</span>
              )}
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
        <article className={`df2-theater-v3-sla-card${coercedNullRows > 0 ? " is-warn" : ""}`}>
          <span>Coerced to NULL</span>
          <strong>{coercedNullRows.toLocaleString()}</strong>
          <small>
            {coercedNullRows > 0
              ? "Value altered to NULL — not full fidelity"
              : "Normal type fits (ISO→DATETIME) are not counted here"}
          </small>
        </article>
        <article className="df2-theater-v3-sla-card">
          <span>Writer warnings</span>
          <strong>{warningCount.toLocaleString()}</strong>
          <small>
            {warningCount
              ? "Sample of writer messages (capped for display)"
              : "No destination warnings"}
          </small>
        </article>
        <article className="df2-theater-v3-sla-card">
          <span>Checksum evidence</span>
          <strong>
            {job.reconciliation?.target_checksum
              ? String(job.reconciliation.target_checksum).slice(0, 12)
              : checksum
                ? checksum.slice(0, 12)
                : "Pending"}
          </strong>
          <small>
            {job.reconciliation?.source_checksum && job.reconciliation?.target_checksum
              ? (job.reconciliation.source_checksum === job.reconciliation.target_checksum
                ? "Gate-8 source ↔ dest match"
                : "Gate-8 checksum mismatch")
              : checksum
                ? "Writer checksum captured"
                : "Captured on completion"}
          </small>
        </article>
      </div>

      {isComplete && job.reconciliation && (
        <Gate8ProofCard
          report={job.reconciliation}
          explanation={job.explanation}
          className="df2-theater-gate8"
          onOpenValidate={onBackToValidate}
          onOpenQuarantine={
            rejectedRows > 0 || isQuarantine
              ? () => {
                  const el = document.querySelector(".df2-theater-v3-quarantine");
                  el?.scrollIntoView({ behavior: "smooth", block: "start" });
                }
              : undefined
          }
        />
      )}

      {resolvedProof && (
        <section className="df2-theater-v3-mapping-proof" aria-label="Mapping proof">
          <div className="df2-theater-v3-mapping-proof-copy">
            <strong>Mapping proof</strong>
            <p>
              Per-column match evidence for this run
              {resolvedProof.summary?.mapped_count != null
                ? ` · ${resolvedProof.summary.mapped_count} pairs`
                : ""}
              . Explains mapping decisions — not Gate-8 row fidelity.
            </p>
          </div>
          <div className="df2-theater-v3-mapping-proof-actions">
            <button
              type="button"
              className="df2-btn df2-btn-sm df2-btn-primary"
              onClick={() => setMappingProofOpen(true)}
            >
              <DtIcon name="layers" size={14} /> Open mapping proof
            </button>
            <button
              type="button"
              className="df2-btn df2-btn-sm"
              onClick={() => {
                const link = `${window.location.origin}${window.location.pathname}${hashForScreen("jobs", {
                  jobId,
                  panel: "mapping-proof",
                })}`;
                void navigator.clipboard.writeText(link).then(() => {
                  /* best-effort */
                });
              }}
            >
              <DtIcon name="globe" size={14} /> Copy deep-link
            </button>
          </div>
          <MappingProofDrawer
            open={mappingProofOpen}
            onClose={() => setMappingProofOpen(false)}
            proof={resolvedProof}
            sourceLabel={sourceLabel}
            destLabel={destLabel}
          />
        </section>
      )}

      {isComplete && !isQuarantine && (
        <div className="df2-theater-v3-alert success">
          <DtIcon name="check" size={18} />
          <div>
            <strong>Success</strong>
            <p>{job.message || `${processed.toLocaleString()} rows transferred successfully`}</p>
            {droppedRows === 0 && coercedNullRows === 0 && (
              <p className="df2-theater-v3-alert-note">
                No write-time quarantine — every sampled cell fit after Validate remediations
                (e.g. Strip controls). Inspect Quarantine only lists rows rejected during Execute.
              </p>
            )}
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

      {(() => {
        const hist =
          job.load_history_report
          || (destinationSummary.load_history_report as LoadHistoryReport | undefined)
          || preflight?.load_history_report;
        if (!hist) return null;
        return (
          <section className="df2-theater-v3-quarantine" aria-label="Compared to prior loads">
            <LoadHistoryPanel report={hist} title="Compared to prior loads" />
          </section>
        );
      })()}

      {(rejectedRows > 0 || isFailed || isQuarantine) && (
        <section id="df2-theater-quarantine" className="df2-theater-v3-quarantine" aria-label="Quarantined rows">
          <div className="df2-theater-v3-alert warn">
            <DtIcon name="alert" size={18} />
            <div>
              <strong>
                {isFailed
                  ? (rejectedRows > 0
                    ? `${rejectedRows.toLocaleString()} quarantined row(s) — inspect findings`
                    : "Inspect preflight / quarantine findings")
                  : droppedRows > 0
                    ? `${droppedRows.toLocaleString()} rows quarantined`
                    : `${coercedNullRows.toLocaleString()} value(s) coerced to NULL`}
              </strong>
              <p>
                {isFailed
                  ? "Exact columns, sample values, reasons, and policies are listed below. Export CSV saves the file to your downloads. Use Validate for Strip / Quarantine / Fix bad data."
                  : "Review the quarantine details below and export them for remediation."}
              </p>
            </div>
          </div>
          <QuarantinePanel
            jobId={jobId}
            rejectedRows={rejectedRows}
            coercedNullRows={coercedNullRows}
            initialDetails={job.rejected_details}
            autoLoad
            initiallyOpen
            repairMappings={(resolvedProof?.mappings || []).map((m): RepairMapping => ({
              source: m.source,
              destination: m.target || m.source,
              destination_type: m.target_type || m.source_type,
              target_type: m.target_type || m.source_type,
              transform: m.transform || undefined,
            }))}
            onOpenValidate={onBackToValidate}
            onRepairDecided={() => {
              onBackToValidate?.();
            }}
          />
        </section>
      )}

      </div>

      <div className={`df2-theater-v3-log-section ${isRunning ? "is-live" : ""}`}>
        <LiveEventLog
          lines={log}
          live={isRunning}
          variant="theater"
          title="Live event log"
          empty="Waiting for job events…"
        />
      </div>
    </div>
  );
}
