import { useEffect, useMemo, useRef, useState, type Dispatch, type SetStateAction } from "react";
import { ConnectorIcon } from "../app/brand-icons";
import { DtIcon } from "./DtIcon";
import { Spinner } from "./LoadingState";
import { JobProgress } from "../lib/types";
import { streamJobProgress } from "../lib/api";
import { jobStatusBadgeClass } from "../lib/uiUtils";
import {
  formatJobLogLine,
  readJobEventLog,
  writeJobEventLog,
} from "../lib/jobEventLog";

interface JobTheaterProps {
  jobId: string;
  sourceLabel?: string;
  destLabel?: string;
  sourceType?: string;
  destType?: string;
  /** Compact metrics for embedding in result dashboard */
  compact?: boolean;
  /** Prefer showing log panel only (seeded from storage / initialLog) */
  logOnly?: boolean;
  initialLog?: string[];
  onComplete?: (job: JobProgress, eventLog: string[]) => void;
  onFailed?: (job: JobProgress, eventLog: string[]) => void;
  onLogChange?: (eventLog: string[]) => void;
}

const PHASES = [
  { id: "queued", label: "Queued" },
  { id: "reading", label: "Read" },
  { id: "preflight", label: "Gates" },
  { id: "writing", label: "Write" },
  { id: "reconcile", label: "Reconcile" },
  { id: "completed", label: "Done" },
];

function phaseIndex(phase?: string, status?: string): number {
  if (status === "completed") return 5;
  if (status === "failed") return -1;
  const idx = PHASES.findIndex((p) => p.id === (phase || "queued"));
  return idx >= 0 ? idx : 0;
}

function formatDuration(ms: number): string {
  const s = Math.floor(ms / 1000);
  if (s < 60) return `${s}s`;
  const m = Math.floor(s / 60);
  return `${m}m ${s % 60}s`;
}

function formatCompact(n: number): string {
  try {
    return new Intl.NumberFormat("en", { notation: "compact", maximumFractionDigits: 1 }).format(n);
  } catch {
    return n.toLocaleString();
  }
}

function pushLogLine(
  jobId: string,
  setLog: Dispatch<SetStateAction<string[]>>,
  message: string,
  onLogChange?: (eventLog: string[]) => void,
) {
  const line = formatJobLogLine(message);
  setLog((prev) => {
    if (prev.length && prev[prev.length - 1].endsWith(` — ${message}`)) return prev;
    const next = [...prev, line];
    writeJobEventLog(jobId, next);
    onLogChange?.(next);
    return next;
  });
}

export function JobTheater({
  jobId,
  sourceLabel,
  destLabel,
  sourceType = "file",
  destType = "database",
  compact = false,
  logOnly = false,
  initialLog,
  onComplete,
  onFailed,
  onLogChange,
}: JobTheaterProps) {
  const [job, setJob] = useState<JobProgress | null>(null);
  const [throughput, setThroughput] = useState(0);
  const [log, setLog] = useState<string[]>(() => {
    const stored = readJobEventLog(jobId);
    if (stored.length) return stored;
    if (initialLog?.length) {
      writeJobEventLog(jobId, initialLog);
      return initialLog;
    }
    return [];
  });
  const startRef = useRef<number>(Date.now());
  const doneRef = useRef(false);
  const logRef = useRef<HTMLDivElement>(null);
  const lastPctRef = useRef<number>(-1);

  useEffect(() => {
    startRef.current = Date.now();
    doneRef.current = false;
    lastPctRef.current = -1;
    const seeded = readJobEventLog(jobId);
    if (seeded.length) {
      setLog(seeded);
    } else if (initialLog?.length) {
      writeJobEventLog(jobId, initialLog);
      setLog(initialLog);
    } else {
      const boot = [formatJobLogLine(`Connected to job ${jobId.slice(0, 8)}…`)];
      writeJobEventLog(jobId, boot);
      setLog(boot);
    }

    const stop = streamJobProgress(
      jobId,
      (update) => {
        setJob((prev) => {
          if (update.message && update.message !== prev?.message) {
            pushLogLine(jobId, setLog, update.message, onLogChange);
          } else if (update.phase && update.phase !== prev?.phase) {
            pushLogLine(jobId, setLog, `Phase → ${update.phase}`, onLogChange);
          }
          if (update.status && update.status !== prev?.status) {
            pushLogLine(jobId, setLog, `Status → ${update.status}`, onLogChange);
          }
          const pct = update.progress_pct ?? 0;
          if (pct >= 0 && (lastPctRef.current < 0 || pct - lastPctRef.current >= 10 || pct >= 100)) {
            if (pct !== lastPctRef.current) {
              lastPctRef.current = pct;
              const rows = update.records_processed ?? 0;
              pushLogLine(
                jobId,
                setLog,
                `Progress ${pct}% · ${rows.toLocaleString()} rows`,
                onLogChange,
              );
            }
          }
          if (update.error && update.error !== prev?.error) {
            pushLogLine(jobId, setLog, `Error: ${update.error}`, onLogChange);
          }
          return update;
        });

        const elapsed = (Date.now() - startRef.current) / 1000;
        const processed = update.records_processed ?? 0;
        if (elapsed > 0.5 && processed > 0) {
          setThroughput(Math.round(processed / elapsed));
        }

        if (!doneRef.current && update.status === "completed") {
          doneRef.current = true;
          const finishLine = formatJobLogLine("Transfer completed");
          setLog((prev) => {
            const next = prev.length && prev[prev.length - 1].includes("Transfer completed")
              ? prev
              : [...prev, finishLine];
            writeJobEventLog(jobId, next);
            onLogChange?.(next);
            queueMicrotask(() => onComplete?.(update, next));
            return next;
          });
        }
        if (!doneRef.current && update.status === "failed") {
          doneRef.current = true;
          const failMsg = update.error ? `Failed: ${update.error}` : "Transfer failed";
          const failLine = formatJobLogLine(failMsg);
          setLog((prev) => {
            const next = [...prev, failLine];
            writeJobEventLog(jobId, next);
            onLogChange?.(next);
            queueMicrotask(() => onFailed?.(update, next));
            return next;
          });
        }
      },
      () => {
        pushLogLine(jobId, setLog, "Live stream disconnected", onLogChange);
        setJob((j) => (j ? { ...j, status: "failed", progress_pct: j.progress_pct ?? 0 } : null));
      },
    );
    return stop;
  }, [jobId, onComplete, onFailed, onLogChange, initialLog]);

  useEffect(() => {
    logRef.current?.scrollTo({ top: logRef.current.scrollHeight, behavior: "smooth" });
  }, [log]);

  const progress = job?.progress_pct ?? 5;
  const total = job?.total_rows ?? 0;
  const processed = job?.records_processed ?? 0;
  const currentPhase = phaseIndex(job?.phase, job?.status);
  const isFailed = job?.status === "failed";
  const isComplete = job?.status === "completed";
  const isRunning = !isFailed && !isComplete;
  const elapsed = Date.now() - startRef.current;

  const eta = useMemo(() => {
    if (!isRunning || throughput <= 0 || total <= processed) return null;
    const secs = Math.ceil((total - processed) / throughput);
    return secs < 60 ? `${secs}s` : `${Math.ceil(secs / 60)}m`;
  }, [isRunning, throughput, total, processed]);

  const logPanel = (
    <section className="df2-job-log-panel is-open" aria-label="Job event log">
      <header className="df2-job-log-panel-head">
        <div className="df2-job-log-panel-title">
          <DtIcon name="activity" size={14} />
          <strong>Job log</strong>
          <span className="df2-job-log-count">{log.length} events</span>
          {isRunning && <span className="df2-job-log-live">Live</span>}
        </div>
      </header>
      <div className="df2-job-log-panel-body" ref={logRef} role="log" aria-live="polite">
        {log.length === 0 ? (
          <div className="df2-job-log-empty">Waiting for transfer events…</div>
        ) : (
          log.map((line, i) => (
            <div key={`${i}-${line.slice(0, 24)}`} className="df2-job-log-line">
              {line}
            </div>
          ))
        )}
      </div>
    </section>
  );

  if (logOnly) {
    return (
      <div className="df2-theater-v3 df2-theater-v3-log-only">
        {logPanel}
      </div>
    );
  }

  if (!job) {
    return (
      <div className="df2-theater-v3 df2-theater-v3-loading">
        <div className="df2-theater-v3-top">
          <Spinner size="md" label="Connecting" />
          <p>Connecting to live job stream…</p>
        </div>
        {logPanel}
      </div>
    );
  }

  return (
    <div
      className={[
        "df2-theater-v3",
        compact ? "is-compact" : "",
        isRunning ? "is-live" : "",
        isFailed ? "is-failed" : "",
        isComplete ? "is-done" : "",
      ]
        .filter(Boolean)
        .join(" ")}
    >
      <div className="df2-theater-v3-top">
        <header className="df2-theater-v3-header">
          <div className="df2-theater-v3-route">
            <div className="df2-theater-v3-endpoint">
              <ConnectorIcon id={sourceType} size={compact ? 18 : 22} />
              <div>
                <span>Source</span>
                <strong title={sourceLabel}>{sourceLabel || "Source"}</strong>
              </div>
            </div>
            <div className="df2-theater-v3-arrow" aria-hidden>
              <DtIcon name="transfer" size={14} />
            </div>
            <div className="df2-theater-v3-endpoint">
              <ConnectorIcon id={destType} size={compact ? 18 : 22} />
              <div>
                <span>Destination</span>
                <strong title={destLabel}>{destLabel || "Destination"}</strong>
              </div>
            </div>
          </div>
          <div className="df2-theater-v3-header-meta">
            <span className={jobStatusBadgeClass(job.status)}>{job.status}</span>
            <span className="df2-theater-v3-job-id" title={jobId}>#{jobId.slice(0, 8)}</span>
          </div>
        </header>

        <div className="df2-theater-v3-progress-block">
          <div className="df2-theater-v3-progress-top">
            <div className="df2-theater-v3-ring" aria-hidden>
              <svg viewBox="0 0 56 56">
                <circle cx="28" cy="28" r="24" className="track" />
                <circle
                  cx="28"
                  cy="28"
                  r="24"
                  className="fill"
                  strokeDasharray={`${(progress / 100) * 150.8} 150.8`}
                  transform="rotate(-90 28 28)"
                />
              </svg>
              <div className="df2-theater-v3-ring-label">
                <strong>{progress}%</strong>
              </div>
            </div>
            <div className="df2-theater-v3-progress-copy">
              <h3>{isComplete ? "Transfer complete" : isFailed ? "Transfer failed" : job.phase || "Running"}</h3>
              <p>{job.message || (isRunning ? "Streaming rows to destination…" : "Job finished")}</p>
              <span className="df2-theater-v3-count">
                {processed.toLocaleString()}
                {total > 0 ? ` / ${total.toLocaleString()} rows` : " rows processed"}
              </span>
            </div>
          </div>
          <div className="df2-theater-v3-bar" role="progressbar" aria-valuenow={progress} aria-valuemin={0} aria-valuemax={100}>
            <div className="df2-theater-v3-bar-fill" style={{ width: `${Math.min(progress, 100)}%` }} />
          </div>
        </div>

        <div className="df2-theater-v3-metrics">
          <article className="df2-theater-v3-metric">
            <strong>{formatCompact(processed)}</strong>
            <span>Moved</span>
          </article>
          {total > 0 && (
            <article className="df2-theater-v3-metric">
              <strong>{formatCompact(total)}</strong>
              <span>Total</span>
            </article>
          )}
          {throughput > 0 && (
            <article className="df2-theater-v3-metric">
              <strong>{formatCompact(throughput)}/s</strong>
              <span>Rate</span>
            </article>
          )}
          {eta && (
            <article className="df2-theater-v3-metric">
              <strong>{eta}</strong>
              <span>ETA</span>
            </article>
          )}
          <article className="df2-theater-v3-metric">
            <strong>{formatDuration(elapsed)}</strong>
            <span>Elapsed</span>
          </article>
        </div>

        <div className="df2-theater-v3-phases" aria-label="Pipeline phases">
          {PHASES.map((phase, i) => {
            const state =
              isFailed && i === currentPhase
                ? "failed"
                : i < currentPhase || isComplete
                  ? "done"
                  : i === currentPhase
                    ? "active"
                    : "pending";
            return (
              <div key={phase.id} className={`df2-theater-v3-phase ${state}`}>
                <span className="df2-theater-v3-phase-dot" aria-hidden>
                  {state === "done" && <DtIcon name="check" size={10} />}
                  {state === "failed" && <DtIcon name="x" size={10} />}
                </span>
                <span className="df2-theater-v3-phase-label">{phase.label}</span>
              </div>
            );
          })}
        </div>

        {isFailed && job.error && (
          <div className="df2-theater-v3-alert error">
            <DtIcon name="alert" size={16} />
            <div>
              <strong>Error</strong>
              <p>{job.error}</p>
            </div>
          </div>
        )}
      </div>

      {logPanel}
    </div>
  );
}
