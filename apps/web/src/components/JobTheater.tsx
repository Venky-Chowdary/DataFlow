import { useEffect, useMemo, useRef, useState } from "react";
import { ConnectorIcon } from "../app/brand-icons";
import { DtIcon } from "./DtIcon";
import { Spinner } from "./LoadingState";
import { JobProgress } from "../lib/types";
import { streamJobProgress } from "../lib/api";
import { jobStatusBadgeClass } from "../lib/uiUtils";

interface JobTheaterProps {
  jobId: string;
  sourceLabel?: string;
  destLabel?: string;
  sourceType?: string;
  destType?: string;
  onComplete?: (job: JobProgress) => void;
  onFailed?: (job: JobProgress) => void;
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

export function JobTheater({
  jobId,
  sourceLabel,
  destLabel,
  sourceType = "file",
  destType = "database",
  onComplete,
  onFailed,
}: JobTheaterProps) {
  const [job, setJob] = useState<JobProgress | null>(null);
  const [throughput, setThroughput] = useState(0);
  const [logOpen, setLogOpen] = useState(true);
  const [log, setLog] = useState<string[]>([]);
  const startRef = useRef<number>(Date.now());
  const doneRef = useRef(false);
  const logRef = useRef<HTMLDivElement>(null);

  useEffect(() => {
    startRef.current = Date.now();
    doneRef.current = false;
    setLog([]);
    const stop = streamJobProgress(
      jobId,
      (update) => {
        setJob((prev) => {
          if (update.message && update.message !== prev?.message) {
            setLog((l) => [...l.slice(-40), `${new Date().toLocaleTimeString()} — ${update.message}`]);
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
          onComplete?.(update);
        }
        if (!doneRef.current && update.status === "failed") {
          doneRef.current = true;
          onFailed?.(update);
        }
      },
      () => setJob((j) => (j ? { ...j, status: "failed", progress_pct: j.progress_pct ?? 0 } : null)),
    );
    return stop;
  }, [jobId, onComplete, onFailed]);

  useEffect(() => {
    if (logOpen) logRef.current?.scrollTo({ top: logRef.current.scrollHeight, behavior: "smooth" });
  }, [log, logOpen]);

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

  if (!job) {
    return (
      <div className="df2-theater-v3 df2-theater-v3-loading">
        <Spinner size="md" label="Connecting" />
        <p>Connecting to live job stream…</p>
      </div>
    );
  }

  return (
    <div className={`df2-theater-v3 ${isRunning ? "is-live" : ""} ${isFailed ? "is-failed" : ""} ${isComplete ? "is-done" : ""}`}>
      <header className="df2-theater-v3-header">
        <div className="df2-theater-v3-route">
          <div className="df2-theater-v3-endpoint">
            <ConnectorIcon id={sourceType} size={22} />
            <div>
              <span>Source</span>
              <strong title={sourceLabel}>{sourceLabel || "Source"}</strong>
            </div>
          </div>
          <div className="df2-theater-v3-arrow" aria-hidden>
            <DtIcon name="transfer" size={16} />
          </div>
          <div className="df2-theater-v3-endpoint">
            <ConnectorIcon id={destType} size={22} />
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
              <small>{total > 0 ? `${processed.toLocaleString()}/${total.toLocaleString()}` : processed.toLocaleString()}</small>
            </div>
          </div>
          <div className="df2-theater-v3-progress-copy">
            <h3>{isComplete ? "Transfer complete" : isFailed ? "Transfer failed" : job.phase || "Running"}</h3>
            <p>{job.message || (isRunning ? "Streaming rows to destination…" : "Job finished")}</p>
            {job.chunk_current != null && job.chunk_total != null && job.chunk_total > 0 && (
              <span className="df2-theater-v3-chunk">
                Batch {job.chunk_current} of {job.chunk_total}
              </span>
            )}
          </div>
        </div>
        <div className="df2-theater-v3-bar" role="progressbar" aria-valuenow={progress} aria-valuemin={0} aria-valuemax={100}>
          <div className="df2-theater-v3-bar-fill" style={{ width: `${Math.min(progress, 100)}%` }} />
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
        {throughput > 0 && (
          <article className="df2-theater-v3-metric">
            <DtIcon name="activity" size={16} />
            <div>
              <strong>{throughput.toLocaleString()}/s</strong>
              <span>Throughput</span>
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
      </div>

      <div className="df2-theater-v3-phases" aria-label="Pipeline phases">
        {PHASES.map((phase, i) => {
          const state = isFailed && i === currentPhase ? "failed"
            : i < currentPhase || isComplete ? "done"
            : i === currentPhase ? "active"
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
          <DtIcon name="alert" size={18} />
          <div>
            <strong>Error</strong>
            <p>{job.error}</p>
          </div>
        </div>
      )}

      {isComplete && (
        <div className="df2-theater-v3-alert success">
          <DtIcon name="check" size={18} />
          <div>
            <strong>Success</strong>
            <p>{job.message || `${processed.toLocaleString()} rows transferred successfully`}</p>
          </div>
        </div>
      )}

      {job.rejected_rows != null && job.rejected_rows > 0 && (
        <div className="df2-theater-v3-alert warn">
          <DtIcon name="alert" size={18} />
          <div>
            <strong>{job.rejected_rows.toLocaleString()} rows rejected</strong>
            <p>Review rejected details in the event log.</p>
          </div>
        </div>
      )}

      {log.length > 0 && (
        <div className="df2-theater-v3-log-section">
          <button type="button" className="df2-theater-v3-log-toggle" onClick={() => setLogOpen((o) => !o)}>
            <DtIcon name="activity" size={14} />
            {logOpen ? "Hide event log" : `Show event log (${log.length})`}
          </button>
          {logOpen && (
            <div className="df2-theater-v3-log" ref={logRef}>
              {log.map((line, i) => (
                <div key={i}>{line}</div>
              ))}
            </div>
          )}
        </div>
      )}
    </div>
  );
}
