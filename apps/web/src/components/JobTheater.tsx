import { useEffect, useMemo, useRef, useState } from "react";
import { ConnectorIcon } from "../app/brand-icons";
import { DtIcon } from "./DtIcon";
import { Spinner } from "./LoadingState";
import { JobProgress } from "../lib/types";
import { streamJobProgress } from "../lib/api";

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
  { id: "queued", label: "Queued", icon: "clock" as const },
  { id: "reading", label: "Reading source", icon: "download" as const },
  { id: "preflight", label: "Preflight gates", icon: "shield" as const },
  { id: "writing", label: "Writing batches", icon: "upload" as const },
  { id: "reconcile", label: "Reconciliation", icon: "check" as const },
  { id: "completed", label: "Complete", icon: "check" as const },
];

function phaseIndex(phase?: string, status?: string): number {
  if (status === "completed") return 5;
  if (status === "failed") return -1;
  const idx = PHASES.findIndex((p) => p.id === (phase || "queued"));
  return idx >= 0 ? idx : 0;
}

function parseChunkMessage(msg?: string): { current: number; total: number } | null {
  if (!msg) return null;
  const m = msg.match(/batch\s+(\d+)\/(\d+)/i);
  if (m) return { current: parseInt(m[1], 10), total: parseInt(m[2], 10) };
  return null;
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
  const [eta, setEta] = useState<string | null>(null);
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
            setLog((l) => [...l.slice(-24), `${new Date().toLocaleTimeString()} — ${update.message}`]);
          }
          return update;
        });
        const elapsed = (Date.now() - startRef.current) / 1000;
        const processed = update.records_processed ?? 0;
        const total = update.total_rows ?? 0;
        if (elapsed > 0.5 && processed > 0) {
          const rps = Math.round(processed / elapsed);
          setThroughput(rps);
          if (total > processed && rps > 0) {
            const secs = Math.ceil((total - processed) / rps);
            setEta(secs < 60 ? `${secs}s` : secs < 3600 ? `${Math.ceil(secs / 60)}m` : `${Math.ceil(secs / 3600)}h`);
          } else {
            setEta(null);
          }
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
    logRef.current?.scrollTo({ top: logRef.current.scrollHeight, behavior: "smooth" });
  }, [log]);

  const progress = job?.progress_pct ?? (job?.status === "completed" ? 100 : 5);
  const total = job?.total_rows ?? 0;
  const processed = job?.records_processed ?? 0;
  const currentPhase = phaseIndex(job?.phase, job?.status);
  const isFailed = job?.status === "failed";
  const isComplete = job?.status === "completed";
  const isRunning = !isFailed && !isComplete;

  const chunkInfo = useMemo(() => {
    const fromMsg = parseChunkMessage(job?.message);
    if (fromMsg) return fromMsg;
    if (job?.chunk_current && job?.chunk_total) {
      return { current: job.chunk_current, total: job.chunk_total };
    }
    return null;
  }, [job?.message, job?.chunk_current, job?.chunk_total]);

  if (!job) {
    return (
      <div className="df2-theater df2-theater-loading">
        <Spinner size="md" label="Connecting to live stream" />
        <p>Connecting to migration stream…</p>
      </div>
    );
  }

  return (
    <div className={`df2-theater ${isRunning ? "df2-theater-live" : ""} ${isFailed ? "df2-theater-failed" : ""}`}>
      <div className="df2-theater-head">
        <div>
          <h3 className="df2-theater-title">
            {isComplete ? "Migration complete" : isFailed ? "Migration failed" : "Live migration"}
          </h3>
          <p className="df2-theater-sub">
            {sourceLabel && destLabel ? `${sourceLabel} → ${destLabel}` : `Job ${jobId.slice(-8)}`}
          </p>
        </div>
        <div className="df2-theater-metrics">
          {!isFailed && (
            <>
              <div className="df2-theater-metric">
                <span className="df2-theater-metric-val">{processed.toLocaleString()}</span>
                <span className="df2-theater-metric-lbl">Rows moved</span>
              </div>
              {throughput > 0 && (
                <div className="df2-theater-metric">
                  <span className="df2-theater-metric-val">{throughput.toLocaleString()}</span>
                  <span className="df2-theater-metric-lbl">Rows/sec</span>
                </div>
              )}
              {eta && isRunning && (
                <div className="df2-theater-metric">
                  <span className="df2-theater-metric-val">{eta}</span>
                  <span className="df2-theater-metric-lbl">ETA</span>
                </div>
              )}
            </>
          )}
          <div className="df2-theater-metric">
            <span className="df2-theater-metric-val">{progress}%</span>
            <span className="df2-theater-metric-lbl">Progress</span>
          </div>
        </div>
      </div>

      {/* Animated pipeline: source → chunks → destination */}
      <div className="df2-migration-pipeline" aria-hidden={isComplete || isFailed}>
        <div className="df2-pipeline-node df2-pipeline-source">
          <ConnectorIcon id={sourceType} size={28} />
          <span>{sourceLabel?.split(" ")[0] || "Source"}</span>
        </div>
        <div className="df2-pipeline-track">
          <div className="df2-pipeline-track-fill" style={{ width: `${Math.min(progress, 100)}%` }} />
          {isRunning && (
            <>
              <span className="df2-pipeline-packet" style={{ animationDelay: "0s" }} />
              <span className="df2-pipeline-packet" style={{ animationDelay: "0.6s" }} />
              <span className="df2-pipeline-packet" style={{ animationDelay: "1.2s" }} />
            </>
          )}
        </div>
        <div className="df2-pipeline-node df2-pipeline-dest">
          <ConnectorIcon id={destType} size={28} />
          <span>{destLabel?.split(".")[0] || "Target"}</span>
        </div>
      </div>

      <div className="df2-theater-progress">
        <div className="df2-theater-progress-fill" style={{ width: `${Math.min(progress, 100)}%` }} />
      </div>
      {total > 0 && (
        <p className="df2-theater-rows">
          {processed.toLocaleString()} / {total.toLocaleString()} rows
          {chunkInfo && ` · batch ${chunkInfo.current}/${chunkInfo.total}`}
        </p>
      )}

      {/* Chunk batch grid */}
      {chunkInfo && chunkInfo.total > 1 && (
        <div className="df2-chunk-grid" aria-label="Batch progress">
          {Array.from({ length: Math.min(chunkInfo.total, 48) }, (_, i) => {
            const n = i + 1;
            const state = n < chunkInfo.current ? "done" : n === chunkInfo.current && isRunning ? "active" : "pending";
            return <span key={n} className={`df2-chunk-cell ${state}`} title={`Batch ${n}`} />;
          })}
          {chunkInfo.total > 48 && (
            <span className="df2-chunk-more">+{chunkInfo.total - 48} batches</span>
          )}
        </div>
      )}

      <div className="df2-theater-phases">
        {PHASES.map((phase, i) => {
          const state = isFailed && i === currentPhase ? "failed"
            : i < currentPhase || isComplete ? "done"
            : i === currentPhase ? "active"
            : "pending";
          return (
            <div key={phase.id} className={`df2-theater-phase ${state}`}>
              <span className="df2-theater-phase-dot">
                {state === "done" && <DtIcon name="check" size={10} />}
                {state === "failed" && <DtIcon name="x" size={10} />}
                {state === "active" && <span className="df2-phase-pulse" />}
              </span>
              {phase.label}
            </div>
          );
        })}
      </div>

      {/* Live event log */}
      {log.length > 0 && (
        <div className="df2-theater-log" ref={logRef} aria-live="polite">
          {log.map((line, i) => (
            <div key={i} className="df2-theater-log-line">{line}</div>
          ))}
        </div>
      )}

      {isFailed && job?.error && (
        <div className="df2-theater-error">
          <DtIcon name="alert" size={16} />
          <div>
            <strong>Transfer failed</strong>
            <p>{job.error}</p>
          </div>
        </div>
      )}

      {isComplete && job?.message && (
        <div className="df2-theater-success">
          <DtIcon name="check" size={16} />
          <span>{job.message}</span>
        </div>
      )}
    </div>
  );
}
