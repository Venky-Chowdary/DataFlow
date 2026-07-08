import { useCallback, useEffect, useMemo, useState } from "react";
import { DtIcon } from "../components/DtIcon";
import { JobTheater } from "../components/JobTheater";
import { ButtonLoader, LoadingBlock } from "../components/LoadingState";
import { PageShell } from "../components/ui/PageShell";
import { StatCard } from "../components/ui/StatCard";
import { useToast } from "../components/Toast";
import { fetchJob, retryJob } from "../lib/api";
import { JobProgress, TransferJob } from "../lib/types";

interface JobsPageProps {
  jobs: TransferJob[];
  onRefresh?: () => void;
  onStartTransfer?: () => void;
}

type JobFilter = "all" | "running" | "completed" | "failed";

function statusBadge(status: string) {
  if (status === "completed") return "df2-badge-live";
  if (status === "failed") return "df2-badge-error";
  if (status === "running") return "df2-badge-run";
  return "df2-badge-muted";
}

export function JobsPage({ jobs, onRefresh, onStartTransfer }: JobsPageProps) {
  const { toast } = useToast();
  const [selectedId, setSelectedId] = useState<string | null>(null);
  const [liveJob, setLiveJob] = useState<JobProgress | null>(null);
  const [detailLoading, setDetailLoading] = useState(false);
  const [retrying, setRetrying] = useState(false);
  const [filter, setFilter] = useState<JobFilter>("all");

  const counts = useMemo(() => ({
    all: jobs.length,
    running: jobs.filter((j) => j.status === "running" || j.status === "pending").length,
    completed: jobs.filter((j) => j.status === "completed").length,
    failed: jobs.filter((j) => j.status === "failed").length,
  }), [jobs]);

  const filtered = useMemo(() => {
    if (filter === "all") return jobs;
    if (filter === "running") return jobs.filter((j) => j.status === "running" || j.status === "pending");
    return jobs.filter((j) => j.status === filter);
  }, [jobs, filter]);

  useEffect(() => {
    if (filter === "failed" && counts.failed > 0 && !selectedId) {
      const first = jobs.find((j) => j.status === "failed");
      if (first) setSelectedId(first._id);
    } else if (filtered.length > 0 && !filtered.some((j) => j._id === selectedId)) {
      setSelectedId(filtered[0]._id);
    }
  }, [filter, filtered, jobs, counts.failed, selectedId]);

  useEffect(() => {
    if (!selectedId) {
      setLiveJob(null);
      setDetailLoading(false);
      return;
    }
    setDetailLoading(true);
    fetchJob(selectedId)
      .then(setLiveJob)
      .catch(() => setLiveJob(null))
      .finally(() => setDetailLoading(false));
  }, [selectedId]);

  const selected = jobs.find((j) => j._id === selectedId);
  const isLive = selected?.status === "running" || selected?.status === "pending";

  const handleComplete = useCallback(() => {
    onRefresh?.();
  }, [onRefresh]);

  const handleRetry = useCallback(async () => {
    if (!selectedId) return;
    setRetrying(true);
    try {
      const result = await retryJob(selectedId);
      toast({ title: "Retry started", message: "Watching new job in Job Theater.", tone: "success" });
      setSelectedId(result.job_id);
      onRefresh?.();
    } catch (e) {
      const msg = e instanceof Error ? e.message : "Retry failed";
      if (msg.includes("File uploads") || msg.includes("Transfer Studio")) {
        toast({ title: "Re-upload required", message: msg, tone: "warning" });
        onStartTransfer?.();
      } else {
        toast({ title: "Retry failed", message: msg, tone: "error" });
      }
    } finally {
      setRetrying(false);
    }
  }, [selectedId, onRefresh, onStartTransfer, toast]);

  const totalRows = jobs.reduce((s, j) => s + (j.records_processed || 0), 0);
  const selectedProgress = liveJob?.progress_pct ?? selected?.progress_pct ?? (selected?.status === "completed" ? 100 : 0);
  const selectedRoute = selected
    ? `${selected.source_name} → ${selected.destination_collection || selected.destination_database}`
    : "No job selected";
  const latestFailed = jobs.find((j) => j.status === "failed");

  return (
    <PageShell
      wide
      title="Job Theater"
      description="Live migration progress, batch throughput, and reconciliation."
      actions={
        <>
          {onRefresh && (
            <button type="button" className="df2-btn" onClick={onRefresh}>
              <DtIcon name="activity" size={16} /> Refresh
            </button>
          )}
          {onStartTransfer && (
            <button type="button" className="df2-btn df2-btn-primary" onClick={onStartTransfer}>
              <DtIcon name="plus" size={16} /> New transfer
            </button>
          )}
        </>
      }
    >
      <section className="df2-jobs-command" aria-label="Job theater command center">
        <div className="df2-jobs-command-main">
          <span className="df2-rail-kicker">Runtime command center</span>
          <h2>{counts.running ? `${counts.running} live migration${counts.running > 1 ? "s" : ""}` : counts.failed ? "Failure triage ready" : "Job theater is standing by"}</h2>
          <p>
            {counts.running
              ? "Watch row movement, batch checkpoints, and reconciliation without leaving this screen."
              : counts.failed
                ? "Open failed jobs, inspect the error, and retry from the same operational view."
                : "Start a governed transfer and the live stream appears here automatically."}
          </p>
          <div className="df2-jobs-command-actions">
            {onStartTransfer && (
              <button type="button" className="df2-btn df2-btn-primary" onClick={onStartTransfer}>
                <DtIcon name="plus" size={16} /> New transfer
              </button>
            )}
            {latestFailed && (
              <button type="button" className="df2-btn" onClick={() => { setFilter("failed"); setSelectedId(latestFailed._id); }}>
                <DtIcon name="alert" size={16} /> Triage failed
              </button>
            )}
          </div>
        </div>
        <div className="df2-jobs-now">
          <span>Selected run</span>
          <strong>{selectedRoute}</strong>
          <div className="df2-jobs-now-bar">
            <i style={{ width: `${Math.max(0, Math.min(100, selectedProgress))}%` }} />
          </div>
          <small>{selected ? `${selected.status} · ${(selected.records_processed ?? 0).toLocaleString()} rows` : "Choose a job to inspect phases and proof."}</small>
        </div>
      </section>

      <div className="df2-stats">
        <StatCard label="Total jobs" value={counts.all} />
        <StatCard label="Running" value={counts.running} tone={counts.running > 0 ? "blue" : undefined} />
        <StatCard label="Completed" value={counts.completed} tone="green" />
        <StatCard label="Failed" value={counts.failed} tone={counts.failed > 0 ? "red" : undefined} />
        <StatCard label="Rows moved" value={totalRows.toLocaleString()} tone="blue" />
      </div>

      {counts.failed > 0 && (
        <div className="df2-alert df2-alert-error" role="alert">
          <DtIcon name="alert" size={18} />
          <div>
            <strong>{counts.failed} failed migration{counts.failed > 1 ? "s" : ""}</strong>
            <p>Review errors below — common causes: SSL mismatch, reconciliation on re-run, or missing credentials.</p>
          </div>
          <button type="button" className="df2-btn df2-btn-sm" onClick={() => setFilter("failed")}>
            View failed
          </button>
        </div>
      )}

      {jobs.length === 0 ? (
        <div className="df2-empty">
          <DtIcon name="jobs" size={32} />
          <h3 className="df2-empty-title">No transfer jobs yet</h3>
          <p className="df2-empty-desc">Run a transfer from Transfer Studio — live batch progress appears here.</p>
          {onStartTransfer && (
            <button type="button" className="df2-btn df2-btn-primary" onClick={onStartTransfer}>
              Open Transfer Studio
            </button>
          )}
        </div>
      ) : (
        <>
          <div className="df2-tabs" role="tablist">
            {(["all", "running", "completed", "failed"] as const).map((f) => (
              <button
                key={f}
                type="button"
                role="tab"
                className={`df2-tab ${filter === f ? "active" : ""}`}
                onClick={() => setFilter(f)}
              >
                {f === "all" ? "All" : f.charAt(0).toUpperCase() + f.slice(1)} ({counts[f]})
              </button>
            ))}
          </div>

          <div className="df2-jobs-layout">
            <div className="df2-jobs-list" role="list">
              {filtered.length === 0 ? (
                <p style={{ padding: 16, color: "#64748b", fontSize: 13 }}>No {filter} jobs.</p>
              ) : (
                filtered.map((job) => (
                  <button
                    key={job._id}
                    type="button"
                    role="listitem"
                    className={`df2-job-item ${selectedId === job._id ? "active" : ""} ${job.status === "failed" ? "failed" : ""}`}
                    onClick={() => setSelectedId(job._id)}
                  >
                    <div style={{ display: "flex", justifyContent: "space-between", alignItems: "flex-start", gap: 8 }}>
                      <div className="df2-job-route">
                        {job.source_name}
                        <span className="df2-job-arrow"> → </span>
                        {job.destination_collection || job.destination_database}
                      </div>
                      <span className={`df2-badge ${statusBadge(job.status)}`}>{job.status}</span>
                    </div>
                    <div className="df2-job-meta">
                      <span>{job.source_type} → {job.destination_type}</span>
                      <span>{(job.records_processed ?? 0).toLocaleString()} rows</span>
                      <span>{new Date(job.created_at).toLocaleString()}</span>
                    </div>
                    {(job.status === "running" || job.status === "pending") && job.progress_pct != null && (
                      <div className="df2-job-mini-bar">
                        <div className="df2-job-mini-fill" style={{ width: `${job.progress_pct}%` }} />
                      </div>
                    )}
                    {job.status === "failed" && job.error && (
                      <p className="df2-job-error-preview">{job.error.slice(0, 80)}{job.error.length > 80 ? "…" : ""}</p>
                    )}
                  </button>
                ))
              )}
            </div>

            <div className="df2-job-detail">
              {detailLoading ? (
                <LoadingBlock title="Loading job details" size="md" />
              ) : isLive && selected ? (
                <JobTheater
                  jobId={selectedId!}
                  sourceLabel={selected.source_name}
                  destLabel={`${selected.destination_database}.${selected.destination_collection}`}
                  sourceType={selected.source_type}
                  destType={selected.destination_type}
                  onComplete={handleComplete}
                  onFailed={handleComplete}
                />
              ) : liveJob && selected ? (
                <div className={`df2-card ${selected.status === "failed" ? "df2-card-error" : ""}`}>
                  <div className="df2-card-head">
                    <h2 className="df2-card-title">
                      {selected.status === "failed" ? "Failed migration" : "Transfer summary"}
                    </h2>
                    <span className={`df2-badge ${statusBadge(liveJob.status)}`}>{liveJob.status}</span>
                  </div>
                  <div className="df2-card-body">
                    <div className="df2-job-stats">
                      <div className="df2-job-stat">
                        <span className="df2-job-stat-val">{(liveJob.records_processed ?? 0).toLocaleString()}</span>
                        <span className="df2-job-stat-lbl">Rows processed</span>
                      </div>
                      <div className="df2-job-stat">
                        <span className="df2-job-stat-val">{liveJob.progress_pct ?? 100}%</span>
                        <span className="df2-job-stat-lbl">Progress</span>
                      </div>
                      <div className="df2-job-stat">
                        <span className="df2-job-stat-val">{liveJob.operation || "transfer"}</span>
                        <span className="df2-job-stat-lbl">Operation</span>
                      </div>
                    </div>
                    <dl className="df2-dl">
                      <div><dt>Source</dt><dd>{liveJob.source_name || selected.source_name}</dd></div>
                      <div><dt>Destination</dt><dd>{liveJob.destination_database}.{liveJob.destination_collection}</dd></div>
                      {liveJob.phase && <div><dt>Last phase</dt><dd>{liveJob.phase}</dd></div>}
                      {liveJob.message && <div><dt>Message</dt><dd>{liveJob.message}</dd></div>}
                    </dl>
                    {liveJob.error && (
                      <div className="df2-theater-error" style={{ marginTop: 16 }}>
                        <DtIcon name="alert" size={16} />
                        <div>
                          <strong>Error</strong>
                          <p>{liveJob.error}</p>
                        </div>
                      </div>
                    )}
                    {selected.status === "failed" && (
                      <div className="df2-segment" style={{ marginTop: 16 }}>
                        <button
                          type="button"
                          className="df2-btn df2-btn-primary"
                          onClick={() => void handleRetry()}
                          disabled={retrying}
                        >
                          {retrying ? <ButtonLoader label="Retrying…" /> : <><DtIcon name="transfer" size={16} /> Retry migration</>}
                        </button>
                        {onStartTransfer && (
                          <button type="button" className="df2-btn" onClick={onStartTransfer}>
                            Open Transfer Studio
                          </button>
                        )}
                      </div>
                    )}
                  </div>
                </div>
              ) : (
                <div className="df2-empty">
                  <h3 className="df2-empty-title">Select a job</h3>
                  <p className="df2-empty-desc">Choose a transfer from the list to view live progress or failure details.</p>
                </div>
              )}
            </div>
          </div>
        </>
      )}
    </PageShell>
  );
}
