import { useCallback, useEffect, useMemo, useState } from "react";
import { ConnectorIcon } from "../app/brand-icons";
import { DtIcon } from "../components/DtIcon";
import { JobTheater } from "../components/JobTheater";
import { ButtonLoader, LoadingBlock } from "../components/LoadingState";
import { EmptyState } from "../components/EmptyState";
import { PageFrame } from "../components/ui/PageFrame";
import { PageShell } from "../components/ui/PageShell";
import { FilterTabs } from "../components/ui/FilterTabs";
import { PageToolbar } from "../components/ui/PageToolbar";
import { useToast } from "../components/Toast";
import { fetchJob, retryJob } from "../lib/api";
import { jobStatusBadgeClass } from "../lib/uiUtils";
import { JobProgress, TransferJob } from "../lib/types";

interface JobDetailRecord extends JobProgress {
  transfer_request?: {
    mappings?: { source?: string; target?: string; source_column?: string; target_column?: string; confidence?: number }[];
    column_types?: Record<string, string>;
    sync_mode?: string;
    validation_mode?: string;
  };
  phases?: { name: string; status: string; message?: string }[];
  ddl_log?: string[];
  started_at?: string;
  completed_at?: string;
}

interface JobsPageProps {
  jobs: TransferJob[];
  onRefresh?: () => void;
  onStartTransfer?: () => void;
  initialJobId?: string;
}

type JobFilter = "all" | "running" | "completed" | "failed";

function statusIcon(status: string) {
  if (status === "completed") return "check";
  if (status === "failed") return "x";
  if (status === "running" || status === "pending") return "activity";
  return "jobs";
}

export function JobsPage({ jobs, onRefresh, onStartTransfer, initialJobId }: JobsPageProps) {
  const { toast } = useToast();
  const [selectedId, setSelectedId] = useState<string | null>(null);
  const [liveJob, setLiveJob] = useState<JobDetailRecord | null>(null);
  const [detailLoading, setDetailLoading] = useState(false);
  const [retrying, setRetrying] = useState(false);
  const [filter, setFilter] = useState<JobFilter>("all");
  const [jobSearch, setJobSearch] = useState("");

  const counts = useMemo(() => ({
    all: jobs.length,
    running: jobs.filter((j) => j.status === "running" || j.status === "pending").length,
    completed: jobs.filter((j) => j.status === "completed").length,
    failed: jobs.filter((j) => j.status === "failed").length,
  }), [jobs]);

  const filtered = useMemo(() => {
    let list = jobs;
    if (filter === "running") list = jobs.filter((j) => j.status === "running" || j.status === "pending");
    else if (filter !== "all") list = jobs.filter((j) => j.status === filter);

    const q = jobSearch.trim().toLowerCase();
    if (!q) return list;
    return list.filter((j) =>
      [j.source_name, j.source_type, j.destination_type, j.destination_database, j.destination_collection, j._id, j.status, j.error]
        .filter(Boolean)
        .some((v) => String(v).toLowerCase().includes(q)),
    );
  }, [jobs, filter, jobSearch]);

  useEffect(() => {
    if (!initialJobId) return;
    if (!jobs.some((j) => j._id === initialJobId)) return;
    setFilter("all");
    setJobSearch("");
    setSelectedId(initialJobId);
    window.requestAnimationFrame(() => {
      document.getElementById(`job-item-${initialJobId}`)?.scrollIntoView({
        behavior: "smooth",
        block: "nearest",
      });
    });
  }, [initialJobId, jobs]);

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

  useEffect(() => {
    if (!counts.running || !onRefresh) return;
    const timer = window.setInterval(onRefresh, 5000);
    return () => window.clearInterval(timer);
  }, [counts.running, onRefresh]);

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
  const latestFailed = jobs.find((j) => j.status === "failed");
  const jobMappings = liveJob?.transfer_request?.mappings ?? [];
  const columnTypes = liveJob?.transfer_request?.column_types ?? {};
  const jobPhases = liveJob?.phases?.length
    ? liveJob.phases
    : selected
      ? [{ name: selected.phase || "Transfer", status: selected.status === "failed" ? "failed" : selected.status === "completed" ? "done" : "active" }]
      : [];
  const ddlLog = liveJob?.ddl_log ?? [];

  return (
    <PageShell
      wide
      className="df2-page-jobs"
      kicker="Runtime"
      title="Job Theater"
      description="Live migration progress, throughput, and reconciliation."
      actions={
        <>
          {onRefresh && (
            <button type="button" className="df2-btn" onClick={onRefresh}>
              <DtIcon name="activity" size={16} /> Refresh
            </button>
          )}
        </>
      }
    >
      <PageFrame className="df2-jobs-workspace df2-jobs-workspace-v3" showHonesty>
        <header className="df2-jobs-v3-header">
          <div className="df2-jobs-v3-stats">
            <div className="df2-jobs-v3-stat">
              <span>Total</span>
              <strong>{counts.all}</strong>
            </div>
            <div className={`df2-jobs-v3-stat ${counts.running ? "live" : ""}`}>
              <span>Running</span>
              <strong>{counts.running}</strong>
            </div>
            <div className="df2-jobs-v3-stat ok">
              <span>Completed</span>
              <strong>{counts.completed}</strong>
            </div>
            <div className={`df2-jobs-v3-stat ${counts.failed ? "warn" : ""}`}>
              <span>Failed</span>
              <strong>{counts.failed}</strong>
            </div>
            <div className="df2-jobs-v3-stat">
              <span>Rows moved</span>
              <strong>{totalRows.toLocaleString()}</strong>
            </div>
          </div>
          {latestFailed && (
            <button
              type="button"
              className="df2-btn df2-btn-sm"
              onClick={() => { setFilter("failed"); setSelectedId(latestFailed._id); }}
            >
              <DtIcon name="alert" size={14} /> Triage failed
            </button>
          )}
        </header>

        {jobs.length === 0 ? (
          <EmptyState
            icon="jobs"
            title="No transfer jobs yet"
            description="Run a transfer from Transfer Studio — live batch progress appears here."
            action={
              onStartTransfer ? (
                <button type="button" className="df2-btn df2-btn-primary" onClick={onStartTransfer}>
                  Open Transfer Studio
                </button>
              ) : undefined
            }
          />
        ) : (
          <>
            <div className="df2-jobs-v3-toolbar">
              <FilterTabs
                ariaLabel="Filter jobs by status"
                value={filter}
                onChange={setFilter}
                items={([
                  { id: "all", label: "All", count: counts.all },
                  { id: "running", label: "Running", count: counts.running },
                  { id: "completed", label: "Completed", count: counts.completed },
                  { id: "failed", label: "Failed", count: counts.failed },
                ] as const)}
              />
              <PageToolbar
                searchValue={jobSearch}
                onSearchChange={setJobSearch}
                searchPlaceholder="Search route, type, status, job id…"
              />
            </div>

            <div className="df2-jobs-v3-layout">
              <aside className="df2-jobs-v3-list" role="list" aria-label="Transfer jobs">
                {filtered.length === 0 ? (
                  <EmptyState
                    compact
                    icon="search"
                    title={jobSearch ? "No matches" : "No jobs in this view"}
                    description={
                      jobSearch
                        ? `No jobs match "${jobSearch}".`
                        : `No ${filter === "all" ? "" : filter} jobs in this filter.`
                    }
                  />
                ) : (
                  filtered.map((job) => (
                    <button
                      key={job._id}
                      id={`job-item-${job._id}`}
                      type="button"
                      role="listitem"
                      className={`df2-jobs-v3-item ${selectedId === job._id ? "active" : ""} ${job.status === "failed" ? "failed" : ""} ${job.status === "running" || job.status === "pending" ? "live" : ""}`}
                      onClick={() => setSelectedId(job._id)}
                    >
                      <span className={`df2-jobs-v3-item-status df2-jobs-v3-item-status--${job.status}`} aria-hidden>
                        <DtIcon name={statusIcon(job.status)} size={14} />
                      </span>
                      <div className="df2-jobs-v3-item-body">
                        <div className="df2-jobs-v3-item-top">
                          <div className="df2-jobs-v3-item-route">
                            <ConnectorIcon id={job.source_type} size={16} />
                            <span className="df2-jobs-v3-item-source">{job.source_name}</span>
                            <DtIcon name="transfer" size={12} />
                            <ConnectorIcon id={job.destination_type} size={16} />
                            <span className="df2-jobs-v3-item-dest">
                              {job.destination_collection || job.destination_database}
                            </span>
                          </div>
                          <span className={jobStatusBadgeClass(job.status)}>{job.status}</span>
                        </div>
                        <div className="df2-jobs-v3-item-meta">
                          <span>{(job.records_processed ?? 0).toLocaleString()} rows</span>
                          <span>{new Date(job.created_at).toLocaleString()}</span>
                          <span className="df2-jobs-v3-item-id">#{job._id.slice(0, 8)}</span>
                        </div>
                        {(job.status === "running" || job.status === "pending") && job.progress_pct != null && (
                          <div className="df2-jobs-v3-item-bar">
                            <div className="df2-jobs-v3-item-bar-fill" style={{ width: `${job.progress_pct}%` }} />
                          </div>
                        )}
                        {job.status === "failed" && job.error && (
                          <p className="df2-jobs-v3-item-error">{job.error.slice(0, 100)}{job.error.length > 100 ? "…" : ""}</p>
                        )}
                      </div>
                    </button>
                  ))
                )}
              </aside>

              <section className="df2-jobs-v3-detail" aria-label="Job detail">
                {detailLoading ? (
                  <LoadingBlock title="Loading job details" size="md" variant="glass" />
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
                  <div className={`df2-jobs-v3-summary ${selected.status === "failed" ? "is-failed" : "is-done"}`}>
                    <header className="df2-jobs-v3-summary-head">
                      <div className="df2-jobs-v3-summary-route">
                        <ConnectorIcon id={selected.source_type} size={22} />
                        <div>
                          <span>Source</span>
                          <strong>{liveJob.source_name || selected.source_name}</strong>
                        </div>
                        <DtIcon name="transfer" size={14} />
                        <ConnectorIcon id={selected.destination_type} size={22} />
                        <div>
                          <span>Destination</span>
                          <strong>{liveJob.destination_database}.{liveJob.destination_collection}</strong>
                        </div>
                      </div>
                      <span className={jobStatusBadgeClass(liveJob.status)}>{liveJob.status}</span>
                    </header>

                    <div className="df2-jobs-v3-summary-metrics">
                      <article><strong>{(liveJob.records_processed ?? 0).toLocaleString()}</strong><span>Rows processed</span></article>
                      <article><strong>{liveJob.progress_pct ?? 100}%</strong><span>Progress</span></article>
                      <article><strong>{jobMappings.length || Object.keys(columnTypes).length || "—"}</strong><span>Columns</span></article>
                      <article><strong>{liveJob.operation || liveJob.transfer_request?.sync_mode || "transfer"}</strong><span>Mode</span></article>
                    </div>

                    <div className="df2-jobs-v3-summary-phases">
                      {jobPhases.map((phase) => (
                        <span
                          key={phase.name}
                          className={`df2-jobs-v3-phase-pill ${
                            phase.status === "done" || phase.status === "completed" ? "done"
                              : phase.status === "failed" ? "failed"
                              : "active"
                          }`}
                        >
                          {phase.name}
                        </span>
                      ))}
                    </div>

                    <dl className="df2-jobs-v3-summary-dl">
                      {liveJob.phase && <div><dt>Phase</dt><dd>{liveJob.phase}</dd></div>}
                      {liveJob.message && <div><dt>Message</dt><dd>{liveJob.message}</dd></div>}
                      {liveJob.started_at && <div><dt>Started</dt><dd>{new Date(liveJob.started_at).toLocaleString()}</dd></div>}
                      {liveJob.completed_at && <div><dt>Completed</dt><dd>{new Date(liveJob.completed_at).toLocaleString()}</dd></div>}
                    </dl>

                    {(jobMappings.length > 0 || Object.keys(columnTypes).length > 0) && (
                      <div className="df2-jobs-v3-mappings">
                        <h3>Column mapping</h3>
                        <div className="df2-jobs-v3-mappings-scroll">
                          <table>
                            <thead>
                              <tr>
                                <th>Source</th>
                                <th>Target</th>
                                <th>Type</th>
                              </tr>
                            </thead>
                            <tbody>
                              {jobMappings.length > 0 ? jobMappings.slice(0, 20).map((m, i) => {
                                const src = m.source ?? m.source_column ?? "—";
                                const tgt = m.target ?? m.target_column ?? "—";
                                return (
                                  <tr key={`${src}-${tgt}-${i}`}>
                                    <td>{src}</td>
                                    <td>{tgt}</td>
                                    <td>{columnTypes[src] ?? columnTypes[tgt] ?? "—"}</td>
                                  </tr>
                                );
                              }) : Object.entries(columnTypes).slice(0, 20).map(([col, typ]) => (
                                <tr key={col}>
                                  <td>{col}</td>
                                  <td>{col}</td>
                                  <td>{typ}</td>
                                </tr>
                              ))}
                            </tbody>
                          </table>
                        </div>
                      </div>
                    )}

                    {ddlLog.length > 0 && (
                      <div className="df2-jobs-v3-log">
                        <h3>DDL log</h3>
                        <pre>{ddlLog.join("\n")}</pre>
                      </div>
                    )}

                    {liveJob.error && (
                      <div className="df2-jobs-v3-alert error">
                        <DtIcon name="alert" size={16} />
                        <p>{liveJob.error}</p>
                      </div>
                    )}

                    {selected.status === "failed" && (
                      <div className="df2-jobs-v3-actions">
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
                ) : (
                  <EmptyState
                    compact
                    icon="jobs"
                    title="Select a job"
                    description="Choose a transfer from the list to view live progress or the final summary."
                  />
                )}
              </section>
            </div>
          </>
        )}
      </PageFrame>
    </PageShell>
  );
}
