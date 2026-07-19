import { useCallback, useEffect, useMemo, useState } from "react";
import { ConnectorIcon } from "../app/brand-icons";
import { DtIcon } from "../components/DtIcon";
import { JobTheater } from "../components/JobTheater";
import { ButtonLoader, LoadingBlock } from "../components/LoadingState";
import { EmptyState } from "../components/ui/EmptyState";
import { Button } from "../components/ui/Button";
import { CopyIdChip } from "../components/ui/CopyIdChip";
import { PageFrame } from "../components/ui/PageFrame";
import { PageShell } from "../components/ui/PageShell";
import { PageContextBar } from "../components/ui/PageContextBar";
import { FilterBar } from "../components/ui/FilterBar";
import { FilterTabs } from "../components/ui/FilterTabs";
import { PageToolbar } from "../components/ui/PageToolbar";
import { useToast } from "../components/Toast";
import { useActiveData } from "../lib/DataContext";
import { cancelJob, fetchJob, retryJob, resumeJob } from "../lib/api";
import { isJobSuccess, jobStatusBadgeClass, jobStatusLabel } from "../lib/uiUtils";
import { JobProgress, TransferJob } from "../lib/types";
import { QuarantinePanel } from "../components/transfer/QuarantinePanel";
import { buildJobTimeline, JobTimeline } from "../components/ui/JobTimeline";

interface JobDetailRecord extends JobProgress {
  transfer_request?: {
    mappings?: {
      source?: string;
      target?: string;
      source_column?: string;
      target_column?: string;
      source_type?: string;
      target_type?: string;
      confidence?: number;
    }[];
    column_types?: Record<string, string>;
    sync_mode?: string;
    validation_mode?: string;
  };
  phases?: { name: string; status: "pending" | "active" | "done" | "failed" | "skipped"; message?: string }[];
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
type JobDetailTab = "detail" | "mapping" | "quarantine" | "log";

function statusIcon(status: string) {
  if (status === "completed") return "check";
  if (status === "completed_with_quarantine") return "alert";
  if (status === "failed" || status === "cancelled") return "x";
  if (status === "running" || status === "pending") return "activity";
  return "jobs";
}

function defaultDetailTab(status: string, rejectedRows: number): JobDetailTab {
  if (status === "completed_with_quarantine" || rejectedRows > 0) return "quarantine";
  if (status === "failed") return "detail";
  return "detail";
}

export function JobsPage({ jobs, onRefresh, onStartTransfer, initialJobId }: JobsPageProps) {
  const { toast } = useToast();
  const { setActiveData } = useActiveData();
  const [selectedId, setSelectedId] = useState<string | null>(null);
  const [liveJob, setLiveJob] = useState<JobDetailRecord | null>(null);
  const [detailLoading, setDetailLoading] = useState(false);
  const [retrying, setRetrying] = useState(false);
  const [resuming, setResuming] = useState(false);
  const [cancelling, setCancelling] = useState(false);
  const [filter, setFilter] = useState<JobFilter>("all");
  const [jobSearch, setJobSearch] = useState("");
  const [detailTab, setDetailTab] = useState<JobDetailTab>("detail");

  const counts = useMemo(() => ({
    all: jobs.length,
    running: jobs.filter((j) => j.status === "running" || j.status === "pending").length,
    completed: jobs.filter((j) => isJobSuccess(j.status)).length,
    quarantine: jobs.filter((j) => j.status === "completed_with_quarantine").length,
    failed: jobs.filter((j) => j.status === "failed").length,
  }), [jobs]);

  const rowsMoved = useMemo(
    () => jobs.reduce((sum, j) => sum + (j.records_processed || 0), 0),
    [jobs],
  );
  const successRate = counts.all
    ? Math.round((counts.completed / counts.all) * 100)
    : null;

  const filtered = useMemo(() => {
    let list = jobs;
    if (filter === "running") list = jobs.filter((j) => j.status === "running" || j.status === "pending");
    else if (filter === "completed") list = jobs.filter((j) => isJobSuccess(j.status));
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

  // Feed the selected job into Data Pilot so NL triage uses the real job ID.
  useEffect(() => {
    const job = jobs.find((j) => j._id === selectedId);
    if (!job) return;
    const dest = job.destination_collection || job.destination_database || job.destination_type || "destination";
    setActiveData((prev) => ({
      name: prev?.name || job.source_name || "job",
      filename: prev?.filename,
      columns: prev?.columns || [],
      row_count: job.records_processed ?? prev?.row_count ?? 0,
      samples: prev?.samples,
      schema: prev?.schema,
      preflight_run_id: prev?.preflight_run_id,
      job_id: job._id,
      validation_status: job.status,
      route: `${job.source_name} → ${dest}`,
      blockers: job.error ? [job.error] : prev?.blockers,
    }));
  }, [jobs, selectedId, setActiveData]);

  useEffect(() => {
    if (!selectedId) {
      setLiveJob(null);
      setDetailLoading(false);
      return;
    }
    setDetailLoading(true);
    const listed = jobs.find((j) => j._id === selectedId);
    // Optimistic hydrate from list so the detail pane never goes blank on slow/404 get.
    if (listed) {
      setLiveJob({
        ...(listed as unknown as JobDetailRecord),
        _id: listed._id,
        status: listed.status,
        progress_pct: listed.progress_pct ?? 0,
        records_processed: listed.records_processed ?? 0,
        message: listed.message || listed.error || "",
      });
    }
    fetchJob(selectedId)
      .then(setLiveJob)
      .catch(() => {
        // Keep list snapshot if detail endpoint fails (workspace/isolation edge cases).
        if (!listed) setLiveJob(null);
      })
      .finally(() => setDetailLoading(false));
  }, [selectedId, jobs]);

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

  const handleResume = useCallback(async () => {
    if (!selectedId || !liveJob?.checkpoint) return;
    setResuming(true);
    try {
      await resumeJob(selectedId);
      toast({ title: "Resume started", message: `Resuming from batch ${liveJob.checkpoint.chunk_index ?? 0} (${(liveJob.checkpoint.rows_processed ?? 0).toLocaleString()} rows already committed).`, tone: "success" });
      onRefresh?.();
    } catch (e) {
      const msg = e instanceof Error ? e.message : "Resume failed";
      if (msg.includes("File uploads") || msg.includes("Transfer Studio")) {
        toast({ title: "Re-upload required", message: msg, tone: "warning" });
        onStartTransfer?.();
      } else {
        toast({ title: "Resume failed", message: msg, tone: "error" });
      }
    } finally {
      setResuming(false);
    }
  }, [selectedId, liveJob?.checkpoint, onRefresh, onStartTransfer, toast]);

  const handleCancel = useCallback(async () => {
    if (!selectedId) return;
    setCancelling(true);
    try {
      await cancelJob(selectedId);
      toast({ title: "Cancellation requested", message: "The job will stop at the next checkpoint.", tone: "info" });
      onRefresh?.();
    } catch (e) {
      toast({ title: "Could not cancel job", message: e instanceof Error ? e.message : "Cancel failed", tone: "error" });
    } finally {
      setCancelling(false);
    }
  }, [selectedId, onRefresh, toast]);

  const jobMappings = liveJob?.transfer_request?.mappings ?? [];
  const columnTypes = liveJob?.transfer_request?.column_types ?? {};
  const ddlLog = liveJob?.ddl_log ?? [];
  const mappingCount = jobMappings.length || Object.keys(columnTypes).length;
  const rejectedCount = liveJob?.rejected_rows ?? 0;
  const showQuarantineTab = Boolean(
    liveJob
    && (
      liveJob.status === "failed"
      || liveJob.status === "completed_with_quarantine"
      || rejectedCount > 0
    ),
  );

  // Reset tabs only when the operator picks a different job.
  useEffect(() => {
    if (!selectedId) {
      setDetailTab("detail");
      return;
    }
    const listed = jobs.find((j) => j._id === selectedId);
    setDetailTab(
      defaultDetailTab(listed?.status ?? "completed", listed?.status === "completed_with_quarantine" ? 1 : 0),
    );
    // jobs intentionally omitted — list polling must not yank the active tab
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [selectedId]);

  useEffect(() => {
    if (detailTab === "quarantine" && !showQuarantineTab) {
      setDetailTab("detail");
    }
  }, [detailTab, showQuarantineTab]);

  const timelineEntries = useMemo(
    () =>
      liveJob && selected
        ? buildJobTimeline({
            createdAt: selected.created_at,
            startedAt: liveJob.started_at,
            completedAt: liveJob.completed_at,
            status: selected.status,
            retryOf: selected.retry_of,
            phases: liveJob.phases,
            notifications: liveJob.notifications,
            rejectedRows: liveJob.rejected_rows,
            coercedNullRows: liveJob.coerced_null_rows,
          })
        : [],
    [liveJob, selected],
  );

  return (
    <PageShell
      fit
      className="df2-page-jobs"
      title="Jobs"
      kicker="Operations"
      description="Live progress, phases, quarantine, and Gate-8 proof for every transfer."
    >
      <PageFrame className="df2-jobs-workspace df2-jobs-workspace-v3">
        {jobs.length === 0 ? (
          <EmptyState
            page
            icon="jobs"
            title="No transfer jobs yet"
            description="Run a transfer from Transfer Studio — live batch progress, phases, and proof reports appear here."
            action={
              onStartTransfer ? (
                <button type="button" className="df2-btn df2-btn-primary" onClick={onStartTransfer}>
                  <DtIcon name="transfer" size={14} /> Open Transfer Studio
                </button>
              ) : undefined
            }
          />
        ) : (
          <>
            <PageContextBar
              ariaLabel="Jobs summary"
              stats={[
                { label: "Total jobs", value: counts.all, icon: "jobs" },
                { label: "Rows moved", value: rowsMoved.toLocaleString(), icon: "layers", tone: "muted", title: "Records processed across all transfer jobs" },
                {
                  label: "Success rate",
                  value: successRate != null ? `${successRate}%` : "—",
                  icon: "check",
                  tone: successRate != null && successRate >= 90 ? "ok" : successRate != null ? "warn" : "muted",
                  title: `${counts.completed} of ${counts.all} jobs completed`,
                },
                {
                  label: "Running",
                  value: counts.running,
                  icon: "activity",
                  tone: counts.running > 0 ? "warn" : "muted",
                },
                {
                  label: "Failed",
                  value: counts.failed,
                  icon: "alert",
                  tone: counts.failed > 0 ? "danger" : "muted",
                },
              ]}
            />
            <PageToolbar
              searchValue={jobSearch}
              onSearchChange={setJobSearch}
              searchPlaceholder="Search route, type, status, job id…"
              filters={
                <FilterBar variant="inline" ariaLabel="Filter jobs by status">
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
                </FilterBar>
              }
              actions={
                onRefresh ? (
                  <Button size="sm" onClick={onRefresh} leadingIcon={<DtIcon name="activity" size={14} />}>
                    Refresh
                  </Button>
                ) : undefined
              }
            />

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
                  filtered.map((job) => {
                    const destLabel = job.destination_collection || job.destination_database || "destination";
                    const isLive = job.status === "running" || job.status === "pending";
                    return (
                      <button
                        key={job._id}
                        id={`job-item-${job._id}`}
                        type="button"
                        role="listitem"
                        className={[
                          "df2-job-row",
                          selectedId === job._id ? "is-active" : "",
                          job.status === "failed" ? "is-failed" : "",
                          isLive ? "is-live" : "",
                        ].filter(Boolean).join(" ")}
                        onClick={() => setSelectedId(job._id)}
                      >
                        <span className={`df2-job-row-status is-${job.status}`} aria-hidden>
                          <DtIcon name={statusIcon(job.status)} size={14} />
                        </span>
                        <div className="df2-job-row-main">
                          <div className="df2-job-row-title">
                            <span className="df2-job-row-route" title={`${job.source_name} → ${destLabel}`}>
                              {job.source_name} → {destLabel}
                            </span>
                            <span className={`${jobStatusBadgeClass(job.status)} df2-job-row-badge`}>
                              {jobStatusLabel(job.status)}
                            </span>
                          </div>
                          <div className="df2-job-row-meta">
                            <span>{(job.records_processed ?? 0).toLocaleString()} rows</span>
                            <span>
                              {new Date(job.created_at).toLocaleString(undefined, {
                                month: "short",
                                day: "numeric",
                                hour: "2-digit",
                                minute: "2-digit",
                              })}
                            </span>
                            <code className="df2-job-row-id" title={job._id}>{job._id.slice(0, 8)}…</code>
                          </div>
                          {isLive && job.progress_pct != null && (
                            <div className="df2-job-row-bar" aria-hidden>
                              <i style={{ width: `${job.progress_pct}%` }} />
                            </div>
                          )}
                        </div>
                      </button>
                    );
                  })
                )}
              </aside>

              <section className="df2-jobs-v3-detail" aria-label="Job detail">
                {detailLoading ? (
                  <LoadingBlock title="Loading job details" size="md" variant="glass" />
                ) : isLive && selected ? (
                  <div className="df2-jobs-v3-live">
                    <div className="df2-jobs-v3-live-actions">
                      <button
                        type="button"
                        className="df2-btn df2-btn-sm df2-btn-ghost"
                        onClick={() => void handleCancel()}
                        disabled={cancelling}
                      >
                        {cancelling ? <ButtonLoader label="Cancelling…" /> : <><DtIcon name="x" size={14} /> Cancel job</>}
                      </button>
                    </div>
                    <JobTheater
                      jobId={selectedId!}
                      sourceLabel={selected.source_name}
                      destLabel={`${selected.destination_database}.${selected.destination_collection}`}
                      sourceType={selected.source_type}
                      destType={selected.destination_type}
                      onComplete={handleComplete}
                      onFailed={handleComplete}
                      onCancelled={handleComplete}
                    />
                  </div>
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
                      <div className="df2-jobs-v3-summary-ids">
                        <span className={jobStatusBadgeClass(liveJob.status)}>{jobStatusLabel(liveJob.status)}</span>
                        <CopyIdChip id={selected._id} label="Job" />
                      </div>
                    </header>

                    <div className="df2-jobs-v3-summary-metrics">
                      <article><strong>{(liveJob.records_processed ?? 0).toLocaleString()}</strong><span>Rows processed</span></article>
                      <article><strong>{liveJob.progress_pct ?? 100}%</strong><span>Progress</span></article>
                      <article><strong>{mappingCount || "—"}</strong><span>Columns</span></article>
                      <article><strong>{liveJob.operation || liveJob.transfer_request?.sync_mode || "transfer"}</strong><span>Mode</span></article>
                      {liveJob.cdc_lag_seconds != null && Number.isFinite(Number(liveJob.cdc_lag_seconds)) && (
                        <article>
                          <strong>{`${Number(liveJob.cdc_lag_seconds).toFixed(1)}s`}</strong>
                          <span>CDC lag</span>
                        </article>
                      )}
                      {liveJob.replication_lag_bytes != null && Number.isFinite(Number(liveJob.replication_lag_bytes)) && (
                        <article>
                          <strong>
                            {Number(liveJob.replication_lag_bytes) >= 1_048_576
                              ? `${(Number(liveJob.replication_lag_bytes) / 1_048_576).toFixed(1)} MB`
                              : `${Number(liveJob.replication_lag_bytes).toLocaleString()} B`}
                          </strong>
                          <span>WAL / binlog</span>
                        </article>
                      )}
                    </div>

                    <div className="df2-jobs-detail-card">
                      <div className="df2-jobs-detail-tabs" role="tablist" aria-label="Job detail sections">
                        {(
                          [
                            { id: "detail" as const, label: "Detail", icon: "activity" },
                            { id: "mapping" as const, label: "Mapping", icon: "connectors", count: mappingCount || undefined },
                            { id: "quarantine" as const, label: "Quarantine", icon: "alert", count: rejectedCount || undefined, hidden: !showQuarantineTab },
                            { id: "log" as const, label: "Log", icon: "jobs", count: ddlLog.length || undefined },
                          ] as const
                        ).filter((t) => !("hidden" in t && t.hidden)).map((tab) => (
                          <button
                            key={tab.id}
                            type="button"
                            role="tab"
                            id={`job-tab-${tab.id}`}
                            aria-selected={detailTab === tab.id}
                            aria-controls={`job-panel-${tab.id}`}
                            className={`df2-jobs-detail-tab${detailTab === tab.id ? " is-active" : ""}`}
                            onClick={() => setDetailTab(tab.id)}
                          >
                            <DtIcon name={tab.icon} size={14} />
                            <span>{tab.label}</span>
                            {"count" in tab && tab.count != null && tab.count > 0 && (
                              <em className="df2-jobs-detail-tab-count">{tab.count.toLocaleString()}</em>
                            )}
                          </button>
                        ))}
                      </div>

                      <div
                        className="df2-jobs-detail-panel"
                        role="tabpanel"
                        id={`job-panel-${detailTab}`}
                        aria-labelledby={`job-tab-${detailTab}`}
                      >
                        {detailTab === "detail" && (
                          <div className="df2-jobs-detail-pane">
                            <div className="df2-jobs-v3-timeline-block">
                              <h3>Timeline</h3>
                              <JobTimeline entries={timelineEntries} />
                            </div>

                            {liveJob.message && (
                              <dl className="df2-jobs-v3-summary-dl">
                                <div><dt>Latest message</dt><dd>{liveJob.message}</dd></div>
                              </dl>
                            )}

                            {liveJob.error && (
                              <div className="df2-jobs-v3-failure-panel" role="alert">
                                <header className="df2-jobs-v3-failure-head">
                                  <DtIcon name="alert" size={18} />
                                  <div>
                                    <strong>What went wrong</strong>
                                    <span>Job stopped before completion — review the failure below and quarantined rows if any.</span>
                                  </div>
                                </header>
                                <p className="df2-jobs-v3-failure-message">{liveJob.error}</p>
                                <dl className="df2-jobs-v3-failure-meta">
                                  {liveJob.phase && (
                                    <div>
                                      <dt>Failed phase</dt>
                                      <dd>{liveJob.phase}</dd>
                                    </div>
                                  )}
                                  {rejectedCount > 0 && (
                                    <div>
                                      <dt>Quarantined rows</dt>
                                      <dd>{rejectedCount.toLocaleString()} — validation failures isolated, not silently dropped</dd>
                                    </div>
                                  )}
                                  {liveJob.records_processed != null && liveJob.records_processed > 0 && (
                                    <div>
                                      <dt>Progress before failure</dt>
                                      <dd>{liveJob.records_processed.toLocaleString()} rows processed</dd>
                                    </div>
                                  )}
                                </dl>
                                {showQuarantineTab && (
                                  <button
                                    type="button"
                                    className="df2-btn df2-btn-sm"
                                    onClick={() => setDetailTab("quarantine")}
                                  >
                                    Open Quarantine tab
                                  </button>
                                )}
                              </div>
                            )}

                            {isJobSuccess(selected.status) && ((rejectedCount - (liveJob.coerced_null_rows ?? 0)) > 0 || (liveJob.coerced_null_rows ?? 0) > 0) && (
                              <div className="df2-data-integrity" role="note">
                                <header className="df2-data-integrity-head">
                                  <DtIcon name="alert" size={16} />
                                  <div>
                                    <strong>Completed with quarantine — not full fidelity</strong>
                                    <span>The transfer finished and data landed, but some rows were affected.</span>
                                  </div>
                                </header>
                                <div className="df2-data-integrity-metrics">
                                  <article className="df2-data-integrity-metric is-dropped">
                                    <strong>{Math.max(rejectedCount - (liveJob.coerced_null_rows ?? 0), 0).toLocaleString()}</strong>
                                    <span>Rows dropped / rejected</span>
                                    <small>Isolated in quarantine — not written to the destination.</small>
                                  </article>
                                  <article className="df2-data-integrity-metric is-coerced">
                                    <strong>{(liveJob.coerced_null_rows ?? 0).toLocaleString()}</strong>
                                    <span>Values coerced to NULL</span>
                                    <small>Row kept, but a value was altered to NULL — the original value was not preserved.</small>
                                  </article>
                                </div>
                              </div>
                            )}

                            {selected.status === "failed" && (
                              <div className="df2-jobs-v3-actions">
                                {liveJob?.checkpoint && (liveJob.checkpoint.rows_processed ?? 0) > 0 && (
                                  <button
                                    type="button"
                                    className="df2-btn df2-btn-primary"
                                    onClick={() => void handleResume()}
                                    disabled={resuming}
                                  >
                                    {resuming ? <ButtonLoader label="Resuming…" /> : <><DtIcon name="activity" size={16} /> Resume from batch {liveJob.checkpoint.chunk_index ?? 0}</>}
                                  </button>
                                )}
                                <button
                                  type="button"
                                  className={liveJob?.checkpoint && (liveJob.checkpoint.rows_processed ?? 0) > 0 ? "df2-btn" : "df2-btn df2-btn-primary"}
                                  onClick={() => void handleRetry()}
                                  disabled={retrying}
                                >
                                  {retrying ? <ButtonLoader label="Retrying…" /> : <><DtIcon name="transfer" size={16} /> Retry from start</>}
                                </button>
                                {onStartTransfer && (
                                  <button type="button" className="df2-btn" onClick={onStartTransfer}>
                                    Open Transfer Studio
                                  </button>
                                )}
                              </div>
                            )}
                          </div>
                        )}

                        {detailTab === "mapping" && (
                          <div className="df2-jobs-detail-pane">
                            {mappingCount > 0 ? (
                              <div className="df2-jobs-v3-mappings is-tab">
                                <div className="df2-jobs-v3-mappings-head">
                                  <h3>Column mapping</h3>
                                  <span className="df2-jobs-v3-mappings-count">
                                    {mappingCount.toLocaleString()} columns
                                  </span>
                                </div>
                                <div className="df2-jobs-v3-mappings-scroll">
                                  <table className="df2-table">
                                    <thead>
                                      <tr>
                                        <th>Source</th>
                                        <th>Target</th>
                                        <th>Type</th>
                                      </tr>
                                    </thead>
                                    <tbody>
                                      {jobMappings.length > 0
                                        ? jobMappings.map((m, i) => {
                                            const src = String(m.source ?? m.source_column ?? "").trim() || "—";
                                            const tgt = String(m.target ?? m.target_column ?? "").trim() || "—";
                                            const typ =
                                              columnTypes[src]
                                              ?? m.source_type
                                              ?? m.target_type
                                              ?? columnTypes[tgt]
                                              ?? columnTypes[src.toLowerCase()]
                                              ?? columnTypes[tgt.toLowerCase()]
                                              ?? "—";
                                            return (
                                              <tr key={`${src}-${tgt}-${i}`}>
                                                <td title={src}>{src}</td>
                                                <td title={tgt}>{tgt}</td>
                                                <td className="df2-cell-mono" title={typ}>{typ}</td>
                                              </tr>
                                            );
                                          })
                                        : Object.entries(columnTypes).map(([col, typ]) => (
                                            <tr key={col}>
                                              <td title={col}>{col}</td>
                                              <td title={col}>{col}</td>
                                              <td className="df2-cell-mono" title={String(typ)}>{String(typ)}</td>
                                            </tr>
                                          ))}
                                    </tbody>
                                  </table>
                                </div>
                              </div>
                            ) : (
                              <EmptyState
                                compact
                                icon="connectors"
                                title="No mapping recorded"
                                description="This job has no persisted column mappings to display."
                              />
                            )}
                          </div>
                        )}

                        {detailTab === "quarantine" && (
                          <div className="df2-jobs-detail-pane">
                            <section className="df2-jobs-v3-quarantine is-tab" aria-label="Quarantined rows">
                              <header className="df2-jobs-v3-quarantine-head">
                                <div>
                                  <h3>Quarantined rows</h3>
                                  <p>
                                    {rejectedCount > 0
                                      ? `${rejectedCount.toLocaleString()} row(s) isolated — inspect columns, values, and reasons. Export CSV downloads to your machine.`
                                      : "Inspect preflight / integrity findings for this job."}
                                  </p>
                                </div>
                                {rejectedCount > 0 && (
                                  <span className="df2-jobs-v3-quarantine-badge">
                                    {rejectedCount.toLocaleString()} quarantined
                                  </span>
                                )}
                              </header>
                              <QuarantinePanel
                                jobId={selected._id}
                                rejectedRows={liveJob.rejected_rows}
                                coercedNullRows={liveJob.coerced_null_rows}
                                autoLoad
                                initiallyOpen
                              />
                            </section>
                          </div>
                        )}

                        {detailTab === "log" && (
                          <div className="df2-jobs-detail-pane">
                            {ddlLog.length > 0 ? (
                              <div className="df2-jobs-v3-log is-tab">
                                <h3>DDL log</h3>
                                <pre>{ddlLog.join("\n")}</pre>
                              </div>
                            ) : (
                              <EmptyState
                                compact
                                icon="jobs"
                                title="No DDL log"
                                description="This job did not record DDL statements, or the log is empty."
                              />
                            )}
                            {liveJob.message && (
                              <dl className="df2-jobs-v3-summary-dl">
                                <div><dt>Latest message</dt><dd>{liveJob.message}</dd></div>
                              </dl>
                            )}
                          </div>
                        )}
                      </div>
                    </div>
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
