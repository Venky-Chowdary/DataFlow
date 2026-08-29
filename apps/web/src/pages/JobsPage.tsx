import { loadMethodDescription, loadMethodLabel } from "../lib/loadMethod";
import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import { ConnectorIcon } from "../app/brand-icons";
import { DtIcon } from "../components/DtIcon";
import { JobTheater } from "../components/JobTheater";
import { JobNameEditor } from "../components/jobs/JobNameEditor";
import { ButtonLoader, LoadingBlock } from "../components/LoadingState";
import { EmptyState } from "../components/ui/EmptyState";
import { Button } from "../components/ui/Button";
import { CopyIdChip } from "../components/ui/CopyIdChip";
import { PageFrame } from "../components/ui/PageFrame";
import { PageShell } from "../components/ui/PageShell";
import { FilterBar } from "../components/ui/FilterBar";
import { FilterTabs } from "../components/ui/FilterTabs";
import { PageToolbar } from "../components/ui/PageToolbar";
import { useToast } from "../components/Toast";
import { useActiveData } from "../lib/DataContext";
import { fetchJob, fetchJobMappingProof, renameJob, retryJob, resumeJob, RetryRefusedError } from "../lib/api";
import {
  jobFilterCounts,
  jobHistoryFromResponse,
  jobWindowNote,
  type JobHistory,
} from "../lib/jobHistory";
import { isJobSuccess, jobStatusBadgeClass, jobStatusLabel } from "../lib/uiUtils";
import { JobProgress, TransferJob } from "../lib/types";
import { QuarantinePanel } from "../components/transfer/QuarantinePanel";
import { Gate8ProofCard, classifyGate8Status } from "../components/transfer/Gate8ProofCard";
import { CdcLeaseConflictPanel } from "../components/transfer/CdcLeaseConflictPanel";
import { CdcCursorGapPanel } from "../components/transfer/CdcCursorGapPanel";
import { CdcRetentionPanel } from "../components/transfer/CdcRetentionPanel";
import { CdcIncrementalSnapshotPanel } from "../components/transfer/CdcIncrementalSnapshotPanel";
import { JobTrustScoreCard } from "../components/transfer/JobTrustScoreCard";
import { ConservationLedgerCard } from "../components/transfer/ConservationLedgerCard";
import { destHeadline, formatJobRowMetric, destMetricCompact, destMetricToneClass } from "../lib/conservationLedger";
import {
  formatSchemaPolicyLabel,
  formatSyncModeLabel,
  formatValidationModeLabel,
} from "../lib/transferConstants";
import { jobStudioDataRules } from "../lib/studioDataRules";
import { jobStudioDeliveryGuarantee } from "../lib/cdcExactlyOnce";
import { blockerTitle } from "../lib/preflightGates";
import { clampPercent } from "../lib/progressRing";
import { nextListSelection, shouldApplyInitialJobFocus } from "../lib/jobSelection";
import { LoadHistoryPanel } from "../components/transfer/LoadHistoryPanel";
import { ConnectionReuseCard } from "../components/transfer/ConnectionReuseCard";
import { PhaseProfileCard } from "../components/transfer/PhaseProfileCard";
import { ReplaySafetyCard } from "../components/transfer/ReplaySafetyCard";
import { TransformationsCard } from "../components/transfer/TransformationsCard";
import type {
  ConnectionReuseReport,
  PhaseProfileReport,
  ReplaySafetyReport,
  TransformationsReport,
} from "../lib/types";
import { inferTransferFailureHint } from "../lib/transferFailure";
import { buildJobTimeline, JobTimeline } from "../components/ui/JobTimeline";
import { readJobEventLog } from "../lib/jobEventLog";
import { MappingProofDrawer, type MappingProof } from "../components/MappingProofDrawer";
import { Drawer } from "../components/ui/Drawer";
import {
  JobEvidenceLaunchGrid,
  JobExplanationView,
  JobLogTable,
  JobOverviewNote,
} from "../components/jobs/JobEvidenceLaunch";
import type { RepairMapping } from "../lib/api";

export type JobsStudioIntent = {
  step?: "validate" | "map" | "source";
  repairProposalId?: string;
  jobId?: string;
  /** Applied or job mappings to seed into Transfer Studio Map/Validate. */
  mappings?: RepairMapping[];
  /** Job-captured preflight so Studio Validate shows real gates, not Pending. */
  preflight?: import("../lib/types").PreflightResult;
  validationMode?: string;
  schemaPolicy?: string;
  deliveryGuarantee?: string;
  /** Schedules → Studio: preload the parked route so Map can persist a contract. */
  sourceConnectorId?: string;
  destConnectorId?: string;
  sourceTable?: string;
  destTable?: string;
};

function asMappingProof(raw: unknown): MappingProof | null {
  if (!raw || typeof raw !== "object") return null;
  const proof = raw as MappingProof;
  if (!Array.isArray(proof.mappings) || proof.mappings.length === 0) return null;
  return proof;
}

type JobEvidenceDrawer =
  | null
  | "gate8"
  | "run-meta"
  | "timeline"
  | "mapping-proof"
  | "mapping-table"
  | "event-log"
  | "ddl-log"
  | "explanation"
  | "streams"
  | "preflight"
  | "writer"
  | "quarantine";

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
    schema_policy?: string;
    delivery_guarantee?: string;
  };
  phases?: { name: string; status: "pending" | "active" | "done" | "failed" | "skipped"; message?: string }[];
  ddl_log?: string[];
  ddl_executed?: string[];
  explanation?: string;
  triggered_by?: string;
  created_by?: string;
  started_at?: string;
  completed_at?: string;
}

function formatJobDuration(startedAt?: string, completedAt?: string): string | null {
  if (!startedAt) return null;
  const start = Date.parse(startedAt);
  if (!Number.isFinite(start)) return null;
  const end = completedAt ? Date.parse(completedAt) : Date.now();
  if (!Number.isFinite(end) || end < start) return null;
  const s = Math.floor((end - start) / 1000);
  if (s < 60) return `${s}s`;
  const m = Math.floor(s / 60);
  if (m < 60) return `${m}m ${s % 60}s`;
  const h = Math.floor(m / 60);
  return `${h}h ${m % 60}m`;
}

interface JobsPageProps {
  jobs: TransferJob[];
  /** Whole-history counts; the table still shows the recent page in `jobs`. */
  history?: JobHistory;
  onRefresh?: () => void;
  /** Open Transfer Studio — optional intent lands on Validate / Map with repair context. */
  onStartTransfer?: (intent?: JobsStudioIntent) => void;
  initialJobId?: string;
  /** Deep-link panel (e.g. mapping-proof). */
  initialPanel?: string;
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

/** Source side of a route label. Never renders a missing name as "undefined". */
function jobSourceLabel(job: Pick<TransferJob, "source_name" | "source_type">) {
  return (job.source_name || "").trim() || (job.source_type || "").trim() || "source";
}

function jobRouteLabel(
  job: Pick<
    TransferJob,
    "source_name" | "source_type" | "destination_database" | "destination_collection" | "destination_type"
  >,
) {
  const dest =
    [job.destination_database, job.destination_collection].filter(Boolean).join(".")
    || job.destination_type
    || "destination";
  return `${jobSourceLabel(job)} → ${dest}`;
}

function jobDisplayName(
  job: Pick<
    TransferJob,
    | "name"
    | "source_name"
    | "source_type"
    | "destination_database"
    | "destination_collection"
    | "destination_type"
  >,
  override?: string | null,
) {
  const named = (override ?? job.name)?.trim();
  return named || jobRouteLabel(job);
}

function normalizeJobName(value: string) {
  return value.trim().toLowerCase();
}

/** Returns a conflict message when another job already uses this display name. */
function duplicateJobNameError(
  draft: string,
  jobId: string,
  jobs: TransferJob[],
  overrides: Record<string, string>,
): string | null {
  const needle = normalizeJobName(draft);
  if (!needle) return "Job name cannot be empty";
  for (const other of jobs) {
    if (other._id === jobId) continue;
    const otherName = normalizeJobName(jobDisplayName(other, overrides[other._id]));
    if (otherName === needle) {
      return "This name already exists";
    }
  }
  return null;
}

export function JobsPage({ jobs, history, onRefresh, onStartTransfer, initialJobId, initialPanel }: JobsPageProps) {
  const { toast } = useToast();
  const { setActiveData } = useActiveData();
  const [selectedId, setSelectedId] = useState<string | null>(null);
  const [liveJob, setLiveJob] = useState<JobDetailRecord | null>(null);
  const [detailLoading, setDetailLoading] = useState(false);
  const [retrying, setRetrying] = useState(false);
  const [resuming, setResuming] = useState(false);
  const [filter, setFilter] = useState<JobFilter>("all");
  const [jobSearch, setJobSearch] = useState("");
  const [detailTab, setDetailTab] = useState<JobDetailTab>("detail");
  const [renamingId, setRenamingId] = useState<string | null>(null);
  const [renameDraft, setRenameDraft] = useState("");
  const [renameSaving, setRenameSaving] = useState(false);
  const [renameError, setRenameError] = useState<string | null>(null);
  const [nameOverrides, setNameOverrides] = useState<Record<string, string>>({});
  const [mappingProofOpen, setMappingProofOpen] = useState(false);
  const [mappingProof, setMappingProof] = useState<MappingProof | null>(null);
  const [evidenceDrawer, setEvidenceDrawer] = useState<JobEvidenceDrawer>(null);
  const appliedFocusRef = useRef<string | null>(null);
  const jobsRef = useRef(jobs);
  jobsRef.current = jobs;

  const openStudio = useCallback((intent?: JobsStudioIntent) => {
    onStartTransfer?.(intent);
  }, [onStartTransfer]);

  const jobRepairMappings = useMemo((): RepairMapping[] => {
    const maps = liveJob?.transfer_request?.mappings;
    if (!Array.isArray(maps)) return [];
    return maps.map((m) => ({
      source: m.source || m.source_column || "",
      destination: m.target || m.target_column || "",
      destination_type: m.target_type || m.source_type,
      target_type: m.target_type || m.source_type,
    })).filter((m) => m.source);
  }, [liveJob?.transfer_request?.mappings]);

  // Counted over the whole history, not the page of rows below: counting the rows
  // showed "All (50)" for a 90-job history and disagreed with Pilot.
  const counts = useMemo(
    () => jobFilterCounts(history || jobHistoryFromResponse({ jobs })),
    [history, jobs],
  );

  const filtered = useMemo(() => {
    let list = jobs;
    if (filter === "running") list = jobs.filter((j) => j.status === "running" || j.status === "pending");
    else if (filter === "completed") list = jobs.filter((j) => isJobSuccess(j.status));
    else if (filter !== "all") list = jobs.filter((j) => j.status === filter);

    const q = jobSearch.trim().toLowerCase();
    if (!q) return list;
    return list.filter((j) =>
      [
        j.name,
        nameOverrides[j._id],
        j.source_name,
        j.source_type,
        j.destination_type,
        j.destination_database,
        j.destination_collection,
        j._id,
        j.status,
        j.error,
      ]
        .filter(Boolean)
        .some((v) => String(v).toLowerCase().includes(q)),
    );
  }, [jobs, filter, jobSearch, nameOverrides]);

  const windowNote = jobWindowNote({
    counts,
    filter,
    loaded: jobs.length,
    shown: jobSearch.trim() ? undefined : filtered.length,
  });

  useEffect(() => {
    const ids = jobs.map((j) => j._id);
    if (!shouldApplyInitialJobFocus(initialJobId, appliedFocusRef.current, ids)) return;
    appliedFocusRef.current = initialJobId!;
    setFilter("all");
    setJobSearch("");
    setSelectedId(initialJobId!);
    if (initialPanel === "mapping-proof") {
      setDetailTab("mapping");
      setEvidenceDrawer("mapping-proof");
      setMappingProofOpen(true);
    }
    window.requestAnimationFrame(() => {
      document.getElementById(`job-item-${initialJobId}`)?.scrollIntoView({
        behavior: "smooth",
        block: "nearest",
      });
    });
  }, [initialJobId, initialPanel, jobs]);

  useEffect(() => {
    if (!selectedId || !liveJob) {
      setMappingProof(asMappingProof(liveJob?.mapping_proof));
      return;
    }
    const fromJob = asMappingProof(liveJob.mapping_proof);
    if (fromJob) {
      setMappingProof(fromJob);
      return;
    }
    let cancelled = false;
    void fetchJobMappingProof(selectedId)
      .then((res) => {
        if (cancelled) return;
        setMappingProof(asMappingProof(res.mapping_proof));
      })
      .catch(() => {
        if (!cancelled) setMappingProof(null);
      });
    return () => {
      cancelled = true;
    };
  }, [selectedId, liveJob]);

  useEffect(() => {
    const next = nextListSelection(selectedId, filtered.map((j) => j._id));
    if (next && next !== selectedId) setSelectedId(next);
  }, [filter, filtered, selectedId]);

  // Feed the selected job into Datawrap Pilot so NL triage uses the real job ID.
  useEffect(() => {
    const job = jobs.find((j) => j._id === selectedId);
    if (!job) return;
    const dest = job.destination_collection || job.destination_database || job.destination_type || "destination";
    setActiveData((prev) => ({
      name: prev?.name || (job.source_name || "").trim() || "job",
      filename: prev?.filename,
      columns: prev?.columns || [],
      row_count: job.records_processed ?? prev?.row_count ?? 0,
      samples: prev?.samples,
      schema: prev?.schema,
      preflight_run_id: prev?.preflight_run_id,
      job_id: job._id,
      validation_status: job.status,
      route: `${jobSourceLabel(job)} → ${dest}`,
      blockers: job.error ? [job.error] : prev?.blockers,
    }));
  }, [jobs, selectedId, setActiveData]);

  useEffect(() => {
    if (!selectedId) {
      setLiveJob(null);
      setDetailLoading(false);
      setEvidenceDrawer(null);
      return;
    }
    let cancelled = false;
    const listed = jobsRef.current.find((j) => j._id === selectedId);
    // Optimistic hydrate from list so the detail pane never goes blank on slow/404 get.
    if (listed) {
      setLiveJob((prev) => ({
        ...(prev && prev._id === selectedId ? prev : {}),
        ...(listed as unknown as JobDetailRecord),
        _id: listed._id,
        status: listed.status,
        progress_pct: listed.progress_pct ?? 0,
        records_processed: listed.records_processed ?? 0,
        message: listed.message || listed.error || "",
      }));
    } else {
      setDetailLoading(true);
    }
    fetchJob(selectedId)
      .then((job) => {
        if (cancelled) return;
        setLiveJob(job);
      })
      .catch(() => {
        if (cancelled) return;
        // Keep list snapshot if detail endpoint fails (workspace/isolation edge cases).
        if (!listed) setLiveJob(null);
      })
      .finally(() => {
        if (cancelled) return;
        setDetailLoading(false);
      });
    return () => {
      cancelled = true;
    };
  }, [selectedId]);

  useEffect(() => {
    if (!selectedId) return;
    const listed = jobs.find((j) => j._id === selectedId);
    if (!listed) return;
    setLiveJob((prev) => {
      if (!prev || prev._id !== selectedId) return prev;
      return {
        ...prev,
        status: listed.status,
        progress_pct: listed.progress_pct ?? prev.progress_pct,
        records_processed: listed.records_processed ?? prev.records_processed,
        message: listed.message || listed.error || prev.message,
      };
    });
  }, [jobs, selectedId]);

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
      if (e instanceof RetryRefusedError) {
        // Retry re-reads the source from zero; the rows this attempt already
        // committed would land a second time. Resume is the safe action.
        toast({
          title: "Retry refused \u2014 it would duplicate committed rows",
          message: `${e.message} Use Resume from checkpoint instead.`,
          tone: "warning",
        });
        setRetrying(false);
        return;
      }
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
    if (!selectedId) return;
    const gapRecovery = Boolean(liveJob?.cdc_cursor_gap);
    if (!liveJob?.checkpoint && !gapRecovery) return;
    setResuming(true);
    try {
      const res = await resumeJob(selectedId);
      // Full refresh restarts rather than continuing; promising a batch offset
      // it will not use is the kind of detail that erodes trust in the report.
      toast({
        title: res.restarted ? "Transfer restarted" : "Resume started",
        message:
          res.message
          || (res.restarted
            ? (gapRecovery
              ? "CDC cursor-gap recovery restarted — not a checkpoint continuation. Purged-window events are gone."
              : "Full refresh re-run from the beginning — it replaces the destination.")
            : `Resuming from batch ${liveJob?.checkpoint?.chunk_index ?? 0} (${(liveJob?.checkpoint?.rows_processed ?? 0).toLocaleString()} rows already committed).`),
        tone: "success",
      });
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
  }, [selectedId, liveJob?.checkpoint, liveJob?.cdc_cursor_gap, onRefresh, onStartTransfer, toast]);

  const beginRename = useCallback((job: TransferJob) => {
    setRenamingId(job._id);
    setRenameDraft(jobDisplayName(job, nameOverrides[job._id]));
    setRenameError(null);
  }, [nameOverrides]);

  const cancelRename = useCallback(() => {
    setRenamingId(null);
    setRenameDraft("");
    setRenameError(null);
  }, []);

  const onRenameDraftChange = useCallback((value: string, jobId: string) => {
    setRenameDraft(value);
    const current = jobs.find((j) => j._id === jobId);
    if (!current) {
      setRenameError(null);
      return;
    }
    const trimmed = value.trim();
    if (!trimmed) {
      setRenameError("Job name cannot be empty");
      return;
    }
    const currentName = jobDisplayName(current, nameOverrides[jobId]);
    if (normalizeJobName(trimmed) === normalizeJobName(currentName)) {
      setRenameError(null);
      return;
    }
    setRenameError(duplicateJobNameError(trimmed, jobId, jobs, nameOverrides));
  }, [jobs, nameOverrides]);

  const commitRename = useCallback(async (jobId: string) => {
    const next = renameDraft.trim();
    if (!next) {
      setRenameError("Job name cannot be empty");
      return;
    }
    const conflict = duplicateJobNameError(next, jobId, jobs, nameOverrides);
    if (conflict) {
      setRenameError(conflict);
      return;
    }
    setRenameSaving(true);
    setRenameError(null);
    try {
      const updated = await renameJob(jobId, next);
      setNameOverrides((prev) => ({ ...prev, [jobId]: updated.name || next }));
      setLiveJob((prev) => (prev && prev._id === jobId ? { ...prev, name: updated.name || next } : prev));
      setRenamingId(null);
      setRenameDraft("");
      toast({ title: "Job renamed", message: `Saved as “${updated.name || next}”.`, tone: "success" });
      onRefresh?.();
    } catch (e) {
      const msg = e instanceof Error ? e.message : "Could not save name";
      if (/already exists/i.test(msg)) {
        setRenameError("This name already exists");
      } else {
        toast({ title: "Rename failed", message: msg, tone: "error" });
      }
    } finally {
      setRenameSaving(false);
    }
  }, [renameDraft, jobs, nameOverrides, onRefresh, toast]);

  // Reset rename when switching jobs (do not clear while editing the same job).
  const prevSelectedRef = useRef<string | null>(null);
  useEffect(() => {
    if (prevSelectedRef.current === selectedId) return;
    prevSelectedRef.current = selectedId;
    setRenamingId(null);
    setRenameDraft("");
    setRenameError(null);
  }, [selectedId]);

  const jobMappings = liveJob?.transfer_request?.mappings ?? [];
  const columnTypes = liveJob?.transfer_request?.column_types ?? {};
  const ddlLog = liveJob?.ddl_executed ?? liveJob?.ddl_log ?? [];
  const sessionEvents = selectedId ? readJobEventLog(selectedId) : [];
  const eventLog = (liveJob?.event_log?.length ? liveJob.event_log : sessionEvents) ?? [];
  const logLineCount = eventLog.length + ddlLog.length;
  const mappingCount = jobMappings.length || Object.keys(columnTypes).length;
  const rejectedCount = liveJob?.rejected_rows ?? 0;
  const recon = liveJob?.reconciliation;
  const gate8 = classifyGate8Status(recon);
  const destMetric = destHeadline(liveJob);
  const jobDuration = formatJobDuration(liveJob?.started_at, liveJob?.completed_at);
  const triggeredBy = liveJob?.triggered_by || liveJob?.created_by || "";
  const syncModeLabel = formatSyncModeLabel(
    liveJob?.sync_mode || liveJob?.transfer_request?.sync_mode,
  );
  const jobPreflight = liveJob?.preflight;
  const openValidateInStudio = useCallback((extra?: Partial<JobsStudioIntent>) => {
    const rules = jobStudioDataRules(liveJob);
    openStudio({
      step: "validate",
      jobId: selectedId || liveJob?._id || undefined,
      mappings: jobRepairMappings,
      preflight: liveJob?.preflight,
      validationMode: rules.validationMode || undefined,
      schemaPolicy: rules.schemaPolicy || undefined,
      deliveryGuarantee: jobStudioDeliveryGuarantee(liveJob) || undefined,
      ...extra,
    });
  }, [openStudio, selectedId, liveJob, jobRepairMappings]);
  const openMapInStudio = useCallback((extra?: Partial<JobsStudioIntent>) => {
    const rules = jobStudioDataRules(liveJob);
    openStudio({
      step: "map",
      jobId: selectedId || liveJob?._id || undefined,
      mappings: jobRepairMappings,
      preflight: liveJob?.preflight,
      validationMode: rules.validationMode || undefined,
      schemaPolicy: rules.schemaPolicy || undefined,
      deliveryGuarantee: jobStudioDeliveryGuarantee(liveJob) || undefined,
      ...extra,
    });
  }, [openStudio, selectedId, liveJob, jobRepairMappings]);
  const destSummary = (liveJob?.destination_summary ?? {}) as Record<string, unknown>;
  const loadHistory =
    liveJob?.load_history_report
    || (destSummary.load_history_report && typeof destSummary.load_history_report === "object"
      ? destSummary.load_history_report as NonNullable<JobDetailRecord["load_history_report"]>
      : undefined);
  const writerRps = liveJob?.records_per_second
    ?? (typeof destSummary.records_per_second === "number" ? destSummary.records_per_second : undefined);
  const writerChunkSize = liveJob?.chunk_size
    ?? (typeof destSummary.chunk_size === "number" ? destSummary.chunk_size : undefined);
  const hasWriterEvidence = Boolean(
    Object.keys(destSummary).length
    || writerRps != null
    || writerChunkSize != null
    || loadHistory
    || liveJob?.chunk_current != null
    || liveJob?.checkpoint,
  );
  const showQuarantineTab = Boolean(
    liveJob
    && (
      liveJob.status === "failed"
      || liveJob.status === "completed_with_quarantine"
      || rejectedCount > 0
    ),
  );
  const failureHint = liveJob?.error
    ? inferTransferFailureHint(
      liveJob.error,
      liveJob.error_code,
      liveJob.error_title,
      liveJob.error_fix,
      liveJob.error_confidence,
    )
    : null;
  const duplicateKeyFailure = Boolean(
    failureHint?.code === "duplicate_primary_key"
    || /duplicate redis key|duplicate primary key/i.test(String(liveJob?.error || "")),
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
                <Button
                  variant="primary"
                  onClick={() => openStudio()}
                  leadingIcon={<DtIcon name="transfer" size={14} />}
                >
                  Open Transfer Studio
                </Button>
              ) : undefined
            }
          />
        ) : (
          <>
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
                    const route = jobRouteLabel(job);
                    const displayName = jobDisplayName(job, nameOverrides[job._id]);
                    const isLiveRow = job.status === "running" || job.status === "pending";
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
                          isLiveRow ? "is-live" : "",
                        ].filter(Boolean).join(" ")}
                        aria-current={selectedId === job._id ? "true" : undefined}
                        onClick={() => setSelectedId(job._id)}
                      >
                        <span className={`df2-job-row-status is-${job.status}`} aria-hidden>
                          <DtIcon name={statusIcon(job.status)} size={14} />
                        </span>
                        <div className="df2-job-row-main">
                          <div className="df2-job-row-title">
                            <span className="df2-job-row-name" title={displayName}>{displayName}</span>
                            <span className={`${jobStatusBadgeClass(job.status)} df2-job-row-badge`}>
                              {jobStatusLabel(job.status)}
                            </span>
                          </div>
                          <div className="df2-job-row-meta">
                            <span className="df2-job-row-route-meta" title={route}>{route}</span>
                            {(() => {
                              const rows = formatJobRowMetric(job);
                              return (
                                <span
                                  className={`df2-job-row-rows ${destMetricToneClass(rows)}`}
                                  title={rows.title}
                                >
                                  {destMetricCompact(rows)}
                                </span>
                              );
                            })()}
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
                          {isLiveRow && job.progress_pct != null && (
                            <div className="df2-job-row-bar" aria-hidden>
                              <i style={{ width: `${clampPercent(job.progress_pct)}%` }} />
                            </div>
                          )}
                        </div>
                      </button>
                    );
                  })
                )}
                {windowNote && <p className="df2-jobs-v3-window-note">{windowNote}</p>}
              </aside>

              <section className="df2-jobs-v3-detail" aria-label="Job detail">
                {detailLoading && !selected && !liveJob ? (
                  <LoadingBlock title="Loading job details" size="md" variant="glass" />
                ) : isLive && selected ? (
                  <div className="df2-jobs-v3-live">
                    <div className="df2-jobs-v3-detail-identity">
                      <JobNameEditor
                        displayName={jobDisplayName(
                          { ...selected, name: liveJob?.name || selected.name },
                          nameOverrides[selected._id],
                        )}
                        editing={renamingId === selected._id}
                        draft={renameDraft}
                        error={renameError}
                        saving={renameSaving}
                        onBegin={() => beginRename({ ...selected, name: liveJob?.name || selected.name })}
                        onDraftChange={(v) => onRenameDraftChange(v, selected._id)}
                        onSave={() => void commitRename(selected._id)}
                        onCancel={cancelRename}
                      />
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
                      onOpenJob={(jobId) => setSelectedId(jobId)}
                    />
                  </div>
                ) : liveJob && selected ? (
                  <div className={`df2-jobs-v3-summary ${selected.status === "failed" ? "is-failed" : "is-done"}`}>
                    <header className="df2-jobs-v3-summary-head">
                      <div className="df2-jobs-v3-summary-identity">
                        <JobNameEditor
                          displayName={jobDisplayName(
                            { ...selected, name: liveJob.name || selected.name },
                            nameOverrides[selected._id],
                          )}
                          editing={renamingId === selected._id}
                          draft={renameDraft}
                          error={renameError}
                          saving={renameSaving}
                          onBegin={() => beginRename({ ...selected, name: liveJob.name || selected.name })}
                          onDraftChange={(v) => onRenameDraftChange(v, selected._id)}
                          onSave={() => void commitRename(selected._id)}
                          onCancel={cancelRename}
                        />
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
                      </div>
                      <div className="df2-jobs-v3-summary-ids">
                        <span className={jobStatusBadgeClass(liveJob.status)}>{jobStatusLabel(liveJob.status)}</span>
                        <span
                          className={`df2-jobs-v3-summary-dest ${destMetricToneClass(destMetric)}`}
                          title={destMetric.title}
                        >
                          {destMetricCompact(destMetric)}
                        </span>
                        <CopyIdChip id={selected._id} label="Job" />
                      </div>
                    </header>

                    <div className="df2-jobs-detail-card">
                      <div className="df2-jobs-detail-tabs" role="tablist" aria-label="Job detail sections">
                        {(
                          [
                            { id: "detail" as const, label: "Detail", icon: "activity" },
                            { id: "mapping" as const, label: "Mapping", icon: "connectors", count: mappingCount || undefined },
                            { id: "quarantine" as const, label: "Quarantine", icon: "alert", count: rejectedCount || undefined, hidden: !showQuarantineTab },
                            { id: "log" as const, label: "Log", icon: "jobs", count: logLineCount || undefined },
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
                            <JobOverviewNote>
                              Overview of this run. Open evidence panels from the right for Gate-8,
                              run metadata, timeline, and logs — keep this pane scannable.
                            </JobOverviewNote>
                            <ConservationLedgerCard
                              job={liveJob}
                              onOpenValidate={() => openValidateInStudio()}
                            />
                            <JobTrustScoreCard
                              job={liveJob}
                              onOpenQuarantine={
                                showQuarantineTab ? () => setDetailTab("quarantine") : undefined
                              }
                              onOpenValidate={() => openValidateInStudio()}
                              onResume={
                                liveJob.checkpoint || liveJob.chunk_current != null
                                  ? () => void handleResume()
                                  : undefined
                              }
                            />

                            <JobEvidenceLaunchGrid
                              label="Evidence panels"
                              items={[
                                {
                                  id: "gate8",
                                  title: "Gate-8 reconcile",
                                  description: "Dest-before delta, row counts, and checksums",
                                  icon: "shield",
                                  meta: recon
                                    ? gate8.label
                                    : "Not captured",
                                  tone: recon
                                    ? (gate8.tone === "ok" ? "ok" : gate8.tone === "danger" ? "warn" : "warn")
                                    : "default",
                                  disabled: !recon,
                                  onOpen: () => setEvidenceDrawer("gate8"),
                                },
                                {
                                  id: "preflight",
                                  title: "Validate / preflight",
                                  description: "Gate results captured when this job ran",
                                  icon: "gate",
                                  meta: jobPreflight
                                    ? `${jobPreflight.passed_count}/${jobPreflight.total_gates} · ${jobPreflight.readiness_score}%`
                                    : "Not on job",
                                  tone: jobPreflight
                                    ? (jobPreflight.passed ? "ok" : "warn")
                                    : "default",
                                  disabled: !jobPreflight?.gates?.length,
                                  onOpen: () => setEvidenceDrawer("preflight"),
                                },
                                {
                                  id: "writer",
                                  title: "Writer & throughput",
                                  description: "RPS, phase timing, destination summary, load history",
                                  icon: "speed",
                                  meta: (() => {
                                    const phase =
                                      destSummary.phase_profile &&
                                      typeof destSummary.phase_profile === "object"
                                        ? (destSummary.phase_profile as PhaseProfileReport)
                                            .dominant_phase
                                        : "";
                                    if (writerRps != null && phase) {
                                      return `${Math.round(writerRps).toLocaleString()} r/s · ${phase}`;
                                    }
                                    if (writerRps != null) {
                                      return `${Math.round(writerRps).toLocaleString()} r/s`;
                                    }
                                    if (phase) return `bottleneck: ${phase}`;
                                    return loadHistory ? "Load history" : undefined;
                                  })(),
                                  disabled: !hasWriterEvidence,
                                  onOpen: () => setEvidenceDrawer("writer"),
                                },
                                {
                                  id: "run-meta",
                                  title: "Run metadata",
                                  description: "Operator, duration, sync mode, CDC lease & watermark",
                                  icon: "activity",
                                  meta: syncModeLabel !== "—" ? syncModeLabel : undefined,
                                  onOpen: () => setEvidenceDrawer("run-meta"),
                                },
                                {
                                  id: "timeline",
                                  title: "Phase timeline",
                                  description: "Queued → read → gates → write → reconcile",
                                  icon: "clock",
                                  meta: `${timelineEntries.length} events`,
                                  disabled: timelineEntries.length === 0,
                                  onOpen: () => setEvidenceDrawer("timeline"),
                                },
                                {
                                  id: "mapping-proof",
                                  title: "Mapping proof",
                                  description: "Column match evidence for this job",
                                  icon: "layers",
                                  meta: mappingProof?.summary?.mapped_count != null
                                    ? `${mappingProof.summary.mapped_count} pairs`
                                    : mappingCount
                                      ? `${mappingCount} columns`
                                      : undefined,
                                  disabled: !mappingProof && mappingCount === 0,
                                  onOpen: () => {
                                    setDetailTab("mapping");
                                    setEvidenceDrawer(mappingProof ? "mapping-proof" : "mapping-table");
                                  },
                                },
                                {
                                  id: "event-log",
                                  title: "Event log",
                                  description: "Durable operator events for this job",
                                  icon: "activity",
                                  meta: eventLog.length ? `${eventLog.length} lines` : "Empty",
                                  disabled: eventLog.length === 0,
                                  onOpen: () => setEvidenceDrawer("event-log"),
                                },
                                {
                                  id: "ddl-log",
                                  title: "DDL & stream log",
                                  description: "Schema statements and stream progress lines",
                                  icon: "code",
                                  meta: ddlLog.length ? `${ddlLog.length} lines` : "Empty",
                                  disabled: ddlLog.length === 0,
                                  onOpen: () => setEvidenceDrawer("ddl-log"),
                                },
                                {
                                  id: "explanation",
                                  title: "Pipeline explanation",
                                  description: "Plain-language summary of what this transfer did",
                                  icon: "book",
                                  disabled: !liveJob.explanation,
                                  onOpen: () => setEvidenceDrawer("explanation"),
                                },
                              ]}
                            />

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
                                    <strong>{failureHint?.title || liveJob.error_title || "What went wrong"}</strong>
                                    <span>
                                      {failureHint?.code === "destination_table_full" || failureHint?.code === "destination_disk_full"
                                        ? "Destination capacity blocked the load — quarantined cells below are separate data-quality findings, not the cause of this stop."
                                        : "Job stopped before completion — review the failure below and quarantined rows if any."}
                                    </span>
                                  </div>
                                </header>
                                <p className="df2-jobs-v3-failure-message">{liveJob.error}</p>
                                {(failureHint?.fix || liveJob.error_fix) && (
                                  <p className="df2-jobs-v3-failure-fix">
                                    <strong>
                                      {(failureHint?.confidence || "low") === "high"
                                        ? "Likely checks: "
                                        : (failureHint?.confidence || "low") === "medium"
                                          ? "Suggested checks: "
                                          : "Next step: "}
                                    </strong>
                                    {failureHint?.fix || liveJob.error_fix}
                                  </p>
                                )}
                                <dl className="df2-jobs-v3-failure-meta">
                                  {(liveJob.failed_at_phase || liveJob.phase) && (
                                    <div>
                                      <dt>Failed during</dt>
                                      <dd>
                                        {liveJob.failed_at_phase
                                          && !["failed", "cancelled"].includes(String(liveJob.failed_at_phase).toLowerCase())
                                          ? liveJob.failed_at_phase
                                          : (liveJob.phase === "failed" ? "load" : liveJob.phase)}
                                      </dd>
                                    </div>
                                  )}
                                  {(failureHint?.code || liveJob.error_code) && (
                                    <div>
                                      <dt>Error code</dt>
                                      <dd className="df2-mono">{failureHint?.code || liveJob.error_code}</dd>
                                    </div>
                                  )}
                                  {rejectedCount > 0 && (
                                    <div>
                                      <dt>Quarantined rows</dt>
                                      <dd>{rejectedCount.toLocaleString()} — validation findings isolated, not the load-stop cause unless noted above</dd>
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

                            {liveJob.cdc_lease_conflict && (
                              <CdcLeaseConflictPanel
                                job={liveJob}
                                onResume={
                                  liveJob.checkpoint || liveJob.chunk_current != null
                                    ? () => void handleResume()
                                    : undefined
                                }
                                onOpenJob={(jobId) => setSelectedId(jobId)}
                              />
                            )}

                            <CdcCursorGapPanel
                              job={liveJob}
                              onResume={() => void handleResume()}
                            />
                            <CdcRetentionPanel
                              status={liveJob.cdc_retention_status}
                              resume={liveJob.cdc_retention_resume}
                              retained={liveJob.cdc_retention_retained}
                              message={liveJob.cdc_retention_message}
                              dialect={liveJob.cdc_retention_dialect}
                              cursorKey={liveJob.cdc_lease_cursor_key}
                              onResume={
                                liveJob.checkpoint || liveJob.chunk_current != null
                                  ? () => void handleResume()
                                  : undefined
                              }
                              hideGap={Boolean(liveJob.cdc_cursor_gap)}
                            />
                            {(liveJob.cdc_plugin || liveJob.watermark || liveJob.cdc_delivery || liveJob.sync_mode === "cdc") && (
                              <CdcIncrementalSnapshotPanel jobId={selected._id} enabled />
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
                                    <span>Rows held out (quarantine)</span>
                                    <small>Not written to the primary table — not silently dropped or NULL-invented.</small>
                                  </article>
                                  <article className="df2-data-integrity-metric is-coerced">
                                    <strong>{(liveJob.coerced_null_rows ?? 0).toLocaleString()}</strong>
                                    <span>Values coerced to NULL</span>
                                    <small>coerce_null policy only — row kept with a NULL cell; original value not in primary.</small>
                                  </article>
                                </div>
                              </div>
                            )}

                            {selected.status === "failed" && (
                              <p className="df2-jobs-detail-actions-hint">
                                Recovery actions are pinned in the bar below this card.
                              </p>
                            )}
                          </div>
                        )}

                        {detailTab === "mapping" && (
                          <div className="df2-jobs-detail-pane">
                            <JobOverviewNote>
                              Mapping overview for this job. Open proof or the column table in a
                              right-side panel — do not scroll three evidence dumps in one pane.
                            </JobOverviewNote>
                            <div className="df2-drawer-facts df2-jobs-map-overview">
                              <div className="df2-drawer-fact">
                                <span>Columns</span>
                                <strong>{mappingCount.toLocaleString()}</strong>
                              </div>
                              <div className="df2-drawer-fact">
                                <span>Proof pairs</span>
                                <strong>
                                  {mappingProof?.summary?.mapped_count != null
                                    ? mappingProof.summary.mapped_count
                                    : mappingProof
                                      ? (mappingProof.mappings?.length ?? 0)
                                      : "—"}
                                </strong>
                              </div>
                              <div className="df2-drawer-fact">
                                <span>Risks / review</span>
                                <strong>
                                  {(mappingProof?.summary?.risk_count ?? 0)} / {(mappingProof?.summary?.review_count ?? 0)}
                                </strong>
                              </div>
                            </div>
                            <JobEvidenceLaunchGrid
                              label="Mapping evidence"
                              items={[
                                {
                                  id: "mapping-proof",
                                  title: "Mapping proof",
                                  description: "Confidence, fidelity, and per-column match evidence",
                                  icon: "layers",
                                  disabled: !mappingProof,
                                  onOpen: () => setEvidenceDrawer("mapping-proof"),
                                },
                                {
                                  id: "mapping-table",
                                  title: "Column map table",
                                  description: "Source → target → type for every mapped column",
                                  icon: "connectors",
                                  meta: mappingCount ? `${mappingCount} columns` : undefined,
                                  disabled: mappingCount === 0,
                                  onOpen: () => setEvidenceDrawer("mapping-table"),
                                },
                              ]}
                            />
                            {mappingCount === 0 && !mappingProof && (
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
                            <section className="df2-jobs-quarantine-overview" aria-label="Quarantine overview">
                              <JobOverviewNote>
                                Quarantine is a small slice of bad cells — not the whole transfer.
                              </JobOverviewNote>
                              <p className="df2-jobs-quarantine-meaning">
                                <strong>What “quarantined” means:</strong>{" "}
                                {(liveJob.records_processed ?? 0).toLocaleString()} row(s) were processed.
                                {" "}
                                {rejectedCount > 0
                                  ? `Only ${rejectedCount.toLocaleString()} finding(s) failed type/integrity checks and were isolated — they were not written as clean destination rows. Inspect details for source row #, column, value, and reason. The rest of the load succeeded.`
                                  : "No write-time quarantine count on this job yet — open details to inspect preflight / integrity findings."}
                                {" "}
                                Example: 37,000 transferred + 30 quarantined = 30 bad findings with reasons — not silent data loss.
                              </p>
                              <div className="df2-jobs-quarantine-metrics">
                                <article className="df2-jobs-quarantine-metric">
                                  <span>Processed</span>
                                  <strong>{(liveJob.records_processed ?? 0).toLocaleString()}</strong>
                                </article>
                                <article className={`df2-jobs-quarantine-metric${rejectedCount > 0 ? " is-warn" : ""}`}>
                                  <span>Quarantined</span>
                                  <strong>{rejectedCount.toLocaleString()}</strong>
                                </article>
                                <article className="df2-jobs-quarantine-metric">
                                  <span>Coerced to NULL</span>
                                  <strong>{Number(liveJob.coerced_null_rows ?? 0).toLocaleString()}</strong>
                                </article>
                              </div>
                            </section>
                          </div>
                        )}

                        {detailTab === "log" && (
                          <div className="df2-jobs-detail-pane">
                            <JobOverviewNote>
                              Event, DDL, and pipeline explanation open as right-side panels — one at a time.
                            </JobOverviewNote>
                            <JobEvidenceLaunchGrid
                              label="Logs"
                              items={[
                                {
                                  id: "event-log",
                                  title: "Event log",
                                  description: "Phase / message / row milestones",
                                  icon: "activity",
                                  meta: eventLog.length ? `${eventLog.length}` : "0",
                                  disabled: eventLog.length === 0,
                                  onOpen: () => setEvidenceDrawer("event-log"),
                                },
                                {
                                  id: "ddl-log",
                                  title: "DDL & stream",
                                  description: "CREATE/ALTER and stream progress",
                                  icon: "code",
                                  meta: ddlLog.length ? `${ddlLog.length}` : "0",
                                  disabled: ddlLog.length === 0,
                                  onOpen: () => setEvidenceDrawer("ddl-log"),
                                },
                                {
                                  id: "explanation",
                                  title: "Explanation",
                                  description: "Human-readable pipeline summary",
                                  icon: "book",
                                  meta: liveJob.explanation ? "Open" : undefined,
                                  disabled: !liveJob.explanation,
                                  onOpen: () => setEvidenceDrawer("explanation"),
                                },
                                {
                                  id: "streams",
                                  title: "Streams",
                                  description: "Per-stream dest COUNT(*) and watermarks",
                                  icon: "zap",
                                  meta: Array.isArray(liveJob.streams) && liveJob.streams.length
                                    ? `${liveJob.streams.length}`
                                    : undefined,
                                  disabled: !(Array.isArray(liveJob.streams) && liveJob.streams.length > 0),
                                  onOpen: () => setEvidenceDrawer("streams"),
                                },
                              ]}
                            />
                            {eventLog.length === 0 && ddlLog.length === 0 && !liveJob.explanation && (
                              <EmptyState
                                compact
                                icon="jobs"
                                title="No log yet"
                                description="Run a transfer to capture event and DDL logs. Older jobs may not have a durable log stored."
                              />
                            )}
                          </div>
                        )}
                      </div>

                      <div className="df2-jobs-detail-footer" role="navigation" aria-label="Job actions">
                        {detailTab === "detail" && selected.status === "failed" && (
                          <div className="df2-jobs-v3-actions">
                            {duplicateKeyFailure && onStartTransfer && (
                              <button
                                type="button"
                                className="df2-btn df2-btn-primary"
                                onClick={() => openMapInStudio()}
                              >
                                <DtIcon name="layers" size={16} /> Open Map · set primary key
                              </button>
                            )}
                            {liveJob?.checkpoint && (liveJob.checkpoint.rows_processed ?? 0) > 0 && !duplicateKeyFailure && (
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
                              className={
                                duplicateKeyFailure
                                  || (liveJob?.checkpoint && (liveJob.checkpoint.rows_processed ?? 0) > 0)
                                  ? "df2-btn"
                                  : "df2-btn df2-btn-primary"
                              }
                              onClick={() => void handleRetry()}
                              disabled={retrying}
                              title="Starts a new job and re-reads the source from the beginning. Use Resume to continue from the last committed batch."
                            >
                              {retrying
                                ? <ButtonLoader label="Retrying…" />
                                : (
                                  <><DtIcon name="transfer" size={16} /> Retry from start</>
                                )}
                            </button>
                            {onStartTransfer && (
                              <button
                                type="button"
                                className="df2-btn"
                                onClick={() => openValidateInStudio()}
                              >
                                Open Validate in Studio
                              </button>
                            )}
                          </div>
                        )}
                        {detailTab === "quarantine" && (
                          <div className="df2-jobs-quarantine-actions">
                            <Button
                              variant="primary"
                              onClick={() => setEvidenceDrawer("quarantine")}
                              leadingIcon={<DtIcon name="alert" size={14} />}
                            >
                              Inspect quarantine details
                            </Button>
                            <Button
                              variant="secondary"
                              onClick={() => openValidateInStudio()}
                              leadingIcon={<DtIcon name="gate" size={14} />}
                            >
                              Open Validate in Studio
                            </Button>
                          </div>
                        )}
                        {detailTab === "mapping" && onStartTransfer && (
                          <div className="df2-jobs-v3-actions">
                            <button
                              type="button"
                              className="df2-btn df2-btn-primary"
                              onClick={() => openMapInStudio()}
                            >
                              <DtIcon name="layers" size={16} /> Open Map in Studio
                            </button>
                            <button
                              type="button"
                              className="df2-btn"
                              onClick={() => openValidateInStudio()}
                            >
                              Open Validate in Studio
                            </button>
                          </div>
                        )}
                        {detailTab === "log" && onStartTransfer && (
                          <div className="df2-jobs-v3-actions">
                            <button
                              type="button"
                              className="df2-btn"
                              onClick={() => openValidateInStudio()}
                            >
                              Open Validate in Studio
                            </button>
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

      {liveJob && evidenceDrawer === "gate8" && recon && (
        <Drawer
          open
          onClose={() => setEvidenceDrawer(null)}
          title="Gate-8 reconcile"
          subtitle="Source vs destination row counts and content fingerprints"
          icon={<DtIcon name="shield" size={18} />}
          size="lg"
        >
          <Gate8ProofCard
            report={recon}
            explanation={liveJob.explanation}
            jobId={selectedId || liveJob._id}
            onOpenValidate={() => {
              setEvidenceDrawer(null);
              openValidateInStudio();
            }}
            onOpenQuarantine={
              showQuarantineTab
                ? () => {
                    setEvidenceDrawer(null);
                    setDetailTab("quarantine");
                  }
                : undefined
            }
          />
        </Drawer>
      )}

      {liveJob && evidenceDrawer === "preflight" && jobPreflight && (
        <Drawer
          open
          onClose={() => setEvidenceDrawer(null)}
          title="Validate / preflight"
          subtitle={
            jobPreflight.run_id
              ? `Run ${jobPreflight.run_id} · ${jobPreflight.passed_count}/${jobPreflight.total_gates} · ${jobPreflight.readiness_score}%`
              : `${jobPreflight.passed_count}/${jobPreflight.total_gates} gates · ${jobPreflight.readiness_score}% readiness`
          }
          icon={<DtIcon name="gate" size={18} />}
          size="lg"
        >
          <div className="df2-drawer-facts" style={{ marginBottom: 14 }}>
            <div className="df2-drawer-fact">
              <span>Decision</span>
              <strong>{jobPreflight.passed ? "Passed" : "Blocked"}</strong>
            </div>
            <div className="df2-drawer-fact">
              <span>Readiness</span>
              <strong>{jobPreflight.readiness_score}%</strong>
            </div>
            <div className="df2-drawer-fact">
              <span>Blockers</span>
              <strong>{jobPreflight.blockers?.length ?? 0}</strong>
            </div>
          </div>
          <div className="df2-jobs-log-table-wrap">
            <table className="df2-table">
              <thead>
                <tr>
                  <th>Gate</th>
                  <th>Status</th>
                  <th>Duration</th>
                  <th>Message</th>
                </tr>
              </thead>
              <tbody>
                {(jobPreflight.gates || []).map((g) => (
                  <tr key={g.id}>
                    <td className="df2-cell-mono">{g.id}</td>
                    <td>{g.status}</td>
                    <td className="df2-cell-mono">
                      {g.duration_ms != null && g.duration_ms > 0 ? `${g.duration_ms} ms` : "—"}
                    </td>
                    <td>{g.message || "—"}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
          {(jobPreflight.blockers?.length ?? 0) > 0 && (
            <ul className="df2-jobs-preflight-blockers">
              {jobPreflight.blockers.slice(0, 8).map((b) => (
                <li key={b.id}>
                  <strong title={b.id}>{blockerTitle(b.id, b.message)}</strong> — {b.message}
                  {b.guidance?.fix ? <span className="df2-muted"> Fix: {b.guidance.fix}</span> : null}
                </li>
              ))}
            </ul>
          )}
        </Drawer>
      )}

      {liveJob && evidenceDrawer === "writer" && (
        <Drawer
          open
          onClose={() => setEvidenceDrawer(null)}
          title="Writer & throughput"
          subtitle="Destination write path, batching, and prior-load comparison"
          icon={<DtIcon name="speed" size={18} />}
          size="lg"
        >
          <dl className="df2-jobs-v3-summary-dl">
            {writerRps != null && (
              <div><dt>Throughput</dt><dd>{Math.round(writerRps).toLocaleString()} rows/s</dd></div>
            )}
            {writerChunkSize != null && writerChunkSize > 0 && (
              <div><dt>Chunk size</dt><dd>{writerChunkSize.toLocaleString()}</dd></div>
            )}
            {(liveJob.chunk_current != null || liveJob.chunk_total != null) && (
              <div>
                <dt>Chunk progress</dt>
                <dd>
                  {liveJob.chunk_current != null ? liveJob.chunk_current.toLocaleString() : "—"}
                  {" / "}
                  {liveJob.chunk_total != null ? liveJob.chunk_total.toLocaleString() : "—"}
                </dd>
              </div>
            )}
            {liveJob.checkpoint && (
              <div>
                <dt>Checkpoint</dt>
                <dd>
                  batch {liveJob.checkpoint.chunk_index ?? 0}
                  {liveJob.checkpoint.rows_processed != null
                    ? ` · ${(liveJob.checkpoint.rows_processed ?? 0).toLocaleString()} rows committed`
                    : ""}
                  {liveJob.checkpoint.updated_at
                    ? ` · updated ${String(liveJob.checkpoint.updated_at)}`
                    : ""}
                  {" · at-least-once resume"}
                </dd>
              </div>
            )}
            {typeof destSummary.load_method === "string" && destSummary.load_method && (
              <div>
                <dt>Load method</dt>
                <dd title={loadMethodDescription(String(destSummary.load_method))}>
                  {loadMethodLabel(String(destSummary.load_method))}
                </dd>
              </div>
            )}
            {typeof destSummary.chunk_policy === "object" && destSummary.chunk_policy !== null ? (
              <div>
                <dt>Chunk policy</dt>
                <dd>
                  {String((destSummary.chunk_policy as { reason?: string }).reason || "adaptive")}
                  {" · final "}
                  {String((destSummary.chunk_policy as { final?: number }).final
                    ?? destSummary.chunk_size
                    ?? "—")}
                  {(destSummary.chunk_policy as { proxy_capped?: boolean }).proxy_capped
                    ? " · proxy-capped"
                    : ""}
                </dd>
              </div>
            ) : null}
            {typeof destSummary.database === "string" && destSummary.database && (
              <div><dt>Database</dt><dd>{String(destSummary.database)}</dd></div>
            )}
            {(typeof destSummary.table === "string" || typeof destSummary.collection === "string") && (
              <div>
                <dt>Table / collection</dt>
                <dd>{String(destSummary.table || destSummary.collection)}</dd>
              </div>
            )}
            {typeof destSummary.checksum === "string" && destSummary.checksum && (
              <div><dt>Writer checksum</dt><dd className="df2-mono">{String(destSummary.checksum)}</dd></div>
            )}
          </dl>
          <PhaseProfileCard
            profile={
              destSummary.phase_profile && typeof destSummary.phase_profile === "object"
                ? (destSummary.phase_profile as PhaseProfileReport)
                : null
            }
          />
          <ReplaySafetyCard
            report={
              destSummary.replay_safety && typeof destSummary.replay_safety === "object"
                ? (destSummary.replay_safety as ReplaySafetyReport)
                : null
            }
          />
          <ConnectionReuseCard
            report={
              destSummary.connection_reuse && typeof destSummary.connection_reuse === "object"
                ? (destSummary.connection_reuse as ConnectionReuseReport)
                : null
            }
            traceId={typeof destSummary.trace_id === "string" ? destSummary.trace_id : null}
            correlationId={
              typeof destSummary.correlation_id === "string" ? destSummary.correlation_id : null
            }
          />
          <TransformationsCard
            report={
              destSummary.transformations && typeof destSummary.transformations === "object"
                ? (destSummary.transformations as TransformationsReport)
                : null
            }
          />
          {Object.keys(destSummary).length > 0 && (
            <details className="df2-jobs-writer-raw">
              <summary>Raw destination summary</summary>
              <pre className="df2-jobs-writer-json">{JSON.stringify(destSummary, null, 2)}</pre>
            </details>
          )}
          {loadHistory && (
            <div style={{ marginTop: 16 }}>
              <LoadHistoryPanel report={loadHistory} title="Compared to prior loads" />
            </div>
          )}
        </Drawer>
      )}

      {liveJob && evidenceDrawer === "run-meta" && (
        <Drawer
          open
          onClose={() => setEvidenceDrawer(null)}
          title="Run metadata"
          subtitle={jobRouteLabel(liveJob)}
          icon={<DtIcon name="activity" size={18} />}
          size="lg"
        >
          <dl className="df2-jobs-v3-summary-dl df2-jobs-operator-meta">
            {triggeredBy && (
              <div><dt>Run by</dt><dd>{triggeredBy}</dd></div>
            )}
            {jobDuration && (
              <div><dt>Duration</dt><dd>{jobDuration}</dd></div>
            )}
            {liveJob.started_at && (
              <div><dt>Started</dt><dd>{new Date(liveJob.started_at).toLocaleString()}</dd></div>
            )}
            {liveJob.completed_at && (
              <div><dt>Completed</dt><dd>{new Date(liveJob.completed_at).toLocaleString()}</dd></div>
            )}
            {syncModeLabel !== "—" && (
              <div><dt>Sync mode</dt><dd>{syncModeLabel}</dd></div>
            )}
            {(liveJob.schema_policy || liveJob.transfer_request?.schema_policy) && (
              <div>
                <dt>Schema policy</dt>
                <dd>{formatSchemaPolicyLabel(liveJob.schema_policy || liveJob.transfer_request?.schema_policy)}</dd>
              </div>
            )}
            {(liveJob.validation_mode || liveJob.transfer_request?.validation_mode) && (
              <div>
                <dt>Validation</dt>
                <dd>{formatValidationModeLabel(liveJob.validation_mode || liveJob.transfer_request?.validation_mode)}</dd>
              </div>
            )}
            {liveJob.watermark && (
              <div><dt>CDC watermark</dt><dd className="df2-mono">{liveJob.watermark}</dd></div>
            )}
            {liveJob.cdc_plugin && (
              <div><dt>CDC plugin</dt><dd>{liveJob.cdc_plugin}</dd></div>
            )}
            {liveJob.cdc_row_filter && (
              <div>
                <dt>CDC row filter</dt>
                <dd className="df2-mono" title="SQL Server CDC TVF row_filter_option used for this run">
                  {liveJob.cdc_row_filter}
                </dd>
              </div>
            )}
            {liveJob.cdc_delivery && (
              <div><dt>CDC delivery</dt><dd>{liveJob.cdc_delivery}</dd></div>
            )}
            {liveJob.source_ha_role && (
              <div>
                <dt>Source HA</dt>
                <dd title={liveJob.source_ha_message || ""}>
                  {liveJob.source_ha_role}
                  {liveJob.source_ha_topology && liveJob.source_ha_topology !== "none"
                    ? ` · ${liveJob.source_ha_topology}`
                    : ""}
                  {liveJob.source_ha_group ? ` · ${liveJob.source_ha_group}` : ""}
                </dd>
              </div>
            )}
            {liveJob.cdc_shared_reader && (
              <div><dt>CDC topology</dt><dd>Shared log reader (one slot / server_id)</dd></div>
            )}
            {liveJob.snapshot_mode && (
              <div>
                <dt>Snapshot mode</dt>
                <dd>
                  {liveJob.snapshot_mode}
                  {liveJob.snapshot_plan?.lost_window ? " · lost window (not continuous CDC)" : ""}
                </dd>
              </div>
            )}
            {(liveJob.cdc_lease_holder || liveJob.cdc_lease_conflict) && (
              <div>
                <dt>CDC lease</dt>
                <dd>
                  {liveJob.cdc_lease_conflict
                    ? `Conflict — held by ${liveJob.cdc_lease_holder || "another worker"}`
                    : `${liveJob.cdc_lease_holder || "—"}${liveJob.cdc_lease_backend ? ` · ${liveJob.cdc_lease_backend}` : ""}${liveJob.cdc_lease_stale ? " (stale)" : ""}`}
                </dd>
              </div>
            )}
            {liveJob.cdc_lease_resource && (
              <div><dt>Lease resource</dt><dd className="df2-mono">{liveJob.cdc_lease_resource}</dd></div>
            )}
            {liveJob.cdc_lease_generation != null && (
              <div><dt>Lease generation</dt><dd>{liveJob.cdc_lease_generation}</dd></div>
            )}
            {liveJob.cdc_lease_cursor_key && (
              <div><dt>Lease cursor</dt><dd className="df2-mono">{liveJob.cdc_lease_cursor_key}</dd></div>
            )}
            {liveJob.message && (
              <div><dt>Latest message</dt><dd>{liveJob.message}</dd></div>
            )}
          </dl>
        </Drawer>
      )}

      {liveJob && evidenceDrawer === "timeline" && (
        <Drawer
          open
          onClose={() => setEvidenceDrawer(null)}
          title="Phase timeline"
          subtitle="Job phase history for this run"
          icon={<DtIcon name="clock" size={18} />}
          size="lg"
        >
          <JobTimeline entries={timelineEntries} />
        </Drawer>
      )}

      {liveJob && evidenceDrawer === "mapping-table" && (
        <Drawer
          open
          onClose={() => setEvidenceDrawer(null)}
          title="Column map"
          subtitle={`${mappingCount.toLocaleString()} columns`}
          icon={<DtIcon name="connectors" size={18} />}
          size="lg"
        >
          {mappingCount > 0 ? (
            <div className="df2-jobs-v3-mappings is-drawer">
              <table className="df2-table">
                <thead>
                  <tr>
                    <th>Source</th>
                    <th>Target</th>
                    <th>Type</th>
                    <th>Confidence</th>
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
                        const conf = typeof m.confidence === "number"
                          ? `${Math.round(m.confidence * 100)}%`
                          : "—";
                        return (
                          <tr key={`${src}-${tgt}-${i}`}>
                            <td title={src}>{src}</td>
                            <td title={tgt}>{tgt}</td>
                            <td className="df2-cell-mono" title={typ}>{typ}</td>
                            <td className="df2-cell-mono">{conf}</td>
                          </tr>
                        );
                      })
                    : Object.entries(columnTypes).map(([col, typ]) => (
                        <tr key={col}>
                          <td title={col}>{col}</td>
                          <td title={col}>{col}</td>
                          <td className="df2-cell-mono" title={String(typ)}>{String(typ)}</td>
                          <td className="df2-cell-mono">—</td>
                        </tr>
                      ))}
                </tbody>
              </table>
            </div>
          ) : (
            <p className="df2-muted">No column mappings recorded on this job.</p>
          )}
        </Drawer>
      )}

      {liveJob && mappingProof && (evidenceDrawer === "mapping-proof" || mappingProofOpen) && (
        <MappingProofDrawer
          open
          onClose={() => {
            setEvidenceDrawer(null);
            setMappingProofOpen(false);
          }}
          proof={mappingProof}
          sourceLabel={liveJob.source_name}
          destLabel={
            liveJob.destination_collection
            || liveJob.destination_database
            || liveJob.destination_type
          }
        />
      )}

      {liveJob && evidenceDrawer === "event-log" && (
        <Drawer
          open
          onClose={() => setEvidenceDrawer(null)}
          title="Event log"
          subtitle={`${eventLog.length.toLocaleString()} durable operator events`}
          icon={<DtIcon name="activity" size={18} />}
          size="lg"
        >
          <JobLogTable lines={eventLog} empty="No events yet" />
        </Drawer>
      )}

      {liveJob && evidenceDrawer === "ddl-log" && (
        <Drawer
          open
          onClose={() => setEvidenceDrawer(null)}
          title="DDL & stream log"
          subtitle={`${ddlLog.length.toLocaleString()} schema / stream lines`}
          icon={<DtIcon name="code" size={18} />}
          size="lg"
        >
          <JobLogTable lines={ddlLog.map(String)} empty="No DDL lines" />
        </Drawer>
      )}

      {liveJob && evidenceDrawer === "explanation" && liveJob.explanation && (
        <Drawer
          open
          onClose={() => setEvidenceDrawer(null)}
          title="Pipeline explanation"
          subtitle="Plain-language summary from the transfer engine"
          icon={<DtIcon name="book" size={18} />}
          size="lg"
        >
          <JobExplanationView text={liveJob.explanation} />
        </Drawer>
      )}

      {liveJob && evidenceDrawer === "streams" && Array.isArray(liveJob.streams) && (
        <Drawer
          open
          onClose={() => setEvidenceDrawer(null)}
          title="Stream conservation"
          subtitle={`${liveJob.streams.length} stream(s)`}
          icon={<DtIcon name="zap" size={18} />}
          size="lg"
        >
          <table className="df2-table df2-jobs-cdc-table">
            <thead>
              <tr>
                <th>Stream</th>
                <th>Status</th>
                <th>Conserved</th>
                <th>Events written</th>
                <th>Lag</th>
                <th>Watermark</th>
              </tr>
            </thead>
            <tbody>
              {liveJob.streams.map((s) => (
                <tr key={s.name}>
                  <td>{s.name}</td>
                  <td>{s.status || "—"}</td>
                  <td>
                    {destMetricCompact(
                      destHeadline({
                        status: s.status,
                        records_processed: s.records_processed,
                        row_accounting: s.row_accounting,
                      }),
                    )}
                  </td>
                  <td>{Number(s.records_processed ?? 0).toLocaleString()}</td>
                  <td>
                    {s.cdc_lag_seconds != null && Number.isFinite(Number(s.cdc_lag_seconds))
                      ? `${Number(s.cdc_lag_seconds).toFixed(1)}s`
                      : "—"}
                  </td>
                  <td className="df2-cell-mono" title={s.watermark || ""}>
                    {s.watermark
                      ? `${String(s.watermark).slice(0, 40)}${String(s.watermark).length > 40 ? "…" : ""}`
                      : "—"}
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </Drawer>
      )}

      {liveJob && selectedId && evidenceDrawer === "quarantine" && (
        <Drawer
          open
          onClose={() => setEvidenceDrawer(null)}
          title="Quarantine details"
          subtitle={
            rejectedCount > 0
              ? `${rejectedCount.toLocaleString()} finding(s) — bad cells isolated; other rows already transferred`
              : "Inspect findings, propose repair, or promote / replay"
          }
          icon={<DtIcon name="alert" size={18} />}
          size="lg"
        >
          <QuarantinePanel
            jobId={selectedId}
            rejectedRows={liveJob.rejected_rows}
            coercedNullRows={liveJob.coerced_null_rows}
            initialDetails={Array.isArray(liveJob.rejected_details) ? liveJob.rejected_details : undefined}
            truncatedDetails={Math.max(
              0,
              Number(
                liveJob.rejected_details_total
                  ?? (liveJob.destination_summary as { rejected_details_total?: number } | undefined)
                    ?.rejected_details_total
                  ?? 0,
              ) - (Array.isArray(liveJob.rejected_details) ? liveJob.rejected_details.length : 0),
            )}
            autoLoad
            initiallyOpen
            repairMappings={jobRepairMappings}
            onOpenValidate={() => {
              setEvidenceDrawer(null);
              openValidateInStudio();
            }}
            onRepairDecided={(proposal) => {
              setEvidenceDrawer(null);
              if (proposal.status === "rejected") return;
              const applied = proposal.apply_result?.mappings;
              const maps = Array.isArray(applied) && applied.length
                ? (applied as RepairMapping[])
                : jobRepairMappings;
              // Only reopen the drawer when the proposal can still be decided/applied.
              const reopen =
                proposal.status === "proposed"
                || (proposal.status === "approved" && maps.length > 0);
              // Closed-loop: always land on Validate so operator re-runs G1–G9 after repair.
              openValidateInStudio({
                repairProposalId: reopen ? proposal.id : undefined,
                mappings: maps,
                step: "validate",
              });
            }}
            onReplayComplete={(childJobId) => {
              void onRefresh?.();
              if (childJobId) {
                // Open the child job so Gate-8 / trust score from replay are visible.
                setSelectedId(childJobId);
                setEvidenceDrawer(null);
              }
            }}
          />
        </Drawer>
      )}
    </PageShell>
  );
}
