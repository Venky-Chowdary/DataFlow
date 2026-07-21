import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import { SectionLoader } from "../components/LoadingState";
import { Button } from "../components/ui/Button";
import { EmptyState } from "../components/ui/EmptyState";
import { PipelineCard } from "../components/ui/PipelineCard";
import { PageFrame } from "../components/ui/PageFrame";
import { FilterBar } from "../components/ui/FilterBar";
import { FilterTabs } from "../components/ui/FilterTabs";
import { PageSection } from "../components/ui/PageSection";
import { PageShell } from "../components/ui/PageShell";
import { PageContextBar } from "../components/ui/PageContextBar";
import { PageToolbar } from "../components/ui/PageToolbar";
import { ScheduleForm } from "../components/schedules/ScheduleForm";
import {
  PIPELINE_TABS,
  PipelineDetailDrawer,
  type PipelineTab,
} from "../components/PipelineDetailDrawer";
import { useToast } from "../components/Toast";
import { useConfirm } from "../components/ui/ConfirmDialog";
import { formatRelativeTime } from "../lib/connectionWorkbench";
import {
  applyGitopsManifest,
  createSchedule,
  deleteSchedule,
  exportDataflowManifest,
  exportScheduleYaml,
  fetchContractBreaker,
  fetchOpsFreshness,
  fetchScheduleIntervals,
  fetchSchedules,
  planGitopsManifest,
  resetContractBreaker,
  runScheduleNow,
  updateSchedule,
} from "../lib/api";
import { breakerBlocksRuns } from "../lib/contractBreakerUi";
import { Connector, PipelineSchedule, ScheduleInput, ScheduleIntervals } from "../lib/types";

interface SchedulesPageProps {
  connectors: Connector[];
  onViewJobs?: () => void;
  onOpenJob?: (jobId: string) => void;
  onSchedulesChange?: () => void | Promise<void>;
  highlightScheduleId?: string;
}

type ScheduleFilter = "all" | "active" | "paused";

export function SchedulesPage({ connectors, onViewJobs, onOpenJob, onSchedulesChange, highlightScheduleId }: SchedulesPageProps) {
  const { toast } = useToast();
  const { confirm } = useConfirm();
  const [schedules, setSchedules] = useState<PipelineSchedule[]>([]);
  const [intervals, setIntervals] = useState<ScheduleIntervals | null>(null);
  const [loading, setLoading] = useState(true);
  const [showForm, setShowForm] = useState(false);
  const [editing, setEditing] = useState<PipelineSchedule | null>(null);
  const [saving, setSaving] = useState(false);
  const [runningId, setRunningId] = useState<string | null>(null);
  const [selectedId, setSelectedId] = useState<string | null>(null);
  const [drawerOpen, setDrawerOpen] = useState(false);
  const [resumeDrawerAfterEdit, setResumeDrawerAfterEdit] = useState(false);
  const [pipelineTab, setPipelineTab] = useState<PipelineTab>(PIPELINE_TABS[0]);
  const [filter, setFilter] = useState<ScheduleFilter>("all");
  const [pipelineSearch, setPipelineSearch] = useState("");
  const [breakers, setBreakers] = useState<Record<string, string>>({});
  /** schedule_id -> worst lag seconds when stale/critical */
  const [freshnessLag, setFreshnessLag] = useState<Record<string, { lag: number; severity: string }>>({});

  const loadBreakers = useCallback(async (rows: PipelineSchedule[]) => {
    const ids = [...new Set(rows.map((s) => s.contract_id).filter(Boolean))] as string[];
    if (ids.length === 0) {
      setBreakers({});
      return;
    }
    const map: Record<string, string> = {};
    await Promise.all(
      ids.slice(0, 40).map(async (id) => {
        try {
          const b = await fetchContractBreaker(id);
          map[id] = b.state;
        } catch {
          /* breaker optional */
        }
      }),
    );
    setBreakers(map);
  }, []);

  const loadFreshness = useCallback(async () => {
    try {
      const f = await fetchOpsFreshness(60);
      const map: Record<string, { lag: number; severity: string }> = {};
      for (const p of f.pipelines || []) {
        if (!p.schedule_id || p.schedule_id === "_") continue;
        if (!(p.stale || p.severity === "warn" || p.severity === "critical")) continue;
        const prev = map[p.schedule_id];
        const sev = p.severity || (p.stale ? "warn" : "ok");
        if (!prev || p.lag_seconds > prev.lag) {
          map[p.schedule_id] = { lag: p.lag_seconds, severity: sev };
        }
      }
      setFreshnessLag(map);
    } catch {
      setFreshnessLag({});
    }
  }, []);

  const load = useCallback(async () => {
    setLoading(true);
    try {
      const rows = await fetchSchedules();
      setSchedules(rows);
      void loadBreakers(rows);
      void loadFreshness();
    } catch (e) {
      console.error(e);
    }
    setLoading(false);
  }, [loadBreakers, loadFreshness]);

  useEffect(() => {
    load();
  }, [load]);

  useEffect(() => {
    fetchScheduleIntervals()
      .then(setIntervals)
      .catch((e) => console.error(e));
  }, []);

  useEffect(() => {
    if (!highlightScheduleId || loading) return;
    setSelectedId(highlightScheduleId);
    setPipelineTab("Overview");
    setDrawerOpen(true);
    window.requestAnimationFrame(() => {
      document.getElementById(`pipeline-card-${highlightScheduleId}`)?.scrollIntoView({
        behavior: "smooth",
        block: "nearest",
      });
    });
  }, [highlightScheduleId, loading, schedules.length]);

  useEffect(() => {
    if (!selectedId) return;
    if (!schedules.some((s) => s.id === selectedId)) {
      setSelectedId(null);
      setDrawerOpen(false);
    }
  }, [schedules, selectedId]);

  const selectedSchedule = schedules.find((s) => s.id === selectedId) ?? null;

  const openDrawer = (id: string, tab: PipelineTab = "Overview") => {
    setSelectedId(id);
    setPipelineTab(tab);
    setDrawerOpen(true);
  };

  const closeDrawer = () => setDrawerOpen(false);

  const enabledCount = schedules.filter((s) => s.enabled).length;
  const pausedCount = schedules.length - enabledCount;
  const runningCount = schedules.filter((s) => s.running).length;
  const lastRunAt = useMemo(() => {
    const times = schedules
      .map((s) => (s.last_run_at ? new Date(s.last_run_at).getTime() : 0))
      .filter((t) => t > 0);
    return times.length ? Math.max(...times) : null;
  }, [schedules]);
  const filteredSchedules = useMemo(() => {
    let list = schedules;
    if (filter === "active") list = schedules.filter((s) => s.enabled);
    else if (filter === "paused") list = schedules.filter((s) => !s.enabled);
    const q = pipelineSearch.trim().toLowerCase();
    if (!q) return list;
    return list.filter((s) =>
      [s.name, s.source_table, s.dest_table, s.interval, s.sync_mode, s.cron]
        .filter(Boolean)
        .some((v) => String(v).toLowerCase().includes(q)),
    );
  }, [schedules, filter, pipelineSearch]);

  const openCreate = () => {
    setResumeDrawerAfterEdit(false);
    setEditing(null);
    setShowForm(true);
  };

  const openEdit = (sched: PipelineSchedule) => {
    setEditing(sched);
    setShowForm(true);
    window.requestAnimationFrame(() => {
      document.querySelector(".df2-pipeline-form")?.scrollIntoView({ behavior: "smooth", block: "start" });
    });
  };

  const closeForm = () => {
    setShowForm(false);
    setEditing(null);
    if (resumeDrawerAfterEdit && selectedId && schedules.some((s) => s.id === selectedId)) {
      setResumeDrawerAfterEdit(false);
      setDrawerOpen(true);
      return;
    }
    setResumeDrawerAfterEdit(false);
  };

  const handleSubmit = async (input: Partial<ScheduleInput>) => {
    setSaving(true);
    try {
      if (editing) {
        await updateSchedule(editing.id, input);
        toast({ title: "Pipeline updated", message: `"${input.name ?? editing.name}" saved.`, tone: "success" });
      } else {
        await createSchedule(input as ScheduleInput);
        toast({ title: "Pipeline created", message: `"${input.name}" is scheduled.`, tone: "success" });
      }
      closeForm();
      await load();
      void onSchedulesChange?.();
    } catch (err) {
      toast({
        title: editing ? "Could not update pipeline" : "Could not create pipeline",
        message: err instanceof Error ? err.message : undefined,
        tone: "error",
      });
      console.error(err);
    }
    setSaving(false);
  };

  const toggleEnabled = async (sched: PipelineSchedule) => {
    try {
      await updateSchedule(sched.id, { enabled: !sched.enabled });
      await load();
      void onSchedulesChange?.();
      toast({ title: sched.enabled ? "Pipeline paused" : "Pipeline activated", tone: "success" });
    } catch (e) {
      toast({ title: "Update failed", tone: "error" });
      console.error(e);
    }
  };

  const handleDelete = async (id: string) => {
    const target = schedules.find((s) => s.id === id);
    const ok = await confirm({
      title: target ? `Delete pipeline “${target.name}”?` : "Delete this pipeline?",
      message: "This cannot be undone. Scheduled runs for this pipeline will stop.",
      confirmLabel: "Delete pipeline",
      cancelLabel: "Keep pipeline",
      tone: "danger",
    });
    if (!ok) return;
    try {
      await deleteSchedule(id);
      if (selectedId === id) {
        setSelectedId(null);
        setDrawerOpen(false);
      }
      await load();
      void onSchedulesChange?.();
      toast({ title: "Pipeline deleted", tone: "success" });
    } catch (e) {
      toast({ title: "Delete failed", tone: "error" });
      console.error(e);
    }
  };

  const handleRunNow = async (id: string) => {
    const sched = schedules.find((s) => s.id === id);
    const contractId = sched?.contract_id;
    if (contractId && breakerBlocksRuns(breakers[contractId])) {
      toast({
        title: "Contract breaker is open",
        message: "Reset the breaker on this pipeline (or Contracts) before running.",
        tone: "error",
      });
      return;
    }
    setRunningId(id);
    try {
      await runScheduleNow(id);
      await load();
      onViewJobs?.();
      toast({ title: "Pipeline run started", message: "Track progress in Job Theater.", tone: "success" });
    } catch (e) {
      toast({
        title: "Run failed",
        message: e instanceof Error ? e.message : undefined,
        tone: "error",
      });
      console.error(e);
    }
    setRunningId(null);
  };

  const handleResetBreaker = async (contractId: string) => {
    try {
      const b = await resetContractBreaker(contractId);
      setBreakers((prev) => ({ ...prev, [contractId]: b.state }));
      toast({ title: "Breaker reset", message: `Contract breaker is now ${b.state}.`, tone: "success" });
    } catch (e) {
      toast({
        title: "Could not reset breaker",
        message: e instanceof Error ? e.message : undefined,
        tone: "error",
      });
    }
  };

  const importInputRef = useRef<HTMLInputElement>(null);
  const [gitopsBusy, setGitopsBusy] = useState(false);
  const [gitopsRequireSigned, setGitopsRequireSigned] = useState(false);

  const downloadBlob = (blob: Blob, filename: string) => {
    const url = URL.createObjectURL(blob);
    const a = document.createElement("a");
    a.href = url;
    a.download = filename;
    a.rel = "noopener";
    document.body.appendChild(a);
    a.click();
    a.remove();
    window.setTimeout(() => URL.revokeObjectURL(url), 1500);
  };

  const handleExportFleet = async () => {
    setGitopsBusy(true);
    try {
      const blob = await exportDataflowManifest();
      downloadBlob(blob, "dataflow.yaml");
      toast({
        title: "Exported dataflow.yaml",
        message: "Schedules + contracts — review in git before apply.",
        tone: "success",
      });
    } catch (e) {
      toast({
        title: "Export failed",
        message: e instanceof Error ? e.message : undefined,
        tone: "error",
      });
    } finally {
      setGitopsBusy(false);
    }
  };

  const handleExportOne = async (id: string) => {
    setGitopsBusy(true);
    try {
      const blob = await exportScheduleYaml(id);
      downloadBlob(blob, `schedule-${id}.yaml`);
      toast({ title: "Pipeline YAML exported", tone: "success" });
    } catch (e) {
      toast({
        title: "Export failed",
        message: e instanceof Error ? e.message : undefined,
        tone: "error",
      });
    } finally {
      setGitopsBusy(false);
    }
  };

  const handleImportFile = async (file: File) => {
    setGitopsBusy(true);
    try {
      const text = await file.text();
      const payload = { yaml: text };
      const plan = await planGitopsManifest(payload);
      const ok = await confirm({
        title: "Apply GitOps manifest?",
        message: gitopsRequireSigned
          ? `Plan: ${plan.creates} create · ${plan.updates} update · ${plan.skips} skip (${plan.resource_count} resources).\n\nSigned-contract gate ON: every schedule must reference a SIGNED contract. Contracts in this file still import as DRAFT.`
          : `Plan: ${plan.creates} create · ${plan.updates} update · ${plan.skips} skip (${plan.resource_count} resources). Contracts import as DRAFT — sign before require-signed schedules run.`,
        confirmLabel: gitopsRequireSigned ? "Apply (signed gate)" : "Apply",
        cancelLabel: "Cancel",
        tone: "danger",
      });
      if (!ok) return;
      const applied = await applyGitopsManifest(payload, false, {
        requireSignedContracts: gitopsRequireSigned,
      });
      await load();
      void onSchedulesChange?.();
      toast({
        title: "GitOps apply complete",
        message: `${applied.applied ?? 0} applied · ${applied.failed ?? 0} failed${
          gitopsRequireSigned ? " · signed-contract gate" : ""
        }`,
        tone: (applied.failed ?? 0) > 0 ? "error" : "success",
      });
    } catch (e) {
      toast({
        title: "Import failed",
        message: e instanceof Error ? e.message : undefined,
        tone: "error",
      });
    } finally {
      setGitopsBusy(false);
    }
  };

  return (
    <PageShell
      wide
      className="df2-page-pipelines"
      title="Pipelines"
      description="Schedule recurring syncs with the same governed transfer engine."
    >
      <PageFrame className="df2-pipeline-page">
      {!loading && schedules.length > 0 && (
        <PageContextBar
          ariaLabel="Pipelines summary"
          stats={[
            { label: "Pipelines", value: schedules.length, icon: "activity" },
            { label: "Active", value: enabledCount, icon: "check", tone: enabledCount > 0 ? "ok" : "muted" },
            { label: "Running", value: runningCount, icon: "activity", tone: runningCount > 0 ? "ok" : "muted", title: "Runs currently in progress" },
            { label: "Paused", value: pausedCount, icon: "pause", tone: pausedCount > 0 ? "warn" : "muted" },
            {
              label: "Last run",
              value: lastRunAt ? formatRelativeTime(new Date(lastRunAt).toISOString()) : "—",
              icon: "clock",
              tone: "muted",
              title: "Most recent scheduled run across all pipelines",
            },
          ]}
        />
      )}
      {!loading && (
        <PageToolbar
          className={showForm ? "df2-toolbar--creating" : ""}
          searchValue={schedules.length > 0 ? pipelineSearch : undefined}
          onSearchChange={schedules.length > 0 && !showForm ? setPipelineSearch : undefined}
          searchPlaceholder="Search pipelines by name, table, cadence, or sync mode…"
          filters={
            schedules.length > 0 ? (
              <FilterBar variant="inline" ariaLabel="Filter pipelines">
                {showForm ? (
                  <span className="df2-toolbar-status" role="status">
                    {editing ? "Editing pipeline" : "Creating pipeline"}
                  </span>
                ) : null}
                <FilterTabs
                  ariaLabel="Filter pipelines"
                  value={filter}
                  onChange={setFilter}
                  disabled={showForm}
                  items={[
                    { id: "all", label: "All", count: schedules.length },
                    { id: "active", label: "Active", count: enabledCount },
                    { id: "paused", label: "Paused", count: pausedCount },
                  ]}
                />
              </FilterBar>
            ) : showForm ? (
              <span className="df2-toolbar-status" role="status">
                Creating your first pipeline
              </span>
            ) : undefined
          }
          actions={
            !showForm ? (
              <>
                <input
                  ref={importInputRef}
                  type="file"
                  accept=".yaml,.yml,.json,application/x-yaml,text/yaml,application/json"
                  hidden
                  onChange={(e) => {
                    const f = e.target.files?.[0];
                    e.target.value = "";
                    if (f) void handleImportFile(f);
                  }}
                />
                <Button
                  size="sm"
                  variant="ghost"
                  loading={gitopsBusy}
                  onClick={() => void handleExportFleet()}
                  title="Export schedules + contracts as dataflow.yaml"
                >
                  Export YAML
                </Button>
                <label
                  className="df2-policy-toggle"
                  title="CD/staging: refuse schedules unless contract_id is SIGNED"
                  style={{ display: "inline-flex", alignItems: "center", gap: "0.35rem", margin: 0 }}
                >
                  <input
                    type="checkbox"
                    checked={gitopsRequireSigned}
                    onChange={(e) => setGitopsRequireSigned(e.target.checked)}
                  />
                  <span style={{ fontSize: "0.8rem", whiteSpace: "nowrap" }}>Require signed</span>
                </label>
                <Button
                  size="sm"
                  variant="ghost"
                  loading={gitopsBusy}
                  onClick={() => importInputRef.current?.click()}
                  title={
                    gitopsRequireSigned
                      ? "Plan then apply with signed-contract gate"
                      : "Plan then apply a dataflow.yaml manifest"
                  }
                >
                  Import YAML
                </Button>
                <Button size="sm" variant="primary" onClick={openCreate}>
                  New pipeline
                </Button>
              </>
            ) : undefined
          }
        />
      )}

      {showForm && (
        <div className="df2-pipeline-form is-active">
          <PageSection
            title={editing ? "Edit pipeline" : "Create recurring sync"}
            subtitle={editing ? editing.name : "Schedule source → destination with your saved connectors"}
            className="df2-pipeline-form-card"
            actions={
              <button type="button" className="df2-btn df2-btn-ghost df2-btn-sm" onClick={closeForm}>
                Cancel
              </button>
            }
          >
            <ScheduleForm
              key={editing?.id ?? "new"}
              connectors={connectors}
              intervals={intervals}
              initial={editing}
              saving={saving}
              onSubmit={handleSubmit}
              onCancel={closeForm}
            />
          </PageSection>
        </div>
      )}

      <div className="df2-pipeline-workspace">
      {loading ? (
        <SectionLoader title="Loading pipelines" hint="Fetching scheduled syncs…" />
      ) : showForm && schedules.length === 0 ? null : (
      <div className="df2-pipeline-list df2-pipeline-scroll">
        {schedules.length === 0 ? (
          <EmptyState
            page
            icon="activity"
            title="No scheduled pipelines"
            description="Create a recurring sync to keep source and destination in step — watermark incremental, upsert, and quarantine included."
            action={
              !showForm ? (
                <Button variant="primary" onClick={openCreate}>
                  Create pipeline
                </Button>
              ) : undefined
            }
          />
        ) : filteredSchedules.length === 0 ? (
          <EmptyState
            compact
            icon="activity"
            title={`No ${filter === "active" ? "active" : "paused"} pipelines`}
            description="Try another filter or create a new pipeline."
          />
        ) : (
          <div className="df2-pipeline-rows" role="list" aria-label="Scheduled pipelines">
            <div className="df2-pipeline-rows-head" aria-hidden>
              <span className="df2-pipeline-rows-head-name">Pipeline</span>
              <span>Cadence</span>
              <span>Mode</span>
              <span>Last run</span>
              <span>Status</span>
              <span />
            </div>
            {filteredSchedules.map((sched) => (
              <PipelineCard
                key={sched.id}
                compact
                schedule={sched}
                source={connectors.find((c) => c.id === sched.source_connector_id)}
                dest={connectors.find((c) => c.id === sched.dest_connector_id)}
                running={runningId === sched.id}
                highlighted={highlightScheduleId === sched.id}
                selected={drawerOpen && selectedId === sched.id}
                breakerState={sched.contract_id ? breakers[sched.contract_id] : null}
                freshnessLagSeconds={freshnessLag[sched.id]?.lag ?? null}
                freshnessSeverity={freshnessLag[sched.id]?.severity ?? null}
                onSelect={() => openDrawer(sched.id)}
              />
            ))}
          </div>
        )}
      </div>
      )}
      </div>

      <PipelineDetailDrawer
        open={drawerOpen && Boolean(selectedSchedule)}
        schedule={selectedSchedule}
        source={
          selectedSchedule
            ? connectors.find((c) => c.id === selectedSchedule.source_connector_id)
            : undefined
        }
        dest={
          selectedSchedule
            ? connectors.find((c) => c.id === selectedSchedule.dest_connector_id)
            : undefined
        }
        tab={pipelineTab}
        setTab={setPipelineTab}
        running={selectedSchedule ? runningId === selectedSchedule.id : false}
        breakerHint={
          selectedSchedule?.contract_id
            ? breakers[selectedSchedule.contract_id] ?? null
            : null
        }
        onClose={closeDrawer}
        onRun={() => selectedSchedule && void handleRunNow(selectedSchedule.id)}
        onEdit={() => {
          if (!selectedSchedule) return;
          setResumeDrawerAfterEdit(true);
          closeDrawer();
          openEdit(selectedSchedule);
        }}
        onDelete={() => selectedSchedule && void handleDelete(selectedSchedule.id)}
        onToggle={() => selectedSchedule && void toggleEnabled(selectedSchedule)}
        onResetBreaker={handleResetBreaker}
        onExportYaml={
          selectedSchedule
            ? () => void handleExportOne(selectedSchedule.id)
            : undefined
        }
        onOpenJob={(jobId) => {
          setResumeDrawerAfterEdit(false);
          closeDrawer();
          onOpenJob?.(jobId);
        }}
      />
      </PageFrame>
    </PageShell>
  );
}
