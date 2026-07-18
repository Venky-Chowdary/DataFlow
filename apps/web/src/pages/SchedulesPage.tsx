import { useCallback, useEffect, useMemo, useState } from "react";
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
import { ScheduleRunHistory } from "../components/schedules/ScheduleRunHistory";
import { useToast } from "../components/Toast";
import { formatRelativeTime } from "../lib/connectionWorkbench";
import {
  createSchedule,
  deleteSchedule,
  fetchScheduleIntervals,
  fetchSchedules,
  runScheduleNow,
  updateSchedule,
} from "../lib/api";
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
  const [schedules, setSchedules] = useState<PipelineSchedule[]>([]);
  const [intervals, setIntervals] = useState<ScheduleIntervals | null>(null);
  const [loading, setLoading] = useState(true);
  const [showForm, setShowForm] = useState(false);
  const [editing, setEditing] = useState<PipelineSchedule | null>(null);
  const [saving, setSaving] = useState(false);
  const [runningId, setRunningId] = useState<string | null>(null);
  const [historyId, setHistoryId] = useState<string | null>(null);
  const [filter, setFilter] = useState<ScheduleFilter>("all");
  const [pipelineSearch, setPipelineSearch] = useState("");

  const load = useCallback(async () => {
    setLoading(true);
    try {
      setSchedules(await fetchSchedules());
    } catch (e) {
      console.error(e);
    }
    setLoading(false);
  }, []);

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
    window.requestAnimationFrame(() => {
      document.getElementById(`pipeline-card-${highlightScheduleId}`)?.scrollIntoView({
        behavior: "smooth",
        block: "nearest",
      });
    });
  }, [highlightScheduleId, loading, schedules.length]);

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
    const ok = window.confirm(
      target
        ? `Delete pipeline “${target.name}”? This cannot be undone.`
        : "Delete this pipeline? This cannot be undone.",
    );
    if (!ok) return;
    try {
      await deleteSchedule(id);
      await load();
      void onSchedulesChange?.();
      toast({ title: "Pipeline deleted", tone: "success" });
    } catch (e) {
      toast({ title: "Delete failed", tone: "error" });
      console.error(e);
    }
  };

  const handleRunNow = async (id: string) => {
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
              <Button size="sm" variant="primary" onClick={openCreate}>
                New pipeline
              </Button>
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
      <div className="df2-pipeline-grid df2-pipeline-scroll">
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
          filteredSchedules.map((sched) => (
            <PipelineCard
              key={sched.id}
              schedule={sched}
              source={connectors.find((c) => c.id === sched.source_connector_id)}
              dest={connectors.find((c) => c.id === sched.dest_connector_id)}
              running={runningId === sched.id}
              highlighted={highlightScheduleId === sched.id}
              historyOpen={historyId === sched.id}
              onToggle={() => void toggleEnabled(sched)}
              onRun={() => void handleRunNow(sched.id)}
              onEdit={() => openEdit(sched)}
              onDelete={() => void handleDelete(sched.id)}
              onToggleHistory={() => setHistoryId((cur) => (cur === sched.id ? null : sched.id))}
            >
              {historyId === sched.id && (
                <ScheduleRunHistory scheduleId={sched.id} onOpenJob={onOpenJob} />
              )}
            </PipelineCard>
          ))
        )}
      </div>
      )}
      </div>
      </PageFrame>
    </PageShell>
  );
}
