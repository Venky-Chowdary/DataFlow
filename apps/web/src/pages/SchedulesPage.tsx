import { FormEvent, useCallback, useEffect, useMemo, useState } from "react";
import { DtIcon } from "../components/DtIcon";
import { ButtonLoader, SectionLoader } from "../components/LoadingState";
import { ConnectorSelect } from "../components/ui/ConnectorSelect";
import { CadenceTiles } from "../components/ui/CadenceTiles";
import { EmptyState } from "../components/EmptyState";
import { PipelineCard } from "../components/ui/PipelineCard";
import { PageFrame } from "../components/ui/PageFrame";
import { PageInsightStrip } from "../components/ui/PageInsightStrip";
import { FilterTabs } from "../components/ui/FilterTabs";
import { PageMetricsRow } from "../components/ui/PageMetricsRow";
import { PageSection } from "../components/ui/PageSection";
import { PageShell } from "../components/ui/PageShell";
import { PageToolbar } from "../components/ui/PageToolbar";
import { useToast } from "../components/Toast";
import {
  createSchedule,
  deleteSchedule,
  fetchSchedules,
  runScheduleNow,
  updateSchedule,
} from "../lib/api";
import { Connector, PipelineSchedule } from "../lib/types";

interface SchedulesPageProps {
  connectors: Connector[];
  onViewJobs?: () => void;
  onSchedulesChange?: () => void | Promise<void>;
  highlightScheduleId?: string;
}

type ScheduleFilter = "all" | "active" | "paused";

export function SchedulesPage({ connectors, onViewJobs, onSchedulesChange, highlightScheduleId }: SchedulesPageProps) {
  const { toast } = useToast();
  const [schedules, setSchedules] = useState<PipelineSchedule[]>([]);
  const [loading, setLoading] = useState(true);
  const [showForm, setShowForm] = useState(false);
  const [saving, setSaving] = useState(false);
  const [runningId, setRunningId] = useState<string | null>(null);
  const [filter, setFilter] = useState<ScheduleFilter>("all");
  const [pipelineSearch, setPipelineSearch] = useState("");

  const [name, setName] = useState("");
  const [sourceId, setSourceId] = useState("");
  const [sourceTable, setSourceTable] = useState("");
  const [destId, setDestId] = useState("");
  const [destTable, setDestTable] = useState("");
  const [interval, setInterval] = useState<"hourly" | "daily" | "weekly">("daily");

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
    if (!highlightScheduleId || loading) return;
    window.requestAnimationFrame(() => {
      document.getElementById(`pipeline-card-${highlightScheduleId}`)?.scrollIntoView({
        behavior: "smooth",
        block: "nearest",
      });
    });
  }, [highlightScheduleId, loading, schedules.length]);

  useEffect(() => {
    if (connectors.length && !sourceId) setSourceId(connectors[0].id);
    if (connectors.length > 1 && !destId) setDestId(connectors[1]?.id ?? connectors[0].id);
  }, [connectors, sourceId, destId]);

  const enabledCount = schedules.filter((s) => s.enabled).length;
  const pausedCount = schedules.length - enabledCount;
  const filteredSchedules = useMemo(() => {
    let list = schedules;
    if (filter === "active") list = schedules.filter((s) => s.enabled);
    else if (filter === "paused") list = schedules.filter((s) => !s.enabled);
    const q = pipelineSearch.trim().toLowerCase();
    if (!q) return list;
    return list.filter((s) =>
      [s.name, s.source_table, s.dest_table, s.interval]
        .filter(Boolean)
        .some((v) => String(v).toLowerCase().includes(q)),
    );
  }, [schedules, filter, pipelineSearch]);
  const sourceConnector = connectors.find((c) => c.id === sourceId);
  const destConnector = connectors.find((c) => c.id === destId);
  const sourceStreamLabel = sourceConnector?.type === "mongodb" ? "Collection" : "Table";
  const destStreamLabel = destConnector?.type === "mongodb" ? "Collection" : "Table";

  const handleCreate = async (e: FormEvent) => {
    e.preventDefault();
    if (!name.trim() || !sourceId || !destId || !sourceTable.trim() || !destTable.trim()) return;
    setSaving(true);
    try {
      await createSchedule({
        name: name.trim(),
        source_connector_id: sourceId,
        source_table: sourceTable.trim(),
        dest_connector_id: destId,
        dest_table: destTable.trim(),
        interval,
      });
      setShowForm(false);
      setName("");
      setSourceTable("");
      setDestTable("");
      await load();
      void onSchedulesChange?.();
      toast({ title: "Pipeline created", message: `"${name.trim()}" is scheduled.`, tone: "success" });
    } catch (err) {
      toast({ title: "Could not create pipeline", tone: "error" });
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
      toast({ title: "Run failed", tone: "error" });
      console.error(e);
    }
    setRunningId(null);
  };

  return (
    <PageShell
      wide
      className="df2-page-pipelines"
      kicker="Automation"
      title="Scheduled pipelines"
      description="Recurring database syncs — hourly, daily, or weekly."
      actions={
        <button type="button" className="df2-btn df2-btn-primary" onClick={() => setShowForm((v) => !v)}>
          <DtIcon name="plus" size={16} />
          {showForm ? "Cancel" : "New pipeline"}
        </button>
      }
    >
      <PageFrame className="df2-pipeline-page" showHonesty>
      <PageInsightStrip
        tone={connectors.length < 2 ? "warn" : enabledCount > 0 ? "live" : schedules.length ? "info" : "ok"}
        pill={
          connectors.length < 2
            ? "Setup needed"
            : enabledCount > 0
              ? `${enabledCount} active`
              : schedules.length
                ? "All paused"
                : "No pipelines"
        }
        message={
          connectors.length < 2
            ? "Add at least two saved connectors before creating a recurring sync."
            : enabledCount
              ? `${enabledCount} pipeline${enabledCount > 1 ? "s" : ""} running on schedule — track runs in Job Theater.`
              : schedules.length
                ? "Pipelines exist but none are enabled — toggle a card to resume syncs."
                : "Create hourly, daily, or weekly syncs between saved connectors."
        }
      />
      <PageMetricsRow
        compact
        columns={4}
        metrics={[
          { label: "Total pipelines", value: schedules.length, icon: "activity" },
          { label: "Active", value: enabledCount, tone: "green", icon: "check" },
          { label: "Paused", value: pausedCount, icon: "activity" },
          { label: "Connectors", value: connectors.length, icon: "connectors" },
        ]}
      />

      {schedules.length > 0 && !loading && (
        <div className="df2-jobs-v3-toolbar">
          <FilterTabs
            ariaLabel="Filter pipelines"
            value={filter}
            onChange={setFilter}
            items={[
              { id: "all", label: "All", count: schedules.length },
              { id: "active", label: "Active", count: enabledCount },
              { id: "paused", label: "Paused", count: pausedCount },
            ]}
          />
          <PageToolbar
            searchValue={pipelineSearch}
            onSearchChange={setPipelineSearch}
            searchPlaceholder="Search pipelines by name, table, or cadence…"
          />
        </div>
      )}

      {showForm && (
        <form className="df2-pipeline-form" onSubmit={handleCreate}>
        <PageSection
          title="Create recurring sync"
          subtitle="Schedule source → destination with your saved connectors"
          elevated
        >
            <div className="df2-field">
              <label className="df2-label" htmlFor="sched-name">Pipeline name</label>
              <input id="sched-name" className="df2-input" value={name} onChange={(e) => setName(e.target.value)} placeholder="Nightly orders sync" required />
            </div>
            <div className="df2-form-row">
              <ConnectorSelect
                id="sched-src"
                label="Source connector"
                value={sourceId}
                onChange={setSourceId}
                connectors={connectors}
                placeholder="Add a connector first"
                required
                disabled={connectors.length === 0}
              />
              <div className="df2-field">
                <label className="df2-label" htmlFor="sched-src-table">Source {sourceStreamLabel.toLowerCase()}</label>
                <input id="sched-src-table" className="df2-input" value={sourceTable} onChange={(e) => setSourceTable(e.target.value)} placeholder={sourceConnector?.type === "mongodb" ? "orders" : "orders"} required />
              </div>
              <ConnectorSelect
                id="sched-dst"
                label="Destination connector"
                value={destId}
                onChange={setDestId}
                connectors={connectors}
                placeholder="Add a connector first"
                required
                disabled={connectors.length === 0}
              />
              <div className="df2-field">
                <label className="df2-label" htmlFor="sched-dst-table">Destination {destStreamLabel.toLowerCase()}</label>
                <input id="sched-dst-table" className="df2-input" value={destTable} onChange={(e) => setDestTable(e.target.value)} placeholder={destConnector?.type === "mongodb" ? "orders_archive" : "orders_warehouse"} required />
              </div>
            </div>
            <div className="df2-field">
              <label className="df2-label">Cadence</label>
              <CadenceTiles value={interval} onChange={setInterval} />
            </div>
            <div className="df2-card-footer df2-card-footer--form">
              <button type="submit" className="df2-btn df2-btn-primary" disabled={saving || connectors.length < 2}>
                {saving ? <ButtonLoader label="Saving…" /> : "Save pipeline"}
              </button>
            </div>
        </PageSection>
        </form>
      )}

      <div className="df2-pipeline-workspace">
      {loading ? (
        <SectionLoader title="Loading pipelines" hint="Fetching scheduled syncs…" />
      ) : (
      <div className="df2-pipeline-grid df2-pipeline-scroll">
        {schedules.length === 0 ? (
          <EmptyState
            icon="activity"
            title="No scheduled pipelines"
            description="Create a recurring sync to keep source and destination in step."
            action={
              <button type="button" className="df2-btn df2-btn-primary" onClick={() => setShowForm(true)}>
                Create pipeline
              </button>
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
              onToggle={() => void toggleEnabled(sched)}
              onRun={() => void handleRunNow(sched.id)}
              onDelete={() => void handleDelete(sched.id)}
            />
          ))
        )}
      </div>
      )}
      </div>
      </PageFrame>
    </PageShell>
  );
}
