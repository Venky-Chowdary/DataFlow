import { FormEvent, useCallback, useEffect, useState } from "react";
import { DtIcon } from "../components/DtIcon";
import { LoadingBlock, ButtonLoader } from "../components/LoadingState";
import { PageShell } from "../components/ui/PageShell";
import { StatCard } from "../components/ui/StatCard";
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
}

const INTERVALS = [
  { id: "hourly" as const, label: "Every hour" },
  { id: "daily" as const, label: "Daily" },
  { id: "weekly" as const, label: "Weekly" },
];

function formatWhen(iso: string | null): string {
  if (!iso) return "—";
  try {
    return new Date(iso).toLocaleString();
  } catch {
    return iso;
  }
}

export function SchedulesPage({ connectors, onViewJobs }: SchedulesPageProps) {
  const { toast } = useToast();
  const [schedules, setSchedules] = useState<PipelineSchedule[]>([]);
  const [loading, setLoading] = useState(true);
  const [showForm, setShowForm] = useState(false);
  const [saving, setSaving] = useState(false);
  const [runningId, setRunningId] = useState<string | null>(null);

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
    if (connectors.length && !sourceId) setSourceId(connectors[0].id);
    if (connectors.length > 1 && !destId) setDestId(connectors[1]?.id ?? connectors[0].id);
  }, [connectors, sourceId, destId]);

  const connectorLabel = (id: string) => connectors.find((c) => c.id === id)?.name ?? id.slice(0, 8);

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

  const enabledCount = schedules.filter((s) => s.enabled).length;

  return (
    <PageShell
      wide
      title="Scheduled pipelines"
      description="Recurring database syncs — hourly, daily, or weekly."
      actions={
        <button type="button" className="df2-btn df2-btn-primary" onClick={() => setShowForm((v) => !v)}>
          <DtIcon name="plus" size={16} />
          {showForm ? "Cancel" : "New pipeline"}
        </button>
      }
    >
      <div className="df2-stats" style={{ gridTemplateColumns: "repeat(3, 1fr)" }}>
        <StatCard label="Total pipelines" value={schedules.length} />
        <StatCard label="Active" value={enabledCount} tone="green" />
        <StatCard label="Connectors" value={connectors.length} />
      </div>

      {showForm && (
        <form className="df2-card df2-stack" style={{ marginBottom: 24 }} onSubmit={handleCreate}>
          <div className="df2-card-head">
            <h2 className="df2-card-title">Create recurring sync</h2>
          </div>
          <div className="df2-card-body">
            <div className="df2-field">
              <label className="df2-label" htmlFor="sched-name">Pipeline name</label>
              <input id="sched-name" className="df2-input" value={name} onChange={(e) => setName(e.target.value)} placeholder="Nightly orders sync" required />
            </div>
            <div className="df2-form-row">
              <div className="df2-field">
                <label className="df2-label" htmlFor="sched-src">Source connector</label>
                <select id="sched-src" className="df2-select" value={sourceId} onChange={(e) => setSourceId(e.target.value)} required>
                  {connectors.map((c) => (
                    <option key={c.id} value={c.id}>{c.name} ({c.type})</option>
                  ))}
                </select>
              </div>
              <div className="df2-field">
                <label className="df2-label" htmlFor="sched-src-table">Source table</label>
                <input id="sched-src-table" className="df2-input" value={sourceTable} onChange={(e) => setSourceTable(e.target.value)} placeholder="orders" required />
              </div>
              <div className="df2-field">
                <label className="df2-label" htmlFor="sched-dst">Destination connector</label>
                <select id="sched-dst" className="df2-select" value={destId} onChange={(e) => setDestId(e.target.value)} required>
                  {connectors.map((c) => (
                    <option key={c.id} value={c.id}>{c.name} ({c.type})</option>
                  ))}
                </select>
              </div>
              <div className="df2-field">
                <label className="df2-label" htmlFor="sched-dst-table">Destination table</label>
                <input id="sched-dst-table" className="df2-input" value={destTable} onChange={(e) => setDestTable(e.target.value)} placeholder="orders_warehouse" required />
              </div>
              <div className="df2-field">
                <label className="df2-label" htmlFor="sched-interval">Cadence</label>
                <select id="sched-interval" className="df2-select" value={interval} onChange={(e) => setInterval(e.target.value as typeof interval)}>
                  {INTERVALS.map((i) => (
                    <option key={i.id} value={i.id}>{i.label}</option>
                  ))}
                </select>
              </div>
            </div>
          </div>
          <div className="df2-card-footer">
            <button type="submit" className="df2-btn df2-btn-primary" disabled={saving || connectors.length < 2}>
              {saving ? <ButtonLoader label="Saving…" /> : "Save pipeline"}
            </button>
            {connectors.length < 2 && (
              <p style={{ margin: 0, fontSize: 13, color: "#64748b" }}>Add at least two connectors first.</p>
            )}
          </div>
        </form>
      )}

      <div className="df2-stack">
        {loading ? (
          <LoadingBlock title="Loading pipelines" hint="Fetching scheduled syncs…" />
        ) : schedules.length === 0 ? (
          <div className="df2-empty">
            <DtIcon name="activity" size={32} />
            <h3 className="df2-empty-title">No scheduled pipelines</h3>
            <p className="df2-empty-desc">Create a recurring sync to keep source and destination in step.</p>
            <button type="button" className="df2-btn df2-btn-primary" onClick={() => setShowForm(true)}>
              Create pipeline
            </button>
          </div>
        ) : (
          schedules.map((sched) => (
            <article key={sched.id} className="df2-pipeline">
              <div className="df2-pipeline-head">
                <div>
                  <h3 className="df2-pipeline-title">{sched.name}</h3>
                  <p style={{ margin: "4px 0 0", fontSize: 13, color: "#64748b" }}>Last run: {formatWhen(sched.last_run_at)}</p>
                </div>
                <button
                  type="button"
                  className={`df2-badge ${sched.enabled ? "df2-badge-live" : "df2-badge-muted"}`}
                  aria-pressed={sched.enabled}
                  onClick={() => toggleEnabled(sched)}
                >
                  {sched.enabled ? "Active" : "Paused"}
                </button>
              </div>
              <div className="df2-pipeline-route">
                <span>{connectorLabel(sched.source_connector_id)}.{sched.source_table}</span>
                <DtIcon name="transfer" size={16} />
                <span>{connectorLabel(sched.dest_connector_id)}.{sched.dest_table}</span>
              </div>
              <div className="df2-pipeline-meta">
                <span>{sched.interval}</span>
                <span>Next: {formatWhen(sched.next_run_at)}</span>
                <span>{sched.run_count} runs</span>
              </div>
              <div className="df2-pipeline-actions">
                <button type="button" className="df2-btn df2-btn-sm" disabled={runningId === sched.id} onClick={() => handleRunNow(sched.id)}>
                  {runningId === sched.id ? "Running…" : "Run now"}
                </button>
                <button type="button" className="df2-btn df2-btn-ghost df2-btn-sm df2-btn-danger" onClick={() => handleDelete(sched.id)}>
                  Delete
                </button>
              </div>
            </article>
          ))
        )}
      </div>
    </PageShell>
  );
}
