import { Connector, PipelineSchedule, TransferJob } from "./types";

export interface ConnectionWorkbenchContext {
  relatedJobs: TransferJob[];
  relatedSchedules: PipelineSchedule[];
  asSourceSchedules: PipelineSchedule[];
  asDestSchedules: PipelineSchedule[];
  lastJob: TransferJob | null;
  lastSuccessAt: string | null;
  runningCount: number;
  failedCount: number;
  completedCount: number;
  enabledScheduleCount: number;
  scheduleLabel: string;
  streams: { name: string; source: "job" | "schedule" }[];
}

function matchesConnector(job: TransferJob, connector: Connector): boolean {
  const name = connector.name.toLowerCase();
  const id = connector.id.toLowerCase();
  const src = (job.source_name ?? "").toLowerCase();
  if (src && (src === name || src === id)) return true;
  if (job.source_type === connector.type && src && src === name) return true;
  if (job.destination_type === connector.type) {
    const dest = (job.destination_collection || job.destination_database || "").toLowerCase();
    const db = (connector.database ?? "").toLowerCase();
    if (dest && (db === dest || connector.name.toLowerCase() === dest)) {
      return true;
    }
  }
  return false;
}

export function jobsForConnector(connector: Connector, jobs: TransferJob[]): TransferJob[] {
  return jobs.filter((j) => matchesConnector(j, connector));
}

/** Most recent transfer touching this connector — drives the "last used" signal in status-first lists. */
export function lastUsedAtForConnector(connector: Connector, jobs: TransferJob[]): string | null {
  const related = jobsForConnector(connector, jobs);
  return related[0]?.created_at ?? null;
}

export function schedulesForConnector(connectorId: string, schedules: PipelineSchedule[]): PipelineSchedule[] {
  return schedules.filter(
    (s) => s.source_connector_id === connectorId || s.dest_connector_id === connectorId,
  );
}

export function buildConnectionWorkbenchContext(
  connector: Connector,
  jobs: TransferJob[],
  schedules: PipelineSchedule[],
): ConnectionWorkbenchContext {
  const relatedJobs = jobsForConnector(connector, jobs);
  const relatedSchedules = schedulesForConnector(connector.id, schedules);
  const asSourceSchedules = relatedSchedules.filter((s) => s.source_connector_id === connector.id);
  const asDestSchedules = relatedSchedules.filter((s) => s.dest_connector_id === connector.id);
  const lastJob = relatedJobs[0] ?? null;
  const lastSuccess = relatedJobs.find((j) => j.status === "completed");
  const runningCount = relatedJobs.filter((j) => j.status === "running" || j.status === "pending").length;
  const failedCount = relatedJobs.filter((j) => j.status === "failed").length;
  const completedCount = relatedJobs.filter((j) => j.status === "completed").length;
  const enabledScheduleCount = relatedSchedules.filter((s) => s.enabled).length;

  const streamNames = new Map<string, "job" | "schedule">();
  for (const s of relatedSchedules) {
    if (s.source_connector_id === connector.id && s.source_table) {
      streamNames.set(s.source_table, "schedule");
    }
    if (s.dest_connector_id === connector.id && s.dest_table) {
      streamNames.set(s.dest_table, "schedule");
    }
  }
  for (const j of relatedJobs) {
    const label = j.source_name || j.destination_collection || j.destination_database;
    if (label) streamNames.set(label, "job");
  }

  let scheduleLabel = "Manual";
  if (enabledScheduleCount === 1) {
    const s = relatedSchedules.find((x) => x.enabled);
    scheduleLabel = s ? `${s.interval} · ${s.name}` : "Manual";
  } else if (enabledScheduleCount > 1) {
    scheduleLabel = `${enabledScheduleCount} pipelines enabled`;
  } else if (relatedSchedules.length > 0) {
    scheduleLabel = `${relatedSchedules.length} pipeline(s) paused`;
  }

  return {
    relatedJobs,
    relatedSchedules,
    asSourceSchedules,
    asDestSchedules,
    lastJob,
    lastSuccessAt: lastSuccess?.created_at ?? null,
    runningCount,
    failedCount,
    completedCount,
    enabledScheduleCount,
    scheduleLabel,
    streams: Array.from(streamNames.entries()).map(([name, source]) => ({ name, source })),
  };
}

export function formatRelativeTime(iso: string | null): string {
  if (!iso) return "Never";
  const then = new Date(iso).getTime();
  if (Number.isNaN(then)) return "Unknown";
  const diffMs = Date.now() - then;
  const mins = Math.floor(diffMs / 60_000);
  if (mins < 1) return "Just now";
  if (mins < 60) return `${mins}m ago`;
  const hours = Math.floor(mins / 60);
  if (hours < 48) return `${hours}h ago`;
  const days = Math.floor(hours / 24);
  if (days < 14) return `${days}d ago`;
  return new Date(iso).toLocaleDateString();
}
