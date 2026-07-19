import { DtIcon } from "../DtIcon";
import type { JobNotificationResult, JobPhase } from "../../lib/types";

export interface JobTimelineEntry {
  key: string;
  title: string;
  timestamp?: string | null;
  status: "done" | "active" | "failed" | "skipped" | "pending" | "warning";
  detail?: string;
}

function fmt(ts?: string | null): string | undefined {
  if (!ts) return undefined;
  const d = new Date(ts);
  if (Number.isNaN(d.getTime())) return undefined;
  return d.toLocaleString(undefined, {
    month: "short",
    day: "numeric",
    hour: "2-digit",
    minute: "2-digit",
    second: "2-digit",
  });
}

function iconFor(status: JobTimelineEntry["status"]) {
  if (status === "done") return "check";
  if (status === "failed") return "x";
  if (status === "warning") return "alert";
  if (status === "skipped") return "activity";
  if (status === "active") return "activity";
  return "jobs";
}

/**
 * Builds the chronological event list for a job: creation, start, each
 * pipeline phase, notification dispatch, and terminal state — the raw
 * material for the Airbyte-style timeline view.
 */
export function buildJobTimeline(opts: {
  createdAt?: string | null;
  startedAt?: string | null;
  completedAt?: string | null;
  status: string;
  retryOf?: string | null;
  phases?: JobPhase[];
  notifications?: JobNotificationResult[];
  rejectedRows?: number;
  coercedNullRows?: number;
}): JobTimelineEntry[] {
  const entries: JobTimelineEntry[] = [];

  entries.push({
    key: "created",
    title: opts.retryOf ? `Job queued (retry of ${opts.retryOf.slice(-8)})` : "Job queued",
    timestamp: opts.createdAt,
    status: "done",
  });

  if (opts.startedAt) {
    entries.push({ key: "started", title: "Transfer started", timestamp: opts.startedAt, status: "done" });
  }

  for (const phase of opts.phases ?? []) {
    entries.push({
      key: `phase-${phase.name}`,
      title: phase.name,
      status: phase.status,
      detail: phase.message,
    });
  }

  const coercedNull = opts.coercedNullRows ?? 0;
  const droppedRows = Math.max((opts.rejectedRows ?? 0) - coercedNull, 0);
  if (droppedRows > 0) {
    entries.push({
      key: "quarantine",
      title: "Rows quarantined",
      status: "warning",
      detail: `${droppedRows.toLocaleString()} row(s) isolated for review — failed validation, not silently dropped`,
    });
  }
  if (coercedNull > 0) {
    entries.push({
      key: "coerced-null",
      title: "Values coerced to NULL",
      status: "warning",
      detail: `${coercedNull.toLocaleString()} row(s) kept but a value was changed to NULL — not full fidelity`,
    });
  }

  for (const n of opts.notifications ?? []) {
    entries.push({
      key: `notify-${n.channel_id}`,
      title: `${n.kind} notification`,
      status: n.ok ? "done" : "failed",
      detail: n.ok ? "Sent" : n.error || "Delivery failed",
    });
  }

  if (
    opts.status === "completed"
    || opts.status === "completed_with_quarantine"
    || opts.status === "failed"
    || opts.status === "cancelled"
  ) {
    const isFailed = opts.status === "failed";
    const isCancelled = opts.status === "cancelled";
    const isQuarantine = opts.status === "completed_with_quarantine";
    entries.push({
      key: "terminal",
      title: isCancelled
        ? "Transfer cancelled"
        : isFailed
          ? "Transfer failed"
          : isQuarantine
            ? "Completed with quarantine"
            : "Transfer completed",
      timestamp: opts.completedAt,
      status: isFailed || isCancelled ? "failed" : isQuarantine ? "warning" : "done",
    });
  } else if (opts.status === "running" || opts.status === "pending") {
    entries.push({ key: "terminal", title: "In progress…", status: "active" });
  }

  return entries;
}

export function JobTimeline({ entries }: { entries: JobTimelineEntry[] }) {
  if (entries.length === 0) return null;
  return (
    <ol className="df2-job-timeline" aria-label="Job event timeline">
      {entries.map((e) => (
        <li key={e.key} className={`df2-job-timeline-item is-${e.status}`}>
          <span className="df2-job-timeline-dot" aria-hidden>
            <DtIcon name={iconFor(e.status)} size={12} />
          </span>
          <div className="df2-job-timeline-body">
            <div className="df2-job-timeline-row">
              <strong>{e.title}</strong>
              {fmt(e.timestamp) && <time>{fmt(e.timestamp)}</time>}
            </div>
            {e.detail && <p>{e.detail}</p>}
          </div>
        </li>
      ))}
    </ol>
  );
}
