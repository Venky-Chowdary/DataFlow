import { ConnectorIcon } from "../../app/brand-icons";
import { DtIcon } from "../DtIcon";
import { Connector, PipelineSchedule } from "../../lib/types";

interface PipelineCardProps {
  schedule: PipelineSchedule;
  source?: Connector;
  dest?: Connector;
  running?: boolean;
  highlighted?: boolean;
  onToggle: () => void;
  onRun: () => void;
  onDelete: () => void;
}

function formatWhen(iso: string | null): string {
  if (!iso) return "—";
  try {
    return new Date(iso).toLocaleString(undefined, { month: "short", day: "numeric", hour: "2-digit", minute: "2-digit" });
  } catch {
    return iso;
  }
}

const INTERVAL_LABEL: Record<string, string> = {
  hourly: "Every hour",
  daily: "Daily",
  weekly: "Weekly",
};

export function PipelineCard({
  schedule: sched,
  source,
  dest,
  running,
  highlighted,
  onToggle,
  onRun,
  onDelete,
}: PipelineCardProps) {
  return (
    <article
      id={`pipeline-card-${sched.id}`}
      className={`df2-pipeline-card ${sched.enabled ? "active" : "paused"} ${highlighted ? "highlighted" : ""}`}
    >
      <div className="df2-pipeline-card-head">
        <div>
          <h3 className="df2-pipeline-card-title">{sched.name}</h3>
          <p className="df2-pipeline-card-sub">Last run {formatWhen(sched.last_run_at)}</p>
        </div>
        <button
          type="button"
          className={`df2-badge ${sched.enabled ? "df2-badge-live" : "df2-badge-muted"}`}
          aria-pressed={sched.enabled}
          onClick={onToggle}
        >
          {sched.enabled ? "Active" : "Paused"}
        </button>
      </div>

      <div className="df2-pipeline-card-route">
        <div className="df2-pipeline-card-node">
          <ConnectorIcon id={source?.type ?? "database"} size={22} />
          <span>{source?.name ?? "Source"}</span>
          <small>{sched.source_table}</small>
        </div>
        <div className="df2-pipeline-card-arrow" aria-hidden>
          <DtIcon name="transfer" size={18} />
        </div>
        <div className="df2-pipeline-card-node">
          <ConnectorIcon id={dest?.type ?? "database"} size={22} />
          <span>{dest?.name ?? "Destination"}</span>
          <small>{sched.dest_table}</small>
        </div>
      </div>

      <div className="df2-pipeline-card-meta">
        <span><DtIcon name="clock" size={12} /> {INTERVAL_LABEL[sched.interval] ?? sched.interval}</span>
        <span>Next {formatWhen(sched.next_run_at)}</span>
        <span>{sched.run_count} runs</span>
      </div>

      <div className="df2-pipeline-card-actions">
        <button type="button" className="df2-btn df2-btn-sm df2-btn-primary" disabled={running} onClick={onRun}>
          {running ? (
            <>
              <span className="df2-inline-spinner" aria-hidden />
              Running…
            </>
          ) : (
            <>
              <DtIcon name="activity" size={14} />
              Run now
            </>
          )}
        </button>
        <button type="button" className="df2-btn df2-btn-sm df2-btn-danger" onClick={onDelete}>
          <DtIcon name="trash" size={14} />
          Delete
        </button>
      </div>
    </article>
  );
}
