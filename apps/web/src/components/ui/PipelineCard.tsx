import { ReactNode } from "react";
import { ConnectorIcon } from "../../app/brand-icons";
import { DtIcon } from "../DtIcon";
import { Connector, PipelineSchedule } from "../../lib/types";
import { jobStatusBadgeClass, jobStatusLabel } from "../../lib/uiUtils";
import { Button } from "./Button";

interface PipelineCardProps {
  schedule: PipelineSchedule;
  source?: Connector;
  dest?: Connector;
  running?: boolean;
  highlighted?: boolean;
  historyOpen?: boolean;
  onToggle: () => void;
  onRun: () => void;
  onEdit: () => void;
  onDelete: () => void;
  onToggleHistory: () => void;
  children?: ReactNode;
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

const SYNC_MODE_LABEL: Record<string, string> = {
  full_refresh_overwrite: "Full overwrite",
  full_refresh_append: "Full append",
  incremental: "Incremental",
  cdc: "CDC",
};

function cadenceLabel(sched: PipelineSchedule): string {
  if (sched.cron) return `Cron ${sched.cron}`;
  return INTERVAL_LABEL[sched.interval] ?? sched.interval;
}

export function PipelineCard({
  schedule: sched,
  source,
  dest,
  running,
  highlighted,
  historyOpen,
  onToggle,
  onRun,
  onEdit,
  onDelete,
  onToggleHistory,
  children,
}: PipelineCardProps) {
  const isRunning = running || sched.running;
  const syncLabel = SYNC_MODE_LABEL[sched.sync_mode] ?? sched.sync_mode;
  return (
    <article
      id={`pipeline-card-${sched.id}`}
      className={[
        "df2-pipe-card",
        "df2-card-interactive",
        sched.enabled ? "is-active" : "is-paused",
        highlighted ? "is-highlighted" : "",
      ].filter(Boolean).join(" ")}
    >
      <div className="df2-pipe-card-head">
        <div className="df2-pipe-card-copy">
          <h3 className="df2-pipe-card-title" title={sched.name}>{sched.name}</h3>
          <p className="df2-pipe-card-sub">Last run {formatWhen(sched.last_run_at)}</p>
        </div>
        <div className="df2-pipe-card-badges">
          {isRunning && (
            <span className="df2-badge df2-badge-run" title="A run is in progress">
              <DtIcon name="activity" size={11} /> Running
            </span>
          )}
          {sched.last_status && (
            <span className={jobStatusBadgeClass(sched.last_status)} title={`Last run: ${jobStatusLabel(sched.last_status)}`}>
              {jobStatusLabel(sched.last_status)}
            </span>
          )}
          <button
            type="button"
            className={`df2-badge ${sched.enabled ? "df2-badge-live" : "df2-badge-muted"}`}
            aria-pressed={sched.enabled}
            onClick={onToggle}
            title={sched.enabled ? "Pause pipeline" : "Enable pipeline"}
          >
            {sched.enabled ? "Active" : "Paused"}
          </button>
        </div>
      </div>

      <div className="df2-pipe-card-route">
        <div className="df2-pipe-card-node">
          <ConnectorIcon id={source?.type ?? "database"} size={18} />
          <span title={source?.name ?? "Source"}>{source?.name ?? "Source"}</span>
          <small title={sched.source_table}>{sched.source_table}</small>
        </div>
        <div className="df2-pipe-card-arrow" aria-hidden>
          <DtIcon name="transfer" size={14} />
        </div>
        <div className="df2-pipe-card-node">
          <ConnectorIcon id={dest?.type ?? "database"} size={18} />
          <span title={dest?.name ?? "Destination"}>{dest?.name ?? "Destination"}</span>
          <small title={sched.dest_table}>{sched.dest_table}</small>
        </div>
      </div>

      <div className="df2-pipe-card-meta">
        <span><DtIcon name="clock" size={12} /> {cadenceLabel(sched)}</span>
        <span title="Sync mode"><DtIcon name="transfer" size={12} /> {syncLabel}</span>
        <span>Next {formatWhen(sched.next_run_at)}</span>
        <span>{sched.run_count} runs</span>
      </div>

      <div className="df2-pipe-card-actions">
        <Button
          size="sm"
          variant="primary"
          loading={running}
          loadingLabel="Running…"
          disabled={isRunning}
          onClick={onRun}
          leadingIcon={<DtIcon name="activity" size={14} />}
        >
          {isRunning ? "Running…" : "Run now"}
        </Button>
        <Button
          size="sm"
          variant="ghost"
          onClick={onToggleHistory}
          aria-expanded={historyOpen}
          leadingIcon={<DtIcon name="jobs" size={14} />}
        >
          History
        </Button>
        <Button
          size="sm"
          variant="ghost"
          onClick={onEdit}
          leadingIcon={<DtIcon name="settings" size={14} />}
        >
          Edit
        </Button>
        <Button
          size="sm"
          variant="danger"
          onClick={onDelete}
          leadingIcon={<DtIcon name="trash" size={14} />}
        >
          Delete
        </Button>
      </div>

      {historyOpen && children && <div className="df2-pipe-card-history">{children}</div>}
    </article>
  );
}
