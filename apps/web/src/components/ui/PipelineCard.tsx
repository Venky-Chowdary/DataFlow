import { ReactNode } from "react";
import { ConnectorIcon } from "../../app/brand-icons";
import { DtIcon } from "../DtIcon";
import { Connector, PipelineSchedule } from "../../lib/types";
import { breakerBadgeClass, breakerWarnLabel } from "../../lib/contractBreakerUi";
import { jobStatusBadgeClass, jobStatusLabel } from "../../lib/uiUtils";
import { Button } from "./Button";
import { CopyIdChip } from "./CopyIdChip";

interface PipelineCardProps {
  schedule: PipelineSchedule;
  source?: Connector;
  dest?: Connector;
  running?: boolean;
  highlighted?: boolean;
  selected?: boolean;
  historyOpen?: boolean;
  /** Dense list row — actions live in the detail drawer. */
  compact?: boolean;
  /** Circuit breaker state for the bound data contract (closed/open/half_open). */
  breakerState?: string | null;
  /** Freshness SLO warn — lag seconds when pipeline is stale/critical. */
  freshnessLagSeconds?: number | null;
  freshnessSeverity?: string | null;
  onSelect?: () => void;
  onToggle?: () => void;
  onRun?: () => void;
  onEdit?: () => void;
  onDelete?: () => void;
  onToggleHistory?: () => void;
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
  selected,
  historyOpen,
  compact,
  breakerState,
  freshnessLagSeconds,
  freshnessSeverity,
  onSelect,
  onToggle,
  onRun,
  onEdit,
  onDelete,
  onToggleHistory,
  children,
}: PipelineCardProps) {
  const isRunning = running || sched.running;
  const syncLabel = SYNC_MODE_LABEL[sched.sync_mode] ?? sched.sync_mode;
  const routeLabel = `${source?.name ?? "Source"} → ${dest?.name ?? "Destination"}`;
  const tableLabel = `${sched.source_table} → ${sched.dest_table}`;
  const breakerText = breakerWarnLabel(breakerState);
  const freshnessText =
    freshnessLagSeconds != null && Number.isFinite(freshnessLagSeconds)
      ? `Lag ${freshnessLagSeconds.toFixed(0)}s`
      : "";

  if (compact) {
    return (
      <div
        id={`pipeline-card-${sched.id}`}
        className={[
          "df2-pipeline-row",
          "df2-card-interactive",
          sched.enabled ? "is-active" : "is-paused",
          highlighted ? "is-highlighted" : "",
          selected ? "selected" : "",
        ].filter(Boolean).join(" ")}
        onClick={onSelect}
        onKeyDown={
          onSelect
            ? (e) => {
                if (e.key === "Enter" || e.key === " ") {
                  e.preventDefault();
                  onSelect();
                }
              }
            : undefined
        }
        role={onSelect ? "button" : undefined}
        tabIndex={onSelect ? 0 : undefined}
        aria-current={selected || undefined}
      >
        <span
          className={`df2-health-dot ${sched.enabled ? "ok" : "err"}`}
          aria-hidden
          title={sched.enabled ? "Active" : "Paused"}
        />
        <span className="df2-pipeline-row-icons" aria-hidden>
          <ConnectorIcon id={source?.type ?? "database"} size={18} />
          <DtIcon name="transfer" size={12} />
          <ConnectorIcon id={dest?.type ?? "database"} size={18} />
        </span>
        <div className="df2-pipeline-row-identity">
          <span className="df2-pipeline-row-name" title={sched.name}>{sched.name}</span>
          <span className="df2-pipeline-row-meta" title={`${routeLabel} · ${tableLabel}`}>
            {routeLabel}
          </span>
        </div>
        <span className="df2-pipeline-row-cadence" title="Schedule cadence">
          {cadenceLabel(sched)}
        </span>
        <span className="df2-pipeline-row-sync" title={sched.contract_id ? `Sync mode · contract ${sched.contract_id}` : "Sync mode"}>
          {syncLabel}
          {sched.contract_id ? " · contract" : ""}
        </span>
        <span className="df2-pipeline-row-signal">
          {breakerText ? (
            <span className={`df2-badge ${breakerBadgeClass(breakerState)}`} title="Data contract circuit breaker">
              {breakerText}
            </span>
          ) : freshnessText ? (
            <span
              className={`df2-badge ${freshnessSeverity === "critical" ? "df2-badge-warn" : "df2-badge-warn"}`}
              title="CDC freshness SLO"
            >
              {freshnessText}
            </span>
          ) : isRunning ? (
            <span className="df2-badge df2-badge-run">Running</span>
          ) : sched.last_status ? (
            <span className={jobStatusBadgeClass(sched.last_status)}>
              {jobStatusLabel(sched.last_status)}
            </span>
          ) : (
            <span className="df2-badge df2-badge-muted">No runs</span>
          )}
        </span>
        <span className={`df2-badge ${sched.enabled ? "df2-badge-live" : "df2-badge-muted"}`}>
          {sched.enabled ? "Active" : "Paused"}
        </span>
        <div className="df2-pipeline-row-quick" onClick={(e) => e.stopPropagation()}>
          <button
            type="button"
            className="df2-pipeline-row-open"
            onClick={onSelect}
            aria-label={`Open ${sched.name} details`}
          >
            <DtIcon name="chevron-right" size={16} />
          </button>
        </div>
      </div>
    );
  }

  return (
    <article
      id={`pipeline-card-${sched.id}`}
      className={[
        "df2-pipe-card",
        "df2-card-interactive",
        sched.enabled ? "is-active" : "is-paused",
        highlighted ? "is-highlighted" : "",
        selected ? "is-selected" : "",
      ].filter(Boolean).join(" ")}
      onClick={onSelect}
      onKeyDown={
        onSelect
          ? (e) => {
              if (e.key === "Enter" || e.key === " ") {
                e.preventDefault();
                onSelect();
              }
            }
          : undefined
      }
      role={onSelect ? "button" : undefined}
      tabIndex={onSelect ? 0 : undefined}
      aria-current={selected || undefined}
    >
      <div className="df2-pipe-card-head">
        <div className="df2-pipe-card-copy">
          <h3 className="df2-pipe-card-title" title={sched.name}>{sched.name}</h3>
          <p className="df2-pipe-card-sub">Last run {formatWhen(sched.last_run_at)}</p>
          <CopyIdChip id={sched.id} label="Pipeline" compact className="df2-pipe-card-id" />
        </div>
        <div className="df2-pipe-card-badges">
          {isRunning && (
            <span className="df2-badge df2-badge-run" title="A run is in progress">
              <DtIcon name="activity" size={11} /> Running
            </span>
          )}
          {breakerText && (
            <span className={`df2-badge ${breakerBadgeClass(breakerState)}`} title="Data contract circuit breaker">
              {breakerText}
            </span>
          )}
          {sched.last_status && (
            <span className={jobStatusBadgeClass(sched.last_status)} title={`Last run: ${jobStatusLabel(sched.last_status)}`}>
              {jobStatusLabel(sched.last_status)}
            </span>
          )}
          <span className={`df2-badge ${sched.enabled ? "df2-badge-live" : "df2-badge-muted"}`}>
            {sched.enabled ? "Active" : "Paused"}
          </span>
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

      {(onRun || onEdit || onDelete || onToggle || onToggleHistory || onSelect) && (
        <div className="df2-pipe-card-actions" onClick={(e) => e.stopPropagation()}>
          {onRun && (
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
          )}
          {(onSelect || onToggleHistory) && (
            <Button
              size="sm"
              variant="ghost"
              onClick={onSelect ?? onToggleHistory}
              aria-expanded={historyOpen}
              leadingIcon={<DtIcon name="jobs" size={14} />}
            >
              {onSelect ? "Open" : "History"}
            </Button>
          )}
          {onEdit && (
            <Button
              size="sm"
              variant="ghost"
              onClick={onEdit}
              leadingIcon={<DtIcon name="settings" size={14} />}
            >
              Edit
            </Button>
          )}
          {onDelete && (
            <Button
              size="sm"
              variant="danger"
              onClick={onDelete}
              leadingIcon={<DtIcon name="trash" size={14} />}
            >
              Delete
            </Button>
          )}
        </div>
      )}

      {historyOpen && children && <div className="df2-pipe-card-history">{children}</div>}
    </article>
  );
}
