import { ConnectorIcon } from "../app/brand-icons";
import { DtIcon } from "./DtIcon";
import { Button } from "./ui/Button";
import { Drawer } from "./ui/Drawer";
import { FilterTabs } from "./ui/FilterTabs";
import { ScheduleRunHistory } from "./schedules/ScheduleRunHistory";
import { Connector, PipelineSchedule } from "../lib/types";
import { jobStatusBadgeClass, jobStatusLabel } from "../lib/uiUtils";

export const PIPELINE_TABS = ["Overview", "History", "Config"] as const;
export type PipelineTab = (typeof PIPELINE_TABS)[number];

interface PipelineDetailDrawerProps {
  open: boolean;
  schedule: PipelineSchedule | null;
  source?: Connector;
  dest?: Connector;
  tab: PipelineTab;
  setTab: (tab: PipelineTab) => void;
  running?: boolean;
  onClose: () => void;
  onRun: () => void;
  onEdit: () => void;
  onDelete: () => void;
  onToggle: () => void;
  onOpenJob?: (jobId: string) => void;
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

function formatWhen(iso: string | null | undefined): string {
  if (!iso) return "—";
  try {
    return new Date(iso).toLocaleString(undefined, {
      month: "short",
      day: "numeric",
      hour: "2-digit",
      minute: "2-digit",
    });
  } catch {
    return iso;
  }
}

export function PipelineDetailDrawer({
  open,
  schedule: sched,
  source,
  dest,
  tab,
  setTab,
  running,
  onClose,
  onRun,
  onEdit,
  onDelete,
  onToggle,
  onOpenJob,
}: PipelineDetailDrawerProps) {
  if (!sched) return null;

  const isRunning = Boolean(running || sched.running);
  const cadence = sched.cron ? `Cron ${sched.cron}` : (INTERVAL_LABEL[sched.interval] ?? sched.interval);
  const syncLabel = SYNC_MODE_LABEL[sched.sync_mode] ?? sched.sync_mode;

  return (
    <Drawer
      open={open}
      onClose={onClose}
      width={620}
      ariaLabel={`${sched.name} pipeline details`}
      icon={<DtIcon name="activity" size={22} />}
      title={sched.name}
      subtitle={`${source?.name ?? "Source"} → ${dest?.name ?? "Destination"}`}
      headerExtra={
        <>
          {isRunning && (
            <span className="df2-badge df2-badge-run">
              <DtIcon name="activity" size={11} /> Running
            </span>
          )}
          <span className={`df2-badge ${sched.enabled ? "df2-badge-live" : "df2-badge-muted"}`}>
            {sched.enabled ? "Active" : "Paused"}
          </span>
          {sched.last_status && (
            <span className={jobStatusBadgeClass(sched.last_status)}>
              {jobStatusLabel(sched.last_status)}
            </span>
          )}
        </>
      }
      footer={
        <div className="df2-drawer-actions">
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
            onClick={onToggle}
            leadingIcon={<DtIcon name={sched.enabled ? "pause" : "check"} size={14} />}
          >
            {sched.enabled ? "Pause" : "Activate"}
          </Button>
          <Button size="sm" variant="ghost" onClick={onEdit} leadingIcon={<DtIcon name="settings" size={14} />}>
            Edit
          </Button>
          <Button
            size="sm"
            variant="danger"
            className="df2-drawer-action-delete"
            onClick={onDelete}
            leadingIcon={<DtIcon name="trash" size={14} />}
          >
            Delete
          </Button>
        </div>
      }
    >
      <div className="df2-drawer-facts" aria-label="Pipeline summary">
        <div className="df2-drawer-fact">
          <span>Cadence</span>
          <strong title={cadence}>{cadence}</strong>
        </div>
        <div className="df2-drawer-fact">
          <span>Sync mode</span>
          <strong>{syncLabel}</strong>
        </div>
        <div className="df2-drawer-fact">
          <span>Last run</span>
          <strong>{formatWhen(sched.last_run_at)}</strong>
        </div>
        <div className="df2-drawer-fact">
          <span>Next run</span>
          <strong>{formatWhen(sched.next_run_at)}</strong>
        </div>
      </div>

      <div className="df2-drawer-section df2-drawer-workbench">
        <FilterTabs
          ariaLabel="Pipeline detail sections"
          value={tab}
          onChange={setTab}
          items={PIPELINE_TABS.map((id) => ({
            id,
            label: id,
            count: id === "History" ? sched.run_count : undefined,
          }))}
        />

        {tab === "Overview" && (
          <section className="df2-drawer-section" aria-label="Route">
            <div className="df2-drawer-section-head">
              <h3><DtIcon name="transfer" size={14} /> Route</h3>
            </div>
            <div className="df2-drawer-related-list" role="list">
              <div className="df2-drawer-related-row" role="listitem">
                <span className="df2-drawer-related-main">
                  <span className="df2-drawer-route-node">
                    <ConnectorIcon id={source?.type ?? "database"} size={16} />
                    <strong title={source?.name}>{source?.name ?? "Source"}</strong>
                  </span>
                  <small>{sched.source_table || "—"}</small>
                </span>
                <span className="df2-badge df2-badge-muted">Source</span>
              </div>
              <div className="df2-drawer-related-row" role="listitem">
                <span className="df2-drawer-related-main">
                  <span className="df2-drawer-route-node">
                    <ConnectorIcon id={dest?.type ?? "database"} size={16} />
                    <strong title={dest?.name}>{dest?.name ?? "Destination"}</strong>
                  </span>
                  <small>{sched.dest_table || "—"}</small>
                </span>
                <span className="df2-badge df2-badge-muted">Destination</span>
              </div>
            </div>
            <dl className="df2-drawer-kv">
              <div><dt>Timezone</dt><dd>{sched.timezone || "UTC"}</dd></div>
              <div><dt>Validation</dt><dd>{sched.validation_mode || "—"}</dd></div>
              <div><dt>Schema policy</dt><dd>{sched.schema_policy || "—"}</dd></div>
              <div><dt>Runs</dt><dd>{sched.run_count.toLocaleString()}</dd></div>
              {sched.primary_key && <div><dt>Primary key</dt><dd>{sched.primary_key}</dd></div>}
              {sched.cursor_column && <div><dt>Cursor</dt><dd>{sched.cursor_column}</dd></div>}
            </dl>
          </section>
        )}

        {tab === "History" && (
          <section className="df2-drawer-section" aria-label="Run history">
            <div className="df2-drawer-section-head">
              <h3><DtIcon name="jobs" size={14} /> Run history</h3>
              <span className="df2-drawer-count">{sched.run_count}</span>
            </div>
            <ScheduleRunHistory scheduleId={sched.id} onOpenJob={onOpenJob} />
          </section>
        )}

        {tab === "Config" && (
          <section className="df2-drawer-section" aria-label="Configuration">
            <div className="df2-drawer-section-head">
              <h3><DtIcon name="settings" size={14} /> Configuration</h3>
            </div>
            <dl className="df2-drawer-kv">
              <div><dt>Pipeline ID</dt><dd className="df2-cell-mono">{sched.id}</dd></div>
              <div><dt>Interval</dt><dd>{sched.interval || "—"}</dd></div>
              <div><dt>Cron</dt><dd>{sched.cron || "—"}</dd></div>
              <div><dt>Max retries</dt><dd>{sched.max_retries ?? "—"}</dd></div>
              <div><dt>Retry backoff</dt><dd>{sched.retry_backoff_seconds != null ? `${sched.retry_backoff_seconds}s` : "—"}</dd></div>
              <div><dt>Notify on failure</dt><dd>{sched.notify_on_failure ? "Yes" : "No"}</dd></div>
              <div><dt>Notify on success</dt><dd>{sched.notify_on_success ? "Yes" : "No"}</dd></div>
              <div><dt>Backfill new fields</dt><dd>{sched.backfill_new_fields ? "Yes" : "No"}</dd></div>
              <div><dt>Created</dt><dd>{formatWhen(sched.created_at)}</dd></div>
            </dl>
            <p className="df2-drawer-empty-line">
              Use Edit to change connectors, tables, cadence, or sync mode.
            </p>
          </section>
        )}
      </div>
    </Drawer>
  );
}
