import { useEffect, useState } from "react";
import { ConnectorIcon } from "../app/brand-icons";
import { DtIcon } from "./DtIcon";
import { Button } from "./ui/Button";
import { Drawer } from "./ui/Drawer";
import { FilterTabs } from "./ui/FilterTabs";
import { ScheduleRunHistory } from "./schedules/ScheduleRunHistory";
import { fetchJob, fetchSchedule } from "../lib/api";
import { Connector, PipelineSchedule, TransferJob } from "../lib/types";
import { jobStatusBadgeClass, jobStatusLabel } from "../lib/uiUtils";

export const PIPELINE_TABS = ["Overview", "Schema", "History", "Config"] as const;
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
  incremental_append: "Incremental append",
  incremental_deduped: "Incremental deduped",
  cdc: "CDC",
  scd2: "SCD Type 2",
  mirror: "Mirror",
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
  const [mappingCount, setMappingCount] = useState(0);
  const [mappings, setMappings] = useState<{ source: string; target: string }[]>([]);
  const [lastJob, setLastJob] = useState<TransferJob | null>(null);

  useEffect(() => {
    if (!open || !sched?.id) {
      setMappingCount(0);
      setMappings([]);
      setLastJob(null);
      return;
    }
    let cancelled = false;
    void fetchSchedule(sched.id)
      .then((full) => {
        if (cancelled) return;
        const maps = Array.isArray(full.mappings) ? full.mappings : [];
        setMappings(
          maps.map((m) => ({
            source: String(m.source ?? ""),
            target: String(m.target ?? ""),
          })).filter((m) => m.source || m.target),
        );
        setMappingCount(
          typeof full.mapping_count === "number" ? full.mapping_count : maps.length,
        );
      })
      .catch(() => {
        if (!cancelled) {
          setMappingCount(0);
          setMappings([]);
        }
      });
    if (sched.last_job_id) {
      void fetchJob(sched.last_job_id)
        .then((j) => {
          if (!cancelled) setLastJob(j);
        })
        .catch(() => {
          if (!cancelled) setLastJob(null);
        });
    } else {
      setLastJob(null);
    }
    return () => {
      cancelled = true;
    };
  }, [open, sched?.id, sched?.last_job_id]);

  if (!sched) return null;

  const isRunning = Boolean(running || sched.running);
  const cadence = sched.cron ? `Cron ${sched.cron}` : (INTERVAL_LABEL[sched.interval] ?? sched.interval);
  const syncLabel = SYNC_MODE_LABEL[sched.sync_mode] ?? sched.sync_mode;
  const rejected = Number(lastJob?.rejected_rows ?? 0);
  const coerced = Number(lastJob?.coerced_null_rows ?? 0);
  const needsAttention =
    Boolean(sched.last_status && /fail|error/i.test(String(sched.last_status)))
    || rejected > 0
    || !sched.enabled;

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
          {needsAttention && (
            <span className="df2-badge df2-badge-warn">Needs attention</span>
          )}
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
          {sched.last_job_id && onOpenJob && (
            <Button
              size="sm"
              variant="ghost"
              onClick={() => onOpenJob(sched.last_job_id!)}
              leadingIcon={<DtIcon name="jobs" size={14} />}
            >
              Last job
            </Button>
          )}
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

      {(lastJob || mappingCount > 0) && (
        <div className="df2-drawer-facts df2-drawer-trust" aria-label="Integrity summary">
          <div className="df2-drawer-fact">
            <span>Mapped columns</span>
            <strong>{mappingCount || "—"}</strong>
          </div>
          <div className="df2-drawer-fact">
            <span>Last rows</span>
            <strong>{(lastJob?.records_processed ?? 0).toLocaleString()}</strong>
          </div>
          <div className="df2-drawer-fact">
            <span>Quarantine</span>
            <strong className={rejected > 0 ? "df2-text-warn" : undefined}>{rejected}</strong>
          </div>
          <div className="df2-drawer-fact">
            <span>Coerced nulls</span>
            <strong>{coerced}</strong>
          </div>
        </div>
      )}

      <div className="df2-drawer-section df2-drawer-workbench">
        <FilterTabs
          ariaLabel="Pipeline detail sections"
          value={tab}
          onChange={setTab}
          items={PIPELINE_TABS.map((id) => ({
            id,
            label: id,
            count: id === "History" ? sched.run_count : id === "Schema" ? (mappingCount || undefined) : undefined,
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

        {tab === "Schema" && (
          <section className="df2-drawer-section" aria-label="Schema mapping">
            <div className="df2-drawer-section-head">
              <h3><DtIcon name="connectors" size={14} /> Schema map</h3>
              <span className="df2-drawer-count">{mappingCount}</span>
            </div>
            <dl className="df2-drawer-kv">
              <div><dt>Mapped columns</dt><dd>{mappingCount || "None stored on schedule"}</dd></div>
              <div><dt>Primary key</dt><dd>{sched.primary_key || "—"}</dd></div>
              <div><dt>Cursor column</dt><dd>{sched.cursor_column || "—"}</dd></div>
              <div><dt>Schema policy</dt><dd>{sched.schema_policy || "—"}</dd></div>
              <div><dt>Validation mode</dt><dd>{sched.validation_mode || "—"}</dd></div>
              <div><dt>Backfill new fields</dt><dd>{sched.backfill_new_fields ? "Yes" : "No"}</dd></div>
            </dl>
            {mappings.length > 0 ? (
              <ul className="df2-drawer-map-list" aria-label="Column mappings">
                {mappings.map((m) => (
                  <li key={`${m.source}->${m.target}`}>
                    <code>{m.source || "—"}</code>
                    <span aria-hidden>→</span>
                    <code>{m.target || "—"}</code>
                  </li>
                ))}
              </ul>
            ) : (
              <p className="df2-drawer-empty-line">
                No column mappings stored on this pipeline yet. Use Edit to set the schema map, or open the last job for quarantine replay.
              </p>
            )}
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
