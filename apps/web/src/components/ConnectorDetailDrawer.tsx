import { ConnectorIcon } from "../app/brand-icons";
import { DtIcon } from "./DtIcon";
import { Button } from "./ui/Button";
import { Drawer } from "./ui/Drawer";
import { EmptyState } from "./ui/EmptyState";
import { ConnectionWorkbench, CONNECTION_TABS } from "./ConnectionWorkbench";
import { Connector } from "../lib/types";
import { ConnectionWorkbenchContext, formatRelativeTime } from "../lib/connectionWorkbench";
import {
  formatConnectorRoleLabel,
  resolveConnectorUsage,
  resolveDisplayRole,
} from "../lib/topologyUtils";
import { jobStatusBadgeClass, jobStatusLabel } from "../lib/uiUtils";

interface ConnectorDetailDrawerProps {
  open: boolean;
  connector: Connector | null;
  workbench: ConnectionWorkbenchContext | null;
  connectors: Connector[];
  connectionTab: (typeof CONNECTION_TABS)[number];
  setConnectionTab: (tab: (typeof CONNECTION_TABS)[number]) => void;
  testing?: boolean;
  onClose: () => void;
  onTest: () => void;
  onEdit: () => void;
  onDelete: () => void;
  onOpenTransfer?: (connectorId?: string) => void;
  onSelectConnection: (id: string) => void;
  onOpenJob?: (jobId: string) => void;
}

const INTERVAL_LABEL: Record<string, string> = {
  hourly: "Hourly",
  daily: "Daily",
  weekly: "Weekly",
};

export function ConnectorDetailDrawer({
  open,
  connector: c,
  workbench,
  connectors,
  connectionTab,
  setConnectionTab,
  testing,
  onClose,
  onTest,
  onEdit,
  onDelete,
  onOpenTransfer,
  onSelectConnection,
  onOpenJob,
}: ConnectorDetailDrawerProps) {
  if (!c) return null;

  const healthy = c.status !== "error" && c.last_test_ok !== false;
  const neverTested = c.last_test_ok == null && c.status !== "error";
  const endpoint = c.host ? `${c.host}${c.port ? `:${c.port}` : ""}` : "Managed endpoint";
  const relatedJobs = workbench?.relatedJobs ?? [];
  const relatedSchedules = workbench?.relatedSchedules ?? [];
  const displayRole = resolveDisplayRole(c, relatedJobs, relatedSchedules);
  const roleLabel = formatConnectorRoleLabel(displayRole);
  const usage = resolveConnectorUsage(c, relatedJobs, relatedSchedules);

  return (
    <Drawer
      open={open}
      onClose={onClose}
      width={620}
      ariaLabel={`${c.name} connection details`}
      icon={<ConnectorIcon id={c.type} size={22} />}
      title={c.name}
      subtitle={`${c.type.replace(/_/g, " ")} · ${endpoint}`}
      headerExtra={
        <span className={`df2-badge ${healthy ? "df2-badge-live" : "df2-badge-error"}`}>
          <span className={`df2-health-dot ${healthy ? "ok" : "err"}`} aria-hidden />
          {neverTested ? "Never tested" : healthy ? "Healthy" : "Connection error"}
        </span>
      }
      footer={
        <div className="df2-drawer-actions">
          <Button
            size="sm"
            variant="secondary"
            loading={testing}
            loadingLabel="Testing…"
            onClick={onTest}
            leadingIcon={<DtIcon name="activity" size={14} />}
          >
            Test
          </Button>
          <Button size="sm" variant="ghost" onClick={onEdit} leadingIcon={<DtIcon name="settings" size={14} />}>
            Edit
          </Button>
          {onOpenTransfer && (
            <Button
              size="sm"
              variant="primary"
              onClick={() => onOpenTransfer(c.id)}
              leadingIcon={<DtIcon name="transfer" size={14} />}
            >
              New transfer
            </Button>
          )}
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
      {/* Identity / endpoint summary */}
      <div className="df2-drawer-facts" aria-label="Connection details">
        <div className="df2-drawer-fact">
          <span>Type</span>
          <strong>{c.type.replace(/_/g, " ")}</strong>
        </div>
        <div className="df2-drawer-fact">
          <span>Capability</span>
          <strong title={usage.hint || roleLabel}>
            {roleLabel}
            {usage.hint ? ` · ${usage.hint}` : ""}
          </strong>
        </div>
        <div className="df2-drawer-fact">
          <span>Database / bucket</span>
          <strong>{c.database || "—"}</strong>
        </div>
        <div className="df2-drawer-fact df2-drawer-fact-endpoint">
          <span>Endpoint</span>
          <strong title={endpoint}>{endpoint}</strong>
        </div>
        <div className="df2-drawer-fact">
          <span>Last test</span>
          <strong className={healthy ? "df2-fact-ok" : neverTested ? "" : "df2-fact-err"}>
            {neverTested ? "Never tested" : healthy ? "Passed" : "Failed"}
          </strong>
        </div>
      </div>
      {displayRole === "both" && (
        <p className="df2-drawer-empty-line" style={{ marginTop: 0 }}>
          This connection can be selected as a <strong>source</strong> or a <strong>destination</strong> in Transfer Studio.
          Using it as a destination does not make it “source-only.”
        </p>
      )}

      {/* Related jobs & pipelines */}
      <section className="df2-drawer-section" aria-label="Related jobs and pipelines">
        <div className="df2-drawer-section-head">
          <h3><DtIcon name="activity" size={14} /> Related pipelines</h3>
          <span className="df2-drawer-count">{relatedSchedules.length}</span>
        </div>
        {relatedSchedules.length === 0 ? (
          <p className="df2-drawer-empty-line">No pipelines reference this connection yet.</p>
        ) : (
          <ul className="df2-drawer-related-list">
            {relatedSchedules.slice(0, 6).map((s) => {
              const roleTag = s.source_connector_id === c.id
                ? (s.dest_connector_id === c.id ? "source · destination" : "source")
                : "destination";
              return (
                <li key={s.id} className="df2-drawer-related-row">
                  <span className="df2-drawer-related-main">
                    <strong title={s.name}>{s.name}</strong>
                    <small>{INTERVAL_LABEL[s.interval] ?? s.interval} · {roleTag}</small>
                  </span>
                  {s.last_status ? (
                    <span className={jobStatusBadgeClass(s.last_status)}>{jobStatusLabel(s.last_status)}</span>
                  ) : (
                    <span className={`df2-badge ${s.enabled ? "df2-badge-live" : "df2-badge-muted"}`}>{s.enabled ? "Active" : "Paused"}</span>
                  )}
                </li>
              );
            })}
          </ul>
        )}

        <div className="df2-drawer-section-head">
          <h3><DtIcon name="jobs" size={14} /> Recent jobs</h3>
          <span className="df2-drawer-count">{relatedJobs.length}</span>
        </div>
        {relatedJobs.length === 0 ? (
          <p className="df2-drawer-empty-line">No transfer jobs have used this connection yet.</p>
        ) : (
          <ul className="df2-drawer-related-list">
            {relatedJobs.slice(0, 6).map((job) => (
              <li
                key={job._id}
                className={`df2-drawer-related-row${onOpenJob ? " is-clickable" : ""}`}
                onClick={onOpenJob ? () => onOpenJob(job._id) : undefined}
                role={onOpenJob ? "button" : undefined}
                tabIndex={onOpenJob ? 0 : undefined}
                onKeyDown={onOpenJob ? (e) => { if (e.key === "Enter" || e.key === " ") { e.preventDefault(); onOpenJob(job._id); } } : undefined}
              >
                <span className="df2-drawer-related-main">
                  <strong title={job.source_name}>{job.source_name || "Transfer job"}</strong>
                  <small>{job.source_type} → {job.destination_type} · {formatRelativeTime(job.created_at)}</small>
                </span>
                <span className={jobStatusBadgeClass(job.status)}>{jobStatusLabel(job.status)}</span>
              </li>
            ))}
          </ul>
        )}
      </section>

      {/* Live schema introspection / deep tabs — reuse ConnectionWorkbench */}
      <section className="df2-drawer-section df2-drawer-workbench" aria-label="Live inspection">
        {connectors.length === 0 ? (
          <EmptyState compact icon="connectors" title="No connection" description="Select a saved connection to inspect." />
        ) : (
          <ConnectionWorkbench
            hideHeader
            selectedConnection={c}
            workbench={workbench}
            connectionTab={connectionTab}
            setConnectionTab={setConnectionTab}
            connectors={connectors}
            onSelectConnection={onSelectConnection}
            onOpenTransfer={onOpenTransfer}
          />
        )}
      </section>
    </Drawer>
  );
}
