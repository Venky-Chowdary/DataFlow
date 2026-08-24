import { DtIcon } from "./DtIcon";
import { Button } from "./ui/Button";
import { Drawer } from "./ui/Drawer";
import { formatSeconds } from "../lib/phaseProfile";
import type { TransformProject, TransformRunResult } from "../lib/api";

interface TransformDetailDrawerProps {
  open: boolean;
  project: TransformProject | null;
  destinationName?: string;
  running?: boolean;
  lastRun?: TransformRunResult | null;
  onClose: () => void;
  onDryRun: () => void;
  onRun: () => void;
  onExportDbt: () => void;
  onEdit: () => void;
  onDelete: () => void;
}

/**
 * Schedules-style right rail for a post-load transform project.
 * List stays clean; Dry run / Run / Export / Edit / Delete live here.
 */
export function TransformDetailDrawer({
  open,
  project,
  destinationName,
  running,
  lastRun,
  onClose,
  onDryRun,
  onRun,
  onExportDbt,
  onEdit,
  onDelete,
}: TransformDetailDrawerProps) {
  if (!project) return null;

  const autoLabel = project.run_after_transfer ? "After transfer" : "Manual";

  return (
    <Drawer
      open={open}
      onClose={onClose}
      size="lg"
      ariaLabel={`${project.name} transform details`}
      icon={<DtIcon name="layers" size={22} />}
      title={project.name}
      subtitle={`${destinationName || project.destination_connector_id}${
        project.schema ? ` · ${project.schema}` : ""
      }`}
      headerExtra={
        <>
          <span className={`df2-badge ${project.enabled ? "df2-badge-live" : "df2-badge-muted"}`}>
            {project.enabled ? "Enabled" : "Disabled"}
          </span>
          <span className="df2-badge df2-badge-muted">{autoLabel}</span>
          <span className="df2-badge df2-badge-muted">v{project.version ?? 0}</span>
        </>
      }
      footer={
        <div className="df2-drawer-actions">
          <Button
            size="sm"
            variant="ghost"
            loading={running}
            onClick={onDryRun}
            leadingIcon={<DtIcon name="activity" size={14} />}
          >
            Dry run
          </Button>
          <Button
            size="sm"
            variant="primary"
            loading={running}
            onClick={onRun}
            leadingIcon={<DtIcon name="play" size={14} />}
          >
            Run now
          </Button>
          <Button
            size="sm"
            variant="ghost"
            onClick={onExportDbt}
            leadingIcon={<DtIcon name="download" size={14} />}
          >
            Export dbt
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
            className="df2-drawer-action-delete"
            onClick={onDelete}
            leadingIcon={<DtIcon name="trash" size={14} />}
          >
            Delete
          </Button>
        </div>
      }
    >
      <div className="df2-drawer-facts" aria-label="Transform summary">
        <div className="df2-drawer-fact">
          <span>Destination</span>
          <strong title={project.destination_connector_id}>
            {destinationName || project.destination_connector_id}
          </strong>
        </div>
        <div className="df2-drawer-fact">
          <span>Schema</span>
          <strong>{project.schema || "—"}</strong>
        </div>
        <div className="df2-drawer-fact">
          <span>Models</span>
          <strong>{project.models.length}</strong>
        </div>
        <div className="df2-drawer-fact">
          <span>Trigger</span>
          <strong>{autoLabel}</strong>
        </div>
        <div className="df2-drawer-fact">
          <span>Contract</span>
          <strong title={project.contract_id || undefined}>
            {project.contract_id ? project.contract_id : "None"}
          </strong>
        </div>
        <div className="df2-drawer-fact">
          <span>Version</span>
          <strong>v{project.version ?? 0}</strong>
        </div>
      </div>

      {project.description ? (
        <p className="df2-drawer-empty-line">{project.description}</p>
      ) : null}

      {project.run_after_transfer && (
        <section className="df2-drawer-section" aria-label="Auto-run triggers">
          <div className="df2-drawer-section-head">
            <h3>Runs after transfer</h3>
          </div>
          <p className="df2-drawer-empty-line">
            {project.trigger_tables.length
              ? `When these tables land: ${project.trigger_tables.join(", ")}`
              : "When any table lands on this destination."}
          </p>
        </section>
      )}

      <section className="df2-drawer-section" aria-label="Models">
        <div className="df2-drawer-section-head">
          <h3>Models</h3>
          <span className="df2-drawer-count">{project.models.length}</span>
        </div>
        <ul className="df2-xform-drawer-models">
          {project.models.map((m) => (
            <li key={m.name || m.sql.slice(0, 24)}>
              <DtIcon name="layers" size={13} />
              <code>{m.name || "(unnamed)"}</code>
              <span className="df2-badge df2-badge-muted">{m.materialization}</span>
              {m.enabled === false && <span className="df2-badge df2-badge-muted">off</span>}
              {(m.refs?.length ?? 0) > 0 && (
                <span className="df2-xform-refs">→ {m.refs!.join(", ")}</span>
              )}
            </li>
          ))}
        </ul>
      </section>

      {project.plan?.layers && project.plan.layers.length > 0 && (
        <section className="df2-drawer-section" aria-label="Execution plan">
          <div className="df2-drawer-section-head">
            <h3>Plan layers</h3>
          </div>
          <ol className="df2-xform-drawer-layers">
            {project.plan.layers.map((layer, i) => (
              <li key={i}>
                <span>L{i + 1}</span>
                <code>{layer.join(", ")}</code>
              </li>
            ))}
          </ol>
        </section>
      )}

      {lastRun && (
        <section className="df2-drawer-section" aria-label="Last run">
          <div className="df2-drawer-section-head">
            <h3>Last run</h3>
            <span className="df2-drawer-count">
              {lastRun.status} · {formatSeconds(lastRun.seconds)}
            </span>
          </div>
          {lastRun.error && <p className="df2-xform-preview-error">{lastRun.error}</p>}
          <ul className="df2-xform-run-list">
            {lastRun.models.map((m) => (
              <li key={m.name} className={`is-${m.status}`}>
                <DtIcon
                  name={
                    m.status === "success" ? "check" : m.status === "failed" ? "alert" : "minus"
                  }
                  size={13}
                />
                <code>{m.name}</code>
                <span>{m.materialization}</span>
                <span>{formatSeconds(m.seconds)}</span>
                {m.error && <em>{m.error}</em>}
              </li>
            ))}
          </ul>
        </section>
      )}
    </Drawer>
  );
}
