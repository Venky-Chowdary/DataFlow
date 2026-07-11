import { ConnectorIcon } from "../../app/brand-icons";
import { Connector } from "../../lib/types";
import { DtIcon } from "../DtIcon";
import { inferTopologyRole } from "../../lib/topologyUtils";

interface ConnectorCardProps {
  connector: Connector;
  index?: number;
  selected?: boolean;
  highlighted?: boolean;
  testing?: boolean;
  onSelect?: () => void;
  onTest: () => void;
  onEdit: () => void;
  onDelete: () => void;
}

export function ConnectorCard({
  connector: c,
  selected,
  highlighted,
  testing,
  onSelect,
  onTest,
  onEdit,
  onDelete,
}: ConnectorCardProps) {
  const role = inferTopologyRole(c.type, c.name, c.role);
  const healthy = c.status !== "error";

  return (
    <article
      id={`connector-card-${c.id}`}
      className={`df2-connector-card ${selected ? "selected" : ""} ${highlighted ? "highlighted" : ""} ${healthy ? "" : "error"}`}
      onClick={onSelect}
      onKeyDown={(e) => {
        if (onSelect && (e.key === "Enter" || e.key === " ")) {
          e.preventDefault();
          onSelect();
        }
      }}
      role={onSelect ? "button" : undefined}
      tabIndex={onSelect ? 0 : undefined}
    >
      <div className="df2-connector-card-head">
        <div className="df2-connector-card-icon">
          <ConnectorIcon id={c.type} size={28} />
        </div>
        <div className="df2-connector-card-badges">
          <span className={`df2-badge ${healthy ? "df2-badge-live" : "df2-badge-error"}`}>
            {c.status || "ready"}
          </span>
          <span className="df2-badge df2-badge-muted">{role}</span>
        </div>
      </div>

      <h3 className="df2-connector-card-title" title={c.name}>{c.name}</h3>
      <p className="df2-connector-card-meta" title={c.type}>
        {c.type.replace(/_/g, " ")}
        {c.database ? ` · ${c.database}` : ""}
      </p>
      {(c.host || c.port) && (
        <p className="df2-connector-card-endpoint" title={`${c.host}${c.port ? `:${c.port}` : ""}`}>
          {c.host}{c.port ? `:${c.port}` : ""}
        </p>
      )}

      <div className="df2-connector-card-actions" onClick={(e) => e.stopPropagation()}>
        <button
          type="button"
          className="df2-btn df2-btn-ghost df2-btn-sm"
          disabled={testing}
          onClick={onTest}
        >
          {testing ? (
            <>
              <span className="df2-inline-spinner" aria-hidden />
              Testing…
            </>
          ) : (
            <>
              <DtIcon name="activity" size={14} />
              Test
            </>
          )}
        </button>
        <button type="button" className="df2-btn df2-btn-ghost df2-btn-sm" onClick={onEdit}>
          <DtIcon name="settings" size={14} />
          Edit
        </button>
        <button type="button" className="df2-btn df2-btn-ghost df2-btn-sm df2-btn-danger" onClick={onDelete}>
          <DtIcon name="x" size={14} />
        </button>
      </div>
    </article>
  );
}
