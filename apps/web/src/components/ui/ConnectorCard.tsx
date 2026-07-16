import { ConnectorIcon } from "../../app/brand-icons";
import { Connector } from "../../lib/types";
import { DtIcon } from "../DtIcon";
import { inferTopologyRole } from "../../lib/topologyUtils";
import { Button } from "./Button";

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
  const createdAt = c.created_at ? new Date(c.created_at) : null;
  const createdLabel = createdAt && !Number.isNaN(createdAt.getTime())
    ? createdAt.toLocaleDateString()
    : "Unknown";

  const endpoint = c.host ? `${c.host}${c.port ? `:${c.port}` : ""}` : "";

  return (
    <article
      id={`connector-card-${c.id}`}
      className={`df2-connector-card df2-card-interactive ${selected ? "selected" : ""} ${highlighted ? "highlighted" : ""} ${healthy ? "" : "error"}`}
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
      <div className="df2-connector-card-icon" aria-hidden>
        <ConnectorIcon id={c.type} size={22} />
      </div>

      <div className="df2-connector-card-head">
        <div className="df2-connector-card-badges">
          <span className={`df2-badge ${healthy ? "df2-badge-live" : "df2-badge-error"}`}>
            {c.status || "ready"}
          </span>
          <span className="df2-badge df2-badge-muted">{role}</span>
        </div>
      </div>

      <h3 className="df2-connector-card-title" title={c.name}>{c.name}</h3>
      <p
        className="df2-connector-card-meta"
        title={[c.type.replace(/_/g, " "), c.database, endpoint].filter(Boolean).join(" · ")}
      >
        {c.type.replace(/_/g, " ")}
        {c.database ? ` · ${c.database}` : ""}
      </p>
      {endpoint && (
        <p className="df2-connector-card-endpoint" title={endpoint}>{endpoint}</p>
      )}
      <p className="df2-connector-card-created" title={c.created_at || ""}>
        Added {createdLabel}
      </p>

      <div className="df2-connector-card-actions" onClick={(e) => e.stopPropagation()}>
        <Button
          size="sm"
          variant="ghost"
          loading={testing}
          loadingLabel="Testing…"
          onClick={onTest}
          leadingIcon={<DtIcon name="activity" size={14} />}
        >
          Test
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
          className="df2-connector-delete-btn"
          onClick={onDelete}
          aria-label={`Delete ${c.name}`}
          title={`Delete ${c.name}`}
          leadingIcon={<DtIcon name="x" size={14} />}
        >
          Delete
        </Button>
      </div>
    </article>
  );
}
