import { ConnectorIcon } from "../../app/brand-icons";
import { Connector } from "../../lib/types";
import { DtIcon } from "../DtIcon";
import { formatRelativeTime } from "../../lib/connectionWorkbench";
import { inferTopologyRole } from "../../lib/topologyUtils";
import { Button } from "./Button";

interface ConnectorCardProps {
  connector: Connector;
  index?: number;
  selected?: boolean;
  highlighted?: boolean;
  testing?: boolean;
  /** Dense single-line row for scannable lists; opens a detail drawer on click. */
  compact?: boolean;
  /** ISO timestamp of the most recent transfer that touched this connection, if any. */
  lastUsedAt?: string | null;
  onSelect?: () => void;
  onTest: () => void;
  onEdit?: () => void;
  onDelete?: () => void;
}

/**
 * Status-first connection row (Airbyte-style): health is the first thing you
 * see, then identity, then last-test and last-used signals, then actions.
 * Used for the saved-connections list. The catalog/add flow uses its own
 * tile layout in ConnectorCatalogPanel.
 */
export function ConnectorCard({
  connector: c,
  selected,
  highlighted,
  testing,
  compact,
  lastUsedAt,
  onSelect,
  onTest,
  onEdit,
  onDelete,
}: ConnectorCardProps) {
  const role = inferTopologyRole(c.type, c.name, c.role);
  const healthy = c.status !== "error" && c.last_test_ok !== false;
  const neverTested = c.last_test_ok == null && c.status !== "error";
  const endpoint = c.host ? `${c.host}${c.port ? `:${c.port}` : ""}` : "";

  if (compact) {
    return (
      <div
        id={`connector-card-${c.id}`}
        className={`df2-connector-row df2-card-interactive ${selected ? "selected" : ""} ${highlighted ? "highlighted" : ""} ${healthy ? "" : "error"}`}
        onClick={onSelect}
        onKeyDown={(e) => {
          if (onSelect && (e.key === "Enter" || e.key === " ")) {
            e.preventDefault();
            onSelect();
          }
        }}
        role={onSelect ? "button" : undefined}
        tabIndex={onSelect ? 0 : undefined}
        aria-current={selected || undefined}
      >
        <span
          className={`df2-health-dot ${healthy ? "ok" : "err"}`}
          aria-hidden
          title={healthy ? "Healthy" : "Connection error"}
        />
        <span className="df2-connector-row-icon" aria-hidden>
          <ConnectorIcon id={c.type} size={20} />
        </span>
        <div className="df2-connector-row-identity">
          <span className="df2-connector-row-name" title={c.name}>{c.name}</span>
          <span
            className="df2-connector-row-meta"
            title={[c.type.replace(/_/g, " "), c.database, endpoint].filter(Boolean).join(" · ")}
          >
            {c.type.replace(/_/g, " ")}{c.database ? ` · ${c.database}` : ""}
          </span>
        </div>
        <span className="df2-badge df2-badge-muted df2-connector-row-role">{role}</span>
        <span className={`df2-connector-row-signal ${healthy ? "ok" : neverTested ? "" : "err"}`} title="Last connection test">
          <DtIcon name={healthy ? "check" : neverTested ? "activity" : "x"} size={12} />
          <span className="df2-connector-row-signal-text">{neverTested ? "Never tested" : healthy ? "Test passed" : "Test failed"}</span>
        </span>
        <span className="df2-connector-row-used" title="Last transfer that used this connection">
          {lastUsedAt ? formatRelativeTime(lastUsedAt) : "Not used"}
        </span>
        <div className="df2-connector-row-quick" onClick={(e) => e.stopPropagation()}>
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
          <button type="button" className="df2-connector-row-open" onClick={onSelect} aria-label={`Open ${c.name} details`}>
            <DtIcon name="chevron-right" size={16} />
          </button>
        </div>
      </div>
    );
  }

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
      <div className="df2-connector-row-top">
        <span
          className={`df2-health-dot ${healthy ? "ok" : "err"}`}
          aria-hidden
          title={healthy ? "Healthy" : "Connection error"}
        />
        <div className="df2-connector-card-icon" aria-hidden>
          <ConnectorIcon id={c.type} size={20} />
        </div>
        <div className="df2-connector-row-identity">
          <h3 className="df2-connector-card-title" title={c.name}>{c.name}</h3>
          <p
            className="df2-connector-card-meta"
            title={[c.type.replace(/_/g, " "), c.database, endpoint].filter(Boolean).join(" · ")}
          >
            {c.type.replace(/_/g, " ")}
            {c.database ? ` · ${c.database}` : ""}
          </p>
        </div>
        <span className="df2-badge df2-badge-muted df2-connector-row-role">{role}</span>
      </div>

      <div className="df2-connector-row-signals">
        <span className={`df2-connector-signal ${healthy ? "ok" : neverTested ? "" : "err"}`}>
          <DtIcon name={healthy ? "check" : neverTested ? "activity" : "x"} size={12} />
          {neverTested ? "Never tested" : healthy ? "Last test passed" : "Last test failed"}
        </span>
        <span className="df2-connector-signal">
          <DtIcon name="transfer" size={12} />
          {lastUsedAt ? `Used ${formatRelativeTime(lastUsedAt)}` : "Not used yet"}
        </span>
      </div>

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
