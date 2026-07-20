import { useCallback, useEffect, useState } from "react";
import { EmptyState } from "./ui/EmptyState";
import { FilterTabs } from "./ui/FilterTabs";
import { FilterBar } from "./ui/FilterBar";
import { Button } from "./ui/Button";
import { Connector } from "../lib/types";
import { ConnectionWorkbenchContext, formatRelativeTime } from "../lib/connectionWorkbench";
import { connectorHealthLabel, jobStatusBadgeClass, jobStatusLabel } from "../lib/uiUtils";
import { introspectTransferEndpoints, type EndpointIntrospection } from "../lib/api";
import {
  formatConnectorRoleLabel,
  resolveConnectorUsage,
  resolveDisplayRole,
} from "../lib/topologyUtils";

export const CONNECTION_TABS = ["Status", "Streams", "Schema", "Mappings", "Sync History", "Settings"] as const;

interface ConnectionWorkbenchProps {
  selectedConnection: Connector | undefined;
  workbench: ConnectionWorkbenchContext | null;
  connectionTab: (typeof CONNECTION_TABS)[number];
  setConnectionTab: (tab: (typeof CONNECTION_TABS)[number]) => void;
  connectors: Connector[];
  onSelectConnection: (id: string) => void;
  onOpenTransfer?: (connectorId?: string) => void;
  /** Hide the identity header + connection picker (e.g. when embedded in a drawer already scoped to one connection). */
  hideHeader?: boolean;
}

type SchemaObject = { name: string; columns?: { name: string; type?: string }[] };

export function ConnectionWorkbench({
  selectedConnection,
  workbench,
  connectionTab,
  setConnectionTab,
  connectors,
  onSelectConnection,
  onOpenTransfer,
  hideHeader,
}: ConnectionWorkbenchProps) {
  const [schemaObjects, setSchemaObjects] = useState<SchemaObject[]>([]);
  const [schemaError, setSchemaError] = useState("");
  const [schemaLoading, setSchemaLoading] = useState(false);

  const roleLabel = selectedConnection
    ? formatConnectorRoleLabel(
        resolveDisplayRole(
          selectedConnection,
          workbench?.relatedJobs ?? [],
          workbench?.relatedSchedules ?? [],
        ),
      )
    : "";
  const usageHint = selectedConnection
    ? resolveConnectorUsage(
        selectedConnection,
        workbench?.relatedJobs ?? [],
        workbench?.relatedSchedules ?? [],
      ).hint
    : null;

  const loadSchema = useCallback(async () => {
    if (!selectedConnection?.id) {
      setSchemaObjects([]);
      return;
    }
    setSchemaLoading(true);
    setSchemaError("");
    try {
      const res = await introspectTransferEndpoints({
        source: {
          kind: "database",
          format: selectedConnection.type,
          connector_id: selectedConnection.id,
        },
        destination: {
          kind: "database",
          format: "sqlite",
          connection_string: ":memory:",
          table: "_probe",
        },
      });
      const src = res.source as EndpointIntrospection & {
        tables?: SchemaObject[];
        collections?: SchemaObject[];
        objects?: Array<string | SchemaObject>;
        message?: string;
      };
      const objsRaw = (src.tables || src.collections || src.objects || []) as Array<string | SchemaObject>;
      const objs = objsRaw.map((o) =>
        typeof o === "string"
          ? { name: o }
          : { name: o.name || "", columns: o.columns },
      );
      setSchemaObjects(objs.filter((o) => o.name));
      if (!objs.length) {
        setSchemaError(src.message || "No tables/collections returned. Verify credentials and database name.");
      }
    } catch (e) {
      setSchemaObjects([]);
      setSchemaError(e instanceof Error ? e.message : "Schema introspection failed");
    } finally {
      setSchemaLoading(false);
    }
  }, [selectedConnection?.id, selectedConnection?.type]);

  useEffect(() => {
    if (connectionTab === "Schema" && selectedConnection) {
      void loadSchema();
    }
  }, [connectionTab, selectedConnection, loadSchema]);

  return (
    <section className="df2-connection-workbench" aria-label="Connection operations workbench">
      {!hideHeader && (
        <div className="df2-connection-workbench-head">
          <div>
            <span className="df2-rail-kicker">Connection workbench</span>
            <h2>{selectedConnection?.name ?? "Select a connection"}</h2>
            <p>
              {selectedConnection
                ? `${selectedConnection.type} · ${selectedConnection.host || "managed endpoint"}${selectedConnection.port ? `:${selectedConnection.port}` : ""}`
                : "Pick a saved connection to inspect health, live schema, streams, and sync history."}
            </p>
          </div>
          <div className="df2-connection-picker">
            <label className="df2-label" htmlFor="connection-workbench-picker">Connection</label>
            <select
              id="connection-workbench-picker"
              className="df2-input df2-select"
              value={selectedConnection?.id ?? ""}
              onChange={(e) => onSelectConnection(e.target.value)}
              disabled={connectors.length === 0}
            >
              {connectors.length === 0 ? (
                <option value="">No saved connections</option>
              ) : connectors.map((c) => (
                <option key={c.id} value={c.id}>{c.name} · {c.type}</option>
              ))}
            </select>
          </div>
        </div>
      )}

      <FilterBar ariaLabel="Connection sections">
        <FilterTabs
          ariaLabel="Connection sections"
          value={connectionTab}
          onChange={setConnectionTab}
          items={CONNECTION_TABS.map((item) => ({ id: item, label: item }))}
        />
      </FilterBar>

      <div className="df2-connection-panel">
        {connectionTab === "Status" && (
          <div className="df2-connection-status-grid">
            <div>
              <span>Health</span>
              <strong>
                {selectedConnection
                  ? connectorHealthLabel(selectedConnection.status, selectedConnection.last_test_ok)
                  : "Not configured"}
              </strong>
            </div>
            <div>
              <span>Last sync</span>
              <strong>
                {workbench?.lastJob
                  ? `${jobStatusLabel(workbench.lastJob.status)} · ${formatRelativeTime(workbench.lastJob.created_at)}`
                  : "No runs yet"}
              </strong>
            </div>
            <div>
              <span>Schedule</span>
              <strong>{workbench?.scheduleLabel ?? "Manual"}</strong>
            </div>
            <div>
              <span>Activity</span>
              <strong>
                {workbench
                  ? `${workbench.completedCount} done · ${workbench.runningCount} live · ${workbench.failedCount} failed`
                  : "—"}
              </strong>
            </div>
          </div>
        )}
        {connectionTab === "Streams" && (
          selectedConnection ? (
            workbench && workbench.streams.length > 0 ? (
              <div className="df2-stream-list">
                {workbench.streams.map((stream) => (
                  <div key={stream.name} className="df2-stream-row">
                    <strong>{stream.name}</strong>
                    <span className="df2-badge df2-badge-muted">
                      {stream.source === "schedule" ? "Scheduled pipeline" : "Transfer job"}
                    </span>
                  </div>
                ))}
              </div>
            ) : (
              <EmptyState
                compact
                icon="activity"
                title="No streams yet"
                description={`Streams appear when you run a transfer or enable a pipeline from ${selectedConnection.name}.`}
                action={onOpenTransfer ? (
                  <Button
                    variant="primary"
                    size="sm"
                    onClick={() => onOpenTransfer(selectedConnection.id)}
                  >
                    New transfer
                  </Button>
                ) : undefined}
              />
            )
          ) : (
            <EmptyState compact icon="connectors" title="Select a connection" description="Choose a saved connection above to view streams." />
          )
        )}
        {connectionTab === "Schema" && (
          selectedConnection ? (
            <div className="df2-schema-live">
              <div className="df2-schema-live-toolbar">
                <div>
                  <span className="df2-rail-kicker">Live introspection</span>
                  <h4>{selectedConnection.name}</h4>
                  <p className="df2-muted">Tables/collections returned by the connector — not marketing placeholders.</p>
                </div>
                <Button variant="secondary" size="sm" onClick={() => void loadSchema()} disabled={schemaLoading}>
                  {schemaLoading ? "Refreshing…" : "Refresh schema"}
                </Button>
              </div>
              {schemaError && !schemaObjects.length ? (
                <EmptyState compact icon="alert" title="Schema unavailable" description={schemaError} />
              ) : schemaLoading && !schemaObjects.length ? (
                <p className="df2-muted">Loading schema…</p>
              ) : schemaObjects.length === 0 ? (
                <EmptyState compact icon="database" title="No objects found" description="This database has no introspectable tables yet, or the connector needs a database/schema name." />
              ) : (
                <div className="df2-table-wrap">
                  <table className="df2-table" aria-label="Live schema">
                    <thead>
                      <tr>
                        <th>Object</th>
                        <th>Columns</th>
                        <th>Sample types</th>
                      </tr>
                    </thead>
                    <tbody>
                      {schemaObjects.slice(0, 40).map((obj) => (
                        <tr key={obj.name}>
                          <td><strong>{obj.name}</strong></td>
                          <td>{obj.columns?.length ?? "—"}</td>
                          <td className="df2-cell-meta">
                            {(obj.columns || []).slice(0, 4).map((c) => `${c.name}:${c.type || "?"}`).join(", ") || "—"}
                          </td>
                        </tr>
                      ))}
                    </tbody>
                  </table>
                </div>
              )}
            </div>
          ) : (
            <EmptyState compact icon="connectors" title="Select a connection" description="Choose a saved connection above to introspect schema." />
          )
        )}
        {connectionTab === "Mappings" && (
          selectedConnection ? (
            <div className="df2-mapping-policy">
              <div className="df2-mapping-policy-grid">
                <div>
                  <strong>Recent routes</strong>
                  <span>{workbench?.relatedJobs.length ?? 0} job(s) reference this connector.</span>
                </div>
                <div>
                  <strong>Pipelines</strong>
                  <span>{workbench?.relatedSchedules.length ?? 0} schedule(s) · {workbench?.enabledScheduleCount ?? 0} enabled.</span>
                </div>
                <div>
                  <strong>Role</strong>
                  <span>
                    {roleLabel}
                    {usageHint ? ` · ${usageHint}` : " · usable as source or destination"}
                  </span>
                </div>
                <div>
                  <strong>Edit mappings</strong>
                  <span>Column mapping lives in Transfer Studio Map step (semantic + confidence).</span>
                </div>
              </div>
              {onOpenTransfer ? (
                <div className="df2-mapping-policy-cta">
                  <p>Continue with this connection as the Transfer Studio source.</p>
                  <Button
                    variant="primary"
                    size="sm"
                    onClick={() => onOpenTransfer(selectedConnection.id)}
                  >
                    Open Transfer Studio
                  </Button>
                </div>
              ) : null}
            </div>
          ) : (
            <EmptyState compact icon="sparkle" title="Select a connection" description="Choose a saved connection above to view mapping activity." />
          )
        )}
        {connectionTab === "Sync History" && (
          selectedConnection ? (
            workbench && workbench.relatedJobs.length > 0 ? (
              <div className="df2-table-wrap df2-card-body-flush">
                <table className="df2-table" aria-label="Sync history">
                  <thead>
                    <tr>
                      <th>Route</th>
                      <th>Status</th>
                      <th>Rows</th>
                      <th>When</th>
                    </tr>
                  </thead>
                  <tbody>
                    {workbench.relatedJobs.slice(0, 12).map((job) => (
                      <tr key={job._id}>
                        <td>
                          <div className="df2-cell-title">{job.source_name}</div>
                          <div className="df2-cell-meta">{job.source_type} → {job.destination_type}</div>
                        </td>
                        <td><span className={jobStatusBadgeClass(job.status)}>{jobStatusLabel(job.status)}</span></td>
                        <td>{job.records_processed?.toLocaleString() ?? "—"}</td>
                        <td className="df2-cell-meta">{formatRelativeTime(job.created_at)}</td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            ) : (
              <EmptyState
                compact
                icon="jobs"
                title="No sync history"
                description={`Run a transfer or enable a pipeline to populate history for ${selectedConnection.name}.`}
                action={onOpenTransfer ? (
                  <Button
                    variant="primary"
                    size="sm"
                    onClick={() => onOpenTransfer(selectedConnection.id)}
                  >
                    New transfer
                  </Button>
                ) : undefined}
              />
            )
          ) : (
            <EmptyState compact icon="connectors" title="Select a connection" description="Choose a saved connection above to view sync history." />
          )
        )}
        {connectionTab === "Settings" && (
          selectedConnection ? (
            <div className="df2-settings-mini-grid">
              <div>
                <span>Sync frequency</span>
                <strong>{workbench?.scheduleLabel ?? "Manual"}</strong>
              </div>
              <div>
                <span>Database / bucket</span>
                <strong title={selectedConnection.database || undefined}>
                  {selectedConnection.database || "—"}
                </strong>
              </div>
              <div className="df2-settings-mini-endpoint">
                <span>Endpoint</span>
                <strong
                  title={
                    selectedConnection.host
                      ? `${selectedConnection.host}${selectedConnection.port ? `:${selectedConnection.port}` : ""}`
                      : "managed"
                  }
                >
                  {selectedConnection.host || "managed"}
                  {selectedConnection.port ? `:${selectedConnection.port}` : ""}
                </strong>
              </div>
              <div>
                <span>Pipelines</span>
                <strong>{workbench?.relatedSchedules.length ?? 0} configured</strong>
              </div>
            </div>
          ) : (
            <EmptyState compact icon="settings" title="Select a connection" description="Choose a saved connection above to view settings." />
          )
        )}
      </div>
    </section>
  );
}
