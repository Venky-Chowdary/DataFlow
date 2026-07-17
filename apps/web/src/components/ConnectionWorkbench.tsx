import { EmptyState } from "./EmptyState";
import { FilterTabs } from "./ui/FilterTabs";
import { FilterBar } from "./ui/FilterBar";
import { Connector } from "../lib/types";
import { ConnectionWorkbenchContext, formatRelativeTime } from "../lib/connectionWorkbench";
import { connectorHealthLabel, jobStatusBadgeClass } from "../lib/uiUtils";

export const CONNECTION_TABS = ["Status", "Streams", "Schema", "Mappings", "Sync History", "Settings"] as const;

interface ConnectionWorkbenchProps {
  selectedConnection: Connector | undefined;
  workbench: ConnectionWorkbenchContext | null;
  connectionTab: (typeof CONNECTION_TABS)[number];
  setConnectionTab: (tab: (typeof CONNECTION_TABS)[number]) => void;
  connectors: Connector[];
  onSelectConnection: (id: string) => void;
}

export function ConnectionWorkbench({
  selectedConnection,
  workbench,
  connectionTab,
  setConnectionTab,
  connectors,
  onSelectConnection,
}: ConnectionWorkbenchProps) {
  return (
    <section className="df2-connection-workbench" aria-label="Connection operations workbench">
      <div className="df2-connection-workbench-head">
        <div>
          <span className="df2-rail-kicker">Connection workbench</span>
          <h2>{selectedConnection?.name ?? "Connection workbench preview"}</h2>
          <p>
            {selectedConnection
              ? `${selectedConnection.type} · ${selectedConnection.host || "managed endpoint"}${selectedConnection.port ? `:${selectedConnection.port}` : ""}`
              : "Preview the production controls every saved connection receives: streams, schema drift, mappings, sync history, and policy settings."}
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
                  ? `${workbench.lastJob.status} · ${formatRelativeTime(workbench.lastJob.created_at)}`
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
              <EmptyState compact icon="activity" title="No streams yet" description={`Streams appear when you run a transfer or enable a pipeline from ${selectedConnection.name}.`} />
            )
          ) : (
            <EmptyState compact icon="connectors" title="Select a connection" description="Choose a saved connection above to view streams." />
          )
        )}
        {connectionTab === "Schema" && (
          selectedConnection ? (
            <div className="df2-policy-console df2-policy-console-flush">
              <div className="df2-policy-head">
                <div>
                  <span className="df2-rail-kicker">Schema contract</span>
                  <h4>{selectedConnection.name}</h4>
                </div>
                <span className={`df2-badge ${workbench?.lastJob?.status === "failed" ? "df2-badge-error" : "df2-badge-live"}`}>
                  {workbench?.relatedJobs.length ? "Observed from jobs" : "Awaiting first run"}
                </span>
              </div>
              <div className="df2-schema-review-grid">
                <div><span>Connector</span><strong>{selectedConnection.type}</strong><p>{selectedConnection.database || selectedConnection.host}</p></div>
                <div><span>Preflight</span><strong>8-gate validation</strong><p>Schema contract enforced in Transfer Studio before write.</p></div>
                <div><span>Jobs</span><strong>{workbench?.relatedJobs.length ?? 0}</strong><p>Historical migrations involving this connection.</p></div>
                <div className="block"><span>Last success</span><strong>{formatRelativeTime(workbench?.lastSuccessAt ?? null)}</strong><p>From completed transfer jobs.</p></div>
              </div>
            </div>
          ) : (
            <EmptyState compact icon="connectors" title="Select a connection" description="Choose a saved connection above to review schema status." />
          )
        )}
        {connectionTab === "Mappings" && (
          selectedConnection ? (
            <div className="df2-mapping-policy-grid">
              <div><strong>Recent routes</strong><span>{workbench?.relatedJobs.length ?? 0} job(s) reference this connector.</span></div>
              <div><strong>Pipelines</strong><span>{workbench?.relatedSchedules.length ?? 0} schedule(s) · {workbench?.enabledScheduleCount ?? 0} enabled.</span></div>
              <div><strong>Role</strong><span>{selectedConnection.role ?? "source or destination"} · inferred from usage.</span></div>
              <div><strong>Review</strong><span>Open Transfer Studio to edit column mappings with live type intelligence.</span></div>
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
                    {workbench.relatedJobs.slice(0, 8).map((job) => (
                      <tr key={job._id}>
                        <td>
                          <div className="df2-cell-title">{job.source_name}</div>
                          <div className="df2-cell-meta">{job.source_type} → {job.destination_type}</div>
                        </td>
                        <td><span className={jobStatusBadgeClass(job.status)}>{job.status}</span></td>
                        <td>{job.records_processed?.toLocaleString() ?? "—"}</td>
                        <td className="df2-cell-meta">{formatRelativeTime(job.created_at)}</td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            ) : (
              <EmptyState compact icon="jobs" title="No sync history" description={`Run a transfer or enable a pipeline to populate history for ${selectedConnection.name}.`} />
            )
          ) : (
            <EmptyState compact icon="connectors" title="Select a connection" description="Choose a saved connection above to view sync history." />
          )
        )}
        {connectionTab === "Settings" && (
          selectedConnection ? (
            <div className="df2-settings-mini-grid">
              <div><span>Sync frequency</span><strong>{workbench?.scheduleLabel ?? "Manual"}</strong></div>
              <div><span>Database / bucket</span><strong>{selectedConnection.database || "—"}</strong></div>
              <div><span>Endpoint</span><strong>{selectedConnection.host || "managed"}{selectedConnection.port ? `:${selectedConnection.port}` : ""}</strong></div>
              <div><span>Pipelines</span><strong>{workbench?.relatedSchedules.length ?? 0} configured</strong></div>
            </div>
          ) : (
            <EmptyState compact icon="settings" title="Select a connection" description="Choose a saved connection above to view settings." />
          )
        )}
      </div>
    </section>
  );
}
