/**
 * Start a rehearsal: a real, timed extract of every declared table of one
 * system to the export store. The dialog states what will run before it does.
 */

import { useEffect, useId, useState } from "react";
import { Button } from "../ui/Button";
import { Callout } from "../ui/Callout";
import { Dialog } from "../ui/Dialog";
import { PERMISSIONS, usePermissions } from "../../lib/PermissionsContext";
import { plural } from "../../lib/plural";
import { startErrorCopy, staleProof } from "./rehearsalSummary";
import {
  engineLabel,
  formatWindow,
  readinessPresentation,
  sortBoardRows,
  type ExitBoardRow,
} from "../../lib/exitReadiness";

export interface RunRehearsalDialogProps {
  open: boolean;
  systems: ExitBoardRow[];
  /** Preselected system; null lets the person choose. */
  systemId: string | null;
  starting: boolean;
  error?: string;
  onClose: () => void;
  onStart: (systemId: string) => void;
}

export function RunRehearsalDialog({ open, systems: given, systemId, starting, error, onClose, onStart }: RunRehearsalDialogProps) {
  // The board's order: overdue first, then by criticality, so the default is the one most due.
  const systems = sortBoardRows(given);
  const startable = systems.filter((s) => s.state !== "running");
  const [chosen, setChosen] = useState(systemId ?? startable[0]?.id ?? "");
  const selectId = useId();

  useEffect(() => {
    if (open) setChosen(systemId ?? startable[0]?.id ?? "");
  }, [open, systemId]); // eslint-disable-line react-hooks/exhaustive-deps

  const system = systems.find((s) => s.id === chosen) ?? null;
  const running = system?.state === "running";
  const { can } = usePermissions();
  const refusal = error ? startErrorCopy(error) : null;

  return (
    <Dialog
      open={open}
      onClose={onClose}
      size="md"
      title="Run an exit rehearsal"
      subtitle="A real, timed extract of every declared table, verified for completeness and sealed."
      footer={
        <>
          <Button variant="ghost" onClick={onClose} disabled={starting}>Cancel</Button>
          <Button
            variant="primary"
            onClick={() => system && onStart(system.id)}
            disabled={!system || running || starting}
            loading={starting}
            loadingLabel="Starting…"
          >
            Start rehearsal
          </Button>
        </>
      }
    >
      <div className="df2-exit-run-dialog">
        {systemId == null && (
          <div className="df2-field">
            <label className="df2-label" htmlFor={selectId}>System</label>
            <select id={selectId} className="df2-select" value={chosen} onChange={(e) => setChosen(e.target.value)}>
              {systems.map((s) => (
                <option key={s.id} value={s.id} disabled={s.state === "running"}>
                  {s.name}{s.state === "running" ? " (running)" : ""}
                </option>
              ))}
            </select>
          </div>
        )}
        {system && (
          <dl className="df2-exit-facts">
            <div>
              <dt>System</dt>
              <dd>{system.name} · {engineLabel(system.engine)}</dd>
            </div>
            <div>
              <dt>Tables</dt>
              <dd>{plural(system.tables, "table")} declared</dd>
            </div>
            <div>
              <dt>Window</dt>
              <dd>{formatWindow(system.window_seconds)}</dd>
            </div>
            <div>
              <dt>Status now</dt>
              <dd>
                {readinessPresentation(system.state, system.overdue, staleProof(system)).label}
                {system.stale ? " · definition changed since the last rehearsal" : ""}
              </dd>
            </div>
          </dl>
        )}
        <p className="df2-parity-muted">
          Reads the source through its saved connection and writes the extract to the private export store. The source
          is only read; nothing is changed or deleted.
        </p>
        {running && <Callout tone="info" role="status" title="A rehearsal of this system is already running." />}
        {refusal && (
          <Callout
            tone="danger"
            role="alert"
            title={refusal.title}
            isAdmin={can(PERMISSIONS.workspaceManage)}
            adminDetails={refusal.adminDetail || undefined}
          >
            {refusal.body ? <p>{refusal.body}</p> : null}
          </Callout>
        )}
      </div>
    </Dialog>
  );
}
