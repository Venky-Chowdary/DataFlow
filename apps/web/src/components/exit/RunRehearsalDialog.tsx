/**
 * Start a rehearsal: a real, timed extract of every declared table of one
 * system to the export store. The dialog states what will run before it does.
 */

import { useEffect, useId, useState } from "react";
import { Button } from "../ui/Button";
import { Dialog } from "../ui/Dialog";
import {
  engineLabel,
  formatWindow,
  readinessPresentation,
  STALE_TEXT,
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
              <dd>{system.tables.toLocaleString("en-US")} declared</dd>
            </div>
            <div>
              <dt>Window</dt>
              <dd>{formatWindow(system.window_seconds)}</dd>
            </div>
            <div>
              <dt>Status now</dt>
              <dd>
                {readinessPresentation(system.state, system.overdue, system.stale).label}
                {system.stale ? ` · ${STALE_TEXT}` : ""}
              </dd>
            </div>
          </dl>
        )}
        <p className="df2-parity-muted">
          The extract reads the source through its saved connection and writes to the private export store. The
          duration is measured from the job clocks. The source is only read; nothing is changed or deleted.
        </p>
        {running && <p className="df2-parity-notice" role="status">A rehearsal of this system is already running.</p>}
        {error && (
          <p className="df2-parity-notice is-danger" role="alert">
            {error}
          </p>
        )}
      </div>
    </Dialog>
  );
}
