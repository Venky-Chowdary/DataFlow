/**
 * One declared system: what would have to be taken, the window it must fit,
 * when it is next due, and every rehearsal so far with its measured duration.
 * Opening a rehearsal shows Page B.
 */

import { useCallback, useEffect, useRef, useState } from "react";
import { DtIcon } from "../DtIcon";
import { Button } from "../ui/Button";
import { Drawer } from "../ui/Drawer";
import { EmptyState } from "../ui/EmptyState";
import { LoadingBlock } from "../LoadingState";
import { DurationTrendChart, WindowBar } from "../charts/ExitCharts";
import { fetchExitSystem, fetchSystemRehearsals } from "../../lib/exitApi";
import {
  criticalityPresentation,
  dueText,
  engineLabel,
  exitErrorMessage,
  formatBytes,
  formatDate,
  formatDuration,
  formatWindow,
  measuredVsWindowText,
  pillClass,
  readinessPresentation,
  rehearsalVerdictPresentation,
  runningRehearsalId,
  staleNotice,
  type ExitSystem,
  type RehearsalSummary,
  type Trend,
} from "../../lib/exitReadiness";
import { formatDateTime, roleLabel } from "../../lib/parity";

export interface SystemDrawerProps {
  systemId: string | null;
  /** Bumped by the page when a rehearsal started or the system changed, to re-read. */
  refreshKey: number;
  onClose: () => void;
  onOpenRehearsal: (rehearsalId: string) => void;
  onRunRehearsal: (system: ExitSystem) => void;
  onEdit: (system: ExitSystem) => void;
  onDelete: (system: ExitSystem) => void;
  canRun: boolean;
  canManage: boolean;
  runDeniedReason?: string;
  manageDeniedReason?: string;
  starting?: boolean;
  now?: Date;
}

export function SystemDrawer({
  systemId,
  refreshKey,
  onClose,
  onOpenRehearsal,
  onRunRehearsal,
  onEdit,
  onDelete,
  canRun,
  canManage,
  runDeniedReason,
  manageDeniedReason,
  starting = false,
  now = new Date(),
}: SystemDrawerProps) {
  const [system, setSystem] = useState<ExitSystem | null>(null);
  const [history, setHistory] = useState<{ rehearsals: RehearsalSummary[]; trend: Trend } | null>(null);
  const [error, setError] = useState("");
  const [loading, setLoading] = useState(false);
  const idRef = useRef(systemId);
  idRef.current = systemId;

  const load = useCallback(async (signal?: AbortSignal) => {
    const forId = systemId;
    if (!forId) return;
    setLoading(true);
    try {
      const [s, h] = await Promise.all([fetchExitSystem(forId, { signal }), fetchSystemRehearsals(forId, 20, { signal })]);
      if (idRef.current !== forId) return;
      setSystem(s);
      setHistory(h);
      setError("");
    } catch (err) {
      if (signal?.aborted || idRef.current !== forId) return;
      setError(exitErrorMessage(err, "Could not load this system."));
    } finally {
      if (!signal?.aborted && idRef.current === forId) setLoading(false);
    }
  }, [systemId]);

  useEffect(() => {
    if (!systemId) {
      setSystem(null);
      setHistory(null);
      return;
    }
    const controller = new AbortController();
    void load(controller.signal);
    return () => controller.abort();
  }, [systemId, refreshKey, load]);

  useEffect(() => {
    setSystem(null);
    setHistory(null);
    setError("");
  }, [systemId]);

  const shown = system && system.id === systemId ? system : null;
  const readiness = shown?.readiness;
  const status = readiness ? readinessPresentation(readiness.state, readiness.overdue, readiness.stale) : null;
  const crit = shown ? criticalityPresentation(shown.criticality) : null;
  const runningId = runningRehearsalId(shown);
  const running = Boolean(runningId) || readiness?.state === "running";
  const stale = staleNotice(readiness);
  const windowSeconds = (shown?.exit_window_hours ?? 0) * 3600;

  return (
    <Drawer
      open={Boolean(systemId)}
      onClose={onClose}
      title={shown?.name ?? "System"}
      subtitle={shown ? `${engineLabel(shown.source.engine)} · ${shown.source.connector_name || shown.source.connector_id}` : undefined}
      headerExtra={
        shown && status && crit ? (
          <span className="df2-exit-drawer-pills">
            <span className={pillClass(crit.tone)}>{crit.label}</span>
            <span className={pillClass(status.tone)}>{status.label}</span>
          </span>
        ) : null
      }
      ariaLabel={shown ? `Exit system ${shown.name}` : "Exit system"}
      className="df2-exit-drawer"
      footer={
        shown ? (
          <div className="df2-exit-drawer-footer">
            <Button
              size="sm"
              variant="danger"
              onClick={() => onDelete(shown)}
              disabled={!canManage || running}
              title={!canManage ? manageDeniedReason : running ? "A rehearsal is running; remove the system when it has finished." : undefined}
              leadingIcon={<DtIcon name="trash" size={14} />}
            >
              Remove
            </Button>
            <span className="df2-exit-drawer-footer-end">
              <Button size="sm" onClick={() => onEdit(shown)} disabled={!canManage} title={!canManage ? manageDeniedReason : undefined} leadingIcon={<DtIcon name="edit" size={14} />}>
                Edit
              </Button>
              <Button
                size="sm"
                variant="primary"
                onClick={() => (running && runningId ? onOpenRehearsal(runningId) : onRunRehearsal(shown))}
                disabled={running ? !runningId : !canRun || starting}
                loading={starting}
                loadingLabel="Starting…"
                title={!running && !canRun ? runDeniedReason : undefined}
                leadingIcon={<DtIcon name={running ? "activity" : "play"} size={14} />}
              >
                {running ? "Open running rehearsal" : "Run rehearsal"}
              </Button>
            </span>
          </div>
        ) : undefined
      }
    >
      {!shown ? (
        loading || !error ? (
          <LoadingBlock size="sm" title="Loading the system" />
        ) : (
          <EmptyState
            compact
            icon="alert"
            role="alert"
            title="Could not load this system"
            description={error}
            action={<Button size="sm" onClick={() => void load()}>Try again</Button>}
          />
        )
      ) : (
        <div className="df2-exit-drawer-body">
          {shown.description && <p className="df2-parity-confidence">{shown.description}</p>}
          {stale && (
            <p className="df2-parity-notice" role="status">
              <DtIcon name="alert" size={14} />
              <span>
                <b>{stale.label}.</b>
                {stale.detail ? ` ${stale.detail}` : ""}
              </span>
            </p>
          )}
          {readiness && (
            <p className={`df2-parity-notice${readiness.overdue ? "" : " is-muted"}`} role="status">
              <DtIcon name="clock" size={14} />
              <span>
                {readiness.next_due_at ? `${dueText(readiness.next_due_at, readiness.overdue, now)} (${formatDate(readiness.next_due_at)}). ` : ""}
                Rehearse every {shown.rehearsal_every_days} {shown.rehearsal_every_days === 1 ? "day" : "days"}
                {shown.rehearsal_schedule ? `; scheduled ${shown.rehearsal_schedule.cron} (${shown.rehearsal_schedule.timezone}).` : "; no schedule, started by hand."}
              </span>
            </p>
          )}

          <section className="df2-parity-card" aria-label="What would have to be taken">
            <h3 className="df2-parity-h3">Exit scope</h3>
            <dl className="df2-exit-facts">
              <div>
                <dt>Window</dt>
                <dd>{formatWindow(windowSeconds)}{shown.window_basis ? ` · ${shown.window_basis}` : ""}</dd>
              </div>
              <div>
                <dt>Extract format</dt>
                <dd>{shown.extract_format.toUpperCase()}</dd>
              </div>
              <div>
                <dt>Sign-off roles</dt>
                <dd>{shown.required_roles.map(roleLabel).join(", ")}</dd>
              </div>
              <div>
                <dt>Regulatory dates</dt>
                <dd>
                  {shown.regulatory_dates.length
                    ? shown.regulatory_dates.map((d) => `${formatDate(d.date)} · ${d.label}`).join("; ")
                    : "None declared"}
                </dd>
              </div>
            </dl>
            <h4 className="df2-parity-h4">Tables ({shown.tables.length.toLocaleString("en-US")})</h4>
            <ul className="df2-exit-scope-tables">
              {shown.tables.map((t) => (
                <li key={`${t.schema ?? ""}.${t.name}`}>
                  <span className="df2-mono">{t.schema ? `${t.schema}.` : ""}{t.name}</span>
                  <span className="df2-parity-muted">
                    {t.key_columns.length ? `key ${t.key_columns.join(", ")}` : "catalog primary key"}
                  </span>
                </li>
              ))}
            </ul>
          </section>

          <section className="df2-parity-card" aria-label="Rehearsals">
            <h3 className="df2-parity-h3">Rehearsals</h3>
            {history && history.trend.points.length > 0 && (
              <DurationTrendChart trend={history.trend} windowSeconds={windowSeconds} />
            )}
            {!history || history.rehearsals.length === 0 ? (
              <p className="df2-parity-muted">
                Never rehearsed. A rehearsal runs a real, timed extract of every declared table and proves it is complete.
              </p>
            ) : (
              <ul className="df2-exit-rehearsals">
                {history.rehearsals.map((r) => {
                  const v = rehearsalVerdictPresentation(r.verdict);
                  return (
                    <li key={r.id}>
                      <button type="button" className="df2-exit-rehearsal" onClick={() => onOpenRehearsal(r.id)}>
                        <span className="df2-exit-rehearsal-n">#{r.n}</span>
                        <span className="df2-exit-rehearsal-main">
                          <span>{formatDateTime(r.started_at)}</span>
                          <span className="df2-parity-muted">
                            {r.tables_proven.toLocaleString("en-US")} of {r.tables_declared.toLocaleString("en-US")} tables proven
                            {r.total_bytes != null ? ` · ${formatBytes(r.total_bytes)}` : ""}
                            {r.signed ? " · signed" : ""}
                            {r.current === false ? " · earlier definition" : ""}
                          </span>
                        </span>
                        <span className="df2-exit-rehearsal-window">
                          {r.measured_seconds != null && <WindowBar measured={r.measured_seconds} window={r.window_seconds} fits={r.fits_window} />}
                          <span className="df2-parity-muted">{measuredVsWindowText(r.measured_seconds, r.window_seconds)}</span>
                        </span>
                        <span className={pillClass(v.tone)}>{v.label}</span>
                        <DtIcon name="chevron-right" size={14} />
                      </button>
                    </li>
                  );
                })}
              </ul>
            )}
            {readiness?.last_proven_rehearsal && readiness.last_proven_rehearsal.id !== readiness.last_rehearsal?.id && (
              <p className="df2-parity-muted">
                Last proven: rehearsal #{readiness.last_proven_rehearsal.n} on {formatDate(readiness.last_proven_rehearsal.started_at)}
                {readiness.last_proven_rehearsal.measured_seconds != null ? `, ${formatDuration(readiness.last_proven_rehearsal.measured_seconds)} measured` : ""}
                {readiness.last_proven_rehearsal.current === false ? ", of an earlier definition (it does not prove this one)" : ""}.
              </p>
            )}
          </section>
        </div>
      )}
    </Drawer>
  );
}
