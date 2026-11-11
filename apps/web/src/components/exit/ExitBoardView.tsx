/**
 * Page A: Exit readiness. One board per workspace: for each declared system,
 * whether its exit has been proven recently and how long it really took
 * against the window it must fit in.
 *
 * Everything on the board is the API's stored evidence (GET /exit/readiness):
 * the state is the latest finished rehearsal's verdict, durations are measured
 * from job clocks, and "Overdue" means the rehearsal interval has passed. The
 * table is the Jobs grid's pattern: one row per system, row focus with the
 * arrow keys, Enter to open, status in words and never colour alone.
 */

import { type KeyboardEvent, useId, useMemo } from "react";
import { DtIcon } from "../DtIcon";
import { Button } from "../ui/Button";
import { EmptyState } from "../ui/EmptyState";
import { FilterBar } from "../ui/FilterBar";
import { FilterTabs } from "../ui/FilterTabs";
import { PageMetricsRow, type MetricItem } from "../ui/PageMetricsRow";
import { PageToolbar } from "../ui/PageToolbar";
import { WindowBar } from "../charts/ExitCharts";
import {
  boardFilterCounts,
  boardFilterMatches,
  criticalityPresentation,
  dueText,
  engineLabel,
  formatDate,
  formatDuration,
  measuredVsWindowText,
  pillClass,
  readinessPresentation,
  sortBoardRows,
  type BoardFilter,
  type ExitBoardRow,
  type ExitReadiness,
} from "../../lib/exitReadiness";

export function boardMetrics(readiness: ExitReadiness): MetricItem[] {
  const { kpis } = readiness;
  const longest = kpis.longest_measured_system_id
    ? readiness.systems.find((s) => s.id === kpis.longest_measured_system_id)
    : null;
  const next = kpis.next_regulatory_date;
  const nextSystem = next ? readiness.systems.find((s) => s.id === next.system_id) : null;
  return [
    {
      label: "Systems with a proven exit",
      value: `${kpis.systems_proven.toLocaleString("en-US")} / ${kpis.systems.toLocaleString("en-US")}`,
      sub: kpis.systems === 0 ? "No systems declared" : "Latest rehearsal proven complete",
      tone: "green",
    },
    {
      label: "Rehearsals overdue",
      value: kpis.overdue.toLocaleString("en-US"),
      sub: kpis.overdue === 0 ? "Every system is within its interval" : "Past their rehearsal interval",
      tone: kpis.overdue > 0 ? "red" : "default",
    },
    {
      label: "Longest measured exit",
      value: kpis.longest_measured_seconds == null ? "—" : formatDuration(kpis.longest_measured_seconds),
      sub: kpis.longest_measured_seconds == null ? "Not measured yet" : `${longest?.name ?? "A declared system"} · measured, not estimated`,
      tone: "default",
    },
    {
      label: "Next regulatory date",
      value: next ? formatDate(next.date) : "—",
      sub: next ? `${next.label}${nextSystem ? ` · ${nextSystem.name}` : ""}` : "None declared",
      tone: "default",
    },
  ];
}

export interface ExitBoardViewProps {
  readiness: ExitReadiness;
  query: string;
  onQueryChange: (q: string) => void;
  filter: BoardFilter;
  onFilterChange: (f: BoardFilter) => void;
  selectedId: string | null;
  onOpenSystem: (systemId: string) => void;
  onRunRehearsal: (systemId: string | null) => void;
  onDeclare: () => void;
  canDeclare: boolean;
  canRun: boolean;
  /** Why the run / declare controls are off, for their titles. */
  runDeniedReason?: string;
  declareDeniedReason?: string;
  startingId?: string | null;
  now?: Date;
}

function SystemCell({ row }: { row: ExitBoardRow }) {
  return (
    <div className="df2-exit-grid-system">
      <span className="df2-cell-title df2-exit-grid-name" title={row.name}>{row.name}</span>
      <span className="df2-exit-grid-meta">
        {engineLabel(row.engine)} · {row.tables.toLocaleString("en-US")} {row.tables === 1 ? "table" : "tables"}
      </span>
    </div>
  );
}

export function ExitBoardView({
  readiness,
  query,
  onQueryChange,
  filter,
  onFilterChange,
  selectedId,
  onOpenSystem,
  onRunRehearsal,
  onDeclare,
  canDeclare,
  canRun,
  runDeniedReason,
  declareDeniedReason,
  startingId = null,
  now = new Date(),
}: ExitBoardViewProps) {
  const hintId = useId();
  const counts = boardFilterCounts(readiness.systems);
  const rows = useMemo(() => {
    const q = query.trim().toLowerCase();
    return sortBoardRows(readiness.systems).filter(
      (row) =>
        boardFilterMatches(row, filter)
        && (!q || [row.name, row.engine ?? "", row.completeness_label, row.criticality].some((v) => v.toLowerCase().includes(q))),
    );
  }, [readiness.systems, filter, query]);
  const activeId = selectedId && rows.some((r) => r.id === selectedId) ? selectedId : rows[0]?.id ?? null;

  const focusRow = (id: string) => document.getElementById(`exit-system-${id}`)?.focus();
  const onRowKeyDown = (event: KeyboardEvent<HTMLTableRowElement>, index: number, id: string) => {
    if (event.target !== event.currentTarget) return;
    let target: number | null = null;
    if (event.key === "ArrowDown") target = Math.min(rows.length - 1, index + 1);
    else if (event.key === "ArrowUp") target = Math.max(0, index - 1);
    else if (event.key === "Home") target = 0;
    else if (event.key === "End") target = rows.length - 1;
    else if (event.key === "Enter" || event.key === " ") {
      event.preventDefault();
      onOpenSystem(id);
      return;
    }
    if (target == null) return;
    event.preventDefault();
    const next = rows[target]?.id;
    if (next) focusRow(next);
  };

  return (
    <>
      <PageMetricsRow className="df2-exit-kpis" metrics={boardMetrics(readiness)} columns={4} compact />
      <PageToolbar
        className="df2-exit-toolbar"
        searchValue={query}
        onSearchChange={onQueryChange}
        searchPlaceholder="Search systems…"
        filters={
          <FilterBar variant="inline" ariaLabel="Filter systems">
            <FilterTabs
              ariaLabel="Filter systems by readiness"
              value={filter}
              onChange={onFilterChange}
              items={[
                { id: "all", label: "All", count: counts.all },
                { id: "proven", label: "Proven", count: counts.proven },
                { id: "attention", label: "Not proven", count: counts.attention },
                { id: "overdue", label: "Overdue", count: counts.overdue },
                { id: "never", label: "Never rehearsed", count: counts.never },
              ]}
            />
          </FilterBar>
        }
        actions={
          <>
            <Button
              size="sm"
              onClick={() => onRunRehearsal(null)}
              disabled={!canRun || readiness.systems.length === 0}
              title={!canRun ? runDeniedReason : readiness.systems.length === 0 ? "Declare a system first." : undefined}
              leadingIcon={<DtIcon name="play" size={14} />}
            >
              Run rehearsal
            </Button>
            <Button
              size="sm"
              variant="primary"
              onClick={onDeclare}
              disabled={!canDeclare}
              title={!canDeclare ? declareDeniedReason : undefined}
              leadingIcon={<DtIcon name="plus" size={14} />}
            >
              Declare a system
            </Button>
          </>
        }
      />
      <section className="df2-exit-grid" aria-label="Declared systems">
        <p id={hintId} className="df2-sr-only">
          Use the up and down arrow keys to move between systems and Enter to open one.
        </p>
        {rows.length === 0 ? (
          <EmptyState
            compact
            icon="search"
            title="No matching systems"
            description="No declared system matches the current search or filter."
          />
        ) : (
          <div className="df2-table-scroll df2-exit-grid-scroll">
            <table className="df2-table df2-exit-grid-table" aria-label="Exit readiness by system" aria-describedby={hintId}>
              <thead>
                <tr>
                  <th scope="col" className="df2-exit-grid-col-system">System</th>
                  <th scope="col">Criticality</th>
                  <th scope="col">Last rehearsal</th>
                  <th scope="col" className="df2-exit-grid-col-window">Measured vs window</th>
                  <th scope="col">Completeness</th>
                  <th scope="col">Status</th>
                  <th scope="col" className="df2-exit-grid-col-actions">
                    <span className="df2-sr-only">Actions</span>
                  </th>
                </tr>
              </thead>
              <tbody>
                {rows.map((row, index) => {
                  const crit = criticalityPresentation(row.criticality);
                  const status = readinessPresentation(row.state, row.overdue);
                  const due = dueText(row.next_due_at, row.overdue, now);
                  const selected = row.id === selectedId;
                  const running = row.state === "running";
                  return (
                    <tr
                      key={row.id}
                      id={`exit-system-${row.id}`}
                      tabIndex={row.id === activeId ? 0 : -1}
                      aria-selected={selected}
                      className={["df2-exit-grid-row", selected ? "is-selected" : "", row.overdue ? "is-overdue" : ""].filter(Boolean).join(" ")}
                      onClick={() => onOpenSystem(row.id)}
                      onKeyDown={(e) => onRowKeyDown(e, index, row.id)}
                    >
                      <td className="df2-exit-grid-col-system" data-label="System">
                        <SystemCell row={row} />
                      </td>
                      <td data-label="Criticality">
                        <span className={pillClass(crit.tone)}>{crit.label}</span>
                      </td>
                      <td data-label="Last rehearsal">
                        <div className="df2-exit-grid-stack">
                          <span>{row.last_rehearsal_at ? formatDate(row.last_rehearsal_at) : "Never"}</span>
                          {due && <span className={`df2-exit-grid-meta${row.overdue ? " is-overdue" : ""}`}>{due}</span>}
                        </div>
                      </td>
                      <td className="df2-exit-grid-col-window" data-label="Measured vs window">
                        <div className="df2-exit-grid-window">
                          {row.measured_seconds != null ? (
                            <WindowBar measured={row.measured_seconds} window={row.window_seconds} fits={row.fits_window} />
                          ) : null}
                          <span className={`df2-exit-grid-num${row.measured_seconds == null ? " df2-exit-grid-meta" : ""}`}>
                            {measuredVsWindowText(row.measured_seconds, row.window_seconds)}
                            {row.fits_window === false ? " · does not fit" : ""}
                          </span>
                        </div>
                      </td>
                      <td data-label="Completeness">
                        <span className="df2-exit-grid-completeness">{row.completeness_label}</span>
                      </td>
                      <td data-label="Status">
                        <span className={pillClass(status.tone)}>{status.label}</span>
                      </td>
                      <td className="df2-exit-grid-col-actions">
                        <div className="df2-exit-grid-actions" role="presentation" onKeyDown={(e) => e.stopPropagation()} onClick={(e) => e.stopPropagation()}>
                          <Button
                            size="sm"
                            variant="ghost"
                            className="df2-exit-grid-icon-btn"
                            onClick={() => onRunRehearsal(row.id)}
                            disabled={!canRun || running || startingId === row.id}
                            loading={startingId === row.id}
                            aria-label={running ? `A rehearsal of ${row.name} is running` : `Run a rehearsal of ${row.name}`}
                            title={!canRun ? runDeniedReason : running ? "A rehearsal is running." : "Run rehearsal"}
                            tabIndex={row.id === activeId ? 0 : -1}
                          >
                            <DtIcon name="play" size={14} />
                          </Button>
                          <Button
                            size="sm"
                            variant="ghost"
                            className="df2-exit-grid-icon-btn"
                            onClick={() => onOpenSystem(row.id)}
                            aria-label={`Open ${row.name}`}
                            title="Open"
                            tabIndex={row.id === activeId ? 0 : -1}
                          >
                            <DtIcon name="chevron-right" size={14} />
                          </Button>
                        </div>
                      </td>
                    </tr>
                  );
                })}
              </tbody>
            </table>
          </div>
        )}
        <p className="df2-exit-grid-note">
          Statuses come from each system's latest finished rehearsal. Durations are measured from job clocks, never
          estimated. Datawrap produces the evidence; your auditors decide what it satisfies.
        </p>
      </section>
    </>
  );
}
