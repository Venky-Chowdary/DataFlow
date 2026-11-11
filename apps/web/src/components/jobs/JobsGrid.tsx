import { type KeyboardEvent, type ReactNode, useEffect, useId, useMemo, useRef, useState } from "react";
import { ConnectorIcon } from "../../app/brand-icons";
import { destMetricCompact, destMetricToneClass, formatJobRowMetric } from "../../lib/conservationLedger";
import {
  formatJobDuration,
  formatJobStarted,
  JOBS_GRID_COLUMNS,
  type JobsGridColumnId,
  type JobsGridDensity,
  type JobsGridSort,
  jobEngineLabel,
  jobModeLabel,
  jobRouteLabel,
  jobStartedAt,
  jobVerification,
  LIVE_JOB_STATUSES,
  nextJobsGridSort,
  verificationBadgeClass,
} from "../../lib/jobGrid";
import { parseServerTimestampMs } from "../../lib/jobTheaterProgress";
import { reconcileBarPercent } from "../../lib/progressRing";
import type { TransferJob } from "../../lib/types";
import { jobStatusBadgeClass, jobStatusLabel, presentedJobStatus, uniqueListKey } from "../../lib/uiUtils";
import { DtIcon } from "../DtIcon";
import { Button } from "../ui/Button";
import { EmptyState } from "../ui/EmptyState";
import { ProgressCell } from "../ui/ProgressCell";
import { JobsMenu, type JobsMenuItem } from "./JobsMenu";

/** Phone width: the grid becomes stacked rows (same breakpoint as the old list/detail swap). */
export const JOBS_GRID_STACK_QUERY = "(max-width: 720px)";

function useMediaQuery(query: string): boolean {
  const read = () => {
    try {
      return typeof window !== "undefined" && typeof window.matchMedia === "function" && window.matchMedia(query).matches;
    } catch {
      return false;
    }
  };
  const [matches, setMatches] = useState(read);
  useEffect(() => {
    if (typeof window.matchMedia !== "function") return;
    const media = window.matchMedia(query);
    const update = () => setMatches(media.matches);
    update();
    media.addEventListener?.("change", update);
    return () => media.removeEventListener?.("change", update);
  }, [query]);
  return matches;
}

export interface JobsGridRowActions {
  /** Row menu items for one job (Open details, Retry, Cancel, Copy ID…). */
  menuItems: (job: TransferJob) => JobsMenuItem[];
}

interface JobsGridProps {
  /** The rows to show, already in display order. */
  jobs: TransferJob[];
  displayName: (job: TransferJob) => string;
  selectedId: string | null;
  columns: JobsGridColumnId[];
  density: JobsGridDensity;
  sort: JobsGridSort;
  onSortChange: (sort: JobsGridSort) => void;
  /** Keyboard / pointer cursor moved to a row (does not open the detail). */
  onSelect: (jobId: string) => void;
  /** Open the detail panel for a row. */
  onOpen: (jobId: string) => void;
  rowActions: JobsGridRowActions;
  loading?: boolean;
  error?: string;
  onRetry?: () => void;
  emptyTitle: string;
  emptyDescription: string;
  /** Paging footer and window note under the grid. */
  footer?: ReactNode;
  /** Shown above the rows when the loaded page is sorted locally. */
  sortNote?: string;
}

function ariaSort(sort: JobsGridSort, column: JobsGridColumnId): "ascending" | "descending" | "none" {
  if (sort.column !== column) return "none";
  return sort.dir === "asc" ? "ascending" : "descending";
}

function RowsCell({ job, live }: { job: TransferJob; live: boolean }) {
  if (live && job.progress_pct != null && !job.progress_indeterminate) {
    const processed = Number(job.records_processed ?? 0);
    const total = Number(job.total_rows ?? 0);
    const reconciling = String(job.phase || "").toLowerCase() === "reconcile" && job.reconcile_pct != null;
    return (
      <div className="df2-jobs-grid-progress">
        <span className="df2-jobs-grid-progress-text">
          {reconciling
            ? `Checksum ${Math.min(99, Number(job.reconcile_pct))}%`
            : total > 0
              ? `${processed.toLocaleString()} / ${total.toLocaleString()}`
              : `${processed.toLocaleString()} rows`}
        </span>
        <ProgressCell value={reconcileBarPercent(job.progress_pct, job.reconcile_pct, job.phase)} />
      </div>
    );
  }
  const metric = formatJobRowMetric(job);
  return (
    <span className={`df2-job-row-rows ${destMetricToneClass(metric)}`} title={metric.title}>
      {destMetricCompact(metric)}
    </span>
  );
}

/**
 * The Jobs operations grid — one row per run with fixed, sortable columns.
 *
 * ARIA grid with row focus (the Fluent details-list pattern): ↑/↓, Home/End
 * move the cursor row, Enter or Space opens its detail. Status is a badge
 * with text, never colour alone. A polite live region announces a row whose
 * status changed on refresh.
 */
export function JobsGrid({
  jobs,
  displayName,
  selectedId,
  columns,
  density,
  sort,
  onSortChange,
  onSelect,
  onOpen,
  rowActions,
  loading,
  error,
  onRetry,
  emptyTitle,
  emptyDescription,
  footer,
  sortNote,
}: JobsGridProps) {
  const stacked = useMediaQuery(JOBS_GRID_STACK_QUERY);
  const hintId = useId();
  const visible = useMemo(
    () => JOBS_GRID_COLUMNS.filter((column) => column.required || columns.includes(column.id)),
    [columns],
  );
  const activeId = selectedId && jobs.some((job) => job._id === selectedId) ? selectedId : jobs[0]?._id ?? null;

  // Announce status transitions of rows already on screen (polling refresh).
  const [announcement, setAnnouncement] = useState("");
  const seenStatus = useRef<Map<string, string>>(new Map());
  useEffect(() => {
    const changed: string[] = [];
    const next = new Map<string, string>();
    for (const job of jobs) {
      const status = presentedJobStatus(job);
      const before = seenStatus.current.get(job._id);
      if (before && before !== status) changed.push(`${displayName(job)}: ${jobStatusLabel(status)}`);
      next.set(job._id, status);
    }
    seenStatus.current = next;
    if (changed.length) setAnnouncement(changed.slice(0, 3).join(". ") + (changed.length > 3 ? `. ${changed.length - 3} more changed.` : "."));
  }, [jobs, displayName]);

  const focusRow = (jobId: string) => {
    onSelect(jobId);
    document.getElementById(`job-item-${jobId}`)?.focus();
  };

  const onRowKeyDown = (event: KeyboardEvent<HTMLTableRowElement>, index: number, jobId: string) => {
    if (event.target !== event.currentTarget) return;
    let target: number | null = null;
    if (event.key === "ArrowDown") target = Math.min(jobs.length - 1, index + 1);
    else if (event.key === "ArrowUp") target = Math.max(0, index - 1);
    else if (event.key === "Home") target = 0;
    else if (event.key === "End") target = jobs.length - 1;
    else if (event.key === "Enter" || event.key === " ") {
      event.preventDefault();
      onOpen(jobId);
      return;
    }
    if (target == null) return;
    event.preventDefault();
    const id = jobs[target]?._id;
    if (id) focusRow(id);
  };

  const body = error ? (
    <EmptyState
      compact
      icon="alert"
      title="Job results unavailable"
      description="The last query did not complete. Retry to load this page."
    />
  ) : jobs.length === 0 ? (
    loading ? null : <EmptyState compact icon="search" title={emptyTitle} description={emptyDescription} />
  ) : (
    <div className="df2-table-scroll df2-jobs-grid-scroll">
      <table
        className="df2-table df2-jobs-grid-table"
        role="grid"
        aria-label="Transfer jobs"
        aria-rowcount={jobs.length + 1}
        aria-busy={loading || undefined}
        aria-describedby={hintId}
      >
        <thead>
          <tr aria-rowindex={1}>
            {visible.map((column) => (
              <th
                key={column.id}
                scope="col"
                aria-sort={ariaSort(sort, column.id)}
                className={`df2-jobs-grid-col-${column.id}${column.numeric ? " is-num" : ""}`}
              >
                <button
                  type="button"
                  className="df2-jobs-grid-sort"
                  onClick={() => onSortChange(nextJobsGridSort(sort, column.id))}
                  title={`Sort this page by ${column.label.toLowerCase()}`}
                >
                  <span>{column.label}</span>
                  <span className={`df2-jobs-grid-sort-icon${sort.column === column.id ? " is-active" : ""}`} aria-hidden="true">
                    <DtIcon name={sort.column === column.id && sort.dir === "asc" ? "chevron-up" : "chevron-down"} size={12} />
                  </span>
                </button>
              </th>
            ))}
            <th scope="col" className="df2-jobs-grid-col-actions">
              <span className="df2-sr-only">Actions</span>
            </th>
          </tr>
        </thead>
        <tbody>
          {jobs.map((job, index) => {
            const status = presentedJobStatus(job);
            const live = LIVE_JOB_STATUSES.has(status);
            const name = displayName(job);
            const route = jobRouteLabel(job);
            const verification = jobVerification(job);
            const selected = job._id === selectedId;
            const active = job._id === activeId;
            const started = jobStartedAt(job);
            const startedMs = parseServerTimestampMs(started);
            const cells: Record<JobsGridColumnId, ReactNode> = {
              status: (
                <span className={`${jobStatusBadgeClass(status)} df2-jobs-grid-status`}>
                  {jobStatusLabel(status)}
                </span>
              ),
              job: (
                <div className="df2-jobs-grid-job">
                  <span className="df2-cell-title df2-jobs-grid-name" title={name}>{name}</span>
                  <code className="df2-job-row-id" title={job._id}>{job._id.slice(0, 8)}</code>
                </div>
              ),
              route: (
                <span className="df2-jobs-grid-route" title={route}>
                  <span className="df2-jobs-grid-engine">
                    <ConnectorIcon id={job.source_type} size={14} />
                    {jobEngineLabel(job.source_type)}
                  </span>
                  <DtIcon name="arrow-right" size={12} />
                  <span className="df2-jobs-grid-engine">
                    <ConnectorIcon id={job.destination_type} size={14} />
                    {jobEngineLabel(job.destination_type)}
                  </span>
                </span>
              ),
              mode: <span className="df2-jobs-grid-muted">{job.sync_mode ? jobModeLabel(job) : "—"}</span>,
              rows: <RowsCell job={job} live={live} />,
              verification: (
                <span className="df2-jobs-grid-verify" title={verification.title}>
                  <span className={verificationBadgeClass(verification.tone)}>{verification.label}</span>
                  {verification.quarantined > 0 && (
                    <span className="df2-jobs-grid-muted">{verification.quarantined.toLocaleString()} quarantined</span>
                  )}
                </span>
              ),
              duration: (
                <span className="df2-jobs-grid-num">
                  {formatJobDuration(job.started_at, job.completed_at) ?? "—"}
                </span>
              ),
              started: (
                <time
                  className="df2-jobs-grid-muted"
                  dateTime={startedMs != null ? new Date(startedMs).toISOString() : undefined}
                  title={startedMs != null ? new Date(startedMs).toLocaleString() : undefined}
                >
                  {formatJobStarted(started)}
                </time>
              ),
            };
            return (
              <tr
                key={uniqueListKey(job._id, index, "job")}
                id={`job-item-${job._id}`}
                aria-rowindex={index + 2}
                aria-selected={selected}
                tabIndex={active ? 0 : -1}
                className={[
                  "df2-jobs-grid-row",
                  selected ? "is-selected" : "",
                  live ? "is-live" : "",
                  status === "failed" || status === "cancelled" ? "is-failed" : "",
                ].filter(Boolean).join(" ")}
                onClick={() => onOpen(job._id)}
                onKeyDown={(event) => onRowKeyDown(event, index, job._id)}
                onFocus={(event) => {
                  if (event.target === event.currentTarget && !selected) onSelect(job._id);
                }}
              >
                {visible.map((column) => (
                  <td
                    key={column.id}
                    className={`df2-jobs-grid-col-${column.id}${column.numeric ? " is-num" : ""}`}
                    data-label={column.label}
                  >
                    {cells[column.id]}
                  </td>
                ))}
                <td className="df2-jobs-grid-col-actions">
                  <JobsMenu
                    label={`Actions for ${name}`}
                    iconOnly
                    icon={<DtIcon name="menu" size={16} />}
                    triggerClassName="df2-btn df2-btn-ghost df2-btn-sm df2-jobs-grid-row-menu"
                    triggerTabIndex={active ? 0 : -1}
                    items={rowActions.menuItems(job)}
                  />
                </td>
              </tr>
            );
          })}
        </tbody>
      </table>
    </div>
  );

  return (
    <section
      className={[
        "df2-jobs-grid",
        `df2-jobs-grid--${density}`,
        stacked ? "is-stacked" : "",
      ].filter(Boolean).join(" ")}
      aria-label="Job list"
    >
      <p id={hintId} className="df2-sr-only">
        Use the up and down arrow keys to move between jobs and Enter to open one.
      </p>
      <p className="df2-sr-only" role="status" aria-live="polite">{announcement}</p>
      {loading && <p className="df2-jobs-page-loading" role="status">Updating job results…</p>}
      {error && (
        <p className="df2-jobs-page-error" role="alert">
          Could not load this job query: {error}{" "}
          {onRetry && <Button size="sm" variant="ghost" onClick={onRetry}>Retry</Button>}
        </p>
      )}
      {sortNote && jobs.length > 0 && !error && <p className="df2-jobs-grid-note">{sortNote}</p>}
      {body}
      {footer}
    </section>
  );
}
