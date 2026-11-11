import { DtIcon } from "../DtIcon";
import { Button } from "../ui/Button";
import { FilterBar } from "../ui/FilterBar";
import { FilterTabs } from "../ui/FilterTabs";
import { PageToolbar } from "../ui/PageToolbar";
import type { JobListStatus } from "../../lib/jobHistory";
import { JOBS_GRID_COLUMNS, type JobsGridColumnId, type JobsGridDensity } from "../../lib/jobGrid";
import { JobsMenu } from "./JobsMenu";

export interface JobsPivotCounts {
  all: number;
  running: number;
  completed: number;
  failed: number;
  /** Absent when the server did not count it; the tab then shows no number. */
  attention?: number;
}

export interface JobsToolbarAction {
  /** Contextual actions render only when the selected run can take them. */
  visible?: boolean;
  disabled: boolean;
  /** Tooltip — what the action will do, or why it is unavailable. */
  title: string;
  onClick: () => void;
  busy?: boolean;
}

interface JobsToolbarProps {
  search: string;
  onSearch: (value: string) => void;
  pivot: JobListStatus;
  onPivot: (value: JobListStatus) => void;
  counts: JobsPivotCounts;
  /** False when the API cannot filter Needs attention (older servers). */
  showAttention?: boolean;
  onRefresh?: () => void;
  rerun: JobsToolbarAction;
  cancel: JobsToolbarAction;
  exportCsv: JobsToolbarAction;
  columns: JobsGridColumnId[];
  onToggleColumn: (column: JobsGridColumnId) => void;
  density: JobsGridDensity;
  onDensity: (density: JobsGridDensity) => void;
  onJournals: () => void;
}

/**
 * The Jobs command bar, composed from the shared PageToolbar the way
 * Connectors, Schedules and Contracts compose theirs: search · status tabs ·
 * actions. Re-run and Cancel appear for the selected row when it can take
 * them; layout and journals sit in one View menu.
 */
export function JobsToolbar({
  search,
  onSearch,
  pivot,
  onPivot,
  counts,
  showAttention = true,
  onRefresh,
  rerun,
  cancel,
  exportCsv,
  columns,
  onToggleColumn,
  density,
  onDensity,
  onJournals,
}: JobsToolbarProps) {
  return (
    <PageToolbar
      className="df2-jobs-toolbar"
      searchValue={search}
      onSearchChange={onSearch}
      searchPlaceholder="Search jobs or IDs…"
      filters={
        <FilterBar variant="inline" ariaLabel="Filter jobs by status">
          <FilterTabs<JobListStatus>
            ariaLabel="Filter jobs by status"
            value={pivot}
            onChange={onPivot}
            items={[
              { id: "all", label: "All", count: counts.all },
              { id: "running", label: "Running", count: counts.running },
              ...(showAttention ? [{ id: "attention" as const, label: "Needs attention", count: counts.attention }] : []),
              { id: "completed", label: "Completed", count: counts.completed },
              { id: "failed", label: "Failed", count: counts.failed },
            ]}
          />
        </FilterBar>
      }
      actions={
        <div className="df2-toolbar-actions-cluster" role="group" aria-label="Job actions">
          {onRefresh && (
            <Button size="sm" onClick={onRefresh} leadingIcon={<DtIcon name="activity" size={14} />}>
              Refresh
            </Button>
          )}
          {rerun.visible !== false && (
            <Button
              size="sm"
              variant="ghost"
              disabled={rerun.disabled}
              loading={rerun.busy}
              title={rerun.title}
              onClick={rerun.onClick}
              leadingIcon={<DtIcon name="play" size={14} />}
            >
              Re-run
            </Button>
          )}
          {cancel.visible !== false && (
            <Button
              size="sm"
              variant="ghost"
              disabled={cancel.disabled}
              loading={cancel.busy}
              title={cancel.title}
              onClick={cancel.onClick}
              leadingIcon={<DtIcon name="x" size={14} />}
            >
              Cancel
            </Button>
          )}
          <Button
            size="sm"
            variant="ghost"
            disabled={exportCsv.disabled}
            title={exportCsv.title}
            onClick={exportCsv.onClick}
            leadingIcon={<DtIcon name="download" size={14} />}
          >
            Export CSV
          </Button>
          <JobsMenu
            label="View"
            icon={<DtIcon name="table" size={14} />}
            title="Columns, row density and journals"
            items={[
              ...JOBS_GRID_COLUMNS.filter((column) => !column.required).map((column) => ({
                id: `column-${column.id}`,
                section: "Columns",
                label: column.label,
                kind: "checkbox" as const,
                checked: columns.includes(column.id),
                onSelect: () => onToggleColumn(column.id),
              })),
              ...(["compact", "comfortable"] as const).map((value) => ({
                id: `density-${value}`,
                section: "Density",
                label: value === "compact" ? "Compact" : "Comfortable",
                kind: "radio" as const,
                checked: density === value,
                onSelect: () => onDensity(value),
              })),
              { id: "journals", section: "More", label: "Journals…", onSelect: onJournals },
            ]}
          />
        </div>
      }
    />
  );
}
