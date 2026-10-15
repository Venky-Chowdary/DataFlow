import { ReactNode, useState } from "react";
import { ApprovalInbox } from "./ApprovalInbox";
import { ScheduleForm } from "./ScheduleForm";
import { ScheduleRunHistory } from "./ScheduleRunHistory";
import { Button } from "../ui/Button";
import { EmptyState } from "../ui/EmptyState";
import { FilterTabs } from "../ui/FilterTabs";
import { HiddenFileInput } from "../ui/HiddenFileInput";
import { PageShell } from "../ui/PageShell";
import { PermissionNotice } from "../PermissionNotice";
import { SectionLoader } from "../LoadingState";
import { scheduleCadenceLabel } from "../../lib/scheduleCadence";
import { jobStatusBadgeClass, jobStatusLabel } from "../../lib/uiUtils";
import type {
  Connector,
  PipelineSchedule,
  ScheduleApprovalInboxItem,
  ScheduleInput,
  ScheduleIntervals,
} from "../../lib/types";

type ScheduleFilter = "all" | "active" | "paused";

export interface SchedulesStudioProps {
  onClassic: () => void;
  connectors: Connector[];
  loading: boolean;
  loadError: string;
  schedules: PipelineSchedule[];
  filteredSchedules: PipelineSchedule[];
  filter: ScheduleFilter;
  onFilter: (filter: ScheduleFilter) => void;
  search: string;
  onSearch: (value: string) => void;
  enabledCount: number;
  pausedCount: number;
  showForm: boolean;
  editing: PipelineSchedule | null;
  saving: boolean;
  intervals: ScheduleIntervals | null;
  runningId: string | null;
  highlightScheduleId?: string;
  selectedId: string | null;
  manageAllowed: boolean;
  manageReason: string;
  runAllowed: boolean;
  runReason: string;
  gitopsBusy: boolean;
  gitopsRequireSigned: boolean;
  onGitopsRequireSigned: (value: boolean) => void;
  approvals: ScheduleApprovalInboxItem[];
  onRetry: () => void;
  onCreate: () => void;
  onSubmit: (input: Partial<ScheduleInput>) => void;
  onCancelForm: () => void;
  onOpenStudio: () => void;
  onOpenStudioSchedule: (id: string) => void;
  onToggle: (sched: PipelineSchedule) => void;
  onRun: (id: string) => void;
  onSync: (id: string) => void;
  onOpenJob: (jobId: string) => void;
  onDetails: (id: string) => void;
  onExportFleet: () => void;
  onImportFile: (file: File) => void;
  onApprovalsDecided: () => void;
  onReviewMapping: (id: string) => void;
  detail: ReactNode;
}

function formatStamp(iso: string | null | undefined): string {
  if (!iso) return "Not scheduled";
  const date = new Date(iso);
  if (Number.isNaN(date.getTime())) return iso;
  return date.toLocaleString(undefined, {
    month: "short",
    day: "numeric",
    hour: "2-digit",
    minute: "2-digit",
  });
}

function connectorName(connectors: Connector[], id: string): string {
  return connectors.find((row) => row.id === id)?.name || "";
}

export function SchedulesStudio({
  onClassic,
  connectors,
  loading,
  loadError,
  schedules,
  filteredSchedules,
  filter,
  onFilter,
  search,
  onSearch,
  enabledCount,
  pausedCount,
  showForm,
  editing,
  saving,
  intervals,
  runningId,
  highlightScheduleId,
  selectedId,
  manageAllowed,
  manageReason,
  runAllowed,
  runReason,
  gitopsBusy,
  gitopsRequireSigned,
  onGitopsRequireSigned,
  approvals,
  onRetry,
  onCreate,
  onSubmit,
  onCancelForm,
  onOpenStudio,
  onOpenStudioSchedule,
  onToggle,
  onRun,
  onSync,
  onOpenJob,
  onDetails,
  onExportFleet,
  onImportFile,
  onApprovalsDecided,
  onReviewMapping,
  detail,
}: SchedulesStudioProps) {
  const [historyId, setHistoryId] = useState<string | null>(null);

  return (
    <PageShell
      wide
      className="df2-page-pipelines df2-schedules-studio df2-studio-next"
      title="Schedules"
      description="Recurring syncs on a cadence. Run now, pause, and history use the schedule API. Classic UI returns to the previous schedules page."
    >
      <div data-testid="schedules-studio">
        <PermissionNotice
          allowed={manageAllowed}
          reason={manageReason}
          what="Schedules are read-only for you."
        />
        <div className="df2-sched-studio-toolbar">
          {schedules.length > 0 && !showForm ? (
            <label className="df2-sched-studio-search">
              <span className="df2-sr-only">Search schedules</span>
              <input
                type="search"
                value={search}
                placeholder="Search schedules"
                onChange={(event) => onSearch(event.target.value)}
              />
            </label>
          ) : null}
          {schedules.length > 0 ? (
            <div className="df2-sched-studio-filters">
              <FilterTabs
                ariaLabel="Filter schedules"
                value={filter}
                onChange={onFilter}
                disabled={showForm}
                items={[
                  { id: "all", label: "All", count: schedules.length },
                  { id: "active", label: "Active", count: enabledCount },
                  { id: "paused", label: "Paused", count: pausedCount },
                ]}
              />
            </div>
          ) : null}
          <div className="df2-sched-studio-actions">
            <Button
              size="sm"
              variant="ghost"
              data-testid="schedules-classic-ui"
              onClick={onClassic}
            >
              Classic UI
            </Button>
            {!showForm ? (
              <>
                <Button
                  size="sm"
                  variant="ghost"
                  loading={gitopsBusy}
                  disabled={gitopsBusy || schedules.length === 0}
                  onClick={onExportFleet}
                >
                  Export YAML
                </Button>
                <label className="df2-sched-studio-gitops">
                  <input
                    type="checkbox"
                    checked={gitopsRequireSigned}
                    onChange={(event) => onGitopsRequireSigned(event.target.checked)}
                  />
                  <span>Require signed</span>
                </label>
                <HiddenFileInput
                  id="df2-schedule-import-studio"
                  accept=".yaml,.yml,.json,application/x-yaml,text/yaml,application/json"
                  disabled={!manageAllowed || gitopsBusy}
                  onChange={(event) => {
                    const file = event.target.files?.[0];
                    event.target.value = "";
                    if (file) onImportFile(file);
                  }}
                />
                <label
                  htmlFor="df2-schedule-import-studio"
                  className="df2-btn df2-btn-sm df2-btn-ghost"
                  aria-disabled={!manageAllowed || gitopsBusy}
                >
                  Import YAML
                </label>
                <Button
                  size="sm"
                  variant="primary"
                  disabled={!manageAllowed}
                  title={manageReason || undefined}
                  onClick={onCreate}
                >
                  New schedule
                </Button>
              </>
            ) : null}
          </div>
        </div>

        {showForm ? (
          <div className="df2-pipeline-form is-active df2-sched-studio-form">
            <div className="df2-sched-studio-actions">
              <Button size="sm" variant="ghost" onClick={onCancelForm}>
                Cancel
              </Button>
            </div>
            <ScheduleForm
              key={editing?.id ?? "new"}
              connectors={connectors}
              intervals={intervals}
              initial={editing}
              saving={saving}
              onSubmit={onSubmit}
              onCancel={onCancelForm}
              onOpenStudio={
                editing ? () => onOpenStudioSchedule(editing.id) : onOpenStudio
              }
            />
          </div>
        ) : null}

        {!loading && !showForm && approvals.length > 0 ? (
          <ApprovalInbox
            items={approvals}
            onDecided={onApprovalsDecided}
            onOpenSchedule={onDetails}
            onReviewMapping={onReviewMapping}
            onOpenStudio={onOpenStudioSchedule}
            onRunNow={onRun}
          />
        ) : null}

        {loading ? (
          <SectionLoader title="Loading schedules" hint="Fetching schedules…" />
        ) : loadError ? (
          <EmptyState
            page
            icon="alert"
            title="Schedules could not be read"
            description={loadError}
            action={
              <Button variant="secondary" onClick={onRetry}>
                Try again
              </Button>
            }
          />
        ) : schedules.length === 0 && !showForm ? (
          <EmptyState
            page
            icon="activity"
            title="No schedules yet"
            description="Unattended runs replay a Validate-approved mapping contract. Open Transfer Studio, Map, then Validate, then schedule from the footer."
            action={
              <>
                <Button variant="primary" onClick={onOpenStudio}>
                  Open Transfer Studio
                </Button>
                <Button
                  variant="secondary"
                  disabled={!manageAllowed}
                  title={manageReason || undefined}
                  onClick={onCreate}
                >
                  Draft cadence only
                </Button>
              </>
            }
          />
        ) : filteredSchedules.length === 0 && !showForm ? (
          <EmptyState
            compact
            icon="activity"
            title={filter === "active" ? "No active schedules" : "No paused schedules"}
            description="Try another filter or create a new schedule."
          />
        ) : (
          <div className="df2-sched-studio-list" role="list" aria-label="Schedules">
            {filteredSchedules.map((sched) => {
              const source = connectorName(connectors, sched.source_connector_id);
              const dest = connectorName(connectors, sched.dest_connector_id);
              const historyOpen = historyId === sched.id;
              const jobId = (sched.last_job_id || "").trim();
              return (
                <article
                  key={sched.id}
                  role="listitem"
                  className={`df2-sched-studio-row${selectedId === sched.id ? " is-selected" : ""}`}
                  data-testid={`sched-row-${sched.id}`}
                  data-highlighted={highlightScheduleId === sched.id ? "true" : undefined}
                >
                  <div className="df2-sched-studio-identity">
                    <h2 className="df2-sched-studio-name">{sched.name}</h2>
                    <p className="df2-sched-studio-route">
                      {[source, sched.source_table].filter(Boolean).join(" · ") || sched.source_table}
                      {" → "}
                      {[dest, sched.dest_table].filter(Boolean).join(" · ") || sched.dest_table}
                    </p>
                  </div>
                  <div className="df2-sched-studio-meta">
                    <div className="df2-sched-studio-fact">
                      <span>Cadence</span>
                      <strong>{scheduleCadenceLabel(sched)}</strong>
                    </div>
                    <div className="df2-sched-studio-fact">
                      <span>Next run</span>
                      <strong>{formatStamp(sched.next_run_at)}</strong>
                    </div>
                    <span className={jobStatusBadgeClass(sched.last_status || (sched.enabled ? "active" : "paused"))}>
                      {sched.last_status
                        ? jobStatusLabel(sched.last_status)
                        : sched.enabled
                          ? "Active"
                          : "Paused"}
                    </span>
                  </div>
                  <div className="df2-sched-studio-row-actions">
                    <Button
                      size="sm"
                      variant="secondary"
                      disabled={!manageAllowed}
                      title={manageReason || undefined}
                      aria-pressed={sched.enabled}
                      onClick={() => onToggle(sched)}
                    >
                      {sched.enabled ? "Pause" : "Enable"}
                    </Button>
                    <Button
                      size="sm"
                      variant="secondary"
                      loading={runningId === sched.id}
                      disabled={!runAllowed || runningId === sched.id}
                      title={runReason || undefined}
                      onClick={() => onRun(sched.id)}
                    >
                      Run now
                    </Button>
                    <Button
                      size="sm"
                      variant="ghost"
                      loading={runningId === sched.id}
                      disabled={!runAllowed || runningId === sched.id}
                      title={runReason || undefined}
                      onClick={() => onSync(sched.id)}
                    >
                      Sync now
                    </Button>
                    <Button
                      size="sm"
                      variant="ghost"
                      aria-expanded={historyOpen}
                      onClick={() => setHistoryId(historyOpen ? null : sched.id)}
                    >
                      History
                    </Button>
                    {jobId ? (
                      <Button size="sm" variant="ghost" onClick={() => onOpenJob(jobId)}>
                        Open job
                      </Button>
                    ) : null}
                    <Button size="sm" variant="ghost" onClick={() => onDetails(sched.id)}>
                      Details
                    </Button>
                  </div>
                  {historyOpen ? (
                    <div className="df2-sched-studio-history">
                      <ScheduleRunHistory
                        scheduleId={sched.id}
                        onOpenJob={onOpenJob}
                        onEditMapping={() => onReviewMapping(sched.id)}
                      />
                    </div>
                  ) : null}
                </article>
              );
            })}
          </div>
        )}
        {detail}
      </div>
    </PageShell>
  );
}
