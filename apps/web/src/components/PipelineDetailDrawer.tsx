import { useEffect, useState } from "react";
import { ConnectorIcon } from "../app/brand-icons";
import { DtIcon } from "./DtIcon";
import { Button } from "./ui/Button";
import { Drawer } from "./ui/Drawer";
import { FilterTabs } from "./ui/FilterTabs";
import { ScheduleRunHistory } from "./schedules/ScheduleRunHistory";
import {
  fetchContractBreaker,
  fetchJob,
  fetchSchedule,
  revokeScheduleAuthorization,
  runScheduleParallelCheck,
  type ContractBreaker,
} from "../lib/api";
import {
  breakerBadgeClass,
  breakerBlocksRuns,
  breakerLabel,
  campaignBadgeClass,
  campaignLabel,
} from "../lib/contractBreakerUi";
import { computeJobTrustScore } from "../lib/jobTrustScore";
import { PERMISSIONS, useWriteGate } from "../lib/PermissionsContext";
import { destHeadline } from "../lib/conservationLedger";
import {
  formatSchemaPolicyLabel,
  formatSyncModeLabel,
  schemaPolicyHonestyLine,
} from "../lib/transferConstants";
import {
  formatValidateIdentitySummary,
  shortHash,
  type ValidateIdentityView,
} from "../lib/studioValidateIdentity";
import { Connector, PipelineSchedule, StandingAuthorization, TransferJob } from "../lib/types";
import { jobStatusBadgeClass, jobStatusLabel } from "../lib/uiUtils";

export const PIPELINE_TABS = ["Overview", "Schema", "History", "Config"] as const;
export type PipelineTab = (typeof PIPELINE_TABS)[number];

interface PipelineDetailDrawerProps {
  open: boolean;
  schedule: PipelineSchedule | null;
  source?: Connector;
  dest?: Connector;
  tab: PipelineTab;
  setTab: (tab: PipelineTab) => void;
  running?: boolean;
  /** Hint from fleet load; refreshed via fetch when drawer opens. */
  breakerHint?: string | null;
  onClose: () => void;
  onRun: () => void;
  onEdit: () => void;
  onDelete: () => void;
  onToggle: () => void;
  /** Why running is refused, when it is — the control says so before it is pressed. */
  runRefusal?: string;
  /** Why changing this schedule is refused, when it is. */
  manageRefusal?: string;
  onOpenJob?: (jobId: string) => void;
  onResetBreaker?: (contractId: string) => void | Promise<void>;
  onExportYaml?: () => void;
}

const INTERVAL_LABEL: Record<string, string> = {
  hourly: "Every hour",
  daily: "Daily",
  weekly: "Weekly",
};

function formatWhen(iso: string | null | undefined): string {
  if (!iso) return "—";
  try {
    return new Date(iso).toLocaleString(undefined, {
      month: "short",
      day: "numeric",
      hour: "2-digit",
      minute: "2-digit",
    });
  } catch {
    return iso;
  }
}

export function PipelineDetailDrawer({
  open,
  schedule: sched,
  source,
  dest,
  tab,
  setTab,
  running,
  breakerHint,
  onClose,
  onRun,
  onEdit,
  onDelete,
  onToggle,
  runRefusal = "",
  manageRefusal = "",
  onOpenJob,
  onResetBreaker,
  onExportYaml,
}: PipelineDetailDrawerProps) {
  const [mappingCount, setMappingCount] = useState(0);
  const [mappings, setMappings] = useState<{ source: string; target: string }[]>([]);
  const [lastJob, setLastJob] = useState<TransferJob | null>(null);
  const [breaker, setBreaker] = useState<ContractBreaker | null>(null);
  const [resettingBreaker, setResettingBreaker] = useState(false);
  const [campaign, setCampaign] = useState<PipelineSchedule["fidelity_campaign"]>();
  const [checkingParallel, setCheckingParallel] = useState(false);
  /** Standing authority currently recorded on this schedule, if any. */
  const [authorization, setAuthorization] = useState<StandingAuthorization | null>(null);
  const [validateIdentity, setValidateIdentity] = useState<ValidateIdentityView>(
    () => formatValidateIdentitySummary(null),
  );
  const [revoking, setRevoking] = useState(false);
  // Resetting a contract breaker is an editor-level write on the contract; every
  // other write in this drawer is refused before the click, so this one is too.
  const breakerReset = useWriteGate(PERMISSIONS.connectorWrite);

  useEffect(() => {
    if (!open || !sched?.id) {
      setMappingCount(0);
      setMappings([]);
      setLastJob(null);
      setBreaker(null);
      setCampaign(undefined);
      setAuthorization(null);
      setValidateIdentity(formatValidateIdentitySummary(null));
      return;
    }
    setValidateIdentity(formatValidateIdentitySummary(sched));
    let cancelled = false;
    void fetchSchedule(sched.id)
      .then((full) => {
        if (cancelled) return;
        const maps = Array.isArray(full.mappings) ? full.mappings : [];
        setMappings(
          maps.map((m) => ({
            source: String(m.source ?? ""),
            target: String(m.target ?? ""),
          })).filter((m) => m.source || m.target),
        );
        setMappingCount(
          typeof full.mapping_count === "number" ? full.mapping_count : maps.length,
        );
        setCampaign(full.fidelity_campaign);
        setValidateIdentity(formatValidateIdentitySummary(full));
        const grant = full.standing_authorization;
        setAuthorization(grant && "id" in grant ? (grant as StandingAuthorization) : null);
      })
      .catch(() => {
        if (!cancelled) {
          setMappingCount(0);
          setMappings([]);
          setAuthorization(null);
          setValidateIdentity(formatValidateIdentitySummary(sched));
        }
      });
    if (sched.last_job_id) {
      void fetchJob(sched.last_job_id)
        .then((j) => {
          if (!cancelled) setLastJob(j);
        })
        .catch(() => {
          if (!cancelled) setLastJob(null);
        });
    } else {
      setLastJob(null);
    }
    if (sched.contract_id) {
      void fetchContractBreaker(sched.contract_id)
        .then((b) => {
          if (!cancelled) setBreaker(b);
        })
        .catch(() => {
          if (!cancelled) setBreaker(null);
        });
    } else {
      setBreaker(null);
    }
    return () => {
      cancelled = true;
    };
  }, [open, sched?.id, sched?.last_job_id, sched?.contract_id]);

  if (!sched) return null;

  const isRunning = Boolean(running || sched.running);
  const breakerState = breaker?.state || breakerHint || null;
  const breakerOpen = breakerBlocksRuns(breakerState);
  const cadence = sched.cron
    ? `Cron ${sched.cron}`
    : (INTERVAL_LABEL[sched.interval] ?? sched.interval);
  const cadenceDetail = sched.cron
    ? `Wall clock in ${sched.timezone || "UTC"}`
    : "Rolling interval from last run — use Cron for a fixed daily time";
  const syncLabel = formatSyncModeLabel(sched.sync_mode);
  const rejected = Number(lastJob?.rejected_rows ?? 0);
  const coerced = Number(lastJob?.coerced_null_rows ?? 0);
  const lastTrust = lastJob ? computeJobTrustScore(lastJob) : null;
  const lastDest = destHeadline(lastJob);
  const needsAttention =
    Boolean(sched.last_status && /fail|error/i.test(String(sched.last_status)))
    || rejected > 0
    || !sched.enabled
    || breakerOpen
    || campaign?.verdict === "diverging"
    || (lastTrust != null && lastTrust.score < 60);

  const liveCampaign = campaign || sched.fidelity_campaign;
  const campaignVerdict = liveCampaign?.verdict;

  const handleResetBreaker = async () => {
    if (!sched.contract_id || !onResetBreaker) return;
    setResettingBreaker(true);
    try {
      await onResetBreaker(sched.contract_id);
      const b = await fetchContractBreaker(sched.contract_id);
      setBreaker(b);
    } catch {
      /* parent toasts */
    } finally {
      setResettingBreaker(false);
    }
  };

  const handleRevoke = async () => {
    setRevoking(true);
    try {
      const result = await revokeScheduleAuthorization(
        sched.id,
        "Revoked from the pipeline drawer",
      );
      setAuthorization(result.authorization);
    } catch {
      /* the grant stays displayed; nothing was revoked */
    } finally {
      setRevoking(false);
    }
  };

  const handleParallelCheck = async () => {
    if (runRefusal) return;
    setCheckingParallel(true);
    try {
      const result = await runScheduleParallelCheck(sched.id);
      if (result.campaign) setCampaign(result.campaign);
    } finally {
      setCheckingParallel(false);
    }
  };

  return (
    <Drawer
      open={open}
      onClose={onClose}
      size="lg"
      ariaLabel={`${sched.name} pipeline details`}
      icon={<DtIcon name="activity" size={22} />}
      title={sched.name}
      subtitle={`${source?.name ?? "Source"} → ${dest?.name ?? "Destination"}`}
      headerExtra={
        <>
          {isRunning && (
            <span className="df2-badge df2-badge-run">
              <DtIcon name="activity" size={11} /> Running
            </span>
          )}
          {breakerState && (
            <span className={`df2-badge ${breakerBadgeClass(breakerState)}`} title="Data contract circuit breaker">
              {breakerLabel(breakerState)}
            </span>
          )}
          {campaignVerdict && (
            <span
              className={`df2-badge ${campaignBadgeClass(campaignVerdict)}`}
              title={liveCampaign?.next_action || "Parallel-run Dual Run campaign"}
            >
              {campaignLabel(campaignVerdict)}
            </span>
          )}
          <span className={`df2-badge ${sched.enabled ? "df2-badge-live" : "df2-badge-muted"}`}>
            {sched.enabled ? "Active" : "Paused"}
          </span>
          {needsAttention && (
            <span className="df2-badge df2-badge-warn">Needs attention</span>
          )}
          {sched.last_status && (
            <span className={jobStatusBadgeClass(sched.last_status)}>
              {jobStatusLabel(sched.last_status)}
            </span>
          )}
        </>
      }
      footer={
        <div className="df2-drawer-actions">
          <Button
            size="sm"
            variant="primary"
            loading={running}
            loadingLabel="Running…"
            disabled={isRunning || breakerOpen || Boolean(runRefusal)}
            title={
              runRefusal
                || (breakerOpen ? "Reset the contract breaker before running" : undefined)
                || (sched.enabled
                  ? undefined
                  : "Runs once now. Does not activate the cadence — the schedule stays paused.")
            }
            onClick={onRun}
            leadingIcon={<DtIcon name="activity" size={14} />}
          >
            {isRunning ? "Running…" : breakerOpen ? "Breaker open" : "Run now"}
          </Button>
          {breakerOpen && sched.contract_id && onResetBreaker && (
            <Button
              size="sm"
              variant="ghost"
              loading={resettingBreaker}
              loadingLabel="Resetting…"
              disabled={!breakerReset.allowed}
              title={breakerReset.reason || undefined}
              onClick={() => void handleResetBreaker()}
              leadingIcon={<DtIcon name="shield" size={14} />}
            >
              Reset breaker
            </Button>
          )}
          {sched.last_job_id && onOpenJob && (
            <Button
              size="sm"
              variant="ghost"
              onClick={() => onOpenJob(sched.last_job_id!)}
              leadingIcon={<DtIcon name="jobs" size={14} />}
            >
              Last job
            </Button>
          )}
          <Button
            size="sm"
            variant="ghost"
            disabled={Boolean(manageRefusal)}
            title={manageRefusal || undefined}
            onClick={onToggle}
            leadingIcon={<DtIcon name={sched.enabled ? "pause" : "check"} size={14} />}
          >
            {sched.enabled ? "Pause" : "Activate"}
          </Button>
          <Button
            size="sm"
            variant="ghost"
            disabled={Boolean(manageRefusal)}
            title={manageRefusal || undefined}
            onClick={onEdit}
            leadingIcon={<DtIcon name="settings" size={14} />}
          >
            Edit
          </Button>
          {onExportYaml && (
            <Button
              size="sm"
              variant="ghost"
              onClick={onExportYaml}
              leadingIcon={<DtIcon name="download" size={14} />}
            >
              Export YAML
            </Button>
          )}
          <Button
            size="sm"
            variant="danger"
            className="df2-drawer-action-delete"
            disabled={Boolean(manageRefusal)}
            title={manageRefusal || undefined}
            onClick={onDelete}
            leadingIcon={<DtIcon name="trash" size={14} />}
          >
            Delete
          </Button>
        </div>
      }
    >
      <div className="df2-drawer-facts" aria-label="Pipeline summary">
        <div className="df2-drawer-fact">
          <span>Cadence</span>
          <strong title={cadenceDetail}>{cadence}</strong>
        </div>
        <div className="df2-drawer-fact">
          <span>Sync mode</span>
          <strong>{syncLabel}</strong>
        </div>
        <div className="df2-drawer-fact">
          <span>Data contract</span>
          <strong title={sched.contract_id || undefined}>
            {sched.contract_id
              ? (sched.require_signed_contract ? "Signed · enforced" : "Bound")
              : "None"}
          </strong>
        </div>
        {sched.contract_id && (
          <div className="df2-drawer-fact">
            <span>Breaker</span>
            <strong className={breakerOpen ? "df2-text-warn" : undefined}>
              {breakerState ? breakerLabel(breakerState) : "—"}
            </strong>
          </div>
        )}
        <div className="df2-drawer-fact">
          <span>Parallel run</span>
          <strong
            className={campaignVerdict === "diverging" ? "df2-text-warn" : undefined}
            title={liveCampaign?.note || undefined}
          >
            {campaignVerdict
              ? `${campaignLabel(campaignVerdict)}${
                typeof liveCampaign?.consecutive_passes === "number"
                  && typeof liveCampaign?.required_consecutive === "number"
                  ? ` · ${liveCampaign.consecutive_passes}/${liveCampaign.required_consecutive}`
                  : ""
              }`
              : "Not started"}
          </strong>
        </div>
        <div className="df2-drawer-fact">
          <span>Last run</span>
          <strong>{formatWhen(sched.last_run_at)}</strong>
        </div>
        <div className="df2-drawer-fact">
          <span>Next run</span>
          <strong title={cadenceDetail}>{formatWhen(sched.next_run_at)}</strong>
        </div>
      </div>

      {(lastJob || mappingCount > 0) && (
        <div className="df2-drawer-facts df2-drawer-trust" aria-label="Integrity summary">
          <div className="df2-drawer-fact">
            <span>Mapped columns</span>
            <strong>{mappingCount || "—"}</strong>
          </div>
          <div className="df2-drawer-fact">
            <span>{lastDest.label}</span>
            <strong title={lastDest.title}>{lastDest.value}</strong>
          </div>
          <div className="df2-drawer-fact">
            <span>Quarantine</span>
            <strong className={rejected > 0 ? "df2-text-warn" : undefined}>{rejected}</strong>
          </div>
          <div className="df2-drawer-fact">
            <span>Coerced nulls</span>
            <strong>{coerced}</strong>
          </div>
          {lastJob && lastTrust && (
            <div className="df2-drawer-fact">
              <span>Trust score</span>
              <strong
                className={lastTrust.tone === "danger" ? "df2-text-warn" : undefined}
                title={lastTrust.next_action.detail}
              >
                {lastTrust.score}
                {" · "}
                {lastTrust.grade}
              </strong>
            </div>
          )}
        </div>
      )}

      <div className="df2-drawer-section df2-drawer-workbench">
        <FilterTabs
          ariaLabel="Pipeline detail sections"
          value={tab}
          onChange={setTab}
          items={PIPELINE_TABS.map((id) => ({
            id,
            label: id,
            count: id === "History" ? sched.run_count : id === "Schema" ? (mappingCount || undefined) : undefined,
          }))}
        />

        {tab === "Overview" && (
          <section className="df2-drawer-section" aria-label="Route">
            <div className="df2-drawer-section-head">
              <h3><DtIcon name="transfer" size={14} /> Route</h3>
            </div>
            <div className="df2-drawer-related-list" role="list">
              <div className="df2-drawer-related-row" role="listitem">
                <span className="df2-drawer-related-main">
                  <span className="df2-drawer-route-node">
                    <ConnectorIcon id={source?.type ?? "database"} size={16} />
                    <strong title={source?.name}>{source?.name ?? "Source"}</strong>
                  </span>
                  <small>{sched.source_table || "—"}</small>
                </span>
                <span className="df2-badge df2-badge-muted">Source</span>
              </div>
              <div className="df2-drawer-related-row" role="listitem">
                <span className="df2-drawer-related-main">
                  <span className="df2-drawer-route-node">
                    <ConnectorIcon id={dest?.type ?? "database"} size={16} />
                    <strong title={dest?.name}>{dest?.name ?? "Destination"}</strong>
                  </span>
                  <small>{sched.dest_table || "—"}</small>
                </span>
                <span className="df2-badge df2-badge-muted">Destination</span>
              </div>
            </div>
            <dl className="df2-drawer-kv">
              <div>
                <dt>Cadence detail</dt>
                <dd>{cadenceDetail}</dd>
              </div>
              <div><dt>Timezone</dt><dd>{sched.cron ? (sched.timezone || "UTC") : "N/A (rolling preset)"}</dd></div>
              <div><dt>Validation</dt><dd>{sched.validation_mode || "—"}</dd></div>
              <div><dt>Schema policy</dt><dd>{formatSchemaPolicyLabel(sched.schema_policy)}</dd></div>
              {sched.sync_mode === "cdc" && (
                <>
                  <div><dt>CDC snapshot</dt><dd>{sched.snapshot_mode || "initial"}</dd></div>
                  <div><dt>Delivery</dt><dd>{sched.delivery_guarantee || "at_least_once"}</dd></div>
                  <div><dt>Append-only CDC</dt><dd>{sched.allow_append_only ? "Yes — duplicates on redelivery" : "No"}</dd></div>
                  {sched.cdc_row_filter && sched.cdc_row_filter !== "all" ? (
                    <div><dt>CDC row filter</dt><dd>{sched.cdc_row_filter}</dd></div>
                  ) : null}
                  {sched.multi_subnet_failover ? (
                    <div><dt>MultiSubnetFailover</dt><dd>Yes</dd></div>
                  ) : null}
                </>
              )}
              <div><dt>Write via staging</dt><dd>{sched.write_via_staging ? "Yes" : "No"}</dd></div>
              {sched.priority_column ? (
                <div>
                  <dt>Priority</dt>
                  <dd>{sched.priority_column} · {sched.priority_direction === "asc" ? "lowest first" : "highest first"}</dd>
                </div>
              ) : null}
              {sched.row_limit ? <div><dt>Row limit</dt><dd>{sched.row_limit.toLocaleString()}</dd></div> : null}
              <div><dt>Date locale</dt><dd>{sched.date_locale || "Auto"}</dd></div>
              <div><dt>Number locale</dt><dd>{sched.number_locale || "Auto"}</dd></div>
              <div><dt>Runs</dt><dd>{sched.run_count.toLocaleString()}</dd></div>
              {sched.contract_id && (
                <div>
                  <dt>Contract breaker</dt>
                  <dd>
                    {breaker
                      ? `${breaker.state} · ${breaker.failure_count}/${breaker.failure_threshold} failures`
                      : (breakerState || "—")}
                  </dd>
                </div>
              )}
              {sched.primary_key && <div><dt>Primary key</dt><dd>{sched.primary_key}</dd></div>}
              {sched.cursor_column && <div><dt>Cursor</dt><dd>{sched.cursor_column}</dd></div>}
            </dl>
            {authorization && !authorization.revoked_at && (
              <div className="df2-approval-grant" role="group" aria-label="Delegated authority">
                <p>
                  <strong>{authorization.actor}</strong> authorized unattended runs of this
                  exact plan until {formatWhen(authorization.expires_at)}
                  {authorization.max_uses === 1 ? " (one run only)" : ""} — “{authorization.reason}”.
                  It stops applying if the mapping, source shape, policy or contract changes.
                </p>
                <Button
                  size="sm"
                  variant="ghost"
                  loading={revoking}
                  disabled={Boolean(manageRefusal)}
                  onClick={() => void handleRevoke()}
                  title={manageRefusal || "Revoke this authority; later runs need a fresh decision"}
                >
                  Revoke authority
                </Button>
              </div>
            )}
            {breakerOpen && (
              <p className="df2-drawer-empty-line">
                Circuit breaker is open — scheduled and manual runs stay blocked until you reset it (after fixing the contract drift or quality failure that tripped it).
              </p>
            )}
            <p className="df2-drawer-empty-line">
              {liveCampaign?.next_action
                || "Parallel run compares live source vs destination column profiles (Google Dual Run cycles). Overwrite loads record a cycle automatically. Not per-cell Gate-8."}
            </p>
            <Button
              size="sm"
              variant="secondary"
              loading={checkingParallel}
              loadingLabel="Comparing…"
              disabled={Boolean(runRefusal)}
              onClick={() => void handleParallelCheck()}
              leadingIcon={<DtIcon name="shield" size={14} />}
              title={
                runRefusal
                || "Compare live source and destination now and record a Dual Run cycle"
              }
            >
              Run parallel-run check
            </Button>
          </section>
        )}

        {tab === "Schema" && (
          <section className="df2-drawer-section" aria-label="Schema mapping">
            <div className="df2-drawer-section-head">
              <h3><DtIcon name="connectors" size={14} /> Schema map</h3>
              <span className="df2-drawer-count">{mappingCount}</span>
            </div>
            <dl className="df2-drawer-kv">
              <div><dt>Mapped columns</dt><dd>{mappingCount || "None stored on schedule"}</dd></div>
              <div><dt>Primary key</dt><dd>{sched.primary_key || "—"}</dd></div>
              <div><dt>Cursor column</dt><dd>{sched.cursor_column || "—"}</dd></div>
              <div>
                <dt>Schema policy</dt>
                <dd>
                  {formatSchemaPolicyLabel(sched.schema_policy)}
                  <span className="df2-drawer-kv-hint">{schemaPolicyHonestyLine(sched.schema_policy || "")}</span>
                </dd>
              </div>
              <div>
                <dt>Validate identity</dt>
                <dd>
                  {validateIdentity.pinned
                    ? "Stamped from Studio Validate — replayed on the beat, not a live green"
                    : "Not stamped — create from Transfer Studio after Validate"}
                </dd>
              </div>
              <div>
                <dt>Shape recipe</dt>
                <dd
                  className="df2-cell-mono"
                  title={validateIdentity.shapeHash || undefined}
                >
                  {validateIdentity.shapeHash
                    ? `${shortHash(validateIdentity.shapeHash)}${validateIdentity.shapeSteps ? ` · ${validateIdentity.shapeSteps} step${validateIdentity.shapeSteps === 1 ? "" : "s"}` : ""}`
                    : "—"}
                </dd>
              </div>
              <div>
                <dt>Decision artifact</dt>
                <dd className="df2-cell-mono" title={validateIdentity.decisionHash || undefined}>
                  {validateIdentity.decisionHash ? shortHash(validateIdentity.decisionHash) : "—"}
                </dd>
              </div>
              <div>
                <dt>DDL identity</dt>
                <dd className="df2-cell-mono" title={validateIdentity.ddlHash || undefined}>
                  {validateIdentity.ddlHash ? shortHash(validateIdentity.ddlHash) : "—"}
                </dd>
              </div>
              {sched.sync_mode === "cdc" && (
                <>
                  <div><dt>CDC snapshot</dt><dd>{sched.snapshot_mode || "initial"}</dd></div>
                  <div><dt>Delivery</dt><dd>{sched.delivery_guarantee || "at_least_once"}</dd></div>
                  <div><dt>Append-only CDC</dt><dd>{sched.allow_append_only ? "Yes — duplicates on redelivery" : "No"}</dd></div>
                  {sched.cdc_row_filter && sched.cdc_row_filter !== "all" ? (
                    <div><dt>CDC row filter</dt><dd>{sched.cdc_row_filter}</dd></div>
                  ) : null}
                  {sched.multi_subnet_failover ? (
                    <div><dt>MultiSubnetFailover</dt><dd>Yes</dd></div>
                  ) : null}
                </>
              )}
              <div><dt>Validation mode</dt><dd>{sched.validation_mode || "—"}</dd></div>
              <div><dt>Backfill new fields</dt><dd>{sched.backfill_new_fields ? "Yes" : "No"}</dd></div>
              <div><dt>Write via staging</dt><dd>{sched.write_via_staging ? "Yes" : "No"}</dd></div>
              <div>
                <dt>Priority column</dt>
                <dd>
                  {sched.priority_column
                    ? `${sched.priority_column} (${sched.priority_direction === "asc" ? "asc" : "desc"})`
                    : "—"}
                </dd>
              </div>
              <div><dt>Row limit</dt><dd>{sched.row_limit ? sched.row_limit.toLocaleString() : "None"}</dd></div>
              <div><dt>Date locale</dt><dd>{sched.date_locale || "Auto"}</dd></div>
              <div><dt>Number locale</dt><dd>{sched.number_locale || "Auto"}</dd></div>
            </dl>
            {mappings.length > 0 ? (
              <ul className="df2-drawer-map-list" aria-label="Column mappings">
                {mappings.map((m) => (
                  <li key={`${m.source}->${m.target}`}>
                    <code>{m.source || "—"}</code>
                    <span aria-hidden>→</span>
                    <code>{m.target || "—"}</code>
                  </li>
                ))}
              </ul>
            ) : (
              <p className="df2-drawer-empty-line">
                No column mappings stored on this pipeline yet. Edit cannot set a schema map — create from Transfer Studio after Validate so the beat replays signed column names.
              </p>
            )}
          </section>
        )}

        {tab === "History" && (
          <section className="df2-drawer-section" aria-label="Run history">
            <div className="df2-drawer-section-head">
              <h3><DtIcon name="jobs" size={14} /> Run history</h3>
              <span className="df2-drawer-count">{sched.run_count}</span>
            </div>
            <ScheduleRunHistory scheduleId={sched.id} onOpenJob={onOpenJob} onEditMapping={onEdit} />
          </section>
        )}

        {tab === "Config" && (
          <section className="df2-drawer-section" aria-label="Configuration">
            <div className="df2-drawer-section-head">
              <h3><DtIcon name="settings" size={14} /> Configuration</h3>
            </div>
            <dl className="df2-drawer-kv">
              <div><dt>Pipeline ID</dt><dd className="df2-cell-mono">{sched.id}</dd></div>
              <div><dt>Interval</dt><dd>{sched.interval || "—"}</dd></div>
              <div><dt>Cron</dt><dd>{sched.cron || "—"}</dd></div>
              <div><dt>Max retries</dt><dd>{sched.max_retries ?? "—"}</dd></div>
              <div><dt>Retry backoff</dt><dd>{sched.retry_backoff_seconds != null ? `${sched.retry_backoff_seconds}s` : "—"}</dd></div>
              <div><dt>Notify on failure</dt><dd>{sched.notify_on_failure ? "Yes" : "No"}</dd></div>
              <div><dt>Notify on success</dt><dd>{sched.notify_on_success ? "Yes" : "No"}</dd></div>
              <div><dt>Backfill new fields</dt><dd>{sched.backfill_new_fields ? "Yes" : "No"}</dd></div>
              <div><dt>Write via staging</dt><dd>{sched.write_via_staging ? "Yes" : "No"}</dd></div>
              <div>
                <dt>Priority column</dt>
                <dd>
                  {sched.priority_column
                    ? `${sched.priority_column} (${sched.priority_direction === "asc" ? "asc" : "desc"})`
                    : "—"}
                </dd>
              </div>
              <div><dt>Row limit</dt><dd>{sched.row_limit ? sched.row_limit.toLocaleString() : "None"}</dd></div>
              <div><dt>Date locale</dt><dd>{sched.date_locale || "Auto"}</dd></div>
              <div><dt>Number locale</dt><dd>{sched.number_locale || "Auto"}</dd></div>
              <div><dt>Created</dt><dd>{formatWhen(sched.created_at)}</dd></div>
            </dl>
            <p className="df2-drawer-empty-line">
              Use Edit to change connectors, tables, cadence, or sync mode.
            </p>
          </section>
        )}
      </div>
    </Drawer>
  );
}
