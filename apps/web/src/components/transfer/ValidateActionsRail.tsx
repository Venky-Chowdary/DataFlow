import type { ReactNode } from "react";
import { useMemo } from "react";
import { DtIcon } from "../DtIcon";
import { Button } from "../ui/Button";
import { plural } from "../../lib/plural";
import { StepBackButton, StepFooterReason, StepPrimaryButton } from "./StudioStepFooter";
import type { PreflightResult } from "../../lib/types";
import { buildDisplayBlockers, buildExecutiveSummary } from "../../lib/validateIssueGrouping";
import { resolveValidateStudioPrimary } from "../../lib/validateStudioPrimary";

interface ValidateActionsRailProps {
  preflight: PreflightResult | null;
  /** File export route: key uniqueness copy does not apply. */
  fileExport?: boolean;
  preflighting: boolean;
  transferring: boolean;
  mappingReviewCount: number;
  /** Lossy/specialty rows still needing Accept risk — Approve-all cannot clear these. */
  riskAckPendingCount?: number;
  rowCount?: number;
  transferLaunch?: { jobId: string; rows: number } | null;
  savingContract?: boolean;
  /**
   * Last Validate attempt failed before a verdict (504 / hung control plane).
   * Status is Failed — never leave the rail on Not run after an operator click.
   */
  preflightError?: string;
  /** Extra execute block (e.g. non-CDC multi-stream). */
  executeBlocked?: boolean;
  executeBlockedReason?: string;
  /**
   * Route a previous completed run wrote, when the plan has since been
   * retargeted. Stated here because Validate is where the operator stands when
   * the retarget happens — the run they remember answers for another table.
   */
  supersededRunLabel?: string;
  /** CDC retention Check control (SQL Server / Oracle) — shown above footer when present. */
  cdcRetentionSlot?: ReactNode;
  /** Bind a signed data contract before Run (plans + schedules persist this). */
  contractSlot?: ReactNode;
  /** Primary remediation for the top blocker (e.g. open identity settings). */
  onPrimaryFix?: () => void;
  primaryFixLabel?: string;
  onBack: () => void;
  onRunPreflight: () => void;
  onApproveMappings: () => void;
  /** Open Map filtered to Accept-risk rows. */
  onOpenMapForRisk?: () => void;
  /** Sign holdout Risk Contracts here and re-validate — no trip back to Map. */
  onHoldOutRows?: () => void;
  holdingOutRows?: boolean;
  onExecute: () => void;
  onOpenJobTheater: () => void;
  onSaveAsContract?: () => void;
}

/**
 * Validate step bottom action bar — same pattern as Source / Dest / Map.
 * Status + blockers summary live in the footer; detail stays in ValidateDashboard.
 */
export function ValidateActionsRail({
  preflight,
  fileExport = false,
  preflighting,
  transferring,
  mappingReviewCount,
  riskAckPendingCount = 0,
  rowCount,
  transferLaunch,
  preflightError = "",
  savingContract,
  executeBlocked = false,
  executeBlockedReason,
  supersededRunLabel,
  cdcRetentionSlot,
  contractSlot,
  onPrimaryFix,
  primaryFixLabel,
  onBack,
  onRunPreflight,
  onApproveMappings,
  onOpenMapForRisk,
  onHoldOutRows,
  holdingOutRows,
  onExecute,
  onOpenJobTheater,
  onSaveAsContract,
}: ValidateActionsRailProps) {
  const passed = preflight?.passed;
  const blocked = preflight && !preflight.passed && !preflighting;
  const transportFailed = Boolean(preflightError) && !preflight && !preflighting;
  const mappingBlocked = preflight?.blockers.some((b) => b.id.includes("mapping"));
  const decision = preflight?.proof_bundle?.transfer_decision?.decision
    ?? (preflight ? "review" : "pending");
  const reviewGrade = Boolean(passed && decision === "review");
  const executeDisabled = transferring || !passed || reviewGrade || executeBlocked;
  const studioPrimary = resolveValidateStudioPrimary({
    preflight,
    preflighting,
    transferring,
    mappingReviewCount,
    riskAckPendingCount,
    transferLaunch,
    executeBlocked,
    hasPrimaryFix: Boolean(onPrimaryFix && primaryFixLabel),
    primaryFixLabel,
    hasHoldOut: Boolean(onHoldOutRows),
  });
  const isPrimary = (kind: typeof studioPrimary.kind) => studioPrimary.kind === kind;
  const executiveSummary = useMemo(
    () => buildExecutiveSummary(preflight, undefined, { fileExport }),
    [preflight, fileExport],
  );
  const displayBlockers = useMemo(
    () => (preflight ? buildDisplayBlockers(preflight) : []),
    [preflight],
  );
  const firstBlocker = displayBlockers[0];
  const firstBlockerMessage = firstBlocker?.impact || firstBlocker?.message;

  const statusLabel = preflighting
    ? "Validating…"
    : transferLaunch
      ? "Transfer started"
      : transportFailed
        ? "Failed"
        : !preflight
          ? "Not run"
          : reviewGrade
            ? "Needs review"
            : passed
              ? "Ready"
              : "Blocked";

  const statusDetail = transferLaunch
    ? `Job queued · ${plural(transferLaunch.rows, "row")}`
    : preflighting
      ? "Checking the route"
      : transportFailed
        ? "Validate did not finish"
        : preflight
          ? (executiveSummary?.railLine
            ?? `${preflight.passed_count} of ${plural(preflight.total_gates, "gate")} passed`)
          : "Run Validate to unlock Run";

  // Why Run transfer is disabled, said once beside it (never only in a tooltip).
  const runReason = !preflight || preflighting || transferLaunch || !studioPrimary.executeIsPrimary
    ? ""
    : executeBlocked
      ? (executeBlockedReason || "Run is blocked for this route")
      : reviewGrade
        ? "Validate returned review: resolve it, then re-run"
        : !passed
          ? (firstBlocker?.title || "Resolve the blocked gates, then re-run")
          : "";

  const runProps = {
    onClick: onExecute,
    disabled: executeDisabled,
    title: rowCount != null && !executeDisabled ? `Run transfer · ${plural(rowCount, "row")}` : undefined,
    leadingIcon: <DtIcon name="play" size={16} />,
  };
  const runLabel = studioPrimary.executeLabel ?? "Run transfer";
  const runButton = !preflight ? null : studioPrimary.executeIsPrimary ? (
    <StepPrimaryButton
      key="run"
      {...runProps}
      arrow={false}
      busy={transferring}
      loadingLabel="Starting…"
      data-studio-primary="true"
      className="df2-validate-studio-primary"
    >
      {runLabel}
    </StepPrimaryButton>
  ) : (
    <Button key="run" variant="ghost" {...runProps} data-studio-primary="false">
      {runLabel}
    </Button>
  );

  // Secondaries first, the one primary last (rightmost), as on every step.
  const actions: Array<{ key: string; primary: boolean; node: ReactNode }> = [];
  if (preflight && onSaveAsContract) {
    actions.push({
      key: "contract",
      primary: false,
      node: (
        <Button
          key="contract"
          variant="ghost"
          onClick={onSaveAsContract}
          loading={savingContract}
          loadingLabel="Saving…"
          disabled={savingContract || preflighting}
          leadingIcon={<DtIcon name="contract" size={16} />}
          title="Save these mappings and gates as a draft data contract"
        >
          Save contract
        </Button>
      ),
    });
  }
  // Available in every state: a green verdict ages the moment the source,
  // destination or mappings move, so re-running the same governed gates must
  // not require a trip back through Map.
  actions.push({
    key: "validate",
    primary: isPrimary("run_preflight"),
    node: (
      <Button
        key="validate"
        variant={isPrimary("run_preflight") ? "primary" : "ghost"}
        onClick={onRunPreflight}
        loading={preflighting}
        loadingLabel="Validating…"
        leadingIcon={<DtIcon name="refresh" size={16} />}
        data-studio-primary={isPrimary("run_preflight") ? "true" : "false"}
        className={isPrimary("run_preflight") ? "df2-validate-studio-primary df2-step-primary" : undefined}
        title={preflight ? "Discard this verdict and run the same checks again" : undefined}
      >
        {!preflight && !transportFailed ? "Run Validate" : "Re-run Validate"}
      </Button>
    ),
  });
  if (blocked && onPrimaryFix && primaryFixLabel) {
    const fixIcon = /map/i.test(primaryFixLabel)
      ? "map"
      : /bad data|strip|quarantine/i.test(primaryFixLabel) ? "inspect" : "sliders";
    actions.push({
      key: "fix",
      primary: isPrimary("primary_fix"),
      node: (
        <Button
          key="fix"
          variant={isPrimary("primary_fix") ? "primary" : "ghost"}
          onClick={onPrimaryFix}
          leadingIcon={<DtIcon name={fixIcon} size={16} />}
          title={primaryFixLabel}
          data-studio-primary={isPrimary("primary_fix") ? "true" : "false"}
          className={isPrimary("primary_fix") ? "df2-validate-studio-primary df2-step-primary" : undefined}
        >
          <span className="df2-validate-studio-primary-label">{primaryFixLabel}</span>
        </Button>
      ),
    });
  }
  if (blocked && riskAckPendingCount > 0 && !onPrimaryFix) {
    if (onHoldOutRows) {
      actions.push({
        key: "hold",
        primary: isPrimary("hold_out"),
        node: (
          <Button
            key="hold"
            variant={isPrimary("hold_out") ? "primary" : "ghost"}
            onClick={onHoldOutRows}
            loading={holdingOutRows}
            loadingLabel="Signing…"
            leadingIcon={<DtIcon name="contract" size={16} />}
            data-studio-primary={isPrimary("hold_out") ? "true" : "false"}
            className={isPrimary("hold_out") ? "df2-step-primary" : undefined}
            title={`Hold out failing rows in ${plural(riskAckPendingCount, "column")} and re-validate. Held rows go to quarantine for replay.`}
          >
            Run with rows held out
          </Button>
        ),
      });
    }
    actions.push({
      key: "policy",
      primary: isPrimary("choose_policy"),
      node: (
        <Button
          key="policy"
          variant={isPrimary("choose_policy") ? "primary" : "ghost"}
          onClick={onOpenMapForRisk || onBack}
          leadingIcon={<DtIcon name="map" size={16} />}
          data-studio-primary={isPrimary("choose_policy") ? "true" : "false"}
          className={isPrimary("choose_policy") ? "df2-step-primary" : undefined}
          title="Choose a per-column policy on Map. Approvals are kept."
        >
          Choose policy on Map
        </Button>
      ),
    });
  }
  if (blocked && mappingBlocked && mappingReviewCount > 0 && riskAckPendingCount === 0 && !onPrimaryFix) {
    actions.push({
      key: "approve",
      primary: isPrimary("approve_mappings"),
      node: (
        <Button
          key="approve"
          variant={isPrimary("approve_mappings") ? "primary" : "ghost"}
          onClick={onApproveMappings}
          leadingIcon={<DtIcon name="check" size={16} />}
          data-studio-primary={isPrimary("approve_mappings") ? "true" : "false"}
          className={isPrimary("approve_mappings") ? "df2-step-primary" : undefined}
        >
          Approve mappings
        </Button>
      ),
    });
  }
  if (runButton) actions.push({ key: "run", primary: studioPrimary.executeIsPrimary, node: runButton });
  const ordered = [...actions.filter((a) => !a.primary), ...actions.filter((a) => a.primary)];

  return (
    <>
      {cdcRetentionSlot ? (
        <div className="df2-validate-footer-cdc" aria-label="CDC retention">
          {cdcRetentionSlot}
        </div>
      ) : null}
      {contractSlot ? (
        <div className="df2-validate-footer-contract" aria-label="Data contract">
          {contractSlot}
        </div>
      ) : null}

      <div className="df2-card-footer df2-wizard-footer df2-validate-footer df2-step-footer" aria-label="Validation actions">
        <StepBackButton onClick={onBack} />

        <div className="df2-validate-footer-status" aria-live="polite">
          <span className={passed && !reviewGrade ? "is-ok" : blocked || reviewGrade || transportFailed ? "is-warn" : undefined}>
            <strong>Validate</strong> {statusLabel}
          </span>
          <span title={firstBlockerMessage || undefined}>{statusDetail}</span>
          {supersededRunLabel && !transferLaunch && (
            <span className="is-warn">
              Last run wrote {supersededRunLabel}. Nothing written for this route yet.
            </span>
          )}
        </div>

        <div className="df2-validate-footer-actions" data-studio-primary-kind={studioPrimary.kind}>
          {transferLaunch ? (
            <StepPrimaryButton
              onClick={onOpenJobTheater}
              arrow={false}
              leadingIcon={<DtIcon name="live" size={16} />}
              data-studio-primary="true"
            >
              Open live progress
            </StepPrimaryButton>
          ) : (
            <>
              {runReason ? <StepFooterReason>{runReason}</StepFooterReason> : null}
              {ordered.map((a) => a.node)}
            </>
          )}
        </div>
      </div>
    </>
  );
}
