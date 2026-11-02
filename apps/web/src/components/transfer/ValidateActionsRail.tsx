import type { ReactNode } from "react";
import { useMemo } from "react";
import { DtIcon } from "../DtIcon";
import { Button } from "../ui/Button";
import type { PreflightResult } from "../../lib/types";
import { buildDisplayBlockers, buildExecutiveSummary } from "../../lib/validateIssueGrouping";
import { resolveValidateStudioPrimary } from "../../lib/validateStudioPrimary";

interface ValidateActionsRailProps {
  preflight: PreflightResult | null;
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
  /** Bind a signed data contract before Execute (plans + schedules persist this). */
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
  const executiveSummary = useMemo(() => buildExecutiveSummary(preflight), [preflight]);
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
            ? "Review-grade"
            : passed
              ? "Ready"
              : "Blocked";

  const statusDetail = transferLaunch
    ? `Job queued · ${transferLaunch.rows.toLocaleString()} rows`
    : preflighting
      ? "Gates running"
      : transportFailed
        ? preflightError
        : preflight
          ? (executiveSummary?.railLine
            ?? `${preflight.passed_count}/${preflight.total_gates} gates · ${preflight.readiness_score}%`)
          : "Run preflight to unlock Execute";

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

      <div className="df2-card-footer df2-wizard-footer df2-validate-footer" aria-label="Validation actions">
        <Button onClick={onBack} leadingIcon={<DtIcon name="chevron-left" size={16} />}>
          Back
        </Button>

        <div className="df2-validate-footer-status" aria-live="polite">
          <span className={passed && !reviewGrade ? "is-ok" : blocked || reviewGrade || transportFailed ? "is-warn" : undefined}>
            <strong>Validate</strong> {statusLabel}
          </span>
          <span title={firstBlockerMessage || undefined}>{statusDetail}</span>
          {blocked && firstBlocker && (
            <span className="is-warn" title={firstBlockerMessage || undefined}>
              <strong>{firstBlocker.title}</strong>
            </span>
          )}
          {executeBlocked && executeBlockedReason && (
            <span className="is-warn" role="alert">{executeBlockedReason}</span>
          )}
          {supersededRunLabel && !transferLaunch && (
            <span className="is-warn">
              Last run wrote {supersededRunLabel} — nothing written for this route yet
            </span>
          )}
        </div>

        <div className="df2-validate-footer-actions" data-studio-primary-kind={studioPrimary.kind}>
          {transferLaunch ? (
            <Button
              variant="primary"
              onClick={onOpenJobTheater}
              leadingIcon={<DtIcon name="activity" size={14} />}
              data-studio-primary="true"
            >
              Open live progress
            </Button>
          ) : (
            <>
              {/* Available in every state: a green verdict ages the moment the
                  source, destination or mappings move, so re-running the same
                  governed gates must not require a trip back through Map. */}
              <Button
                variant={isPrimary("run_preflight") ? "primary" : "ghost"}
                onClick={onRunPreflight}
                loading={preflighting}
                leadingIcon={<DtIcon name="gate" size={16} />}
                data-studio-primary={isPrimary("run_preflight") ? "true" : "false"}
                className={isPrimary("run_preflight") ? "df2-validate-studio-primary" : undefined}
                title={
                  preflight
                    ? "Discard this verdict and re-run the same API gates — acknowledgments and Risk Contracts still apply"
                    : transportFailed
                      ? "Last Validate did not finish — re-run the same API gates"
                      : "Run API preflight gates"
                }
              >
                {!preflight && !transportFailed ? "Run preflight" : "Re-run Validate"}
              </Button>

              {blocked && onPrimaryFix && primaryFixLabel && (
                <Button
                  variant={isPrimary("primary_fix") ? "primary" : "ghost"}
                  onClick={onPrimaryFix}
                  leadingIcon={
                    <DtIcon
                      name={
                        /bad data|strip|quarantine|map/i.test(primaryFixLabel)
                          ? (/map/i.test(primaryFixLabel) ? "layers" : "shield")
                          : "settings"
                      }
                      size={16}
                    />
                  }
                  title={primaryFixLabel}
                  data-studio-primary={isPrimary("primary_fix") ? "true" : "false"}
                  className={isPrimary("primary_fix") ? "df2-validate-studio-primary" : undefined}
                >
                  <span className="df2-validate-studio-primary-label">{primaryFixLabel}</span>
                </Button>
              )}

              {blocked && riskAckPendingCount > 0 && !onPrimaryFix && (
                <>
                  {onHoldOutRows && (
                    <Button
                      variant={isPrimary("hold_out") ? "primary" : "ghost"}
                      onClick={onHoldOutRows}
                      loading={holdingOutRows}
                      loadingLabel="Signing…"
                      leadingIcon={<DtIcon name="shield" size={16} />}
                      data-studio-primary={isPrimary("hold_out") ? "true" : "false"}
                      title={`Sign a quarantine Risk Contract for ${riskAckPendingCount} column(s) and re-validate here. Failing rows go to quarantine for replay — nothing is written lossily.`}
                    >
                      Run with rows held out
                    </Button>
                  )}
                  <Button
                    variant={isPrimary("choose_policy") ? "primary" : "ghost"}
                    onClick={onOpenMapForRisk || onBack}
                    leadingIcon={<DtIcon name="layers" size={16} />}
                    data-studio-primary={isPrimary("choose_policy") ? "true" : "false"}
                    title="Choose a per-column execution policy on Map — approvals are preserved"
                  >
                    Choose policy on Map
                  </Button>
                </>
              )}

              {blocked
                && mappingBlocked
                && mappingReviewCount > 0
                && riskAckPendingCount === 0
                && !onPrimaryFix && (
                <Button
                  variant={isPrimary("approve_mappings") ? "primary" : "ghost"}
                  onClick={onApproveMappings}
                  leadingIcon={<DtIcon name="check" size={16} />}
                  data-studio-primary={isPrimary("approve_mappings") ? "true" : "false"}
                >
                  Approve mappings
                </Button>
              )}

              {preflight && (
                <Button
                  variant={studioPrimary.executeIsPrimary ? "primary" : "ghost"}
                  onClick={onExecute}
                  loading={transferring}
                  loadingLabel="Starting…"
                  disabled={executeDisabled}
                  data-studio-primary={studioPrimary.executeIsPrimary ? "true" : "false"}
                  className={studioPrimary.executeIsPrimary ? "df2-validate-studio-primary" : undefined}
                  title={
                    executeBlocked
                      ? (executeBlockedReason || "Execution blocked")
                      : reviewGrade
                        ? "Review-grade / local preflight — confirm API Validate before Execute"
                        : !passed
                          ? `Blocked: ${firstBlockerMessage || "Resolve failed checks and re-run preflight"}`
                          : rowCount != null
                            ? `Execute transfer · ${rowCount.toLocaleString()} rows`
                            : "Execute transfer"
                  }
                  leadingIcon={<DtIcon name="arrow-right" size={16} />}
                >
                  {studioPrimary.executeLabel
                    ?? (executeBlocked || !passed
                      ? "Execute (blocked)"
                      : reviewGrade
                        ? "Execute (review)"
                        : "Execute")}
                </Button>
              )}

              {preflight && onSaveAsContract && (
                <Button
                  variant="ghost"
                  onClick={onSaveAsContract}
                  loading={savingContract}
                  loadingLabel="Saving…"
                  disabled={savingContract || preflighting}
                  leadingIcon={<DtIcon name="shield" size={16} />}
                  title="Save mappings + gates as a draft data contract under Contracts"
                >
                  Save contract
                </Button>
              )}
            </>
          )}
        </div>
      </div>
    </>
  );
}
