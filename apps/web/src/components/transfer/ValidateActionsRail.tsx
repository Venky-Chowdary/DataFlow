import type { ReactNode } from "react";
import { useMemo } from "react";
import { DtIcon } from "../DtIcon";
import { Button } from "../ui/Button";
import type { PreflightResult } from "../../lib/types";
import { buildDisplayBlockers, buildExecutiveSummary } from "../../lib/validateIssueGrouping";

interface ValidateActionsRailProps {
  preflight: PreflightResult | null;
  preflighting: boolean;
  transferring: boolean;
  mappingReviewCount: number;
  rowCount?: number;
  transferLaunch?: { jobId: string; rows: number } | null;
  savingContract?: boolean;
  /** Extra execute block (e.g. non-CDC multi-stream). */
  executeBlocked?: boolean;
  executeBlockedReason?: string;
  /** CDC retention Check control (SQL Server / Oracle) — shown above footer when present. */
  cdcRetentionSlot?: ReactNode;
  /** Primary remediation for the top blocker (e.g. open identity settings). */
  onPrimaryFix?: () => void;
  primaryFixLabel?: string;
  onBack: () => void;
  onRunPreflight: () => void;
  onApproveMappings: () => void;
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
  rowCount,
  transferLaunch,
  savingContract,
  executeBlocked = false,
  executeBlockedReason,
  cdcRetentionSlot,
  onPrimaryFix,
  primaryFixLabel,
  onBack,
  onRunPreflight,
  onApproveMappings,
  onExecute,
  onOpenJobTheater,
  onSaveAsContract,
}: ValidateActionsRailProps) {
  const passed = preflight?.passed;
  const blocked = preflight && !preflight.passed && !preflighting;
  const mappingBlocked = preflight?.blockers.some((b) => b.id.includes("mapping"));
  const decision = preflight?.proof_bundle?.transfer_decision?.decision
    ?? (passed ? "approve" : preflight ? "review" : "pending");
  const reviewGrade = Boolean(passed && decision === "review");
  const executeDisabled = transferring || !passed || reviewGrade || executeBlocked;
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

      <div className="df2-card-footer df2-wizard-footer df2-validate-footer" aria-label="Validation actions">
        <Button onClick={onBack} leadingIcon={<DtIcon name="chevron-left" size={16} />}>
          Back
        </Button>

        <div className="df2-validate-footer-status" aria-live="polite">
          <span className={passed && !reviewGrade ? "is-ok" : blocked || reviewGrade ? "is-warn" : undefined}>
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
        </div>

        <div className="df2-validate-footer-actions">
          {transferLaunch ? (
            <Button
              variant="primary"
              onClick={onOpenJobTheater}
              leadingIcon={<DtIcon name="activity" size={14} />}
            >
              Open live progress
            </Button>
          ) : (
            <>
              {(blocked || (!preflight && !preflighting)) && (
                <Button
                  variant={!preflight ? "primary" : "secondary"}
                  onClick={onRunPreflight}
                  loading={preflighting}
                  leadingIcon={<DtIcon name="gate" size={16} />}
                >
                  {!preflight ? "Run preflight" : "Re-run"}
                </Button>
              )}

              {blocked && onPrimaryFix && primaryFixLabel && (
                <Button
                  variant="secondary"
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
                >
                  {primaryFixLabel.length > 28
                    ? `${primaryFixLabel.slice(0, 26)}…`
                    : primaryFixLabel}
                </Button>
              )}

              {blocked && mappingBlocked && mappingReviewCount > 0 && (
                <Button
                  variant="secondary"
                  onClick={onApproveMappings}
                  leadingIcon={<DtIcon name="check" size={16} />}
                >
                  Approve mappings
                </Button>
              )}

              {preflight && (
                <Button
                  variant={blocked || reviewGrade ? "secondary" : "primary"}
                  onClick={onExecute}
                  loading={transferring}
                  loadingLabel="Starting…"
                  disabled={executeDisabled}
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
                  {executeBlocked || !passed
                    ? "Execute (blocked)"
                    : reviewGrade
                      ? "Execute (review)"
                      : "Execute"}
                </Button>
              )}

              {preflight && onSaveAsContract && (
                <Button
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
