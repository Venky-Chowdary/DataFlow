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
  /** CDC retention Check control (SQL Server / Oracle). */
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
 * Validate step action rail — actions first, minimal status.
 * Detailed blockers / proof chips live in ValidateDashboard (left column).
 * Do not re-render the same gates, metrics, and captions here.
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
  const scoreLabel = reviewGrade ? "review" : passed ? "ready" : "blocked";
  const scoreTone = reviewGrade ? "review" : passed ? "passed" : "blocked";
  const executeDisabled = transferring || !passed || reviewGrade || executeBlocked;
  const executiveSummary = useMemo(() => buildExecutiveSummary(preflight), [preflight]);
  const displayBlockers = useMemo(
    () => (preflight ? buildDisplayBlockers(preflight) : []),
    [preflight],
  );
  const firstBlocker = displayBlockers[0];
  const firstBlockerMessage = firstBlocker?.impact || firstBlocker?.message;

  return (
    <aside className="df2-validate-rail" aria-label="Validation actions">
      <div className="df2-validate-rail-scroll">
        {transferLaunch ? (
          <div className="df2-validate-rail-panel df2-validate-launch">
            <DtIcon name="transfer" size={18} />
            <strong>Transfer started</strong>
            <p>Job queued — {transferLaunch.rows.toLocaleString()} rows.</p>
            <Button
              variant="primary"
              onClick={onOpenJobTheater}
              leadingIcon={<DtIcon name="activity" size={14} />}
            >
              Open live progress
            </Button>
          </div>
        ) : null}

        {cdcRetentionSlot ? (
          <div className="df2-validate-rail-panel" aria-label="CDC retention">
            {cdcRetentionSlot}
          </div>
        ) : null}

        {preflighting && (
          <div className="df2-validate-rail-panel df2-validate-status df2-validate-rail-scorecard live">
            <div className="df2-validate-rail-score">
              <strong>…</strong>
              <span>validating</span>
            </div>
            <p>Gates running — actions unlock when checks finish.</p>
          </div>
        )}

        {preflight && !preflighting && (
          <div
            className={`df2-validate-rail-panel df2-validate-status df2-validate-rail-scorecard df2-validate-rail-compact ${scoreTone}`}
          >
            <div className="df2-validate-rail-score">
              <strong>{preflight.readiness_score}%</strong>
              <span>{scoreLabel}</span>
            </div>
            <p className="df2-validate-rail-outcome">
              {executiveSummary?.railLine
                ?? `${preflight.passed_count}/${preflight.total_gates} gates · ${passed ? "PASS" : "BLOCK"}`}
            </p>
            {blocked && firstBlocker && (
              <p className="df2-validate-rail-top-blocker" title={firstBlockerMessage || undefined}>
                <strong>{firstBlocker.title}</strong>
                {firstBlockerMessage ? ` · ${firstBlockerMessage}` : null}
              </p>
            )}
            {preflight.run_id && (
              <p className="df2-validate-rail-runid" title="Paste into Data Pilot to triage this validation">
                Run <code>{preflight.run_id}</code>
              </p>
            )}
          </div>
        )}
      </div>

      <div className="df2-validate-rail-actions">
        <div className="df2-validate-rail-actions-row">
          <Button onClick={onBack} leadingIcon={<DtIcon name="chevron-left" size={16} />}>
            Back
          </Button>

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
        </div>

        {blocked && onPrimaryFix && primaryFixLabel && (
          <Button
            variant="primary"
            className="df2-validate-rail-primary-fix"
            onClick={onPrimaryFix}
            leadingIcon={
              <DtIcon
                name={/bad data|strip|quarantine|map/i.test(primaryFixLabel) ? (/map/i.test(primaryFixLabel) ? "layers" : "shield") : "settings"}
                size={16}
              />
            }
            title={primaryFixLabel}
          >
            {primaryFixLabel.length > 32
              ? `${primaryFixLabel.slice(0, 30)}…`
              : primaryFixLabel}
          </Button>
        )}

        {blocked && mappingBlocked && mappingReviewCount > 0 && (
          <Button
            variant={onPrimaryFix ? "secondary" : "primary"}
            onClick={onApproveMappings}
            leadingIcon={<DtIcon name="check" size={16} />}
          >
            Approve mappings
          </Button>
        )}

        {preflight && !transferLaunch && (
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
            Save as contract
          </Button>
        )}

        {reviewGrade && !transferLaunch && !executeBlocked && (
          <p className="df2-validate-rail-explain" role="status">
            Review-grade result — re-run API Validate before Execute unlocks the write.
          </p>
        )}
        {passed && !reviewGrade && !transferLaunch && !executeBlocked && (
          <p className="df2-validate-rail-explain is-ok">
            Execute starts the write and opens Job Theater for live progress.
          </p>
        )}
        {executeBlocked && executeBlockedReason && (
          <p className="df2-validate-rail-explain" role="alert">
            {executeBlockedReason}
          </p>
        )}
      </div>
    </aside>
  );
}
