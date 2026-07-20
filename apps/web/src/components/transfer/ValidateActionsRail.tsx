import type { ReactNode } from "react";
import { DtIcon } from "../DtIcon";
import { Button } from "../ui/Button";
import type { PreflightResult } from "../../lib/types";

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
  onBack: () => void;
  onRunPreflight: () => void;
  onApproveMappings: () => void;
  onExecute: () => void;
  onOpenJobTheater: () => void;
  onSaveAsContract?: () => void;
}

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
  const proofDecision = preflight?.proof_bundle?.transfer_decision?.decision || "approve";
  const proofReason = preflight?.proof_bundle?.transfer_decision?.reason || "No blocking issues detected";
  const executeDisabled = transferring || !passed || executeBlocked;
  const confidenceBand = preflight?.proof_bundle?.confidence_band?.toUpperCase() || "MEDIUM";
  const qualityGrade = preflight?.proof_bundle?.quality_grade?.toUpperCase() || "GOOD";
  const proofWarnings = preflight?.proof_bundle?.transfer_decision?.warnings || [];
  const firstBlocker = preflight?.blockers?.[0];
  const firstBlockerMessage = firstBlocker?.message;
  const firstBlockerFix = firstBlocker?.guidance?.fix;

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
          <div className="df2-validate-rail-panel df2-validate-status live">
            <div className="df2-validate-rail-score">
              <strong>…</strong>
              <span>validating</span>
            </div>
            <p>Safety gates are running. Actions unlock when checks finish.</p>
          </div>
        )}

        {preflight && !preflighting && (
          <div className={`df2-validate-rail-panel df2-validate-status${passed ? " passed" : " blocked"}`}>
            <div className="df2-validate-rail-score">
              <strong>{preflight.readiness_score}%</strong>
              <span>readiness</span>
            </div>
            <p>
              <strong>{preflight.passed_count}/{preflight.total_gates}</strong> checks · {proofDecision.toUpperCase()}
            </p>
            {passed && (
              <p className="df2-validate-rail-hint">
                Review every gate card on the left — rules, duration, and blockers — then Execute.
              </p>
            )}
            {preflight.run_id && (
              <p className="df2-validate-rail-runid" title="Paste into Data Pilot to triage this validation">
                Run <code>{preflight.run_id}</code>
              </p>
            )}

            {(preflight.proof_bundle || preflight.blockers.length > 0) && (
              <div className="df2-validate-rail-details-body">
                {preflight.proof_bundle && (
                  <div className="df2-validate-rail-metrics">
                    <span className="df2-validate-rail-metric">
                      <small>Confidence</small>
                      <strong>{confidenceBand}</strong>
                    </span>
                    <span className="df2-validate-rail-metric">
                      <small>Quality</small>
                      <strong>{qualityGrade}</strong>
                    </span>
                    <span className="df2-validate-rail-metric">
                      <small>Semantic</small>
                      <strong>{preflight.proof_bundle.semantic_mapping_score?.toFixed(2) ?? "—"}</strong>
                    </span>
                    <span className="df2-validate-rail-metric">
                      <small>Compliance</small>
                      <strong>{preflight.proof_bundle.compliance?.risk_score?.toFixed(2) ?? "—"}</strong>
                    </span>
                  </div>
                )}
                {preflight.blockers.length > 0 && (
                  <ul className="df2-validate-rail-blockers">
                    {preflight.blockers.slice(0, 4).map((b) => {
                      const details = b.details || {};
                      const issueTexts = Array.isArray(details.issue_texts)
                        ? (details.issue_texts as string[])
                        : Array.isArray(details.errors)
                          ? (details.errors as unknown[]).map((e) =>
                              typeof e === "string" ? e : String((e as { message?: string })?.message ?? e),
                            )
                          : [];
                      return (
                        <li key={b.id}>
                          {b.message}
                          {issueTexts.slice(0, 3).map((issue) => (
                            <span key={issue} className="df2-validate-rail-issue">{issue}</span>
                          ))}
                          {b.guidance?.fix && <span className="df2-validate-rail-fix">Fix: {b.guidance.fix}</span>}
                        </li>
                      );
                    })}
                  </ul>
                )}
                {proofDecision === "review" && (proofWarnings.length > 0 || proofReason) && (
                  <div className="df2-validate-rail-review">
                    <strong><DtIcon name="shield" size={14} /> Review required</strong>
                    <p>{proofReason}</p>
                    {proofWarnings.length > 0 && (
                      <ul>
                        {proofWarnings.map((w, i) => (
                          <li key={i}>{w}</li>
                        ))}
                      </ul>
                    )}
                  </div>
                )}
              </div>
            )}
          </div>
        )}
      </div>

      <div className="df2-validate-rail-actions">
        <Button onClick={onBack} leadingIcon={<DtIcon name="chevron-left" size={16} />}>
          Back
        </Button>

        {!preflight && !preflighting && (
          <Button
            variant="primary"
            onClick={onRunPreflight}
            leadingIcon={<DtIcon name="gate" size={16} />}
          >
            Run preflight
          </Button>
        )}

        {blocked && (
          <Button
            onClick={onRunPreflight}
            loading={preflighting}
            leadingIcon={<DtIcon name="gate" size={16} />}
          >
            Re-run
          </Button>
        )}

        {blocked && mappingBlocked && mappingReviewCount > 0 && (
          <Button
            variant="primary"
            onClick={onApproveMappings}
            leadingIcon={<DtIcon name="check" size={16} />}
          >
            Approve mappings
          </Button>
        )}

        {preflight && !transferLaunch && (
          <Button
            variant="primary"
            onClick={onExecute}
            loading={transferring}
            loadingLabel="Starting…"
            disabled={executeDisabled}
            title={
              executeBlocked
                ? (executeBlockedReason || "Execution blocked")
                : !passed
                  ? `Blocked: ${firstBlockerMessage || "Resolve failed checks and re-run preflight"}${firstBlockerFix ? ` — Fix: ${firstBlockerFix}` : ""}`
                  : undefined
            }
            leadingIcon={<DtIcon name="arrow-right" size={16} />}
          >
            {executeBlocked
              ? "Execute (blocked)"
              : passed
                ? `Execute transfer${rowCount != null ? ` · ${rowCount.toLocaleString()} rows` : ""}`
                : "Execute (blocked)"}
          </Button>
        )}
        {passed && !transferLaunch && !executeBlocked && (
          <p className="df2-validate-rail-explain">
            Execute starts the write. You stay on Validate until you choose to run.
          </p>
        )}
        {executeBlocked && executeBlockedReason && (
          <p className="df2-validate-rail-explain" role="alert">
            {executeBlockedReason}
          </p>
        )}

        {preflight && onSaveAsContract && (
          <>
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
            <p className="df2-validate-rail-contract-hint">
              Saves a draft schema agreement to <strong>Contracts</strong> (sidebar). Works even while Validate is blocked.
            </p>
          </>
        )}

        {blocked && firstBlockerMessage && (
          <p className="df2-validate-rail-explain" title={firstBlockerMessage}>
            Blocked: {firstBlockerMessage}
          </p>
        )}
      </div>
    </aside>
  );
}
