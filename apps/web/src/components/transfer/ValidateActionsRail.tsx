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
                    {preflight.blockers.slice(0, 4).map((b) => (
                      <li key={b.id}>
                        {b.message}
                        {b.guidance?.fix && <span className="df2-validate-rail-fix">Fix: {b.guidance.fix}</span>}
                      </li>
                    ))}
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
            disabled={transferring || !passed}
            title={
              !passed
                ? `Blocked: ${firstBlockerMessage || "Resolve failed checks and re-run preflight"}${firstBlockerFix ? ` — Fix: ${firstBlockerFix}` : ""}`
                : undefined
            }
            leadingIcon={<DtIcon name="arrow-right" size={16} />}
          >
            {passed
              ? `Execute${rowCount != null ? ` · ${rowCount.toLocaleString()}` : ""}`
              : "Execute (blocked)"}
          </Button>
        )}

        {preflight && onSaveAsContract && (
          <Button
            onClick={onSaveAsContract}
            loading={savingContract}
            loadingLabel="Saving…"
            disabled={savingContract || preflighting}
            leadingIcon={<DtIcon name="shield" size={16} />}
          >
            Save as contract
          </Button>
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
