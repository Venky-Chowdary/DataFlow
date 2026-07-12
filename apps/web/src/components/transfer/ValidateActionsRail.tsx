import { DtIcon } from "../DtIcon";
import { ButtonLoader } from "../LoadingState";
import type { PreflightResult } from "../../lib/types";

interface ValidateActionsRailProps {
  preflight: PreflightResult | null;
  preflighting: boolean;
  transferring: boolean;
  mappingReviewCount: number;
  rowCount?: number;
  transferLaunch?: { jobId: string; rows: number } | null;
  onBack: () => void;
  onRunPreflight: () => void;
  onApproveMappings: () => void;
  onExecute: () => void;
  onOpenJobTheater: () => void;
}

export function ValidateActionsRail({
  preflight,
  preflighting,
  transferring,
  mappingReviewCount,
  rowCount,
  transferLaunch,
  onBack,
  onRunPreflight,
  onApproveMappings,
  onExecute,
  onOpenJobTheater,
}: ValidateActionsRailProps) {
  const passed = preflight?.passed;
  const blocked = preflight && !preflight.passed && !preflighting;
  const mappingBlocked = preflight?.blockers.some((b) => b.id.includes("mapping"));
  const proofDecision = preflight?.proof_bundle?.transfer_decision?.decision || "approve";
  const proofReason = preflight?.proof_bundle?.transfer_decision?.reason || "No blocking issues detected";
  const confidenceBand = preflight?.proof_bundle?.confidence_band?.toUpperCase() || "MEDIUM";
  const qualityGrade = preflight?.proof_bundle?.quality_grade?.toUpperCase() || "GOOD";
  const evidenceSummary = preflight?.proof_bundle?.evidence_summary || "Deterministic proof signals ready for operator review.";
  const proofWarnings = preflight?.proof_bundle?.transfer_decision?.warnings || [];
  const firstBlocker = preflight?.blockers?.[0]?.message;

  return (
    <aside className="df2-validate-rail" aria-label="Validation actions">
      {transferLaunch ? (
        <div className="df2-validate-rail-panel df2-validate-launch">
          <DtIcon name="transfer" size={20} />
          <strong>Transfer started</strong>
          <p>
            Job queued — {transferLaunch.rows.toLocaleString()} rows heading to destination.
          </p>
          <button type="button" className="df2-btn df2-btn-primary" onClick={onOpenJobTheater}>
            <DtIcon name="activity" size={16} /> Open live progress
          </button>
        </div>
      ) : null}

      {preflight && !preflighting && (
        <div className={`df2-validate-rail-panel df2-validate-status${passed ? " passed" : " blocked"}`}>
          <div className="df2-validate-rail-score">
            <strong>{preflight.readiness_score}%</strong>
            <span>readiness</span>
          </div>
          <p>
            <strong>{preflight.passed_count}/{preflight.total_gates}</strong> checks passed
          </p>
          {preflight.proof_bundle && (
            <div className="df2-validate-rail-metrics">
              <span className="df2-validate-rail-metric">
                <small>Proof decision</small>
                <strong>{proofDecision.toUpperCase()}</strong>
              </span>
              <span className="df2-validate-rail-metric">
                <small>Confidence</small>
                <strong>{confidenceBand}</strong>
              </span>
              <span className="df2-validate-rail-metric">
                <small>Quality grade</small>
                <strong>{qualityGrade}</strong>
              </span>
              <span className="df2-validate-rail-metric">
                <small>Semantic confidence</small>
                <strong>{preflight.proof_bundle.semantic_mapping_score.toFixed(2)}</strong>
              </span>
              <span className="df2-validate-rail-metric">
                <small>Sample quality</small>
                <strong>{preflight.proof_bundle.quality_score.toFixed(2)}</strong>
              </span>
              <span className="df2-validate-rail-metric">
                <small>Compliance risk</small>
                <strong>{preflight.proof_bundle.compliance.risk_score.toFixed(2)}</strong>
              </span>
            </div>
          )}
          {preflight.blockers.length > 0 && (
            <ul className="df2-validate-rail-blockers">
              {preflight.blockers.slice(0, 4).map((b) => (
                <li key={b.id}>{b.message}</li>
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

      <div className="df2-validate-rail-actions">
        <button type="button" className="df2-btn df2-btn-ghost" onClick={onBack}>
          <DtIcon name="chevron-left" size={16} /> Back to mapping
        </button>

        {!preflight && !preflighting && (
          <button type="button" className="df2-btn df2-btn-primary df2-btn-lg" onClick={onRunPreflight}>
            <DtIcon name="gate" size={16} /> Run preflight
          </button>
        )}

        {blocked && (
          <button type="button" className="df2-btn df2-btn-lg" onClick={onRunPreflight} disabled={preflighting}>
            <DtIcon name="gate" size={16} /> Re-run
          </button>
        )}

        {blocked && mappingBlocked && mappingReviewCount > 0 && (
          <button type="button" className="df2-btn df2-btn-primary df2-btn-lg" onClick={onApproveMappings}>
            <DtIcon name="check" size={16} /> Approve all mappings
          </button>
        )}

        {preflight && !transferLaunch && (
          <button
            type="button"
            className="df2-btn df2-btn-primary df2-btn-lg"
            onClick={onExecute}
            disabled={transferring || !passed}
            title={!passed ? `Blocked: ${firstBlocker || "Resolve failed checks and re-run preflight"}` : undefined}
          >
            {transferring ? (
              <ButtonLoader label="Starting…" />
            ) : (
              <>
                <DtIcon name="transfer" size={18} />
                {passed
                  ? `Execute transfer${rowCount != null ? ` · ${rowCount.toLocaleString()} rows` : ""}`
                  : "Execute transfer (blocked)"}
              </>
            )}
          </button>
        )}

        {blocked && firstBlocker && (
          <p className="df2-validate-rail-explain">
            Run is blocked: {firstBlocker}
          </p>
        )}
      </div>
    </aside>
  );
}
