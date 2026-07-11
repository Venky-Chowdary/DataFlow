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
          {preflight.blockers.length > 0 && (
            <ul className="df2-validate-rail-blockers">
              {preflight.blockers.slice(0, 4).map((b) => (
                <li key={b.id}>{b.message}</li>
              ))}
            </ul>
          )}
        </div>
      )}

      {blocked && mappingBlocked && mappingReviewCount > 0 && (
        <button type="button" className="df2-btn df2-btn-primary df2-btn-sm" onClick={onApproveMappings}>
          <DtIcon name="check" size={14} /> Approve all mappings
        </button>
      )}

      <div className="df2-validate-rail-actions">
        <button type="button" className="df2-btn df2-btn-sm" onClick={onBack}>
          ← Back to mapping
        </button>

        {!preflight && !preflighting && (
          <button type="button" className="df2-btn df2-btn-primary" onClick={onRunPreflight}>
            <DtIcon name="gate" size={16} /> Run preflight
          </button>
        )}

        {blocked && (
          <button type="button" className="df2-btn df2-btn-sm" onClick={onRunPreflight} disabled={preflighting}>
            <DtIcon name="gate" size={14} /> Re-run
          </button>
        )}

        {passed && !transferLaunch && (
          <button
            type="button"
            className="df2-btn df2-btn-primary df2-btn-lg"
            onClick={onExecute}
            disabled={transferring}
          >
            {transferring ? (
              <ButtonLoader label="Starting…" />
            ) : (
              <>
                <DtIcon name="transfer" size={18} />
                Execute transfer{rowCount != null ? ` · ${rowCount.toLocaleString()} rows` : ""}
              </>
            )}
          </button>
        )}
      </div>
    </aside>
  );
}
