import { DtIcon } from "./DtIcon";
import { PreflightResult } from "../lib/types";

const GATE_LABELS: Record<string, string> = {
  g1_source: "Source readable",
  g2_destination: "Destination reachable",
  g3_schema: "Schema contract",
  g4_mapping: "Column mappings",
  g5_transform: "Dry-run transform",
  g9_data_integrity: "Data integrity",
  g6_ddl: "Target DDL",
  g7_capacity: "Staging capacity",
  g8_reconciliation: "Reconciliation (post-transfer)",
  g9_sync_contract: "Sync contract",
  g10_schema_policy: "Schema change policy",
  g11_validation_posture: "Validation posture",
};

function gateLabel(id: string): string {
  const entry = Object.entries(GATE_LABELS).find(([key]) => id.includes(key));
  return entry ? entry[1] : id.replace(/^g\d+_/, "").replace(/_/g, " ");
}

interface PreflightTimelineProps {
  result: PreflightResult;
  running?: boolean;
  confidenceThreshold?: number;
  compact?: boolean;
  hideActions?: boolean;
  onApproveMappings?: () => void;
  onRerun?: () => void;
  onUseBalanced?: () => void;
}

export function PreflightTimeline({
  result,
  running,
  confidenceThreshold = 0.85,
  compact = false,
  hideActions = false,
  onApproveMappings,
  onRerun,
  onUseBalanced,
}: PreflightTimelineProps) {
  const gates = result.gates.length > 0
    ? result.gates
    : running
      ? [{
          id: "preflight_running",
          status: "skip" as const,
          message: "Running validation…",
          duration_ms: 0,
        }]
      : [];

  const stateClass = result.passed ? "passed" : result.blockers.length ? "blocked" : "";
  const mappingBlocked = result.blockers.some((b) => b.id.includes("mapping"));
  const schemaPolicyBlocked = result.blockers.some((b) => b.id.includes("schema_policy"));

  return (
    <div className={`df2-preflight ${stateClass}${compact ? " is-compact" : ""}`}>
      {!compact && (
      <div className="df2-preflight-head">
        <div className="df2-preflight-score">
          <svg viewBox="0 0 80 80" aria-hidden>
            <circle cx="40" cy="40" r="34" className="df2-score-track" />
            <circle
              cx="40"
              cy="40"
              r="34"
              className="df2-score-fill"
              strokeDasharray={`${(result.readiness_score / 100) * 213.6} 213.6`}
              transform="rotate(-90 40 40)"
            />
          </svg>
          <div className="df2-preflight-score-val">
            <span>{result.readiness_score}</span>
            <small>%</small>
          </div>
        </div>
        <div>
          <h3 className="df2-preflight-title">
            {running ? "Running validation…" : result.passed ? "Ready to transfer" : "Validation — action needed"}
          </h3>
          <p className="df2-preflight-sub">
            {result.passed_count}/{result.total_gates} checks passed
            {result.passed ? " · you can execute the transfer" : " · fix items below, then re-run"}
          </p>
          {result.blockers.length > 0 && (
            <ul className="df2-preflight-blocker-list">
              {result.blockers.map((b) => (
                <li key={b.id}>{b.message}</li>
              ))}
            </ul>
          )}
        </div>
      </div>
      )}

      {compact && (
        <div className="df2-preflight-compact-head">
          <h3 className="df2-preflight-title">
            {running ? "Running checks…" : result.passed ? "All checks passed" : "Checks need attention"}
          </h3>
          <span className="df2-preflight-compact-score">{result.passed_count}/{result.total_gates} passed</span>
        </div>
      )}

      {!hideActions && !result.passed && !running && (
        <div className="df2-preflight-fix-panel">
          <strong>How to fix</strong>
          <div className="df2-preflight-fix-actions">
            {mappingBlocked && onApproveMappings && (
              <button type="button" className="df2-btn df2-btn-primary df2-btn-sm" onClick={onApproveMappings}>
                <DtIcon name="check" size={14} /> Approve all column mappings
              </button>
            )}
            {mappingBlocked && onUseBalanced && confidenceThreshold >= 0.85 && (
              <button type="button" className="df2-btn df2-btn-sm" onClick={onUseBalanced}>
                Use Balanced validation (75% threshold)
              </button>
            )}
            {schemaPolicyBlocked && (
              <span className="df2-preflight-fix-hint">
                Turn off &quot;Backfill new fields&quot; or switch schema policy to Column changes.
              </span>
            )}
            {onRerun && (
              <button type="button" className="df2-btn df2-btn-sm" onClick={onRerun}>
                <DtIcon name="gate" size={14} /> Re-run validation
              </button>
            )}
          </div>
        </div>
      )}

      <div className="df2-preflight-track">
        {gates.map((gate, i) => (
          <div key={gate.id} className={`df2-preflight-step ${gate.status}`} style={{ animationDelay: `${i * 80}ms` }}>
            <div className="df2-preflight-marker">
              {gate.status === "pass" && <DtIcon name="check" size={14} />}
              {gate.status === "block" && <DtIcon name="x" size={14} />}
              {gate.status === "skip" && <span>—</span>}
            </div>
            <div>
              <div className="df2-preflight-step-title">{gateLabel(gate.id)}</div>
              <div className="df2-preflight-step-msg">{gate.message}</div>
            </div>
          </div>
        ))}
      </div>
    </div>
  );
}
