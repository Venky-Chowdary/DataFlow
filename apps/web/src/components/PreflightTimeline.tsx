import { DtIcon } from "./DtIcon";
import { PreflightResult } from "../lib/types";

const GATE_LABELS: Record<string, string> = {
  g1_source: "Source Readable",
  g2_destination: "Destination Reachable",
  g3_schema: "Schema Contract",
  g4_mapping: "Mapping Confidence",
  g5_transform: "Dry-Run Transform",
  g6_ddl: "DDL Compatible",
  g7_capacity: "Staging Capacity",
  g8_reconciliation: "Reconciliation",
};

function gateLabel(id: string): string {
  return GATE_LABELS[id] ?? id.replace(/^g\d+_?/, "Gate ").replace(/_/g, " ");
}

interface PreflightTimelineProps {
  result: PreflightResult;
  running?: boolean;
}

export function PreflightTimeline({ result, running }: PreflightTimelineProps) {
  const gates = result.gates.length > 0
    ? result.gates
    : Array.from({ length: 8 }, (_, i) => ({
        id: `g${i + 1}_pending`,
        status: "skip" as const,
        message: running ? "Running validation…" : "Pending",
        duration_ms: 0,
      }));

  const stateClass = result.passed ? "passed" : result.blockers.length ? "blocked" : "";

  return (
    <div className={`df2-preflight ${stateClass}`}>
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
            {running ? "Running Preflight Gates…" : result.passed ? "All Gates Passed" : "Preflight Validation"}
          </h3>
          <p style={{ margin: 0, fontSize: 13, color: "#64748b" }}>
            {result.passed_count}/{result.total_gates} gates passed · zero rows moved until all pass
          </p>
          {result.blockers.length > 0 && (
            <div className="df2-segment" style={{ marginTop: 10 }}>
              {result.blockers.map((b) => (
                <span key={b.id} className="df2-badge df2-badge-error">{b.message}</span>
              ))}
            </div>
          )}
        </div>
      </div>

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
